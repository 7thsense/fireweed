//! Coupled initialization and readonly adoption of the original winning Genesis.
use super::*;

pub struct QueueNativeGenesisIntent<'a> {
    pub definition: &'a AdmittedQueueDefinition,
    pub candidate_secret: &'a [u8; 32],
    pub known_queue: bool,
}
#[derive(Clone, Copy)]
pub struct QueueNativeProvisionLimits {
    pub preparation: QueueNativeSourcePreparationLimits,
    pub record: QueueNativeSourceRecordLimits,
}
impl QueueNativeProvisionLimits {
    fn validate(self) -> QueueResult<()> {
        self.preparation.validate()?;
        self.record.validate()
    }
}
impl QueueNativeLog {
    /// The immutable captured source establishes membership. Later appends do
    /// not invalidate historical Genesis; no fence or head-equality confirmation.
    pub async fn adopt_genesis(
        &self,
        source: &QueueNativeCaptured,
        incoming: &AdmittedQueueDefinition,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeProvisionLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeGenesis> {
        source.semantic_scope(self, job)?;
        budget.scope(job)?;
        limits.validate()?;
        if !incoming.belongs_to(job) {
            return Err(unknown("genesis definition belongs to another operation"));
        }
        budget.ensure_adoption_capacity(limits.preparation.read.page.work)?;
        let reader = QueueNativeSourceReader::new(
            source,
            self,
            job,
            budget.clone(),
            limits.preparation.read,
        )?;
        let fact = fireweed_engine::queue_source_index::load_source_genesis(&reader, job).await?;
        let record = source
            .read_source_record(self, 0, limits.record, &budget, job)
            .await?;
        let checked = record
            .checked()
            .map_err(|error| unknown(format!("invalid stored genesis: {error:?}")))?;
        fact.verify_record(&checked, 0)?;
        let QueueLogPayload::Genesis(genesis) = checked.view().payload() else {
            return Err(unknown("offset zero is not genesis"));
        };
        let definition = genesis
            .validate_definition(job)
            .map_err(|error| unknown(format!("invalid stored genesis definition: {error:?}")))?;
        // Pins bind persisted bytes. Equivalent legacy/current encodings can
        // differ in pin while naming the SAME validated semantic schema.
        if definition.schema() != incoming.schema() {
            return Err(QueueError::DefinitionConflict);
        }
        drop(definition);
        Ok(QueueNativeGenesis { record })
    }
    /// Namespace open/capture and <=8 single acquisition attempts are separately
    /// bounded protocol metadata. The SAME caller ledger funds semantic reads,
    /// compiler preparation, record lookup attempts and hydration across retries.
    pub async fn provision_record(
        &self,
        address: &QueueAddress,
        intent: QueueNativeGenesisIntent<'_>,
        budget: Arc<QueueNativeSourceBudget>,
        limits: QueueNativeProvisionLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<OwnedQueueLogRecord> {
        budget.scope(job)?;
        limits.validate()?;
        if !intent.definition.belongs_to(job) {
            return Err(unknown("provision definition belongs to another operation"));
        }
        budget.ensure_adoption_capacity(limits.preparation.read.page.work)?;
        let namespace = match self.open(address, job).await? {
            Some(namespace) => namespace,
            None if intent.known_queue => return Err(unknown("known queue binding missing")),
            None => self.provision_namespace(address, job).await?,
        };
        let mut source = self.capture_source(namespace, job).await?;
        if source.head().bounds().1 > 0 {
            return Ok(self
                .adopt_genesis(&source, intent.definition, budget, limits, job)
                .await?
                .record);
        }
        if intent.known_queue {
            return Err(unknown("known queue has empty genesis authority"));
        }
        let mut acquired = None;
        for attempt in 0..8 {
            // Each newly acquired cut gets a NEW opaque identity, never a
            // retargeted source/evidence identity even at equal numeric heads.
            let semantic_identity = QueueNativeCaptured::identity_charge(job)?;
            match self.acquire_with_cut(&source.namespace).await {
                Ok((writer, cut)) => {
                    acquired = Some(QueueNativeAcquired {
                        writer,
                        source: QueueNativeCaptured {
                            log_identity: self.identity.clone(),
                            namespace: source.namespace,
                            cut,
                            semantic_identity,
                        },
                    });
                    break;
                }
                Err(error) => {
                    let namespace = source.retained_namespace(address, job)?;
                    source = self.capture_source(namespace, job).await?;
                    if source.head().bounds().1 > 0 {
                        return Ok(self
                            .adopt_genesis(&source, intent.definition, budget, limits, job)
                            .await?
                            .record);
                    }
                    if attempt == 7 {
                        return Err(error);
                    }
                    // Only a successful authenticated EMPTY capture permits
                    // another fence; no possible submission exists in this loop.
                    tokio::task::yield_now().await;
                }
            }
        }
        let acquired = acquired.ok_or_else(|| unknown("bounded genesis acquisition exhausted"))?;
        if acquired.source.head().bounds().1 > 0 {
            return Ok(self
                .adopt_genesis(acquired.as_source(), intent.definition, budget, limits, job)
                .await?
                .record);
        }
        let record = fireweed_engine::queue_log_record::encode_checked_genesis(
            acquired.source.head().epoch(),
            address,
            intent.definition,
            intent.candidate_secret,
            limits.record.payload_bytes,
            job,
        )?;
        let original = acquired
            .compile_and_prepare(
                self,
                &record,
                intent.definition,
                budget.clone(),
                limits.preparation,
                job,
            )
            .await?;
        let _ = original.submit(self).await;
        // Own rejection and lost replies both require reading the winner. NEVER
        // reacquire or reprepare after publication might have happened.
        let namespace = acquired.source.retained_namespace(address, job)?;
        let fresh = self.capture_source(namespace, job).await?;
        if fresh.head().bounds().1 == 0 {
            return Err(unknown("genesis publication remains unresolved"));
        }
        let winner = self
            .adopt_genesis(&fresh, intent.definition, budget, limits, job)
            .await?;
        drop(original);
        Ok(winner.record)
    }
}
