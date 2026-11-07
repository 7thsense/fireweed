//! Provisioning goes through the queue's loop like every other operation.
use super::owner::{Exit, Plan, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::ProvisionRequest;
use fireweed_engine::{Clock, queue_schema_codec::encode_definition};
use fireweed_objectlog::ProvisionedApi007Genesis;
use futures::future::BoxFuture;
use sha2::Digest;

struct ProvisionPlan {
    request: ProvisionRequest,
}
impl QueueRecoveryPipeline {
    /// The returned owner keeps the genesis authority alive until delivery.
    pub(super) async fn provision(
        self: &Arc<Self>,
        request: ProvisionRequest,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
        input: OwnedQueueBufferCharge,
    ) -> QueueResult<Option<ProvisionedApi007Genesis>> {
        self.submit(
            ProvisionPlan { request },
            limits,
            self.ownership.clock(),
            job,
            input,
        )
        .await
    }
}
impl Plan for ProvisionPlan {
    type Output = Option<ProvisionedApi007Genesis>;
    const MUTATION: bool = true;
    fn address(&self) -> &QueueAddress {
        &self.request.queue
    }
    fn run<'a>(
        &'a self,
        p: &'a QueueRecoveryPipeline,
        t: &'a mut Tenure,
        _limits: QueueLimits,
        _clock: &'a Arc<dyn Clock>,
        job: &'a Arc<QueueReservation>,
    ) -> BoxFuture<'a, Result<QueueResult<Self::Output>, Exit>> {
        Box::pin(async move {
            // A queue this owner already holds: provisioning is a schema comparison.
            if let Some(genesis) = &t.genesis {
                return Ok(
                    match AdmittedQueueDefinition::from_raw(&genesis.definition, job) {
                        Ok(definition) if definition.schema() == &self.request.schema => Ok(None),
                        Ok(_) => Err(QueueError::DefinitionConflict),
                        Err(error) => Err(error),
                    },
                );
            }
            let token = match fireweed_engine::generate_query_lease_token() {
                Ok(token) => token,
                Err(error) => return Ok(Err(unknown(&error.to_string()))),
            };
            let candidate_secret = sha2::Sha256::digest(token.as_str().as_bytes()).into();
            let prepared = (|| {
                let raw = encode_definition(&self.request.schema)?;
                let definition = AdmittedQueueDefinition::from_raw(&raw, job)?;
                let namespace = t.namespace(p, job)?;
                Ok::<_, QueueError>(fireweed_objectlog::Api007GenesisRequest {
                    namespace,
                    definition,
                    candidate_secret,
                    max_record_bytes: p.hard_record_bytes,
                    metadata_bytes: p.metadata_bytes,
                })
            })();
            let request = match prepared {
                Ok(request) => request,
                Err(error) => return Ok(Err(error)),
            };
            let original = match settle(
                p.store
                    .provision_api007_genesis(request, job, &p.workers)
                    .await,
            )? {
                Ok(original) => original,
                Err(error) => return Ok(Err(error)),
            };
            if original.format() != QueueLogFormat::ChecksummedV2 {
                return Ok(Err(unknown("legacy genesis cannot provision native port")));
            }
            if original.definition().schema() != &self.request.schema {
                return Ok(Err(QueueError::DefinitionConflict));
            }
            let target = original.target();
            // Anything but our own single genesis record means the log is not
            // what this owner believes: rebuild from it.
            if target.log_start != 0 || target.next_offset != 1 {
                return Err(Exit);
            }
            t.epoch = target.epoch;
            let genesis = encode_checked_genesis(
                target.epoch,
                &self.request.queue,
                original.definition(),
                original.cursor_secret(),
                p.hard_record_bytes,
                job,
            )
            .map_err(|_| Exit)?;
            t.apply(p, genesis, 0, job).await.map_err(|_| Exit)?;
            Ok(Ok(Some(original)))
        })
    }
}
