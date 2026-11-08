//! Provisioning goes through the queue's loop like every other operation.
use super::owner::{Exit, Plan, Tenure, settle};
use super::*;
use fireweed_core::queue_contract::ProvisionRequest;
use fireweed_engine::{Clock, queue_schema_codec::encode_definition};
use futures::future::BoxFuture;
use sha2::Digest;

struct ProvisionPlan {
    request: ProvisionRequest,
}
impl QueueRecoveryPipeline {
    pub(super) async fn provision(
        self: &Arc<Self>,
        request: ProvisionRequest,
        limits: QueueLimits,
        job: Arc<QueueReservation>,
        input: OwnedQueueBufferCharge,
    ) -> QueueResult<()> {
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
    type Output = ();
    const MUTATION: bool = true;
    const SOLO: bool = true;
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
                        Ok(definition) if definition.schema() == &self.request.schema => Ok(()),
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
            let definition = match encode_definition(&self.request.schema)
                .and_then(|raw| AdmittedQueueDefinition::from_raw(&raw, job))
            {
                Ok(definition) => definition,
                Err(error) => return Ok(Err(error)),
            };
            // The lease makes this the only writer. A tenure that found the
            // namespace empty already holds its fence; otherwise elect the
            // namespace and fence it. A crash or a lost reply leaves an empty
            // head, which this completes, or a genesis, which re-entry rebuilds
            // and compares above.
            if !t.has_writer() {
                let namespace =
                    match settle(p.log.provision_namespace(&self.request.queue, job).await)? {
                        Ok(namespace) => namespace,
                        Err(error) => return Ok(Err(error)),
                    };
                let identity = namespace.identity();
                let acquired = match settle(p.log.acquire_bundle(namespace, job).await)? {
                    Ok(acquired) => acquired,
                    Err(error) => return Ok(Err(error)),
                };
                let (writer, source) = acquired.into_parts();
                // A namespace that already holds records is not what this owner
                // believes: rebuild from it.
                if source.head().bounds() != (0, 0) {
                    return Err(Exit);
                }
                drop(source);
                t.install(identity, writer);
            }
            // Nothing is published yet: a definite error such as TooLarge goes
            // back to the caller. The tenure keeps its fence over an empty head.
            let genesis = match settle(encode_checked_genesis(
                t.epoch,
                &self.request.queue,
                &definition,
                &candidate_secret,
                p.hard_record_bytes,
                job,
            ))? {
                Ok(genesis) => genesis,
                Err(error) => return Ok(Err(error)),
            };
            t.publish(p, genesis, 0, job).await?;
            Ok(Ok(()))
        })
    }
}
