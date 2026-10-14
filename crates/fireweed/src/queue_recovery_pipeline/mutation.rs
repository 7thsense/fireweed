//! One safe resubmission protocol for all immutable request domains.
use super::*;
use fireweed_engine::queue_contract::QueueFuture;
pub(super) trait OriginalMutation: Sync {
    type Output: Send;
    fn pipeline(&self) -> &Arc<QueueRecoveryPipeline>;
    fn address(&self) -> &QueueAddress;
    fn identity(&self) -> (u8, &str);
    fn job(&self) -> &Arc<QueueReservation>;
    fn recover<'a>(&'a self) -> QueueFuture<'a, RecoveredQueue> {
        Box::pin(async move {
            let (domain, id) = self.identity();
            self.pipeline()
                .recover_request(
                    self.address().clone(),
                    domain,
                    id.to_owned(),
                    self.job().clone(),
                )
                .await
        })
    }
    fn continuation_enabled(&self) -> bool {
        false
    }
    fn recover_from<'a>(
        &'a self,
        previous: Option<RecoveredQueue>,
    ) -> QueueFuture<'a, RecoveredQueue> {
        drop(previous);
        self.recover()
    }
    fn replay<'a>(&'a self, recovered: &'a RecoveredQueue)
    -> QueueFuture<'a, Option<Self::Output>>;
}
pub(super) async fn recover_original<M: OriginalMutation>(
    mutation: &M,
) -> QueueResult<RecoveredQueue> {
    mutation.recover().await
}
// Keep the single admitted recovery owner inline. Boxing it would add a
// second allocation at the proof/handoff boundary solely to reduce stack size.
#[allow(clippy::large_enum_variant)]
pub(super) enum Submission<T> {
    Resolved(T, RecoveredQueue),
    Reprepare(RecoveredQueue),
    Classified(QueueError, RecoveredQueue),
}
pub(super) fn check_preparation<T>(
    recovered: &RecoveredQueue,
    result: QueueResult<Option<T>>,
) -> QueueResult<Option<T>> {
    match result {
        Ok(prepared) => match recovered.original_identity_present {
            Some(present) if present != prepared.is_some() => Ok(prepared),
            _ => Err(unknown(
                "projection original identity disagrees with checked log history",
            )),
        },
        Err(
            error @ (QueueError::RequestConflict
            | QueueError::RequestExpired
            | QueueError::ClaimExpired),
        ) => {
            if recovered.original_identity_present == Some(true) {
                Err(error)
            } else {
                Err(unknown("phantom cached original classification"))
            }
        }
        Err(error) => Err(error),
    }
}
pub(super) async fn resolve_submission<M: OriginalMutation>(
    mutation: &M,
    prepared_epoch: Option<u64>,
    covered: Option<RecoveredQueue>,
) -> QueueResult<Submission<M::Output>> {
    resolve_submission_after(mutation, prepared_epoch, covered, None).await
}
pub(super) async fn resolve_submission_after<M: OriginalMutation>(
    mutation: &M,
    prepared_epoch: Option<u64>,
    mut covered: Option<RecoveredQueue>,
    mut previous: Option<RecoveredQueue>,
) -> QueueResult<Submission<M::Output>> {
    let continuation_enabled = mutation.continuation_enabled();
    for _ in 0..3 {
        let recovered = match covered.take() {
            Some(value) => value,
            None => {
                let continuing = previous.is_some();
                match mutation.recover_from(previous.take()).await {
                    Ok(value) => value,
                    // A failed continuation must not forget previously known
                    // authority and retry a shorter cold prefix as fenced absent.
                    Err(error) if continuing => return Err(error),
                    Err(_) => continue,
                }
            }
        };
        match mutation.replay(&recovered).await {
            Ok(Some(response)) => {
                if recovered.original_identity_present != Some(true) {
                    return Err(unknown("phantom cached original response"));
                }
                return Ok(Submission::Resolved(response, recovered));
            }
            Ok(None) => {
                if recovered.original_identity_present != Some(false) {
                    return Err(unknown("durable original is missing from projection"));
                }
                if prepared_epoch.is_some_and(|epoch| recovered.acquired_epoch > epoch) {
                    // This checked complete prefix contains no original. The
                    // strictly newer durable fence prevents every older in-flight
                    // attempt from publishing later, including lost replies.
                    return Ok(Submission::Reprepare(recovered));
                }
            }
            Err(
                error @ (QueueError::RequestConflict
                | QueueError::RequestExpired
                | QueueError::ClaimExpired),
            ) => {
                if recovered.original_identity_present != Some(true) {
                    return Err(unknown("phantom cached original classification"));
                }
                return Ok(Submission::Classified(error, recovered));
            }
            Err(QueueError::TooLarge) if recovered.original_identity_present == Some(true) => {
                return Err(QueueError::TooLarge);
            }
            Err(_) => {}
        }
        if continuation_enabled {
            previous = Some(recovered);
        }
    }
    Err(unknown(
        "submission has no resolved or fenced-absent original",
    ))
}
