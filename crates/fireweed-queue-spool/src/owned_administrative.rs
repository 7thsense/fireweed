//! Shared runtime boundary for the disposable joint scratch owner.
use super::*;
use fireweed_core::queue_contract::QueueAddress;
use fireweed_engine::queue_administration::{
    AdmittedAdministrativeScan, ResolvedAdministrativeRow,
};
use fireweed_engine::queue_schema_codec::DefinitionPin;
use std::path::PathBuf;
use std::sync::Mutex;
enum State {
    Ready(Box<AdministrativeScratch>),
    InFlight,
    Sealed,
    Poisoned,
}
struct Inner {
    state: Mutex<State>,
    _charge: OwnedQueueBufferCharge,
}
impl Inner {
    fn poison(&self) {
        let previous = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::replace(&mut *state, State::Poisoned)
        };
        // Closing files/admission never happens under the state mutex.
        drop(previous);
    }
    fn take(self: &Arc<Self>, sealing: bool) -> QueueResult<TakenOwner> {
        let previous = {
            let mut state = self.state.lock().map_err(unknown)?;
            let previous = std::mem::replace(
                &mut *state,
                if sealing {
                    State::Sealed
                } else {
                    State::InFlight
                },
            );
            if !matches!(previous, State::Ready(_)) {
                *state = State::Poisoned;
            }
            previous
        };
        match previous {
            State::Ready(owner) => Ok(TakenOwner {
                owner: Some(owner),
                inner: Arc::clone(self),
                armed: true,
            }),
            _ => Err(unknown("overlapping, poisoned or terminal owned sink use")),
        }
    }
}
struct TakenOwner {
    owner: Option<Box<AdministrativeScratch>>,
    inner: Arc<Inner>,
    armed: bool,
}
impl TakenOwner {
    fn append_packet(mut self, packet: ResolvedAdministrativeRow) -> QueueResult<()> {
        let mut owner = self.take_owner();
        owner.append(packet)?;
        self.restore_append(owner)
    }
    fn take_owner(&mut self) -> Box<AdministrativeScratch> {
        self.owner.take().expect("taken owner")
    }
    fn restore_append(&mut self, owner: Box<AdministrativeScratch>) -> QueueResult<()> {
        let mut state = self.inner.state.lock().map_err(unknown)?;
        if !matches!(*state, State::InFlight) {
            drop(state);
            return Err(unknown("owned sink poisoned during append"));
        }
        *state = State::Ready(owner);
        drop(state);
        self.armed = false;
        Ok(())
    }
    fn finish_seal(&mut self) -> QueueResult<()> {
        let state = self.inner.state.lock().map_err(unknown)?;
        if !matches!(*state, State::Sealed) {
            drop(state);
            return Err(unknown("owned sink poisoned during seal"));
        }
        drop(state);
        self.armed = false;
        Ok(())
    }
}
impl Drop for TakenOwner {
    fn drop(&mut self) {
        if self.armed {
            self.inner.poison();
        }
        // owner drops after the state mutex is released, before inner's guard.
    }
}
struct Setup {
    directory: PathBuf,
    queue: QueueAddress,
    quota: Arc<ScratchDiskQuota>,
    reservation: Arc<QueueReservation>,
    max_command_bytes: u64,
    _charge: OwnedQueueBufferCharge,
}
/// Clones share one owner, never its files/payloads. A scan awaits each append.
/// Overlapping or terminal use permanently fails closed. The backend retains
/// its fence and owned operation through source EOF and final publication.
#[derive(Clone)]
pub struct OwnedAdministrativeSink {
    inner: Arc<Inner>,
    workers: ScratchWorkers,
}
impl OwnedAdministrativeSink {
    pub async fn new(
        workers: ScratchWorkers,
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
    ) -> QueueResult<Self> {
        Self::new_with_command_limit(
            workers,
            directory,
            quota,
            reservation,
            queue,
            pin,
            initial,
            u64::MAX,
        )
        .await
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn new_with_command_limit(
        workers: ScratchWorkers,
        directory: &Path,
        quota: Arc<ScratchDiskQuota>,
        reservation: &Arc<QueueReservation>,
        queue: &QueueAddress,
        pin: DefinitionPin,
        initial: u64,
        max_command_bytes: u64,
    ) -> QueueResult<Self> {
        let names = add(
            add(queue.tenant.len() as u64, queue.queue.len() as u64)?,
            directory.as_os_str().len() as u64,
        )?;
        let charge = reservation.owned_buffer(
            names
                .checked_mul(4)
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(|| unknown("owned setup admission overflow"))?,
        )?;
        let setup = Setup {
            directory: directory.to_owned(),
            queue: queue.clone(),
            quota,
            reservation: Arc::clone(reservation),
            max_command_bytes,
            _charge: charge,
        };
        let inner = workers
            .run_or_inline(move || {
                let owner = AdministrativeScratch::new_with_command_limit(
                    &setup.directory,
                    setup.quota,
                    &setup.reservation,
                    &setup.queue,
                    pin,
                    initial,
                    setup.max_command_bytes,
                )?;
                Ok(Arc::new(Inner {
                    state: Mutex::new(State::Ready(Box::new(owner))),
                    _charge: setup._charge,
                }))
            })
            .await?;
        Ok(Self { inner, workers })
    }
    pub async fn append(&self, packet: ResolvedAdministrativeRow) -> QueueResult<()> {
        let taken = self.inner.take(false)?;
        let result = self
            .workers
            .run_or_inline(move || taken.append_packet(packet))
            .await;
        if result.is_err() {
            self.inner.poison();
        }
        result
    }
    /// The exact EOF summary travels with sealed resources across cancellation.
    /// This is disposable preparation, never publication or an ACK.
    pub async fn seal(
        self,
        summary: AdmittedAdministrativeScan,
    ) -> QueueResult<(SealedAdministrativeScratch, AdmittedAdministrativeScan)> {
        let mut taken = self.inner.take(true)?;
        let result = self
            .workers
            .run_or_inline(move || {
                let owner = taken.take_owner();
                let sealed = (*owner).seal(&summary)?;
                taken.finish_seal()?;
                Ok((sealed, summary))
            })
            .await;
        if result.is_err() {
            self.inner.poison();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::administrative::tests::{pin, queue, run_scan};
    use fireweed_engine::queue_admission::QueueAdmission;
    use std::task::Poll;
    use tokio::sync::oneshot;
    #[tokio::test]
    async fn owned_database_stage_uses_one_worker_for_complete_scratch_delivery() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 8 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let quota = ScratchDiskQuota::new(8 << 20).unwrap();
        let workers = ScratchWorkers::new(1, 1).unwrap();
        let nested = workers.clone();
        let retained = Arc::clone(&job);
        let disk = Arc::clone(&quota);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            workers.run(move || {
                tokio::runtime::Handle::current().block_on(async move {
                    let sink = OwnedAdministrativeSink::new(
                        nested.clone(),
                        directory.path(),
                        disk,
                        &retained,
                        &queue(),
                        pin(),
                        2,
                    )
                    .await?;
                    let _headers = retained.buffer(4096)?;
                    let mut packets = Vec::with_capacity(2);
                    let summary = run_scan(2, 0, false, &retained, |packet| {
                        packets.push(packet);
                        Ok(())
                    })?;
                    for packet in packets {
                        sink.append(packet).await?;
                    }
                    let (sealed, summary) = sink.seal(summary).await?;
                    assert_eq!(sealed.command_count(), 2);
                    assert_eq!(summary.result().unwrap().histogram.matched, 2);
                    drop((sealed, summary));
                    Ok(())
                })
            }),
        )
        .await
        .unwrap();
        result.unwrap();
        workers.close_and_drain().await.unwrap();
        assert_eq!(admission.usage().unwrap(), (1, 0));
        assert_eq!(quota.usage().unwrap(), 0);
    }
    #[tokio::test]
    async fn queued_append_cancel_and_concurrent_use_poison_and_release_taken_owner() {
        for concurrent in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let admission = QueueAdmission::new(1, 2 << 20).unwrap();
            let job = Arc::new(admission.reserve(0).unwrap());
            let quota = ScratchDiskQuota::new(2 << 20).unwrap();
            let workers = ScratchWorkers::new(1, 2).unwrap();
            let sink = OwnedAdministrativeSink::new(
                workers.clone(),
                directory.path(),
                Arc::clone(&quota),
                &job,
                &queue(),
                pin(),
                2,
            )
            .await
            .unwrap();
            let headers = job.buffer(4096).unwrap();
            let mut packets = Vec::with_capacity(2);
            let summary = run_scan(2, 0, false, &job, |packet| {
                packets.push(packet);
                Ok(())
            })
            .unwrap();
            let (started, active) = oneshot::channel();
            let (release, held) = std::sync::mpsc::channel();
            let busy_workers = workers.clone();
            let busy = tokio::spawn(async move {
                busy_workers
                    .run(move || {
                        started.send(()).unwrap();
                        held.recv().unwrap();
                        Ok(())
                    })
                    .await
            });
            active.await.unwrap();
            let first = packets.remove(0);
            let mut pending = Box::pin(sink.append(first));
            assert!(matches!(futures::poll!(pending.as_mut()), Poll::Pending));
            assert!(matches!(*sink.inner.state.lock().unwrap(), State::InFlight));
            if concurrent {
                assert!(sink.append(packets.remove(0)).await.is_err());
                assert!(matches!(*sink.inner.state.lock().unwrap(), State::Poisoned));
            }
            drop(pending);
            assert!(matches!(*sink.inner.state.lock().unwrap(), State::Poisoned));
            assert!(sink.seal(summary).await.is_err());
            drop(packets);
            drop(headers);
            assert_eq!(quota.usage().unwrap(), 0);
            assert_eq!(admission.usage().unwrap(), (1, 0));
            release.send(()).unwrap();
            busy.await.unwrap().unwrap();
            workers.close_and_drain().await.unwrap();
            drop(job);
            assert_eq!(admission.usage().unwrap(), (0, 0));
        }
    }
    #[tokio::test]
    async fn dispatched_append_outlives_caller_and_closes_files_before_final_reservation() {
        let directory = tempfile::tempdir().unwrap();
        let admission = QueueAdmission::new(1, 2 << 20).unwrap();
        let terminal = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::clone(&terminal);
        let job = Arc::new(
            admission
                .reserve(0)
                .unwrap()
                .with_terminal_callback(Box::new(move || signal.notify_one()))
                .unwrap(),
        );
        let quota = ScratchDiskQuota::new(2 << 20).unwrap();
        let workers = ScratchWorkers::new(1, 2).unwrap();
        let sink = OwnedAdministrativeSink::new(
            workers.clone(),
            directory.path(),
            Arc::clone(&quota),
            &job,
            &queue(),
            pin(),
            1,
        )
        .await
        .unwrap();
        let headers = job.buffer(4096).unwrap();
        let mut packets = Vec::with_capacity(1);
        let summary = run_scan(1, 0, false, &job, |packet| {
            packets.push(packet);
            Ok(())
        })
        .unwrap();
        let packet = packets.pop().unwrap();
        let taken = sink.inner.take(false).unwrap();
        let (started, active) = oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        // Same owned append body, paused immediately after physical dispatch.
        let mut pending = Box::pin(workers.run(move || {
            started.send(()).unwrap();
            held.recv().unwrap();
            taken.append_packet(packet)
        }));
        assert!(matches!(futures::poll!(pending.as_mut()), Poll::Pending));
        active.await.unwrap();
        drop(pending);
        drop(sink);
        drop(summary);
        drop(packets);
        drop(headers);
        admission.close().unwrap();
        drop(job);
        assert_eq!(admission.usage().unwrap().0, 1);
        release.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), terminal.notified())
            .await
            .unwrap();
        workers.close_and_drain().await.unwrap();
        assert_eq!(quota.usage().unwrap(), 0);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
