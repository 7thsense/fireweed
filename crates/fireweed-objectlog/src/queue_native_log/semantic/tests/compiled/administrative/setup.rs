//! Test-only composition of the actual reader, spool and private finalizer.
use super::*;
use fireweed_engine::queue_source_index::QueueSourceAdministrativeScan;
use fireweed_queue_spool::{FinalizationProbe, SourceAdministrativeResolution};
use std::path::Path;
pub(super) type Parts = (
    OwnedAdministrativeSink,
    QueueSourceAdministrativeScan,
    SourceAdministrativeAuthority,
);
#[derive(Clone, Copy)]
pub(super) struct Context<'a> {
    pub source: &'a QueueNativeCaptured,
    pub log: &'a QueueNativeLog,
    pub definition: &'a AdmittedQueueDefinition,
    pub queue: &'a QueueAddress,
    pub job: &'a Arc<QueueReservation>,
    pub ledger: &'a Arc<QueueNativeSourceBudget>,
    pub workers: &'a ScratchWorkers,
    pub directory: &'a Path,
    pub quota: &'a Arc<ScratchDiskQuota>,
}
impl Context<'_> {
    pub async fn scan(&self, group: &CommitGroup, cap: u64) -> Parts {
        let reader = QueueNativeSourceReader::new(
            self.source,
            self.log,
            self.job,
            self.ledger.clone(),
            limits().read,
        )
        .unwrap()
        .with_records(super::super::super::record::record_limits())
        .unwrap();
        let original = reader.cut().unwrap();
        let limit = QueueLimits::default();
        let admission =
            admit_administrative_group(self.queue, group, self.definition, &limit, self.job)
                .unwrap();
        let source =
            open_administrative_source(&reader, original, self.queue, self.definition, self.job)
                .await
                .unwrap();
        let ordinary = SourceAdministrativeAuthority::new(
            source
                .load_admitted_ordinary_authority(admission)
                .await
                .unwrap(),
            self.job,
        )
        .unwrap();
        let floor =
            measure_administrative_success_floor(self.queue, self.definition.pin(), group).unwrap();
        let sink = OwnedAdministrativeSink::new_with_command_limit(
            self.workers.clone(),
            self.directory,
            self.quota.clone(),
            self.job,
            self.queue,
            self.definition.pin(),
            ordinary.ordinary().snapshot().next_sequence,
            source_administrative_publication_limit(&limit, cap).unwrap() - floor,
        )
        .await
        .unwrap();
        let delivery = sink.clone();
        let admin = group.administration.as_ref().unwrap();
        let scan = source
            .scan_addressed_to_sink(
                group,
                self.queue,
                &admin.intent,
                admin.max_result_bytes,
                ordinary.ordinary(),
                &limit,
                move |row| {
                    let sink = delivery.clone();
                    async move { sink.append(row).await }
                },
            )
            .await
            .unwrap();
        (sink, scan, ordinary)
    }
    pub async fn finish(
        &self,
        group: &CommitGroup,
        cap: u64,
        parts: Parts,
        clock: impl FnOnce() -> fireweed_core::queue_contract::QueueResult<i64>,
    ) -> fireweed_core::queue_contract::QueueResult<SourceAdministrativeResolution> {
        finalize_source_administration(
            parts.0,
            parts.1,
            parts.2,
            SourceAdministrativeFinalization {
                original: self.source.source_cut(self.log, self.job)?,
                queue: self.queue,
                group,
                definition: self.definition,
                limits: QueueLimits::default(),
                record_bytes: cap,
            },
            clock,
            self.job,
            self.workers,
            self.directory,
            self.quota.clone(),
        )
        .await
    }
    pub async fn resolution(
        &self,
        group: &CommitGroup,
        cap: u64,
    ) -> SourceAdministrativeResolution {
        self.finish(group, cap, self.scan(group, cap).await, || Ok(10))
            .await
            .unwrap()
    }
    pub async fn probed(
        &self,
        group: &CommitGroup,
        probe: Arc<FinalizationProbe>,
    ) -> SourceAdministrativeResolution {
        let (sink, scan, ordinary) = self.scan(group, 1 << 20).await;
        self.finish(
            group,
            1 << 20,
            (sink, scan, ordinary.with_worker_probe(Some(probe))),
            || Ok(10),
        )
        .await
        .unwrap()
    }
}
