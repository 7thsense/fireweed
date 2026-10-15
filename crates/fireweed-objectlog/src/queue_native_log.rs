//! Bounded native transport for checked queue records; semantic planning stays
//! in the engine and projection. This module grants no QueueStore readiness.
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};
use fireweed_engine::{
    queue_admission::{OwnedQueueBufferCharge, QueueReservation},
    queue_log_record::{
        OwnedQueueLogRecord, QueueLogFormat, QueueLogPayload, view_queue_log_record,
    },
    queue_namespace::{encode_address, framed_address_len, view_address},
    queue_schema_codec::AdmittedQueueDefinition,
};
#[cfg(test)]
use object_log::NativeRecord;
use object_log::{
    NativeAppend, NativeHead, NativeLog, NativeNamespace, NativePublication, NativePublishOutcome,
    NativeReadCut, NativeResolution, NativeWriter,
};
use std::sync::Arc;

/// Observation values for the internal queue composition; SDK roots stay sealed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueueNativeHead {
    inner: NativeHead,
}
impl QueueNativeHead {
    pub fn epoch(&self) -> u64 {
        self.inner.epoch()
    }
    pub fn bounds(&self) -> (u64, u64) {
        self.inner.bounds()
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum QueueNativeSubmit {
    Stored(QueueNativeHead),
    Conflict,
}
#[derive(Debug, PartialEq, Eq)]
pub enum QueueNativeResolution {
    DurableContent,
    IncompatibleCoverage,
    Unknown,
}
mod ownership;
mod semantic;
pub use semantic::*;
mod config;
pub use config::{QueueNativeLogOptions, QueueNativeS3Connection, QueueNativeS3TransportOptions};
pub struct QueueNativeLog {
    identity: Arc<()>,
    log: NativeLog,
}
pub struct QueueNativeNamespace {
    namespace: NativeNamespace,
    identity: Arc<()>,
    address: Vec<u8>,
    log_identity: Arc<()>,
    _charge: Arc<OwnedQueueBufferCharge>,
}
pub struct QueueNativeCut {
    cut: NativeReadCut,
    namespace: Arc<()>,
}
pub struct QueueNativeWriter {
    writer: NativeWriter,
    namespace: Arc<()>,
    epoch: u64,
    next_offset: u64,
}
pub struct QueueNativeAcquired {
    log_identity: Arc<()>,
    namespace: QueueNativeNamespace,
    writer: QueueNativeWriter,
    cut: QueueNativeCut,
    // Last Root guard outlives all retained SDK fields during destruction.
    semantic_identity: Arc<OwnedQueueBufferCharge>,
}
pub struct QueueNativePublication {
    publication: NativePublication,
    log_identity: Arc<()>,
    _charge: Arc<OwnedQueueBufferCharge>,
}
pub struct QueueNativeGenesis {
    record: OwnedQueueLogRecord,
}
fn unknown(error: impl std::fmt::Display) -> QueueError {
    QueueError::OutcomeUnknown(format!("native queue log: {error}"))
}
fn construction_error(error: object_log::ObjectLogError) -> QueueError {
    match error {
        object_log::ObjectLogError::InvalidConfig(reason) => QueueError::Invalid(reason),
        _ => QueueError::Invalid("invalid native queue transport configuration".into()),
    }
}
fn read_error(error: object_log::ObjectLogError) -> QueueError {
    match error {
        object_log::ObjectLogError::ReadLimitExceeded { .. } => QueueError::TooLarge,
        other => unknown(other),
    }
}
impl QueueNativeNamespace {
    pub fn belongs_to(&self, job: &QueueReservation) -> bool {
        std::ptr::eq(self._charge.reservation(), job)
    }
}
impl QueueNativeLog {
    /// Construct through the owning adapter; validation precedes storage access.
    pub fn local(
        root: &std::path::Path,
        prefix: String,
        options: QueueNativeLogOptions,
    ) -> QueueResult<Self> {
        Ok(Self::new(
            NativeLog::local(object_log::LocalBlobStore::new(root), prefix, options.sdk())
                .map_err(unknown)?,
        ))
    }
    /// Construct S3 transport inside the owning adapter; credentials are borrowed.
    pub fn s3(
        connection: QueueNativeS3Connection<'_>,
        prefix: String,
        options: QueueNativeLogOptions,
        transport: QueueNativeS3TransportOptions,
    ) -> QueueResult<Self> {
        let limits = options.sdk();
        NativeLog::preflight_s3(&prefix, connection.bucket, limits).map_err(construction_error)?;
        let store = object_log::S3BlobStore::new_owned_with_options(
            connection.endpoint,
            connection.region,
            connection.bucket,
            connection.access_key,
            connection.secret_key,
            transport.sdk(),
        )
        .map_err(construction_error)?;
        Ok(Self::new(
            NativeLog::s3(store, prefix, limits).map_err(construction_error)?,
        ))
    }
    pub(crate) fn new(log: NativeLog) -> Self {
        Self {
            log,
            identity: Arc::new(()),
        }
    }
    pub async fn open(
        &self,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<QueueNativeNamespace>> {
        let charge = ownership::shared_charge(
            job,
            u64::try_from(
                framed_address_len(address)?
                    .checked_add(
                        std::mem::size_of::<QueueNativeNamespace>()
                            + 2 * std::mem::size_of::<usize>(),
                    )
                    .ok_or(QueueError::TooLarge)?,
            )
            .map_err(unknown)?,
        )?;
        let address = encode_address(address)?;
        let Some(namespace) = self
            .log
            .with_operation_owner(ownership::physical_owner([charge.clone()])?)
            .open(&address)
            .await
            .map_err(unknown)?
        else {
            return Ok(None);
        };
        Ok(Some(QueueNativeNamespace {
            namespace,
            identity: Arc::new(()),
            address,
            log_identity: self.identity.clone(),
            _charge: charge,
        }))
    }
    /// Explicit candidate election only; callers must publish/adopt genesis.
    pub async fn provision_namespace(
        &self,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeNamespace> {
        let charge = ownership::shared_charge(
            job,
            u64::try_from(
                framed_address_len(address)?
                    .checked_add(
                        std::mem::size_of::<QueueNativeNamespace>()
                            + 2 * std::mem::size_of::<usize>(),
                    )
                    .ok_or(QueueError::TooLarge)?,
            )
            .map_err(unknown)?,
        )?;
        let address = encode_address(address)?;
        let namespace = self
            .log
            .with_operation_owner(ownership::physical_owner([charge.clone()])?)
            .provision_new(&address)
            .await
            .map_err(unknown)?;
        Ok(QueueNativeNamespace {
            namespace,
            identity: Arc::new(()),
            address,
            log_identity: self.identity.clone(),
            _charge: charge,
        })
    }
    fn validate_namespace(&self, namespace: &QueueNativeNamespace) -> QueueResult<()> {
        if !Arc::ptr_eq(&self.identity, &namespace.log_identity) {
            return Err(unknown("namespace belongs to another log"));
        }
        Ok(())
    }
    fn preparation_charge(
        &self,
        record: &OwnedQueueLogRecord,
        identity: &str,
        extra: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Arc<OwnedQueueBufferCharge>> {
        let bytes = (record.bytes().len() as u64)
            .checked_add(identity.len() as u64)
            .and_then(|n| n.checked_add(extra))
            .and_then(|n| n.checked_add(std::mem::size_of::<QueueNativePublication>() as u64))
            .ok_or(QueueError::TooLarge)?;
        ownership::shared_charge(job, bytes)
    }
    pub async fn capture(&self, namespace: &QueueNativeNamespace) -> QueueResult<QueueNativeCut> {
        self.validate_namespace(namespace)?;
        Ok(QueueNativeCut {
            cut: self
                .log
                .with_operation_owner(ownership::physical_owner([namespace._charge.clone()])?)
                .capture(&namespace.namespace)
                .await
                .map_err(unknown)?,
            namespace: namespace.identity.clone(),
        })
    }
    async fn acquire_with_cut(
        &self,
        namespace: &QueueNativeNamespace,
    ) -> QueueResult<(QueueNativeWriter, QueueNativeCut)> {
        self.validate_namespace(namespace)?;
        let writer = self
            .log
            .with_operation_owner(ownership::physical_owner([namespace._charge.clone()])?)
            .acquire(&namespace.namespace)
            .await
            .map_err(unknown)?;
        let cut = self
            .log
            .with_operation_owner(ownership::physical_owner([namespace._charge.clone()])?)
            .refresh_writer(&writer)
            .await
            .map_err(unknown)?;
        Ok((
            QueueNativeWriter {
                writer,
                namespace: namespace.identity.clone(),
                epoch: cut.head().epoch(),
                next_offset: cut.head().bounds().1,
            },
            QueueNativeCut {
                cut,
                namespace: namespace.identity.clone(),
            },
        ))
    }
    pub async fn acquire(
        &self,
        namespace: &QueueNativeNamespace,
    ) -> QueueResult<QueueNativeWriter> {
        Ok(self.acquire_with_cut(namespace).await?.0)
    }
    pub async fn refresh(
        &self,
        namespace: &QueueNativeNamespace,
        writer: &mut QueueNativeWriter,
    ) -> QueueResult<QueueNativeCut> {
        self.validate_namespace(namespace)?;
        if !Arc::ptr_eq(&namespace.identity, &writer.namespace) {
            return Err(unknown("writer belongs to another namespace"));
        }
        let cut = self
            .log
            .with_operation_owner(ownership::physical_owner([namespace._charge.clone()])?)
            .refresh_writer(&writer.writer)
            .await
            .map_err(unknown)?;
        if cut.head().epoch() != writer.epoch {
            return Err(unknown("writer refresh changed acquired epoch"));
        }
        writer.next_offset = cut.head().bounds().1;
        Ok(QueueNativeCut {
            cut,
            namespace: namespace.identity.clone(),
        })
    }
    pub async fn acquire_bundle(
        &self,
        namespace: QueueNativeNamespace,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeAcquired> {
        if !std::ptr::eq(namespace._charge.reservation(), job.as_ref()) {
            return Err(unknown(
                "acquisition namespace belongs to another operation",
            ));
        }
        let semantic_identity = QueueNativeAcquired::identity_charge(job)?;
        for attempt in 0..8 {
            match self.acquire_with_cut(&namespace).await {
                Ok((writer, cut)) => {
                    return Ok(QueueNativeAcquired {
                        log_identity: self.identity.clone(),
                        semantic_identity,
                        namespace,
                        writer,
                        cut,
                    });
                }
                Err(error) => {
                    // Capture validates the same family's monotonic floor. No
                    // original append exists yet, so a fresh fence is safe.
                    let _ = self.capture(&namespace).await?;
                    if attempt == 7 {
                        return Err(error);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        Err(unknown("bounded authority acquisition exhausted"))
    }
    fn validate_prepare_record(
        &self,
        namespace: &QueueNativeNamespace,
        writer: &QueueNativeWriter,
        record: &OwnedQueueLogRecord,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<()> {
        if !record.belongs_to(job) || !std::ptr::eq(namespace._charge.reservation(), job.as_ref()) {
            return Err(unknown("record or namespace belongs to another operation"));
        }
        self.validate_namespace(namespace)?;
        if !Arc::ptr_eq(&namespace.identity, &writer.namespace) {
            return Err(unknown("writer belongs to another namespace"));
        }
        let view = record.view()?;
        if view.epoch() != writer.epoch
            || view.format() != QueueLogFormat::ChecksummedV2
            || view.address() != view_address(&namespace.address)?
        {
            return Err(unknown("record format or complete queue address differs"));
        }
        let genesis = matches!(view.payload(), QueueLogPayload::Genesis(_));
        if genesis != (writer.next_offset == 0) {
            return Err(unknown("genesis must be the unique offset-zero record"));
        }
        Ok(())
    }
    pub async fn prepare(
        &self,
        namespace: &QueueNativeNamespace,
        writer: &QueueNativeWriter,
        record: &OwnedQueueLogRecord,
        identity: &str,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        self.validate_prepare_record(namespace, writer, record, job)?;
        let charge = self.preparation_charge(record, identity, 0, job)?;
        let publication = self
            .log
            .with_operation_owner(ownership::physical_owner([
                charge.clone(),
                namespace._charge.clone(),
            ])?)
            .prepare(
                &writer.writer,
                &[NativeAppend {
                    identity,
                    payload: record.bytes(),
                    records: 1,
                }],
            )
            .await
            .map_err(unknown)?;
        Ok(QueueNativePublication {
            publication,
            log_identity: self.identity.clone(),
            _charge: charge,
        })
    }
    pub async fn submit(
        &self,
        publication: &QueueNativePublication,
    ) -> QueueResult<QueueNativeSubmit> {
        if !Arc::ptr_eq(&self.identity, &publication.log_identity) {
            return Err(unknown("publication belongs to another log"));
        }
        Ok(
            match self
                .log
                .with_operation_owner(ownership::physical_owner([publication._charge.clone()])?)
                .submit(&publication.publication)
                .await
                .map_err(unknown)?
            {
                NativePublishOutcome::Stored(inner) => {
                    QueueNativeSubmit::Stored(QueueNativeHead { inner })
                }
                NativePublishOutcome::Conflict => QueueNativeSubmit::Conflict,
            },
        )
    }
    pub async fn resolve(
        &self,
        publication: &QueueNativePublication,
        entries: usize,
    ) -> QueueResult<QueueNativeResolution> {
        if !Arc::ptr_eq(&self.identity, &publication.log_identity) {
            return Err(unknown("publication belongs to another log"));
        }
        Ok(
            match self
                .log
                .with_operation_owner(ownership::physical_owner([publication._charge.clone()])?)
                .resolve(&publication.publication, entries)
                .await
                .map_err(unknown)?
            {
                NativeResolution::DurableContent => QueueNativeResolution::DurableContent,
                NativeResolution::IncompatibleCoverage => {
                    QueueNativeResolution::IncompatibleCoverage
                }
                NativeResolution::Unknown => QueueNativeResolution::Unknown,
            },
        )
    }
    #[cfg(test)]
    pub async fn read(
        &self,
        namespace: &QueueNativeNamespace,
        cut: &QueueNativeCut,
        offset: u64,
    ) -> QueueResult<Option<NativeRecord>> {
        if !Arc::ptr_eq(&namespace.identity, &cut.namespace) {
            return Err(unknown("cut belongs to another namespace"));
        }
        self.validate_namespace(namespace)?;
        let Some(record) = self
            .log
            .with_operation_owner(ownership::physical_owner([namespace._charge.clone()])?)
            .read(&cut.cut, offset)
            .await
            .map_err(unknown)?
        else {
            return Ok(None);
        };
        let view = view_queue_log_record(record.bytes())?;
        if record.base_offset() != offset
            || record.record_count() != 1
            || view.epoch() > cut.head().epoch()
            || view.format() != QueueLogFormat::ChecksummedV2
            || view.address() != view_address(&namespace.address)?
        {
            return Err(unknown(
                "captured record range, format or full address differs",
            ));
        }
        Ok(Some(record))
    }
    /// Called only inside the existing owned recovery/mutation task.
    pub async fn read_admitted(
        &self,
        namespace: &QueueNativeNamespace,
        cut: &QueueNativeCut,
        offset: u64,
        hard_record_bytes: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<OwnedQueueLogRecord>> {
        if !std::ptr::eq(namespace._charge.reservation(), job.as_ref())
            || !Arc::ptr_eq(&namespace.identity, &cut.namespace)
        {
            return Err(unknown("read namespace, cut or operation differs"));
        }
        self.validate_namespace(namespace)?;
        let metadata = ownership::shared_charge(job, 65536)?;
        let Some(plan) = self
            .log
            .with_operation_owner(ownership::physical_owner([metadata.clone()])?)
            .plan_read(&cut.cut, offset)
            .await
            .map_err(unknown)?
        else {
            return Ok(None);
        };
        if plan.base_offset() != offset || plan.record_count() != 1 {
            return Err(unknown("planned queue record logical range differs"));
        }
        let actual = u64::try_from(plan.payload_bytes()).map_err(|_| QueueError::TooLarge)?;
        if actual > hard_record_bytes {
            return Err(QueueError::TooLarge);
        }
        let capacity = actual
            .checked_mul(4)
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?;
        let charge = ownership::shared_charge(job, capacity)?;
        let limit = plan.payload_bytes();
        let scoped = self.log.with_operation_owner(ownership::physical_owner([
            metadata.clone(),
            charge.clone(),
        ])?);
        let result = scoped.read_planned(plan, limit).await;
        // The SDK has awaited physical retirement; remove its scoped view's
        // independent token before transferring the unique output guard.
        drop(scoped);
        let record = result.map_err(read_error)?;
        let view = view_queue_log_record(record.bytes())?;
        if record.base_offset() != offset
            || record.record_count() != 1
            || view.format() != QueueLogFormat::ChecksummedV2
            || view.epoch() > cut.head().epoch()
            || view.address() != view_address(&namespace.address)?
        {
            return Err(unknown("admitted record scope, range or epoch differs"));
        }
        let charge = match Arc::try_unwrap(charge) {
            Ok(charge) => charge,
            Err(remaining) => {
                // Never refund output capacity while returned bytes remain live.
                drop(record);
                drop(remaining);
                return Err(unknown("planned read retained physical output ownership"));
            }
        };
        Ok(Some(OwnedQueueLogRecord::from_admitted_bytes(
            record.into_bytes(),
            charge,
            job,
        )?))
    }
    pub async fn adopt_genesis(
        &self,
        namespace: &QueueNativeNamespace,
        cut: &QueueNativeCut,
        incoming: &AdmittedQueueDefinition,
        hard_record_bytes: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeGenesis> {
        if !incoming.belongs_to(job) || !std::ptr::eq(namespace._charge.reservation(), job.as_ref())
        {
            return Err(unknown("genesis definition belongs to another operation"));
        }
        let record = self
            .read_admitted(namespace, cut, 0, hard_record_bytes, job)
            .await?
            .ok_or_else(|| unknown("selected queue has no genesis"))?;
        let view = view_queue_log_record(record.bytes())?;
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            return Err(unknown("offset zero is not genesis"));
        };
        let definition = genesis.validate_definition(job)?;
        if definition.schema() != incoming.schema() {
            return Err(QueueError::DefinitionConflict);
        }
        drop(definition);
        let confirmed = self.capture(namespace).await?;
        if confirmed.head() != cut.head() {
            return Err(unknown("genesis authority changed before confirmation"));
        }
        Ok(QueueNativeGenesis { record })
    }
    /// Internal provisioning retains each original until fresh winner adoption.
    pub async fn provision_record(
        &self,
        address: &QueueAddress,
        definition: &AdmittedQueueDefinition,
        candidate_secret: &[u8; 32],
        known_queue: bool,
        hard_record_bytes: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<OwnedQueueLogRecord> {
        if !definition.belongs_to(job) {
            return Err(unknown("provision definition belongs to another operation"));
        }
        let namespace = match self.open(address, job).await? {
            Some(namespace) => namespace,
            None if known_queue => return Err(unknown("known queue binding missing")),
            None => self.provision_namespace(address, job).await?,
        };
        let cut = self.capture(&namespace).await?;
        if cut.head().bounds().1 > 0 {
            return Ok(self
                .adopt_genesis(&namespace, &cut, definition, hard_record_bytes, job)
                .await?
                .record);
        }
        if known_queue {
            return Err(unknown("known queue has empty genesis authority"));
        }
        let semantic_identity = QueueNativeAcquired::identity_charge(job)?;
        let mut acquired_parts = None;
        for attempt in 0..8 {
            match self.acquire_with_cut(&namespace).await {
                Ok(parts) => {
                    acquired_parts = Some(parts);
                    break;
                }
                Err(error) => {
                    let fresh = self.capture(&namespace).await?;
                    if fresh.head().bounds().1 > 0 {
                        return Ok(self
                            .adopt_genesis(&namespace, &fresh, definition, hard_record_bytes, job)
                            .await?
                            .record);
                    }
                    if attempt == 7 {
                        return Err(error);
                    }
                    tokio::task::yield_now().await;
                }
            }
        }
        let (writer, cut) =
            acquired_parts.ok_or_else(|| unknown("bounded genesis acquisition exhausted"))?;
        let acquired = QueueNativeAcquired {
            log_identity: self.identity.clone(),
            semantic_identity,
            namespace,
            writer,
            cut,
        };
        if acquired.head().bounds().1 > 0 {
            return Ok(self
                .adopt_genesis(
                    &acquired.namespace,
                    &acquired.cut,
                    definition,
                    hard_record_bytes,
                    job,
                )
                .await?
                .record);
        }
        let record = fireweed_engine::queue_log_record::encode_checked_genesis(
            acquired.head().epoch(),
            address,
            definition,
            candidate_secret,
            hard_record_bytes,
            job,
        )?;
        let original = acquired.prepare_record(self, &record, job).await?;
        let _ = original.submit(self).await;
        // Never manufacture absence or a replacement after possible publication.
        let fresh = self.capture(&acquired.namespace).await?;
        if fresh.head().bounds().1 == 0 {
            return Err(unknown("genesis publication remains unresolved"));
        }
        let winner = self
            .adopt_genesis(
                &acquired.namespace,
                &fresh,
                definition,
                hard_record_bytes,
                job,
            )
            .await?;
        drop(original);
        Ok(winner.record)
    }
    pub async fn shutdown(&self) -> QueueResult<()> {
        self.log.shutdown().await.map_err(unknown)
    }
}
impl QueueNativePublication {
    /// Consume no authority: the original remains available after ambiguous submit.
    pub async fn submit(&self, log: &QueueNativeLog) -> QueueResult<QueueNativeSubmit> {
        log.submit(self).await
    }
    /// Resolve only this retained original's complete content.
    pub async fn resolve(
        &self,
        log: &QueueNativeLog,
        entries: usize,
    ) -> QueueResult<QueueNativeResolution> {
        log.resolve(self, entries).await
    }
}
impl QueueNativeAcquired {
    /// Preserve the authenticated namespace family when advancing a retained cut.
    pub fn retained_namespace(
        &self,
        address: &QueueAddress,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeNamespace> {
        if !self.namespace.belongs_to(job) {
            return Err(unknown("retained namespace belongs to another operation"));
        }
        let retained = view_address(&self.namespace.address)?;
        if retained.tenant != address.tenant || retained.queue != address.queue {
            return Err(unknown("retained namespace belongs to another queue"));
        }
        let bytes = self
            .namespace
            .address
            .len()
            .checked_add(std::mem::size_of::<QueueNativeNamespace>())
            .ok_or(QueueError::TooLarge)?;
        let charge = ownership::shared_charge(job, u64::try_from(bytes).map_err(unknown)?)?;
        Ok(QueueNativeNamespace {
            namespace: self.namespace.namespace.clone(),
            identity: self.namespace.identity.clone(),
            address: self.namespace.address.clone(),
            log_identity: self.namespace.log_identity.clone(),
            _charge: charge,
        })
    }
    pub fn head(&self) -> QueueNativeHead {
        self.cut.head()
    }
    pub async fn confirms(
        &self,
        log: &QueueNativeLog,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<bool> {
        if !self.namespace.belongs_to(job) {
            return Err(unknown("acquired namespace belongs to another operation"));
        }
        Ok(log.capture(&self.namespace).await?.head() == self.cut.head())
    }
    pub async fn read(
        &self,
        log: &QueueNativeLog,
        offset: u64,
        hard_record_bytes: u64,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<Option<OwnedQueueLogRecord>> {
        log.read_admitted(&self.namespace, &self.cut, offset, hard_record_bytes, job)
            .await
    }
    pub async fn prepare_record(
        &self,
        log: &QueueNativeLog,
        record: &OwnedQueueLogRecord,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        use sha2::{Digest, Sha256};
        use std::fmt::Write;
        let _identity_charge = job.owned_buffer(256)?;
        let mut identity = String::with_capacity(config::RECORD_IDENTITY_BYTES);
        write!(
            &mut identity,
            "queue-record/{:016x}/{:016x}/",
            self.writer.epoch, self.writer.next_offset
        )
        .expect("String write");
        for byte in Sha256::digest(record.bytes()) {
            write!(&mut identity, "{byte:02x}").expect("String write");
        }
        debug_assert_eq!(identity.len(), config::RECORD_IDENTITY_BYTES);
        log.prepare(&self.namespace, &self.writer, record, &identity, job)
            .await
    }
    pub async fn prepare(
        &self,
        log: &QueueNativeLog,
        record: &OwnedQueueLogRecord,
        identity: &str,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativePublication> {
        log.prepare(&self.namespace, &self.writer, record, identity, job)
            .await
    }
}
impl QueueNativeCut {
    pub fn head(&self) -> QueueNativeHead {
        QueueNativeHead {
            inner: self.cut.head(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fireweed_core::queue_contract::{CollectionDefinition, QueueSchema};
    use fireweed_engine::{
        queue_admission::QueueAdmission, queue_log_record::encode_checked_genesis,
        queue_schema_codec::encode_definition,
    };
    fn genesis_secret(adopted: &QueueNativeGenesis) -> [u8; 32] {
        let view = adopted.record.view().unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("expected genesis")
        };
        *genesis.cursor_secret()
    }
    fn genesis_definition(
        adopted: &QueueNativeGenesis,
        job: &Arc<QueueReservation>,
    ) -> AdmittedQueueDefinition {
        let view = adopted.record.view().unwrap();
        let QueueLogPayload::Genesis(genesis) = view.payload() else {
            panic!("expected genesis")
        };
        genesis.validate_definition(job).unwrap()
    }
    async fn genesis_race_contract(make: impl Fn() -> QueueNativeLog) {
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let admission = QueueAdmission::new(2, 32 << 20).unwrap();
        let first_job = Arc::new(admission.reserve(0).unwrap());
        let second_job = Arc::new(admission.reserve(0).unwrap());
        let raw = encode_definition(&schema).unwrap();
        let first_definition = AdmittedQueueDefinition::from_raw(&raw, &first_job).unwrap();
        let second_definition = AdmittedQueueDefinition::from_raw(&raw, &second_job).unwrap();
        let first = make();
        let second = make();
        let first_namespace = first
            .provision_namespace(&address, &first_job)
            .await
            .unwrap();
        let first_writer = first.acquire(&first_namespace).await.unwrap();
        let first_record = encode_checked_genesis(
            first_writer.epoch,
            &address,
            &first_definition,
            &[11; 32],
            4096,
            &first_job,
        )
        .unwrap();
        let original = first
            .prepare(
                &first_namespace,
                &first_writer,
                &first_record,
                "first-genesis",
                &first_job,
            )
            .await
            .unwrap();
        let second_namespace = second.open(&address, &second_job).await.unwrap().unwrap();
        let second_writer = second.acquire(&second_namespace).await.unwrap();
        let second_record = encode_checked_genesis(
            second_writer.epoch,
            &address,
            &second_definition,
            &[22; 32],
            4096,
            &second_job,
        )
        .unwrap();
        let winner = second
            .prepare(
                &second_namespace,
                &second_writer,
                &second_record,
                "second-genesis",
                &second_job,
            )
            .await
            .unwrap();
        assert!(matches!(
            second.submit(&winner).await.unwrap(),
            QueueNativeSubmit::Stored(_)
        ));
        assert!(matches!(
            first.submit(&original).await.unwrap(),
            QueueNativeSubmit::Conflict
        ));
        assert_eq!(
            first.resolve(&original, 1).await.unwrap(),
            QueueNativeResolution::IncompatibleCoverage
        );
        let first_cut = first.capture(&first_namespace).await.unwrap();
        let second_cut = second.capture(&second_namespace).await.unwrap();
        assert!(matches!(
            first
                .read_admitted(&first_namespace, &first_cut, 0, 4096, &second_job)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(matches!(
            first
                .read_admitted(&first_namespace, &second_cut, 0, 4096, &first_job)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(matches!(
            first
                .prepare(
                    &first_namespace,
                    &second_writer,
                    &first_record,
                    "foreign-writer",
                    &first_job
                )
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        let adopted = first
            .adopt_genesis(
                &first_namespace,
                &first_cut,
                &first_definition,
                4096,
                &first_job,
            )
            .await
            .unwrap();
        assert_eq!(genesis_secret(&adopted), [22; 32]);
        let mut different = schema.clone();
        different.max_attempts += 1;
        let different =
            AdmittedQueueDefinition::from_raw(&encode_definition(&different).unwrap(), &first_job)
                .unwrap();
        assert!(matches!(
            first
                .adopt_genesis(&first_namespace, &first_cut, &different, 4096, &first_job)
                .await,
            Err(QueueError::DefinitionConflict)
        ));
        assert_eq!(first_cut.head().bounds(), (0, 1));
        first.shutdown().await.unwrap();
        second.shutdown().await.unwrap();
    }

    fn test_limits() -> object_log::NativeLimits {
        object_log::NativeLimits {
            workers: 2,
            blocking_workers: 2,
            operations: 4,
            bytes: 4 << 20,
            version_bytes: 4096,
            binding_bytes: 4096,
            group_entries: 4,
            group_bytes: 1 << 20,
        }
    }
    #[tokio::test]
    async fn known_missing_or_empty_queue_provision_has_no_storage_effects() {
        let directory = tempfile::tempdir().unwrap();
        let provider = object_log::LocalBlobStore::new(directory.path());
        let log = QueueNativeLog::new(
            NativeLog::local(provider.clone(), "known-provision".into(), test_limits()).unwrap(),
        );
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        assert!(matches!(
            log.provision_record(&address, &definition, &[9; 32], true, 4096, &job)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert!(
            object_log::BlobStore::list(&provider, "known-provision")
                .await
                .unwrap()
                .is_empty()
        );
        let namespace = log.provision_namespace(&address, &job).await.unwrap();
        let before = log.capture(&namespace).await.unwrap().head();
        let mut bytes = Vec::new();
        for key in object_log::BlobStore::list(&provider, "known-provision")
            .await
            .unwrap()
        {
            bytes.push((
                key.clone(),
                object_log::BlobStore::get(&provider, &key)
                    .await
                    .unwrap()
                    .unwrap(),
            ));
        }
        assert!(matches!(
            log.provision_record(&address, &definition, &[9; 32], true, 4096, &job)
                .await,
            Err(QueueError::OutcomeUnknown(_))
        ));
        assert_eq!(log.capture(&namespace).await.unwrap().head(), before);
        for (key, old) in bytes {
            assert_eq!(
                object_log::BlobStore::get(&provider, &key)
                    .await
                    .unwrap()
                    .unwrap(),
                old
            );
        }
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn retained_namespace_rejects_reauthored_prefix_with_same_numeric_head() {
        use object_log::BlobStore;
        use sha2::{Digest, Sha256};
        let directory = tempfile::tempdir().unwrap();
        let provider = object_log::LocalBlobStore::new(directory.path());
        let log = QueueNativeLog::new(
            NativeLog::local(provider.clone(), "rewrite".into(), test_limits()).unwrap(),
        );
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let namespace = log.provision_namespace(&address, &job).await.unwrap();
        let first = log.acquire_bundle(namespace, &job).await.unwrap();
        let record = encode_checked_genesis(
            first.head().epoch(),
            &address,
            &definition,
            &[7; 32],
            4096,
            &job,
        )
        .unwrap();
        let original = first.prepare_record(&log, &record, &job).await.unwrap();
        assert!(matches!(
            log.submit(&original).await.unwrap(),
            QueueNativeSubmit::Stored(_)
        ));
        let retained = first.retained_namespace(&address, &job).unwrap();
        let captured = log.capture(&retained).await.unwrap();
        let keys = provider.list("rewrite").await.unwrap();
        let head_key = keys.iter().find(|key| key.ends_with("/head")).unwrap();
        let mut head = provider.get(head_key).await.unwrap().unwrap().to_vec();
        let hash = head[96..128]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let page_key = keys
            .iter()
            .find(|key| key.ends_with(&format!("/{hash}")))
            .unwrap();
        let mut leaf = provider.get(page_key).await.unwrap().unwrap().to_vec();
        // Alter the old payload certificate while preserving a valid leaf,
        // content-addressed page key, head checksum, and all numeric progress.
        leaf[112 + 88] ^= 1;
        let changed_hash = Sha256::digest(&leaf);
        let changed_hex = changed_hash
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let changed_key = format!("{}/{}", page_key.rsplit_once('/').unwrap().0, changed_hex);
        provider
            .put(&changed_key, bytes::Bytes::from(leaf))
            .await
            .unwrap();
        head[96..128].copy_from_slice(&changed_hash);
        let checksum = Sha256::digest(&head[..208]);
        head[208..240].copy_from_slice(&checksum);
        provider
            .put(head_key, bytes::Bytes::from(head))
            .await
            .unwrap();
        // Fresh-open characterization proves this is a well-framed root;
        // only the retained authenticated prefix makes it unacceptable.
        let fresh = log.open(&address, &job).await.unwrap().unwrap();
        let fresh_head = log.capture(&fresh).await.unwrap().head();
        assert_eq!(fresh_head.epoch(), captured.head().epoch());
        assert_eq!(fresh_head.bounds(), captured.head().bounds());
        assert!(log.capture(&retained).await.is_err());
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn generated_identity_and_sdk_frame_fit_exact_transport_ceiling() {
        use object_log::BlobStore;
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let address = QueueAddress {
            tenant: "tenant\0雪".into(),
            queue: "queue/λ".into(),
        };
        let admission = QueueAdmission::new(1, 32 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let record_bytes =
            fireweed_engine::queue_log_record::checked_genesis_record_len(&address, &definition)
                .unwrap();
        let frame_bytes = usize::try_from(record_bytes + 135).unwrap();
        for ceiling in [frame_bytes, frame_bytes - 1] {
            let directory = tempfile::tempdir().unwrap();
            let provider = object_log::LocalBlobStore::new(directory.path());
            let mut limits = test_limits();
            limits.group_bytes = ceiling;
            let log = QueueNativeLog::new(
                NativeLog::local(provider.clone(), "exact-frame".into(), limits).unwrap(),
            );
            let namespace = log.provision_namespace(&address, &job).await.unwrap();
            let acquired = log.acquire_bundle(namespace, &job).await.unwrap();
            let record = encode_checked_genesis(
                acquired.head().epoch(),
                &address,
                &definition,
                &[17; 32],
                record_bytes,
                &job,
            )
            .unwrap();
            let publication = acquired.prepare_record(&log, &record, &job).await;
            if ceiling == frame_bytes {
                let publication = publication.unwrap();
                assert!(matches!(
                    publication.submit(&log).await.unwrap(),
                    QueueNativeSubmit::Stored(_)
                ));
                let mut found = 0;
                for key in provider.list("exact-frame").await.unwrap() {
                    let raw = provider.get(&key).await.unwrap().unwrap();
                    if raw.starts_with(b"OND2") {
                        found += 1;
                        assert_eq!(raw.len(), frame_bytes);
                        let identity_len = u64::from_be_bytes(raw[8..16].try_into().unwrap());
                        assert_eq!(identity_len, config::RECORD_IDENTITY_BYTES as u64);
                    }
                }
                assert_eq!(found, 1);
            } else {
                assert!(
                    matches!(publication, Err(QueueError::OutcomeUnknown(reason)) if reason.contains("native group framed limit"))
                );
            }
            log.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn adapter_constructor_validates_before_local_resource_access() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("uncreated");
        let configured = test_limits();
        let mut options = QueueNativeLogOptions {
            workers: configured.workers,
            blocking_workers: configured.blocking_workers,
            operations: configured.operations,
            bytes: configured.bytes,
            version_bytes: configured.version_bytes,
            binding_bytes: configured.binding_bytes,
            group_entries: configured.group_entries,
            group_bytes: configured.group_bytes,
        };
        options.operations = 0;
        assert!(QueueNativeLog::local(&root, "factory".into(), options).is_err());
        // Invalid admission is rejected without needing a usable S3 endpoint,
        // credentials, client, executor, or network operation.
        assert!(
            QueueNativeLog::s3(
                QueueNativeS3Connection {
                    endpoint: "",
                    region: "",
                    bucket: "",
                    access_key: "",
                    secret_key: ""
                },
                "factory".into(),
                options,
                QueueNativeS3TransportOptions::default()
            )
            .is_err()
        );
        assert!(!root.exists());
        options.operations = configured.operations;
        let log = QueueNativeLog::local(&root, "factory".into(), options).unwrap();
        assert!(!root.exists());
        log.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn abandoned_reply_keeps_root_job_until_owned_native_read_finishes() {
        let directory = tempfile::tempdir().unwrap();
        let log = QueueNativeLog::new(
            NativeLog::local(
                object_log::LocalBlobStore::new(directory.path()),
                "owned-read".into(),
                test_limits(),
            )
            .unwrap(),
        );
        let admission = QueueAdmission::new(1, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let address = QueueAddress {
            tenant: "tenant".into(),
            queue: "queue".into(),
        };
        let namespace = log.provision_namespace(&address, &job).await.unwrap();
        let acquired = log.acquire_bundle(namespace, &job).await.unwrap();
        let weak = Arc::downgrade(&job);
        let (started, admitted) = tokio::sync::oneshot::channel();
        let (release, resume) = tokio::sync::oneshot::channel();
        let (send, reply) = tokio::sync::oneshot::channel();
        let owned = tokio::spawn(async move {
            started.send(()).unwrap();
            resume.await.unwrap();
            let result = acquired.read(&log, 0, 256 << 20, &job).await;
            assert!(matches!(&result, Ok(None)));
            log.shutdown().await.unwrap();
            drop((acquired, job));
            let _ = send.send(result);
        });
        admitted.await.unwrap();
        drop(reply);
        assert!(weak.upgrade().is_some());
        assert_eq!(admission.usage().unwrap().0, 1);
        assert!(matches!(admission.reserve(0), Err(QueueError::Busy)));
        release.send(()).unwrap();
        owned.await.unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }

    #[tokio::test]
    async fn competing_genesis_attempts_adopt_one_original_secret() {
        let directory = tempfile::tempdir().unwrap();
        genesis_race_contract(|| {
            QueueNativeLog::new(
                NativeLog::local(
                    object_log::LocalBlobStore::new(directory.path()),
                    "genesis-race".into(),
                    test_limits(),
                )
                .unwrap(),
            )
        })
        .await;
    }
    #[tokio::test]
    async fn s3_competing_genesis_runs_identical_contract_both_signing_modes() {
        let env = crate::shared_s3_test_env();
        for disabled in [false, true] {
            let prefix = format!(
                "native-queue-genesis/{}/{disabled}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            genesis_race_contract(|| {
                let store = object_log::S3BlobStore::new(
                    &env.endpoint,
                    &env.region,
                    &env.bucket,
                    &env.access_key,
                    &env.secret_key,
                )
                .with_payload_signing_disabled(disabled);
                QueueNativeLog::new(NativeLog::s3(store, prefix.clone(), test_limits()).unwrap())
            })
            .await;
            let cleanup = object_log::S3BlobStore::new(
                &env.endpoint,
                &env.region,
                &env.bucket,
                &env.access_key,
                &env.secret_key,
            );
            for key in object_log::BlobStore::list(&cleanup, &prefix)
                .await
                .unwrap()
            {
                object_log::BlobStore::delete(&cleanup, &key).await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn checked_genesis_fixed_cut_and_cold_adoption_preserve_original_secret() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().to_path_buf();
        let limits = object_log::NativeLimits {
            workers: 2,
            blocking_workers: 2,
            operations: 4,
            bytes: 4 << 20,
            version_bytes: 4096,
            binding_bytes: 4096,
            group_entries: 4,
            group_bytes: 1 << 20,
        };
        let address = QueueAddress {
            tenant: "tenant\0β".into(),
            queue: "queue/λ".into(),
        };
        let schema = QueueSchema {
            work: CollectionDefinition {
                fields: Default::default(),
                indexes: Default::default(),
                unique_indexes: Default::default(),
                additional_field_prefixes: Default::default(),
            },
            state: Default::default(),
            progress_bound_nanos: 1,
            max_attempts: 3,
        };
        let admission = QueueAdmission::new(2, 16 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let definition =
            AdmittedQueueDefinition::from_raw(&encode_definition(&schema).unwrap(), &job).unwrap();
        let transport = QueueNativeLog::new(
            NativeLog::local(
                object_log::LocalBlobStore::new(&root),
                "queue-records".into(),
                limits,
            )
            .unwrap(),
        );
        let namespace = transport.provision_namespace(&address, &job).await.unwrap();
        let empty = transport.capture(&namespace).await.unwrap();
        let mut writer = transport.acquire(&namespace).await.unwrap();
        let record =
            encode_checked_genesis(writer.epoch, &address, &definition, &[37; 32], 4096, &job)
                .unwrap();
        let original = transport
            .prepare(&namespace, &writer, &record, "genesis-original", &job)
            .await
            .unwrap();
        assert!(matches!(
            transport.submit(&original).await.unwrap(),
            QueueNativeSubmit::Stored(_)
        ));
        assert_eq!(
            transport.resolve(&original, 1).await.unwrap(),
            QueueNativeResolution::DurableContent
        );
        assert!(
            transport
                .read(&namespace, &empty, 0)
                .await
                .unwrap()
                .is_none()
        );
        let refreshed = transport.refresh(&namespace, &mut writer).await.unwrap();
        assert_eq!(refreshed.head().bounds(), (0, 1));
        assert!(
            transport
                .prepare(&namespace, &writer, &record, "duplicate-genesis", &job)
                .await
                .is_err()
        );
        let cut = transport.capture(&namespace).await.unwrap();
        assert!(matches!(
            transport
                .read_admitted(&namespace, &cut, 0, record.bytes().len() as u64 - 1, &job)
                .await,
            Err(QueueError::TooLarge)
        ));
        let before_read = admission.usage().unwrap();
        assert!(
            transport
                .read_admitted(&namespace, &cut, 1, 256 << 20, &job)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(admission.usage().unwrap(), before_read);
        let tiny = transport
            .read_admitted(&namespace, &cut, 0, 256 << 20, &job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tiny.bytes(), record.bytes());
        drop(tiny);
        assert_eq!(admission.usage().unwrap(), before_read);
        let admitted = transport
            .read_admitted(&namespace, &cut, 0, 4096, &job)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admitted.bytes(), record.bytes());
        let retained = admitted.into_admitted_bytes();
        let retained_alias = retained.clone();
        drop(retained);
        assert_eq!(retained_alias.as_ref(), record.bytes());
        drop(retained_alias);
        let genesis = transport
            .adopt_genesis(&namespace, &cut, &definition, 4096, &job)
            .await
            .unwrap();
        assert_eq!(genesis_secret(&genesis), [37; 32]);
        assert_eq!(genesis_definition(&genesis, &job).raw(), definition.raw());
        assert_eq!(genesis.record.bytes(), record.bytes());
        assert_eq!(cut.head().bounds(), (0, 1));
        transport.shutdown().await.unwrap();
        drop((
            transport, namespace, writer, original, cut, empty, refreshed, genesis,
        ));
        let fresh = QueueNativeLog::new(
            NativeLog::local(
                object_log::LocalBlobStore::new(&root),
                "queue-records".into(),
                limits,
            )
            .unwrap(),
        );
        let namespace = fresh.open(&address, &job).await.unwrap().unwrap();
        let cut = fresh.capture(&namespace).await.unwrap();
        let genesis = fresh
            .adopt_genesis(&namespace, &cut, &definition, 4096, &job)
            .await
            .unwrap();
        assert_eq!(genesis_secret(&genesis), [37; 32]);
        drop(genesis);
        let acquired = fresh.acquire_bundle(namespace, &job).await.unwrap();
        assert_eq!(acquired.head().bounds(), (0, 1));
        assert_eq!(
            acquired
                .read(&fresh, 0, 256 << 20, &job)
                .await
                .unwrap()
                .unwrap()
                .bytes(),
            record.bytes()
        );
        assert!(
            acquired
                .prepare(&fresh, &record, "old-genesis", &job)
                .await
                .is_err()
        );
        fresh.shutdown().await.unwrap();
    }
}
