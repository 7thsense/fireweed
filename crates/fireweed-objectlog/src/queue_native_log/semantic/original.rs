//! Exact original resolution retains its immutable recovered authority.
use super::*;
use std::ops::Range;

/// Replay always supplies the caller's canonical fingerprint. Receipt lookup
/// explicitly omits that comparison; both modes require full source membership.
pub enum QueueNativeOriginalRequest<'a> {
    Replay {
        domain: u8,
        request_id: &'a str,
        fingerprint_format: u8,
        fingerprint: &'a [u8; 32],
    },
    Lookup {
        domain: u8,
        request_id: &'a str,
    },
}
/// Constructed only after exact-cut point evidence and, when present, checked
/// receipt membership. Replacing the capsule requires discarding this result.
pub struct QueueNativeResolvedOriginal<'a> {
    // Retire admitted bytes before the output identity and its guard.
    record: Option<OwnedQueueLogRecord>,
    request_id: String,
    domain: u8,
    offset: Option<u64>,
    fingerprint: Option<(u8, [u8; 32])>,
    outcome: Option<Range<usize>>,
    recovered: &'a QueueNativeRecovered,
    _charge: OwnedQueueBufferCharge,
}
impl QueueNativeResolvedOriginal<'_> {
    pub fn recovered(&self) -> &QueueNativeRecovered {
        self.recovered
    }
    pub fn domain(&self) -> u8 {
        self.domain
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn source_offset(&self) -> Option<u64> {
        self.offset
    }
    pub fn fingerprint(&self) -> Option<(u8, [u8; 32])> {
        self.fingerprint
    }
    /// Historical FWQP outcomes for domains 0/2; domain 1 contains FWCR1
    /// lease entries, not a current claim response or proof leases remain usable.
    pub fn outcome_bytes(&self) -> QueueResult<Option<&[u8]>> {
        match (&self.record, &self.outcome) {
            (None, None) => Ok(None),
            (Some(record), Some(range)) => record
                .bytes()
                .get(range.clone())
                .map(Some)
                .ok_or_else(|| unknown("verified original outcome interval differs")),
            _ => Err(unknown("verified original owner differs")),
        }
    }
}
impl QueueNativeRecovered {
    pub async fn resolve_original<'a>(
        &'a self,
        log: &QueueNativeLog,
        request: QueueNativeOriginalRequest<'_>,
        limits: QueueNativeRecoveryLimits,
        job: &Arc<QueueReservation>,
    ) -> QueueResult<QueueNativeResolvedOriginal<'a>> {
        self.as_source().semantic_scope(log, job)?;
        self.budget().scope(job)?;
        limits.read.validate()?;
        limits.record.validate()?;
        let (domain, id, expected) = match request {
            QueueNativeOriginalRequest::Replay {
                domain,
                request_id,
                fingerprint_format,
                fingerprint,
            } => {
                if !(fingerprint_format == 3 || (domain == 2 && fingerprint_format == 4)) {
                    return Err(QueueError::Invalid(
                        "invalid expected fingerprint format".into(),
                    ));
                }
                (domain, request_id, Some((fingerprint_format, fingerprint)))
            }
            QueueNativeOriginalRequest::Lookup { domain, request_id } => (domain, request_id, None),
        };
        if domain > 2 || id.trim().is_empty() {
            return Err(QueueError::Invalid(
                "invalid original domain or literal ID".into(),
            ));
        }
        let bytes = (id.len() as u64)
            .checked_add(std::mem::size_of::<QueueNativeResolvedOriginal<'_>>() as u64)
            .ok_or(QueueError::TooLarge)?;
        // Separately fund the literal identity that outlives local point evidence.
        let charge = job.owned_buffer(bytes)?;
        let reader = QueueNativeSourceReader::new(
            self.as_source(),
            log,
            job,
            self.budget().clone(),
            limits.read,
        )?;
        let evidence =
            fireweed_engine::queue_source_index::load_source_original(&reader, domain, id, job)
                .await?;
        if let Some((format, fingerprint)) = expected {
            evidence.check_fingerprint(format, fingerprint)?;
        }
        // Copy identity from verified evidence, not from a later caller argument.
        let request_id = evidence.request_id().to_owned();
        let domain = evidence.domain();
        let fingerprint = evidence.fingerprint();
        let offset = evidence.source_offset();
        let (record, outcome) = if let Some(offset) = offset {
            let record = self
                .as_source()
                .read_source_record(log, offset, limits.record, self.budget(), job)
                .await?;
            let range = {
                let checked = record
                    .checked()
                    .map_err(|error| unknown(format!("invalid original record: {error:?}")))?;
                let raw = evidence.verify_record(&checked, offset, self.definition().pin(), job)?;
                let start = raw
                    .as_ptr()
                    .addr()
                    .checked_sub(record.bytes().as_ptr().addr())
                    .ok_or_else(|| unknown("verified original outcome start differs"))?;
                let end = start.checked_add(raw.len()).ok_or(QueueError::TooLarge)?;
                if end > record.bytes().len() {
                    return Err(unknown("verified original outcome end differs"));
                }
                start..end
            };
            (Some(record), Some(range))
        } else {
            (None, None)
        };
        Ok(QueueNativeResolvedOriginal {
            record,
            request_id,
            domain,
            offset,
            fingerprint,
            outcome,
            recovered: self,
            _charge: charge,
        })
    }
}
