//! FWCR1 logical claim replay entries. Physical IDs never enter this format.
//! Borrowed parsing checks complete structure; native publication additionally
//! binds every entry to one resolved lease transition and its immutable request.
use crate::queue_admission::{OwnedQueueBufferCharge, QueueReservation};
use fireweed_core::queue_contract::{QueueError, QueueResult};
use std::{collections::BTreeSet, sync::Arc};
const MAGIC: &[u8; 5] = b"FWCR1";
fn invalid(message: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("claim receipt: {message}"))
}
pub struct ClaimLease {
    pub key: String,
    pub token: Vec<u8>,
    pub version: u64,
}
pub struct OwnedClaimReceipt {
    bytes: Vec<u8>,
    _charge: OwnedQueueBufferCharge,
}
impl OwnedClaimReceipt {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}
#[derive(Clone, Copy, Debug)]
pub struct ClaimLeaseRef<'a> {
    pub key: &'a str,
    pub token: &'a [u8],
    pub version: u64,
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> QueueResult<&'a [u8]> {
        let bytes = self
            .0
            .get(..len)
            .ok_or_else(|| invalid("truncated entry"))?;
        self.0 = &self.0[len..];
        Ok(bytes)
    }
    fn number(&mut self) -> QueueResult<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| invalid("integer"))?,
        ))
    }
    fn blob(&mut self) -> QueueResult<&'a [u8]> {
        let len = usize::try_from(self.number()?).map_err(|_| invalid("length overflow"))?;
        self.take(len)
    }
    fn entry(&mut self) -> QueueResult<ClaimLeaseRef<'a>> {
        let key = std::str::from_utf8(self.blob()?).map_err(|_| invalid("invalid UTF-8 key"))?;
        let token = self.blob()?;
        let version = self.number()?;
        if key.trim().is_empty() || token.is_empty() || version == 0 {
            return Err(invalid("invalid lease entry"));
        }
        Ok(ClaimLeaseRef {
            key,
            token,
            version,
        })
    }
}
#[derive(Clone, Copy)]
pub struct ClaimReceiptView<'a> {
    count: u64,
    entries: &'a [u8],
}
impl<'a> ClaimReceiptView<'a> {
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Entries preserve native claim priority order. Duplicate key semantics
    /// require admitted validation at the publication boundary.
    pub fn entries(self) -> ClaimReceiptIter<'a> {
        ClaimReceiptIter {
            reader: Reader(self.entries),
            left: self.count,
        }
    }
}
pub struct ClaimReceiptIter<'a> {
    reader: Reader<'a>,
    left: u64,
}
impl<'a> Iterator for ClaimReceiptIter<'a> {
    type Item = QueueResult<ClaimLeaseRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let result = self.reader.entry();
        if result.is_err() {
            self.left = 0;
        }
        Some(result)
    }
}
/// Allocation-free complete structural validation. This establishes neither
/// request identity nor current unexpired ownership of the returned leases.
pub fn view_claim_receipt(raw: &[u8]) -> QueueResult<ClaimReceiptView<'_>> {
    let mut reader = Reader(raw);
    if reader.take(5)? != MAGIC {
        return Err(invalid("unsupported format"));
    }
    let count = reader.number()?;
    // Every nonempty key/token entry needs at least 26 bytes.
    if count > u64::try_from(reader.0.len() / 26).map_err(|_| invalid("entry count overflow"))? {
        return Err(invalid("impossible entry count"));
    }
    let entries = reader.0;
    for _ in 0..count {
        reader.entry()?;
    }
    if !reader.0.is_empty() {
        return Err(invalid("trailing bytes"));
    }
    Ok(ClaimReceiptView { count, entries })
}
fn put_blob(out: &mut Vec<u8>, bytes: &[u8]) -> QueueResult<()> {
    out.extend_from_slice(
        &u64::try_from(bytes.len())
            .map_err(|_| QueueError::TooLarge)?
            .to_le_bytes(),
    );
    out.extend_from_slice(bytes);
    Ok(())
}
/// Admit the complete output and duplicate-key check before allocation/copy.
/// No entry count/key length limit is introduced beyond the supplied byte bound.
pub fn encode_claim_receipt(
    entries: &[ClaimLease],
    max_bytes: u64,
    job: &Arc<QueueReservation>,
) -> QueueResult<OwnedClaimReceipt> {
    let size = entries.iter().try_fold(13u64, |size, entry| {
        size.checked_add(u64::try_from(entry.key.len()).map_err(|_| QueueError::TooLarge)?)
            .and_then(|n| n.checked_add(entry.token.len() as u64))
            .and_then(|n| n.checked_add(24))
            .ok_or(QueueError::TooLarge)
    })?;
    if size > max_bytes {
        return Err(QueueError::TooLarge);
    }
    let charge = job.owned_buffer(
        size.checked_mul(4)
            .and_then(|n| n.checked_add((entries.len() as u64).checked_mul(128)?))
            .and_then(|n| n.checked_add(4096))
            .ok_or(QueueError::TooLarge)?,
    )?;
    let mut keys = BTreeSet::new();
    for entry in entries {
        if entry.key.trim().is_empty()
            || entry.token.is_empty()
            || entry.version == 0
            || !keys.insert(entry.key.as_str())
        {
            return Err(invalid("invalid or duplicate lease entry"));
        }
    }
    // Validation scratch must be gone before its shared charge moves into an
    // output owner whose fallible allocation/writes could otherwise drop first.
    drop(keys);
    let mut out = OwnedClaimReceipt {
        bytes: Vec::new(),
        _charge: charge,
    };
    out.bytes
        .try_reserve_exact(usize::try_from(size).map_err(|_| QueueError::TooLarge)?)
        .map_err(|_| QueueError::TooLarge)?;
    out.bytes.extend_from_slice(MAGIC);
    out.bytes
        .extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        put_blob(&mut out.bytes, entry.key.as_bytes())?;
        put_blob(&mut out.bytes, &entry.token)?;
        out.bytes.extend_from_slice(&entry.version.to_le_bytes());
    }
    view_claim_receipt(out.bytes())?;
    Ok(out)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_admission::QueueAdmission;
    #[test]
    fn literal_order_unsigned_versions_and_exact_bounds() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let entries = [
            ClaimLease {
                key: "z".into(),
                token: vec![0, 255],
                version: u64::MAX,
            },
            ClaimLease {
                key: "a\0β".into(),
                token: vec![9],
                version: 1,
            },
        ];
        let receipt = encode_claim_receipt(&entries, 128, &job).unwrap();
        let expected = [
            b"FWCR1".as_slice(),
            &2u64.to_le_bytes(),
            &1u64.to_le_bytes(),
            b"z",
            &2u64.to_le_bytes(),
            &[0, 255],
            &u64::MAX.to_le_bytes(),
            &4u64.to_le_bytes(),
            b"a\0\xce\xb2",
            &1u64.to_le_bytes(),
            &[9],
            &1u64.to_le_bytes(),
        ]
        .concat();
        assert_eq!(receipt.bytes(), expected);
        let parsed = view_claim_receipt(receipt.bytes()).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed
                .entries()
                .map(|entry| entry.unwrap().key)
                .collect::<Vec<_>>(),
            ["z", "a\0β"]
        );
        let n = receipt.bytes().len() as u64;
        assert!(encode_claim_receipt(&entries, n, &job).is_ok());
        assert!(matches!(
            encode_claim_receipt(&entries, n - 1, &job),
            Err(QueueError::TooLarge)
        ));
        assert!(view_claim_receipt(&expected[..expected.len() - 1]).is_err());
        let mut trailing = expected;
        trailing.push(0);
        assert!(view_claim_receipt(&trailing).is_err());
        drop(receipt);
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
    #[test]
    fn empty_replay_and_malformed_entries_fail_without_truncation() {
        let admission = QueueAdmission::new(1, 1 << 20).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let receipt = encode_claim_receipt(&[], 13, &job).unwrap();
        assert_eq!(
            receipt.bytes(),
            [b"FWCR1".as_slice(), &0u64.to_le_bytes()].concat()
        );
        assert!(view_claim_receipt(receipt.bytes()).unwrap().is_empty());
        let duplicate = [
            ClaimLease {
                key: "a".into(),
                token: vec![1],
                version: 1,
            },
            ClaimLease {
                key: "a".into(),
                token: vec![2],
                version: 2,
            },
        ];
        assert!(encode_claim_receipt(&duplicate, 1024, &job).is_err());
        for entry in [
            ClaimLease {
                key: " ".into(),
                token: vec![1],
                version: 1,
            },
            ClaimLease {
                key: "a".into(),
                token: vec![],
                version: 1,
            },
            ClaimLease {
                key: "a".into(),
                token: vec![1],
                version: 0,
            },
        ] {
            assert!(encode_claim_receipt(&[entry], 1024, &job).is_err());
        }
        let mut impossible = receipt.bytes().to_vec();
        impossible[5..13].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(view_claim_receipt(&impossible).is_err());
    }
    #[test]
    fn raw_parser_rejects_every_truncation_and_malformed_lengths_with_tight_admission() {
        let admission = QueueAdmission::new(1, 4380).unwrap();
        let job = Arc::new(admission.reserve(0).unwrap());
        let entries = [ClaimLease {
            key: "a".into(),
            token: vec![1],
            version: 1,
        }];
        let receipt = encode_claim_receipt(&entries, 39, &job).unwrap();
        assert_eq!(receipt.bytes().len(), 39);
        let baseline = admission.usage().unwrap();
        assert_eq!(baseline, (1, 4380));
        for end in 0..receipt.bytes().len() {
            assert!(view_claim_receipt(&receipt.bytes()[..end]).is_err());
        }
        for variant in 0..5 {
            let mut raw = receipt.bytes().to_vec();
            match variant {
                0 => raw[21] = 255,
                1 => raw[31..39].fill(0),
                2 => raw[22..30].fill(0),
                3 => raw[13..21].copy_from_slice(&u64::MAX.to_le_bytes()),
                _ => raw[22..30].copy_from_slice(&u64::MAX.to_le_bytes()),
            }
            assert!(view_claim_receipt(&raw).is_err());
        }
        assert!(encode_claim_receipt(&entries, 39, &job).is_err());
        assert_eq!(admission.usage().unwrap(), baseline);
        drop(receipt);
        assert_eq!(admission.usage().unwrap(), (1, 0));
        let invalid = [ClaimLease {
            key: "a".into(),
            token: vec![],
            version: 1,
        }];
        assert!(encode_claim_receipt(&invalid, 39, &job).is_err());
        assert_eq!(admission.usage().unwrap(), (1, 0));
        drop(job);
        assert_eq!(admission.usage().unwrap(), (0, 0));
    }
}
