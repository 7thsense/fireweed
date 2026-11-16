//! Exact-cut leaf memberships. None of these syntax checks seals range coverage
//! or authorizes a selected result without its same-cut live record evidence.
use super::{VERSION, literal::Literal};
use crate::{
    queue_group_state_authority::WorkMetadata,
    queue_source_index::{QueueSourceCut, QueueSourcePoint, QueueSourceWorkIndex},
};
use fireweed_core::queue_contract::{QueueError, QueueResult};

/// Full literal identity borrowed from the actual original-bound source point.
/// Retained copies require admission before allocating key/collection storage.
pub struct QueueSourceDecodedIdentity<'a, 'b> {
    collection: Option<&'a str>,
    key: Literal<'b>,
    _point: QueueSourcePoint<'b>,
}
impl<'a, 'b> QueueSourceDecodedIdentity<'a, 'b> {
    pub(super) fn new(
        collection: Option<&'a str>,
        key: Literal<'b>,
        point: QueueSourcePoint<'b>,
    ) -> Self {
        Self {
            collection,
            key,
            _point: point,
        }
    }
    pub fn collection(&self) -> Option<&str> {
        self.collection
    }
    pub fn key_len(&self) -> usize {
        self.key.len()
    }
    pub fn key_bytes(&self) -> impl Iterator<Item = u8> + Clone + '_ {
        self.key.bytes()
    }
}
/// Parse a complete live-key membership in exactly the requested collection.
/// Complete universe coverage still requires its uninterrupted range certificate.
pub fn decode_live_key<'a, 'b>(
    original: &QueueSourceCut<'_>,
    point: QueueSourcePoint<'b>,
    collection: Option<&'a str>,
) -> QueueResult<QueueSourceDecodedIdentity<'a, 'b>> {
    marker(original, &point)?;
    let input = point
        .key()
        .strip_prefix(&[VERSION, 10])
        .ok_or_else(|| unknown("live-key family differs"))?;
    let input = collection_suffix(input, collection)?;
    let key = record_key(input)?;
    Ok(QueueSourceDecodedIdentity::new(collection, key, point))
}

pub struct QueueSourceDecodedWorkPosting<'a> {
    identity: QueueSourceDecodedIdentity<'static, 'a>,
    index: QueueSourceWorkIndex,
    sort: &'a [u8],
}
impl QueueSourceDecodedWorkPosting<'_> {
    pub fn identity(&self) -> &QueueSourceDecodedIdentity<'static, '_> {
        &self.identity
    }
    pub fn index(&self) -> QueueSourceWorkIndex {
        self.index
    }
    pub fn sort(&self) -> &[u8] {
        self.sort
    }
    /// This tests only the static tuple. Selection must obtain the metadata from
    /// the same original-cut full record and apply its one sampled evaluation time.
    pub fn matches_metadata(&self, metadata: WorkMetadata) -> QueueResult<bool> {
        Ok(self
            .index
            .posting(metadata)?
            .is_some_and(|posting| posting.sort() == self.sort))
    }
}
impl QueueSourceWorkIndex {
    pub fn decode<'a>(
        self,
        original: &QueueSourceCut<'_>,
        point: QueueSourcePoint<'a>,
    ) -> QueueResult<QueueSourceDecodedWorkPosting<'a>> {
        marker(original, &point)?;
        let input = point
            .key()
            .strip_prefix(&self.prefix())
            .ok_or_else(|| unknown("intrinsic family/index differs"))?;
        let sort = input
            .get(..self.width())
            .ok_or_else(|| unknown("truncated intrinsic tuple"))?;
        match self {
            Self::RawLifecycle if sort[0] > 3 => {
                return Err(unknown("invalid intrinsic lifecycle"));
            }
            Self::PriorityAscending
            | Self::PriorityDescending
            | Self::Schedule
            | Self::OverduePriorityAscending
            | Self::OverduePriorityDescending
            | Self::OverdueSchedule
                if sort[sort.len() - 8..].iter().all(|byte| *byte == 0) =>
            {
                return Err(unknown("zero intrinsic insertion sequence"));
            }
            _ => (),
        }
        let key = record_key(&input[self.width()..])?;
        Ok(QueueSourceDecodedWorkPosting {
            identity: QueueSourceDecodedIdentity::new(None, key, point),
            index: self,
            sort,
        })
    }
}
pub(super) fn marker(
    original: &QueueSourceCut<'_>,
    point: &QueueSourcePoint<'_>,
) -> QueueResult<()> {
    if point.value_for(original, point.key())? != Some(&[]) {
        return Err(unknown("absent or nonempty posting marker"));
    }
    Ok(())
}
pub(super) fn record_key(input: &[u8]) -> QueueResult<Literal<'_>> {
    let (key, rest) = Literal::parse(input)?;
    key.validate_identifier()?;
    if !rest.is_empty() {
        return Err(unknown("trailing membership components"));
    }
    Ok(key)
}
pub(super) fn collection_suffix<'a>(
    input: &'a [u8],
    expected: Option<&str>,
) -> QueueResult<&'a [u8]> {
    if expected.is_some_and(|name| name.trim().is_empty()) {
        return Err(unknown("blank collection identifier"));
    }
    match (input.split_first(), expected) {
        (Some((&0, rest)), None) => Ok(rest),
        (Some((&1, rest)), Some(name)) => {
            let (actual, rest) = Literal::parse(rest)?;
            if !actual.matches(name.as_bytes()) {
                return Err(unknown("posting collection differs"));
            }
            Ok(rest)
        }
        _ => Err(unknown("posting collection domain differs")),
    }
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source membership: {reason}"))
}
#[cfg(test)]
mod tests;
