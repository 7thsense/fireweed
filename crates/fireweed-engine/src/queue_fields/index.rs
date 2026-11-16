//! Shared sparse tuple policy for owned plans and checked source compilation.
use crate::{queue_index::encode_values, queue_publication::ValueRef};
use fireweed_core::queue_contract::QueueResult;

pub(crate) fn index_is_affected(
    columns: &[String],
    all: bool,
    explicitly_changed: impl Fn(&str) -> bool,
) -> bool {
    all || columns.iter().any(|name| explicitly_changed(name))
}

/// A live tuple always has a query key, including missing components. Only a
/// complete tuple can own a declared unique index. Record existence remains an
/// enclosing planner/compiler decision and is never inferred from field count.
pub(crate) struct EncodedIndexTuple {
    pub sort: Vec<u8>,
    pub complete: bool,
}
pub(crate) fn encode_index_tuple<'a>(
    values: impl Iterator<Item = Option<ValueRef<'a>>> + Clone,
) -> QueueResult<EncodedIndexTuple> {
    let complete = values.clone().all(|value| value.is_some());
    Ok(EncodedIndexTuple {
        sort: encode_values(values)?,
        complete,
    })
}
