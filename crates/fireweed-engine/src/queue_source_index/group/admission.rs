//! Checked structural scratch bounds for borrowed source-group declarations.
use super::*;

// Borrowed traversal only: no validation/name/identity container is built until
// this checked bound is admitted. Per-address 8KiB covers tree nodes, work/state
// metadata, versions, validation sets and field-guard vector headers. Name bytes
// include 256B per owned name/node, with four simultaneous coverage/read copies
// per addressed work key. Literal identities cover retained copies; borrowed
// request body/state payload lengths do not amplify planning scratch.
pub(super) fn request_scratch(group: &CommitGroup, schema: &QueueSchema) -> QueueResult<u64> {
    let work = group
        .claimed_updates
        .len()
        .checked_add(group.work_updates.len())
        .and_then(|n| n.checked_add(group.work_inserts.len()))
        .ok_or(QueueError::TooLarge)? as u64;
    let records = work
        .checked_add(group.state_updates.len() as u64)
        .ok_or(QueueError::TooLarge)?;
    let mut names = 0u64;
    let mut add_name = |name: &str| -> QueueResult<()> {
        names = names
            .checked_add(name.len() as u64)
            .and_then(|n| n.checked_add(256))
            .ok_or(QueueError::TooLarge)?;
        Ok(())
    };
    for name in schema.work.indexes.values().flatten() {
        add_name(name)?;
    }
    for patch in
        group
            .claimed_updates
            .iter()
            .map(|u| &u.patch)
            .chain(group.work_updates.iter().filter_map(|u| match &u.change {
                UnclaimedChange::Patch(p) | UnclaimedChange::Administrative { patch: p, .. } => {
                    Some(p)
                }
                _ => None,
            }))
    {
        for name in patch.fields.keys() {
            add_name(name)?;
        }
    }
    let identities = group
        .claimed_updates
        .iter()
        .map(|u| (None, u.key.as_str()))
        .chain(group.work_updates.iter().map(|u| (None, u.key.as_str())))
        .chain(group.work_inserts.iter().map(|u| (None, u.key.as_str())))
        .chain(
            group
                .state_updates
                .iter()
                .map(|u| (Some(u.collection.as_str()), u.key.as_str())),
        )
        .try_fold(group.request_id.len() as u64, |n, (collection, key)| {
            n.checked_add(key.len() as u64)
                .and_then(|n| n.checked_add(collection.map_or(0, |v| v.len() as u64)))
                .ok_or(QueueError::TooLarge)
        })?;
    // Schema validation also builds borrowed sets, including state definitions.
    let schema_headers = std::iter::once(&schema.work)
        .chain(schema.state.values())
        .try_fold(0u64, |n, definition| {
            definition.indexes.values().try_fold(
                n.checked_add(
                    (definition.fields.len() as u64)
                        .checked_mul(256)
                        .ok_or(QueueError::TooLarge)?,
                )
                .ok_or(QueueError::TooLarge)?,
                |n, columns| {
                    n.checked_add(
                        (columns.len() as u64)
                            .checked_mul(256)
                            .ok_or(QueueError::TooLarge)?,
                    )
                    .ok_or(QueueError::TooLarge)
                },
            )
        })?;
    identities
        .checked_mul(16)
        .and_then(|n| n.checked_add(schema_headers))
        .and_then(|n| n.checked_add(records.checked_mul(8192)?))
        .and_then(|n| n.checked_add(names.checked_mul(work.checked_add(2)?)?.checked_mul(4)?))
        .and_then(|n| n.checked_add(8192))
        .ok_or(QueueError::TooLarge)
}
