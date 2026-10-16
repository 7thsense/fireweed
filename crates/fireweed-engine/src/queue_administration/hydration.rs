//! Shared one-row administrative read proxy and prefix resolution. The storage
//! adapter admits input/field/tuple allocations and retains its covered snapshot.
use super::*;
use crate::queue_planner::RecordSnapshot;

pub fn administrative_hydration_group(key: &str, action: &AdministrativeAction) -> CommitGroup {
    hydration_group(key, action, true)
}
/// Field-only read proxy for prospective unique tuples. Old-body comparison is
/// a separate hydration capability; tuple discovery never clones request bodies.
pub fn administrative_unique_hydration_group(
    key: &str,
    action: &AdministrativeAction,
) -> CommitGroup {
    hydration_group(key, action, false)
}
fn hydration_group(key: &str, action: &AdministrativeAction, include_body: bool) -> CommitGroup {
    CommitGroup {
        administration: None,
        request_id: String::new(),
        claimed_updates: vec![],
        state_updates: vec![],
        work_inserts: vec![],
        work_updates: vec![UnclaimedUpdate {
            key: key.to_owned(),
            expect: StateExpect::Version(1),
            change: match action {
                AdministrativeAction::Delete { .. } => UnclaimedChange::Delete,
                AdministrativeAction::Update {
                    patch,
                    lifecycle,
                    lease_guard,
                    ..
                } => UnclaimedChange::Administrative {
                    patch: WorkPatch {
                        priority: patch.priority,
                        available_at: patch.available_at,
                        fields: patch.fields.clone(),
                        body: if include_body {
                            patch.body.clone()
                        } else {
                            None
                        },
                    },
                    lifecycle: *lifecycle,
                    lease_guard: *lease_guard,
                },
            },
        }],
    }
}

/// Called only after complete one-row authority has been loaded. This prepares
/// unique probes; it does not authorize an update or alter the stored row.
pub fn resolve_administrative_hydration(
    group: &mut CommitGroup,
    action: &AdministrativeAction,
    image: &RecordSnapshot,
    schema: &QueueSchema,
) -> QueueResult<()> {
    let [update] = group.work_updates.as_mut_slice() else {
        return Err(unknown("administrative hydration is not one update"));
    };
    let target = image
        .work
        .get(&update.key)
        .ok_or_else(|| unknown("administrative hydration lacks live authority"))?;
    update.expect = StateExpect::Version(target.record.version);
    if let AdministrativeAction::Update {
        patch,
        remove_field_prefixes,
        ..
    } = action
    {
        let view = CheckedFields::new(&target.record.work.fields, &target.field_coverage)?;
        let resolved = crate::queue_fields::resolve_edits(
            &view,
            &patch.fields,
            remove_field_prefixes,
            &schema.work,
        )?;
        let UnclaimedChange::Administrative { patch, .. } = &mut update.change else {
            return Err(unknown("administrative update lacks update proxy"));
        };
        patch.fields = match resolved {
            ResolvedFields::Preserve => Default::default(),
            ResolvedFields::Edit(edits) => edits,
            ResolvedFields::Replace(_) => {
                return Err(unknown(
                    "administrative prefix unexpectedly replaced fields",
                ));
            }
        };
    }
    Ok(())
}

/// One backend exact-key lookup for each original position, including duplicates
/// and absence. The shared scan independently checks the returned ordinal/key.
pub fn addressed_administrative_source<'s, 'r: 's, F, Fut>(
    entries: &'s [AdministrativeEntry],
    hydrate: F,
) -> impl futures::Stream<Item = QueueResult<AddressedAdministrativeInput<'s, 'r>>> + 's
where
    F: FnMut(usize, &'s AdministrativeEntry) -> Fut + 's,
    Fut: std::future::Future<Output = QueueResult<AddressedAdministrativeInput<'s, 'r>>> + 's,
{
    futures::stream::try_unfold(
        (0usize, hydrate),
        move |(position, mut hydrate)| async move {
            let Some(entry) = entries.get(position) else {
                return Ok(None);
            };
            let row = hydrate(position, entry).await?;
            Ok(Some((row, (position + 1, hydrate))))
        },
    )
}

/// A storage-owned fallible selected-row cursor. Completion remains EOF-only in
/// the shared scanner; this helper adds no filtering or planning policy.
pub fn selected_administrative_source<'s, 'r: 's, S, F, Fut>(
    state: S,
    next: F,
) -> impl futures::Stream<Item = QueueResult<SelectedAdministrativeInput<'s, 'r>>> + 's
where
    S: 's,
    F: FnMut(S) -> Fut + 's,
    Fut: std::future::Future<Output = QueueResult<Option<(SelectedAdministrativeInput<'s, 'r>, S)>>>
        + 's,
{
    futures::stream::try_unfold((state, next), move |(state, mut next)| async move {
        let Some((row, state)) = next(state).await? else {
            return Ok(None);
        };
        Ok(Some((row, (state, next))))
    })
}
