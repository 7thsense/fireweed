//! One positional binding check for owned construction and borrowed replay.
use super::*;
enum Position<'a> {
    Inserted(&'a str, u64),
    Replaced(&'a str, u64),
    Rejected(&'a str),
}
fn validate<'a>(
    request: &InsertRequest,
    count: u64,
    positions: impl IntoIterator<Item = PublicationResult<Position<'a>>>,
) -> PublicationResult<()> {
    if request.items.len() as u64 != count {
        return invalid("insert result cardinality");
    }
    for (item, outcome) in request.items.iter().zip(positions) {
        let key = match outcome? {
            Position::Replaced(_, _) if request.mode == InsertMode::InsertOnly => {
                return invalid("replacement result for insert-only request");
            }
            Position::Inserted(key, version) | Position::Replaced(key, version) => {
                if version == 0 {
                    return invalid("zero insert result version");
                }
                key
            }
            Position::Rejected(key) => key,
        };
        if key != item.key {
            return invalid("insert positional identity");
        }
    }
    Ok(())
}
pub(super) fn validate_owned(
    request: &InsertRequest,
    outcomes: &[InsertOutcome],
) -> PublicationResult<()> {
    validate(
        request,
        outcomes.len() as u64,
        outcomes.iter().map(|outcome| {
            Ok(match outcome {
                InsertOutcome::Inserted { key, version } => Position::Inserted(key, *version),
                InsertOutcome::Replaced { key, version } => Position::Replaced(key, *version),
                InsertOutcome::Rejected { key, .. } => Position::Rejected(key),
            })
        }),
    )
}
/// Structural/request binding only. Callers separately establish the original
/// fingerprint, live request validation and immutable publication authority.
pub fn validate_insert_receipt(
    request: &InsertRequest,
    outcomes: InsertsRef<'_>,
) -> PublicationResult<()> {
    validate(
        request,
        outcomes.len(),
        outcomes.iter().map(|outcome| {
            Ok(match outcome? {
                InsertOutcomeRef::Inserted { key, version } => Position::Inserted(key, version),
                InsertOutcomeRef::Replaced { key, version } => Position::Replaced(key, version),
                InsertOutcomeRef::Rejected { key, .. } => Position::Rejected(key),
            })
        }),
    )
}
