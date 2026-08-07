//! Canonical TP-005 cell register: log--projection (exactly 20 for full).

/// Full matrix: 5 logs × 4 projections.
pub const FULL_CELL_IDS: &[&str] = &[
    "memory--memory",
    "memory--sqlite",
    "memory--turso",
    "memory--postgres",
    "sqlite--memory",
    "sqlite--sqlite",
    "sqlite--turso",
    "sqlite--postgres",
    "postgres--memory",
    "postgres--sqlite",
    "postgres--turso",
    "postgres--postgres",
    "filesystem--memory",
    "filesystem--sqlite",
    "filesystem--turso",
    "filesystem--postgres",
    "s3--memory",
    "s3--sqlite",
    "s3--turso",
    "s3--postgres",
];

/// Smoke: local logs × local projections (no live PG/S3 required).
/// 3 logs × 3 projections = 9 cells.
pub const SMOKE_CELL_IDS: &[&str] = &[
    "memory--memory",
    "memory--sqlite",
    "memory--turso",
    "sqlite--memory",
    "sqlite--sqlite",
    "sqlite--turso",
    "filesystem--memory",
    "filesystem--sqlite",
    "filesystem--turso",
];

/// Response-barrier class for baseline Strict qualification (TP-005).
pub fn barrier_class(_cell: &str) -> &'static str {
    "Strict"
}

/// Parse `log--projection` cell id.
pub fn parse_cell(cell: &str) -> Result<(&str, &str), String> {
    let (log, proj) = cell
        .split_once("--")
        .ok_or_else(|| format!("cell id must be log--projection, got {cell:?}"))?;
    match log {
        "memory" | "sqlite" | "postgres" | "filesystem" | "s3" => {}
        _ => return Err(format!("unknown log axis {log:?} in cell {cell:?}")),
    }
    match proj {
        "memory" | "sqlite" | "turso" | "postgres" => {}
        _ => return Err(format!("unknown projection axis {proj:?} in cell {cell:?}")),
    }
    Ok((log, proj))
}

/// Class A durable log (not pure process-local memory log).
pub fn is_durable_log_cell(cell: &str) -> bool {
    parse_cell(cell)
        .map(|(log, _)| log != "memory")
        .unwrap_or(false)
}

/// Disposable projection rebuild: durable object log + non-memory projection.
pub fn is_maintenance_cell(cell: &str) -> bool {
    // Disposable projection rebuild (verify/delete/rebuild) is only available for
    // object-log cells with SQLite or Postgres projections. Memory has no durable
    // projection to rebuild; Turso does not advertise the maintenance control plane.
    parse_cell(cell)
        .map(|(log, proj)| {
            matches!(log, "filesystem" | "s3") && matches!(proj, "sqlite" | "postgres")
        })
        .unwrap_or(false)
}

/// Cells that use async projection barrier (none in baseline Strict matrix).
pub fn is_async_projection_cell(_cell: &str) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_register_is_exactly_twenty_canonical_pairs() {
        assert_eq!(FULL_CELL_IDS.len(), 20);
        let mut seen = std::collections::BTreeSet::new();
        for cell in FULL_CELL_IDS {
            let (log, proj) = parse_cell(cell).expect("parse");
            assert!(seen.insert((log, proj)));
            assert_eq!(barrier_class(cell), "Strict");
        }
        assert_eq!(seen.len(), 20);
    }

    #[test]
    fn smoke_is_nine_local_cells() {
        assert_eq!(SMOKE_CELL_IDS.len(), 9);
        for cell in SMOKE_CELL_IDS {
            let (log, proj) = parse_cell(cell).unwrap();
            assert!(matches!(log, "memory" | "sqlite" | "filesystem"));
            assert!(matches!(proj, "memory" | "sqlite" | "turso"));
        }
    }

    #[test]
    fn maintenance_cells_are_object_log_with_rebuildable_projection() {
        let cells: Vec<_> = FULL_CELL_IDS
            .iter()
            .copied()
            .filter(|c| is_maintenance_cell(c))
            .collect();
        // filesystem|s3 × sqlite|postgres (turso has no projection rebuild control plane)
        assert_eq!(cells.len(), 4);
        for cell in cells {
            let (log, proj) = parse_cell(cell).unwrap();
            assert!(matches!(log, "filesystem" | "s3"));
            assert!(matches!(proj, "sqlite" | "postgres"));
        }
    }
}
