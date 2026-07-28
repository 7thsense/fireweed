//! Product storage-matrix durability classes (Class A / Class B) and the
//! conformance **capability claims** each public 5×3 cell may make.
//!
//! Normative product law:
//! - [orthogonal-storage-matrix-brief](../../../../docs/helix/02-design/orthogonal-storage-matrix-brief.md)
//! - [storage-matrix-conformance-classes](../../../../docs/helix/04-build/storage-matrix-conformance-classes.md)
//!
//! This module is the checked-in map adapters and CI docs should agree with. It
//! does **not** drive which macros an adapter expands — it documents and
//! unit-tests the **claims** so Class B never falsely advertises durable
//! log-replay.

/// Public log axis values (product matrix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatrixLog {
    Memory,
    Sqlite,
    Postgres,
    Filesystem,
    S3,
}

impl MatrixLog {
    /// All five public log backends, in matrix-brief order.
    pub const ALL: [MatrixLog; 5] = [
        MatrixLog::Memory,
        MatrixLog::Sqlite,
        MatrixLog::Postgres,
        MatrixLog::Filesystem,
        MatrixLog::S3,
    ];

    /// Product name used in docs and env examples.
    pub const fn as_str(self) -> &'static str {
        match self {
            MatrixLog::Memory => "memory",
            MatrixLog::Sqlite => "sqlite",
            MatrixLog::Postgres => "postgres",
            MatrixLog::Filesystem => "filesystem",
            MatrixLog::S3 => "s3",
        }
    }

    /// Whether this log is a durable system of record after process death.
    pub const fn is_durable(self) -> bool {
        !matches!(self, MatrixLog::Memory)
    }
}

/// Public projection axis values (product matrix).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MatrixProjection {
    Memory,
    Sqlite,
    Postgres,
}

impl MatrixProjection {
    /// All three public projections, in matrix-brief order.
    pub const ALL: [MatrixProjection; 3] = [
        MatrixProjection::Memory,
        MatrixProjection::Sqlite,
        MatrixProjection::Postgres,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            MatrixProjection::Memory => "memory",
            MatrixProjection::Sqlite => "sqlite",
            MatrixProjection::Postgres => "postgres",
        }
    }

    /// Whether the projection can retain acknowledged state across process death.
    pub const fn is_durable(self) -> bool {
        !matches!(self, MatrixProjection::Memory)
    }
}

/// Product durability class for a matrix cell (matrix brief §2.4).
///
/// Independent of engine [`fireweed_engine::DurabilityClass`] (`Atomic` vs
/// `EventualApply`), which describes commit visibility, not log durability
/// after process death.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProductDurabilityClass {
    /// Class A — durable log (`sqlite`, `postgres`, `filesystem`, `s3`).
    ClassA,
    /// Class B — memory log; after process death only the projection remains.
    ClassB,
}

impl ProductDurabilityClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            ProductDurabilityClass::ClassA => "Class A",
            ProductDurabilityClass::ClassB => "Class B",
        }
    }

    /// Class A: durable log is system of record.
    pub const fn is_class_a(self) -> bool {
        matches!(self, ProductDurabilityClass::ClassA)
    }

    /// Class B: memory log; no durable log-rebuild claims.
    pub const fn is_class_b(self) -> bool {
        matches!(self, ProductDurabilityClass::ClassB)
    }
}

/// Conformance capability claims a matrix cell may make in evidence.
///
/// See `docs/helix/04-build/storage-matrix-conformance-classes.md` §2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CellConformanceClaims {
    /// Substrate-independent core suite — every cell.
    pub core: bool,
    /// Durable log-replay after process death / reopen of the same log substrate.
    ///
    /// **Class B cells must always have this false.**
    pub durable_log_replay: bool,
    /// Projection-only reopen after process death (durable projection).
    pub projection_reopen: bool,
    /// Relational reconnect suite (`durable_reconnect_suite!`).
    pub relational_reconnect: bool,
    /// Engine eventual-apply visibility model (object-log group-commit path).
    pub eventual_apply: bool,
    /// In-process `LogRead` exercises while the process lives.
    ///
    /// On Class B this is **not** durable log-replay and must not be reported as such.
    pub in_process_log_read: bool,
}

/// One public 5×3 matrix cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MatrixCell {
    pub log: MatrixLog,
    pub projection: MatrixProjection,
}

impl MatrixCell {
    pub const fn new(log: MatrixLog, projection: MatrixProjection) -> Self {
        Self { log, projection }
    }

    /// Product durability class: Class B iff `log == memory`.
    pub const fn product_durability_class(self) -> ProductDurabilityClass {
        if self.log.is_durable() {
            ProductDurabilityClass::ClassA
        } else {
            ProductDurabilityClass::ClassB
        }
    }

    /// Capability claims this cell is allowed to make.
    pub const fn claims(self) -> CellConformanceClaims {
        let class_a = self.product_durability_class().is_class_a();
        let durable_proj = self.projection.is_durable();
        let object_log = matches!(self.log, MatrixLog::Filesystem | MatrixLog::S3);

        CellConformanceClaims {
            core: true,
            // Hard rule: Class B never claims durable log-replay.
            durable_log_replay: class_a,
            projection_reopen: durable_proj,
            relational_reconnect: durable_proj,
            // Object-log peers may use eventual-apply compositions.
            eventual_apply: class_a && object_log,
            // In-process LogRead is available whenever a log exists in-process.
            // For Class B this is live-process only — never a product recovery claim.
            in_process_log_read: true,
        }
    }

    /// Human-readable cell id (`"memory×sqlite"`).
    pub fn id(self) -> String {
        format!("{}×{}", self.log.as_str(), self.projection.as_str())
    }
}

/// All 15 public matrix cells (row-major: log outer, projection inner).
pub fn all_matrix_cells() -> [MatrixCell; 15] {
    let mut cells = [MatrixCell::new(MatrixLog::Memory, MatrixProjection::Memory); 15];
    let mut i = 0;
    for log in MatrixLog::ALL {
        for projection in MatrixProjection::ALL {
            cells[i] = MatrixCell::new(log, projection);
            i += 1;
        }
    }
    debug_assert_eq!(i, 15);
    cells
}

/// Product class for a log name (unknown names return `None`).
pub fn product_class_for_log_name(log: &str) -> Option<ProductDurabilityClass> {
    match log {
        "memory" => Some(ProductDurabilityClass::ClassB),
        "sqlite" | "postgres" | "filesystem" | "s3" => Some(ProductDurabilityClass::ClassA),
        // Legacy env alias maps to Class A object-log (filesystem or s3).
        "objectlog" => Some(ProductDurabilityClass::ClassA),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_has_exactly_fifteen_cells() {
        assert_eq!(all_matrix_cells().len(), 15);
        let mut seen = std::collections::BTreeSet::new();
        for cell in all_matrix_cells() {
            assert!(seen.insert((cell.log.as_str(), cell.projection.as_str())));
        }
        assert_eq!(seen.len(), 15);
    }

    #[test]
    fn memory_log_is_class_b_for_every_projection() {
        for projection in MatrixProjection::ALL {
            let cell = MatrixCell::new(MatrixLog::Memory, projection);
            assert_eq!(
                cell.product_durability_class(),
                ProductDurabilityClass::ClassB,
                "memory × {} must be Class B",
                projection.as_str()
            );
        }
    }

    #[test]
    fn durable_logs_are_class_a_for_every_projection() {
        for log in [
            MatrixLog::Sqlite,
            MatrixLog::Postgres,
            MatrixLog::Filesystem,
            MatrixLog::S3,
        ] {
            for projection in MatrixProjection::ALL {
                let cell = MatrixCell::new(log, projection);
                assert_eq!(
                    cell.product_durability_class(),
                    ProductDurabilityClass::ClassA,
                    "{} × {} must be Class A",
                    log.as_str(),
                    projection.as_str()
                );
            }
        }
    }

    #[test]
    fn class_b_never_claims_durable_log_replay() {
        for cell in all_matrix_cells() {
            if cell.product_durability_class().is_class_b() {
                let claims = cell.claims();
                assert!(
                    !claims.durable_log_replay,
                    "Class B cell {} must not claim durable_log_replay (no log-replay for memory log)",
                    cell.id()
                );
            }
        }
    }

    #[test]
    fn class_a_always_claims_durable_log_replay() {
        for cell in all_matrix_cells() {
            if cell.product_durability_class().is_class_a() {
                assert!(
                    cell.claims().durable_log_replay,
                    "Class A cell {} must claim durable_log_replay",
                    cell.id()
                );
            }
        }
    }

    #[test]
    fn class_b_with_durable_projection_claims_projection_reopen_only() {
        for projection in [MatrixProjection::Sqlite, MatrixProjection::Postgres] {
            let cell = MatrixCell::new(MatrixLog::Memory, projection);
            let claims = cell.claims();
            assert!(!claims.durable_log_replay);
            assert!(claims.projection_reopen);
            assert!(claims.relational_reconnect);
            assert!(claims.core);
            // Live-process LogRead is fine; product recovery path is projection-only.
            assert!(claims.in_process_log_read);
        }
    }

    #[test]
    fn class_b_memory_memory_has_no_cross_restart_claims() {
        let cell = MatrixCell::new(MatrixLog::Memory, MatrixProjection::Memory);
        assert_eq!(
            cell.product_durability_class(),
            ProductDurabilityClass::ClassB
        );
        let claims = cell.claims();
        assert!(claims.core);
        assert!(!claims.durable_log_replay);
        assert!(!claims.projection_reopen);
        assert!(!claims.relational_reconnect);
        assert!(!claims.eventual_apply);
    }

    #[test]
    fn filesystem_and_s3_are_class_a_object_log_peers() {
        for log in [MatrixLog::Filesystem, MatrixLog::S3] {
            for projection in MatrixProjection::ALL {
                let cell = MatrixCell::new(log, projection);
                assert!(cell.product_durability_class().is_class_a());
                assert!(cell.claims().durable_log_replay);
                assert!(cell.claims().eventual_apply);
            }
        }
    }

    #[test]
    fn product_class_for_log_name_covers_public_axis_and_objectlog_alias() {
        assert_eq!(
            product_class_for_log_name("memory"),
            Some(ProductDurabilityClass::ClassB)
        );
        for name in ["sqlite", "postgres", "filesystem", "s3", "objectlog"] {
            assert_eq!(
                product_class_for_log_name(name),
                Some(ProductDurabilityClass::ClassA),
                "{name}"
            );
        }
        assert_eq!(product_class_for_log_name("hybrid"), None);
    }

    #[test]
    fn durability_class_labels_match_docs() {
        assert_eq!(ProductDurabilityClass::ClassA.as_str(), "Class A");
        assert_eq!(ProductDurabilityClass::ClassB.as_str(), "Class B");
        assert_eq!(MatrixLog::Filesystem.as_str(), "filesystem");
    }
}
