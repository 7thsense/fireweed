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
///
/// Fields are public for inspection and under-claiming, but **declaring** claims
/// for a cell (CI evidence, suite registration) MUST go through
/// [`validate_claims_for_cell`] or [`register_suite_claims`]. Those APIs enforce
/// the Class B hard rule: memory log never claims [`Self::durable_log_replay`].
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

impl CellConformanceClaims {
    /// Empty claim set (no suites asserted). Always legal for any cell.
    pub const fn none() -> Self {
        Self {
            core: false,
            durable_log_replay: false,
            projection_reopen: false,
            relational_reconnect: false,
            eventual_apply: false,
            in_process_log_read: false,
        }
    }

    /// True when every flag set in `self` is also set in `allowed` (under-claiming OK).
    pub const fn is_subset_of(self, allowed: Self) -> bool {
        (!self.core || allowed.core)
            && (!self.durable_log_replay || allowed.durable_log_replay)
            && (!self.projection_reopen || allowed.projection_reopen)
            && (!self.relational_reconnect || allowed.relational_reconnect)
            && (!self.eventual_apply || allowed.eventual_apply)
            && (!self.in_process_log_read || allowed.in_process_log_read)
    }
}

/// Why a claimed suite set is illegal for a matrix cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IllegalConformanceClaim {
    /// Cell id (`"memory×sqlite"`).
    pub cell_id: String,
    /// Product class of that cell.
    pub product_class: ProductDurabilityClass,
    /// Capability flag that was illegally asserted.
    pub flag: &'static str,
    /// Human-readable reason (stable for tests / CI messages).
    pub reason: &'static str,
}

impl std::fmt::Display for IllegalConformanceClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "illegal conformance claim on {} ({}): {} — {}",
            self.cell_id,
            self.product_class.as_str(),
            self.flag,
            self.reason
        )
    }
}

impl std::error::Error for IllegalConformanceClaim {}

/// Validated suite-registration record for one matrix cell.
///
/// Only constructible via [`register_suite_claims`], so Class B cannot carry
/// `durable_log_replay` through any registration path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegisteredSuiteClaims {
    cell: MatrixCell,
    claims: CellConformanceClaims,
}

impl RegisteredSuiteClaims {
    pub const fn cell(self) -> MatrixCell {
        self.cell
    }

    pub const fn claims(self) -> CellConformanceClaims {
        self.claims
    }

    pub const fn product_durability_class(self) -> ProductDurabilityClass {
        self.cell.product_durability_class()
    }
}

/// Validate that `claims` does not assert any capability the cell may not claim.
///
/// **Hard rule (storage-matrix-completion-brief §1.2 / conformance-classes §2.2):**
/// Class B (`log=memory`) must never claim `durable_log_replay`.
///
/// Under-claiming is allowed (register a subset of the cell's max allowed set).
pub fn validate_claims_for_cell(
    cell: MatrixCell,
    claims: &CellConformanceClaims,
) -> Result<(), IllegalConformanceClaim> {
    // Explicit Class B hard rule first — clearest diagnostic for the product ban.
    if cell.product_durability_class().is_class_b() && claims.durable_log_replay {
        return Err(IllegalConformanceClaim {
            cell_id: cell.id(),
            product_class: ProductDurabilityClass::ClassB,
            flag: "durable_log_replay",
            reason: "Class B (memory log) must not claim durable_log_replay; after process death only the projection remains",
        });
    }

    let allowed = cell.claims();
    if claims.is_subset_of(allowed) {
        return Ok(());
    }

    // Name the first over-claimed flag for a precise error (order matches struct fields).
    let (flag, reason) = if claims.core && !allowed.core {
        ("core", "cell does not allow core claim")
    } else if claims.durable_log_replay && !allowed.durable_log_replay {
        (
            "durable_log_replay",
            "cell does not allow durable_log_replay",
        )
    } else if claims.projection_reopen && !allowed.projection_reopen {
        (
            "projection_reopen",
            "cell does not allow projection_reopen (projection is not durable)",
        )
    } else if claims.relational_reconnect && !allowed.relational_reconnect {
        (
            "relational_reconnect",
            "cell does not allow relational_reconnect",
        )
    } else if claims.eventual_apply && !allowed.eventual_apply {
        (
            "eventual_apply",
            "cell does not allow eventual_apply (not a Class A object-log peer)",
        )
    } else if claims.in_process_log_read && !allowed.in_process_log_read {
        (
            "in_process_log_read",
            "cell does not allow in_process_log_read",
        )
    } else {
        ("unknown", "claims are not a subset of the cell allow-list")
    };

    Err(IllegalConformanceClaim {
        cell_id: cell.id(),
        product_class: cell.product_durability_class(),
        flag,
        reason,
    })
}

/// Register suite capability claims for a public matrix cell.
///
/// This is the only supported registration entry point: it refuses any set that
/// [`validate_claims_for_cell`] rejects, including the Class B
/// `durable_log_replay` ban. Adapters and CI evidence builders should call this
/// (or the validator) rather than treating raw [`CellConformanceClaims`] as trusted.
pub fn register_suite_claims(
    cell: MatrixCell,
    claims: CellConformanceClaims,
) -> Result<RegisteredSuiteClaims, IllegalConformanceClaim> {
    validate_claims_for_cell(cell, &claims)?;
    Ok(RegisteredSuiteClaims { cell, claims })
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

    /// Maximum capability claims this cell is allowed to make.
    ///
    /// **Hard rule:** Class B never includes `durable_log_replay`.
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

    /// Abusive construction: raw claims with durable_log_replay must not register on Class B.
    #[test]
    fn register_suite_claims_rejects_class_b_durable_log_replay() {
        for projection in MatrixProjection::ALL {
            let cell = MatrixCell::new(MatrixLog::Memory, projection);
            let mut abused = CellConformanceClaims::none();
            abused.core = true;
            abused.durable_log_replay = true; // illegal for memory log

            let err = register_suite_claims(cell, abused)
                .expect_err("memory log must not register durable_log_replay");
            assert_eq!(err.flag, "durable_log_replay");
            assert_eq!(err.product_class, ProductDurabilityClass::ClassB);
            assert!(
                err.reason.contains("Class B") || err.reason.contains("memory log"),
                "reason should name Class B / memory log: {}",
                err.reason
            );

            let validate_err = validate_claims_for_cell(cell, &abused)
                .expect_err("validator must agree with registration");
            assert_eq!(validate_err.flag, "durable_log_replay");
        }
    }

    /// Even when other Class B flags are correctly set, durable_log_replay remains banned.
    #[test]
    fn register_suite_claims_rejects_class_b_mixed_with_projection_reopen() {
        let cell = MatrixCell::new(MatrixLog::Memory, MatrixProjection::Sqlite);
        let mut claims = cell.claims();
        assert!(!claims.durable_log_replay);
        claims.durable_log_replay = true; // flip the hard-rule bit

        let err = register_suite_claims(cell, claims).unwrap_err();
        assert_eq!(err.flag, "durable_log_replay");
        assert_eq!(err.cell_id, "memory×sqlite");
    }

    #[test]
    fn register_suite_claims_accepts_canonical_allow_list_for_every_cell() {
        for cell in all_matrix_cells() {
            let registered = register_suite_claims(cell, cell.claims()).unwrap_or_else(|e| {
                panic!("canonical claims must register for {}: {e}", cell.id())
            });
            assert_eq!(registered.cell(), cell);
            assert_eq!(registered.claims(), cell.claims());
            if cell.product_durability_class().is_class_b() {
                assert!(!registered.claims().durable_log_replay);
            }
        }
    }

    #[test]
    fn register_suite_claims_allows_under_claiming() {
        let cell = MatrixCell::new(MatrixLog::Sqlite, MatrixProjection::Sqlite);
        let core_only = CellConformanceClaims {
            core: true,
            durable_log_replay: false,
            projection_reopen: false,
            relational_reconnect: false,
            eventual_apply: false,
            in_process_log_read: false,
        };
        let registered = register_suite_claims(cell, core_only).expect("under-claim ok");
        assert!(registered.claims().core);
        assert!(!registered.claims().durable_log_replay);
    }

    #[test]
    fn register_suite_claims_accepts_class_a_durable_log_replay() {
        let cell = MatrixCell::new(MatrixLog::Filesystem, MatrixProjection::Memory);
        let claims = CellConformanceClaims {
            core: true,
            durable_log_replay: true,
            projection_reopen: false,
            relational_reconnect: false,
            eventual_apply: false,
            in_process_log_read: true,
        };
        let registered = register_suite_claims(cell, claims).expect("Class A may claim log replay");
        assert!(registered.claims().durable_log_replay);
        assert_eq!(
            registered.product_durability_class(),
            ProductDurabilityClass::ClassA
        );
    }

    #[test]
    fn register_suite_claims_rejects_eventual_apply_on_memory_log() {
        let cell = MatrixCell::new(MatrixLog::Memory, MatrixProjection::Memory);
        let mut claims = CellConformanceClaims::none();
        claims.eventual_apply = true;
        let err = register_suite_claims(cell, claims).unwrap_err();
        assert_eq!(err.flag, "eventual_apply");
    }

    #[test]
    fn every_class_b_cell_canonical_claims_pass_validator() {
        for cell in all_matrix_cells() {
            if cell.product_durability_class().is_class_b() {
                validate_claims_for_cell(cell, &cell.claims()).unwrap_or_else(|e| {
                    panic!(
                        "Class B allow-list must be self-consistent for {}: {e}",
                        cell.id()
                    )
                });
                assert!(!cell.claims().durable_log_replay);
            }
        }
    }
}
