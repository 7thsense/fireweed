//! Fixture, standard schema and request helpers.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fireweed_interface::*;
use futures::executor::block_on;

pub enum Fail {
    Failed(String),
    Skipped(String),
}

pub type T = Result<(), Fail>;

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err($crate::support::Fail::Failed(format!($($fmt)+)));
        }
    };
}

pub fn skip(why: &str) -> T {
    Err(Fail::Skipped(why.to_owned()))
}

pub fn ok<V, E: std::fmt::Debug>(r: Result<V, E>, what: &str) -> Result<V, Fail> {
    r.map_err(|e| Fail::Failed(format!("{what}: {e:?}")))
}

pub const START: i64 = 1_700_000_000_000;

pub struct Fixture {
    pub plane: Arc<dyn DataPlane>,
    pub clock: Arc<ManualClock>,
    seq: AtomicU64,
}

pub fn sp() -> SpaceKey {
    SpaceKey::new("tenant", "space")
}

pub fn f(name: &str) -> FieldName {
    FieldName::new(name)
}

pub fn o(name: &str) -> ObjectName {
    ObjectName::new(name)
}

pub fn text(s: &str) -> Value {
    Value::Text(s.to_owned())
}

/// Percentile thresholds declared as reads (AGG-4).
pub const PCTS: &[u32] = &[1, 70_000, 140_000, 500_000, 550_000, 999_999, 1_000_000];

pub fn base_schema() -> SpaceSchema {
    use ScalarType::*;
    let mut spec = QueueSpec::basic(PriorityKind::Int64);
    spec.gates = Some(GatePolicy {
        max_gate_keys_per_item: 4,
        max_gates_per_request: 16,
    });
    spec.recurrence = true;
    spec.max_attempts = 2;
    // Different retentions, so addressability and key reservation end at
    // different instants (RET-1).
    spec.client_item_key_retention_ms = 120_000;
    spec.terminal_retention_ms = 60_000;
    let work = QueueDecl {
        name: o("work"),
        spec,
        body: BodyKind::Json,
        fields: vec![
            FieldDecl::stored("campaign", Text, true, IndexHint::Lookup),
            FieldDecl::stored("n", Int, true, IndexHint::Range),
            FieldDecl::stored("uniq", Text, true, IndexHint::Unique),
            FieldDecl::stored("tags", TextSet, true, IndexHint::Lookup),
            FieldDecl {
                name: f("ext_id"),
                ty: Text,
                nullable: true,
                source: FieldSource::BodyPointer(JsonPointer::new("/id")),
                index: IndexHint::Unique,
            },
        ],
        history: None,
    };
    let kv = TableDecl {
        name: o("kv"),
        key: vec![f("k")],
        fields: vec![
            FieldDecl::stored("k", Int, false, IndexHint::Unique),
            FieldDecl::stored("b", Bool, true, IndexHint::None),
            FieldDecl::stored("i", Int, true, IndexHint::Range),
            FieldDecl::stored("fl", Float, true, IndexHint::None),
            FieldDecl::stored("s", Text, true, IndexHint::Lookup),
            FieldDecl::stored("by", Bytes, true, IndexHint::None),
            FieldDecl::stored("ts", Timestamp, true, IndexHint::None),
            FieldDecl::stored("j", Json, true, IndexHint::None),
            FieldDecl::stored("set", TextSet, true, IndexHint::None),
            FieldDecl::stored("u", Text, true, IndexHint::Unique),
        ],
        expiry: None,
    };
    let strict = TableDecl {
        name: o("strict"),
        key: vec![f("k")],
        fields: vec![
            FieldDecl::stored("k", Int, false, IndexHint::Unique),
            FieldDecl::stored("req", Text, false, IndexHint::None),
        ],
        expiry: Some(RowExpiry::AfterWrite { ttl_ms: 10_000 }),
    };
    let timed = TableDecl {
        name: o("timed"),
        key: vec![f("k")],
        fields: vec![
            FieldDecl::stored("k", Int, false, IndexHint::Unique),
            FieldDecl::stored("at", Timestamp, true, IndexHint::None),
        ],
        expiry: Some(RowExpiry::AtField(f("at"))),
    };
    let nums = TableDecl {
        name: o("nums"),
        key: vec![f("id")],
        fields: vec![
            FieldDecl::stored("id", Int, false, IndexHint::Unique),
            FieldDecl::stored("g", Text, true, IndexHint::Lookup),
            FieldDecl::stored("v", Int, true, IndexHint::None),
            FieldDecl::stored("x", Float, true, IndexHint::None),
        ],
        expiry: None,
    };
    let by_g = |name: &str,
                values: Vec<(FieldName, Aggregate)>,
                group_by: Vec<(FieldName, Expr)>| ReadDecl {
        name: ReadName::new(name),
        version: 1,
        source: o("nums"),
        params: vec![(ParamName::new("g"), Text)],
        filter: Expr::eq(Expr::field("g"), Expr::param("g")),
        output: ReadOutput::Aggregate { group_by, values },
        order: vec![],
        max_rows: 100,
    };
    let mut reads = vec![
        by_g(
            "stats",
            vec![
                (f("count"), Aggregate::Count),
                (f("count_v"), Aggregate::CountOf(Expr::field("v"))),
                (f("sum_v"), Aggregate::Sum(Expr::field("v"))),
                (f("min_v"), Aggregate::Min(Expr::field("v"))),
                (f("max_v"), Aggregate::Max(Expr::field("v"))),
                (f("avg_v"), Aggregate::Avg(Expr::field("v"))),
                (f("sum_x"), Aggregate::Sum(Expr::field("x"))),
                (f("avg_x"), Aggregate::Avg(Expr::field("x"))),
            ],
            vec![],
        ),
        by_g(
            "grouped",
            vec![(f("count"), Aggregate::Count)],
            vec![(f("v"), Expr::field("v"))],
        ),
        ReadDecl {
            name: ReadName::new("rows_by_s"),
            version: 1,
            source: o("kv"),
            params: vec![(ParamName::new("s"), Text)],
            filter: Expr::eq(Expr::field("s"), Expr::param("s")),
            output: ReadOutput::Rows {
                fields: vec![f("k"), f("i")],
            },
            order: vec![(f("i"), Direction::Asc)],
            max_rows: 2,
        },
        ReadDecl {
            name: ReadName::new("rows_by_s_desc"),
            version: 1,
            source: o("kv"),
            params: vec![(ParamName::new("s"), Text)],
            filter: Expr::eq(Expr::field("s"), Expr::param("s")),
            output: ReadOutput::Rows {
                fields: vec![f("k"), f("i")],
            },
            order: vec![(f("i"), Direction::Desc)],
            max_rows: 10,
        },
        ReadDecl {
            name: ReadName::new("rows_by_i"),
            version: 1,
            source: o("kv"),
            params: vec![(ParamName::new("lo"), Int)],
            filter: Expr::cmp(CmpOp::Ge, Expr::field("i"), Expr::param("lo")),
            output: ReadOutput::Rows {
                fields: vec![f("k"), f("i")],
            },
            order: vec![],
            max_rows: 10,
        },
        ReadDecl {
            name: ReadName::new("rows_sparse"),
            version: 1,
            source: o("kv"),
            params: vec![(ParamName::new("s"), Text)],
            filter: Expr::And(vec![
                Expr::eq(Expr::field("s"), Expr::param("s")),
                Expr::eq(Expr::field("i"), Expr::int(-1)),
            ]),
            output: ReadOutput::Rows {
                fields: vec![f("k")],
            },
            order: vec![],
            max_rows: 10,
        },
    ];
    for p in PCTS {
        reads.push(by_g(
            &format!("pct_{p}"),
            vec![(
                f("p"),
                Aggregate::Percentile {
                    expr: Expr::field("v"),
                    p_millionths: *p,
                },
            )],
            vec![],
        ));
    }
    SpaceSchema {
        version: 1,
        request_id_retention_ms: 600_000,
        queues: vec![work],
        tables: vec![kv, strict, timed, nums],
        event_logs: vec![],
        blobs: None,
        reads,
    }
}

impl Fixture {
    pub fn new(factory: crate::Factory<'_>) -> Result<Self, DataPlaneError> {
        let clock = Arc::new(ManualClock::new(START));
        let plane = factory(clock.clone());
        let fx = Fixture {
            plane,
            clock,
            seq: AtomicU64::new(0),
        };
        block_on(fx.plane.apply_schema(ApplySchemaRequest {
            space: sp(),
            schema: base_schema(),
            expected_version: None,
            min_durability: None,
        }))?;
        Ok(fx)
    }

    pub fn rid(&self) -> String {
        format!("r{}", self.seq.fetch_add(1, Ordering::SeqCst))
    }

    pub fn hdr(&self) -> WriteHeader {
        WriteHeader::new(sp(), self.rid())
    }

    pub fn caps(&self) -> Capabilities {
        self.plane.capabilities()
    }

    pub fn limits(&self) -> ImplementationLimits {
        self.plane.capabilities().limits
    }

    pub fn advance(&self, ms: i64) {
        self.clock.advance(ms);
    }

    pub fn commit_with(
        &self,
        header: WriteHeader,
        ops: Vec<WriteOp>,
        params: Params,
        dry_run: bool,
    ) -> DpResult<CommitResponse> {
        block_on(self.plane.commit(CommitRequest {
            header,
            ops,
            params,
            dry_run,
        }))
    }

    pub fn commit(&self, ops: Vec<WriteOp>) -> DpResult<CommitResponse> {
        self.commit_with(self.hdr(), ops, Params::new(), false)
    }

    pub fn enqueue(&self, items: Vec<NewItem>, policy: KeyConflict) -> DpResult<EnqueueResponse> {
        block_on(self.plane.enqueue(EnqueueRequest {
            header: self.hdr(),
            queue: o("work"),
            items,
            on_key_conflict: policy,
        }))
    }

    pub fn push(&self, key: &str, prio: i64) -> ItemId {
        let r = self
            .enqueue(
                vec![NewItem::new(Some(key), PriorityValue::Int(prio))],
                KeyConflict::Duplicate,
            )
            .expect("enqueue");
        r.entries[0].item_id.expect("item id")
    }

    pub fn claim(&self, max: u32, lease_ms: u64) -> DpResult<ClaimResponse> {
        block_on(
            self.plane
                .claim(ClaimRequest::new(self.hdr(), "work", "w1", max, lease_ms)),
        )
    }

    pub fn claim_req(&self, r: ClaimRequest) -> DpResult<ClaimResponse> {
        block_on(self.plane.claim(r))
    }

    pub fn finalize(&self, entries: Vec<FinalizeEntry>) -> DpResult<FinalizeResponse> {
        block_on(self.plane.finalize(FinalizeRequest {
            header: self.hdr(),
            queue: o("work"),
            entries,
            params: Params::new(),
        }))
    }

    pub fn get_item(&self, id: ItemId, include_terminal: bool) -> Option<ItemView> {
        let r = block_on(self.plane.get(GetRequest {
            space: sp(),
            targets: vec![GetTarget::Item {
                queue: o("work"),
                item: ItemRef::Id(id),
                include_terminal,
            }],
        }))
        .expect("get");
        match r.results.into_iter().next().flatten() {
            Some(GetResult::Item(v)) => Some(v),
            _ => None,
        }
    }

    pub fn get_row(&self, table: &str, k: i64) -> Option<RowView> {
        let r = block_on(self.plane.get(GetRequest {
            space: sp(),
            targets: vec![GetTarget::Row {
                table: o(table),
                key: vec![Value::Int(k)],
            }],
        }))
        .expect("get");
        match r.results.into_iter().next().flatten() {
            Some(GetResult::Row(v)) => Some(v),
            _ => None,
        }
    }

    pub fn put_kv(
        &self,
        k: i64,
        fields: Vec<(&str, Value)>,
        policy: RowConflict,
    ) -> DpResult<CommitResponse> {
        let mut row: BTreeMap<FieldName, Value> =
            fields.into_iter().map(|(n, v)| (f(n), v)).collect();
        row.insert(f("k"), Value::Int(k));
        self.commit(vec![WriteOp::PutRows {
            table: o("kv"),
            rows: vec![row],
            on_conflict: policy,
        }])
    }

    pub fn read(&self, name: &str, params: Vec<(&str, Value)>) -> DpResult<ReadResponse> {
        block_on(
            self.plane.read(ReadRequest {
                space: sp(),
                name: ReadName::new(name),
                version: 1,
                params: params
                    .into_iter()
                    .map(|(k, v)| (ParamName::new(k), v))
                    .collect(),
                page_token: None,
            }),
        )
    }
}

/// The `kv` field holding a value of type `t`.
pub fn kv_field(t: ScalarType) -> Option<&'static str> {
    use ScalarType::*;
    Some(match t {
        Bool => "b",
        Int => "i",
        Float => "fl",
        Text => "s",
        Bytes => "by",
        Timestamp => "ts",
        Json => "j",
        TextSet => "set",
        _ => return None,
    })
}

/// Evaluates `e` in an action position: `Set` into the `kv` field of the
/// expression's type on row 1. `Err` carries the rejection reason.
pub fn eval_action(
    fx: &Fixture,
    e: Expr,
    t: ScalarType,
) -> Result<Result<Value, FailureReason>, Fail> {
    ok(fx.put_kv(1, vec![], RowConflict::Skip), "put row 1")?;
    let field = kv_field(t).ok_or_else(|| Fail::Failed(format!("no kv field for {t:?}")))?;
    let m = Mutate::new(
        "kv",
        Selection::Keys {
            keys: vec![Keyed {
                key: KeyRef::Row(vec![Value::Int(1)]),
                expected_version: None,
            }],
            filter: None,
        },
        Action::Set(vec![(f(field), e)]),
        1,
    );
    let r = ok(fx.commit(vec![WriteOp::Mutate(m)]), "commit Set")?;
    if r.status == CommitStatus::Rejected {
        return Ok(Err(r
            .failure
            .map(|x| x.reason)
            .unwrap_or(FailureReason::ScanLimitExceeded)));
    }
    let row = fx
        .get_row("kv", 1)
        .ok_or_else(|| Fail::Failed("row 1 missing".into()))?;
    Ok(Ok(row
        .fields
        .get(&f(field))
        .cloned()
        .unwrap_or(Value::Null(t))))
}

/// Evaluates `e` in a predicate position: a dry-run `Where` on row 1.
pub fn eval_pred(fx: &Fixture, e: Expr) -> Result<bool, Fail> {
    ok(fx.put_kv(1, vec![], RowConflict::Skip), "put row 1")?;
    let m = Mutate::new(
        "kv",
        Selection::Where {
            predicate: Expr::And(vec![Expr::eq(Expr::field("k"), Expr::int(1)), e]),
        },
        Action::Set(vec![(f("i"), Expr::int(0))]),
        10,
    );
    let r = ok(
        fx.commit_with(fx.hdr(), vec![WriteOp::Mutate(m)], Params::new(), true),
        "dry run",
    )?;
    Ok(r.ops[0].matched == 1)
}

pub fn keys(ks: &[i64]) -> Selection {
    Selection::Keys {
        keys: ks
            .iter()
            .map(|k| Keyed {
                key: KeyRef::Row(vec![Value::Int(*k)]),
                expected_version: None,
            })
            .collect(),
        filter: None,
    }
}

pub fn item_keys(ids: &[ItemId]) -> Selection {
    Selection::Keys {
        keys: ids
            .iter()
            .map(|id| Keyed {
                key: KeyRef::Item(ItemRef::Id(*id)),
                expected_version: None,
            })
            .collect(),
        filter: None,
    }
}

pub fn set_of(xs: &[&str]) -> BTreeSet<String> {
    xs.iter().map(|s| s.to_string()).collect()
}

pub fn lease_of(c: &ClaimedItem) -> LeaseTarget {
    LeaseTarget::Item(LeaseRef {
        item_id: c.item_id,
        lease_token: c.lease_token.clone().expect("token"),
    })
}
