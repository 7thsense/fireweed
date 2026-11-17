//! Lowering resolved operands, predicates, assignments and patches to SQL
//! fragments with bound parameters. Every value is bound as text and cast in
//! SQL; names are validated DSL names.

use fireweed_dsl::check::{CmpOp, Column, Expr, Kind, Obj as DslObj, Operand, RPatch, RPred};
use fireweed_dsl::{FieldType, MaxAttempts, MergeRule};
use serde_json::Value;

/// A statement being built: SQL text and its parameters.
#[derive(Default)]
pub struct Q {
    pub sql: String,
    pub params: Vec<Option<String>>,
}

impl Q {
    /// Binds a text parameter and returns its placeholder.
    pub fn bind(&mut self, v: Option<String>) -> String {
        self.params.push(v);
        format!("(${}::text)", self.params.len())
    }

    pub fn bind_i64(&mut self, n: i64) -> String {
        format!("{}::bigint", self.bind(Some(n.to_string())))
    }

    /// A JSON value bound as `jsonb`.
    pub fn bind_json(&mut self, v: &Value) -> String {
        format!("{}::jsonb", self.bind(Some(v.to_string())))
    }

    /// A literal of a DSL type.
    pub fn literal(&mut self, v: &Value, ty: FieldType) -> String {
        if v.is_null() {
            return format!("NULL::{}", cast(ty));
        }
        match ty {
            FieldType::Json => self.bind_json(v),
            FieldType::TextSet => format!("fireweed.text_set({})", self.bind_json(v)),
            FieldType::Text | FieldType::Timestamp => {
                let p = self.bind(v.as_str().map(str::to_owned));
                format!("{p}::{}", cast(ty))
            }
            _ => {
                let p = self.bind(Some(v.to_string()));
                format!("{p}::{}", cast(ty))
            }
        }
    }

    pub fn path(&mut self, p: &[String]) -> String {
        let parts: Vec<String> = p.iter().map(|s| self.bind(Some(s.clone()))).collect();
        format!("ARRAY[{}]::text[]", parts.join(", "))
    }

    /// Integers bound as one `bigint[]` parameter.
    pub fn bind_ids(&mut self, ids: &[i64]) -> String {
        let list: Vec<String> = ids.iter().map(i64::to_string).collect();
        format!(
            "{}::bigint[]",
            self.bind(Some(format!("{{{}}}", list.join(","))))
        )
    }

    pub fn duration(&mut self, ms: i64) -> String {
        format!("({} * interval '1 millisecond')", self.bind_i64(ms))
    }
}

pub fn cast(t: FieldType) -> &'static str {
    match t {
        FieldType::Text => "text",
        FieldType::Int => "bigint",
        FieldType::Float => "double precision",
        FieldType::Bool => "boolean",
        FieldType::Timestamp => "timestamptz",
        FieldType::Json => "jsonb",
        FieldType::TextSet => "text[]",
    }
}

/// The object being lowered. A statement's own object is aliased `o`; a
/// lookup into another object of the space gets its own alias.
pub struct Obj {
    /// The qualified table name.
    pub table: String,
    pub kind: Kind,
    pub max_attempts: MaxAttempts,
    /// A table's key fields.
    pub key: Vec<String>,
    /// A table that declares `expire_after_write`.
    pub expires: bool,
    pub alias: String,
    /// The space, for lookups into its other objects.
    pub schema_name: String,
    pub schema: std::sync::Arc<fireweed_dsl::Schema>,
}

impl Obj {
    pub fn new(
        schema_name: &str,
        schema: &std::sync::Arc<fireweed_dsl::Schema>,
        o: &DslObj,
        alias: &str,
    ) -> Obj {
        let (table, max_attempts, key, expires) = match o.kind {
            Kind::Queue => (
                crate::ddl::queue_table(schema_name, &o.name),
                schema.queues[&o.name].max_attempts,
                vec![],
                false,
            ),
            Kind::Table => {
                let t = &schema.tables[&o.name];
                (
                    crate::ddl::table_table(schema_name, &o.name),
                    MaxAttempts::Unlimited,
                    t.key.clone(),
                    t.expire_after_write.is_some(),
                )
            }
        };
        Obj {
            table,
            kind: o.kind,
            max_attempts,
            key,
            expires,
            alias: alias.into(),
            schema_name: schema_name.into(),
            schema: schema.clone(),
        }
    }
}

pub const NOW: &str = "fireweed.now()";

/// True when `new` keeps a field's merge rule against `old` (ADR-027 D20).
pub fn merge_ok(rule: MergeRule, old: &str, new: &str) -> String {
    match rule {
        MergeRule::AddOnly => format!("({old} IS NULL OR coalesce({new} @> {old}, false))"),
        MergeRule::NonDecreasing => format!("({old} IS NULL OR coalesce({new} >= {old}, false))"),
    }
}

impl Obj {
    /// The SQL name of a column, unqualified.
    pub fn col_name(c: &Column) -> String {
        if c.system {
            c.name.clone()
        } else {
            format!("\"f_{}\"", c.name)
        }
    }

    /// `true` when a queue item has used up its attempts.
    pub fn exhausted(&self, alias: &str) -> String {
        match self.max_attempts {
            MaxAttempts::Unlimited => "false".into(),
            MaxAttempts::Count(n) => format!("{alias}.attempts >= {n}"),
        }
    }

    /// A column read through alias `o`. A queue's `state` is its effective
    /// state (API-008 §Time).
    pub fn col(&self, c: &Column) -> String {
        let a = &self.alias;
        if self.kind == Kind::Queue && c.system && c.name == "state" {
            return format!(
                "(CASE WHEN {a}.state = 'leased' AND {a}.due_at <= {NOW} THEN CASE WHEN {} THEN 'failed' ELSE 'pending' END ELSE {a}.state END)",
                self.exhausted(a)
            );
        }
        format!("{a}.{}", Self::col_name(c))
    }

    /// A new version for a written row. Versions come from one sequence
    /// per space, so they never repeat, even after a delete and a reinsert
    /// with the same key (ADR-027 D61).
    pub fn next_version(&self) -> String {
        format!("nextval('\"{}\".fw_versions')", self.schema_name)
    }

    /// Rows that are live: a row of an expiring table past its expiry is
    /// gone. Other objects don't read `expires_at`, so a count over them can
    /// be answered from an index.
    pub fn live(&self) -> String {
        if self.expires {
            format!(
                "({a}.expires_at IS NULL OR {a}.expires_at > {NOW})",
                a = self.alias
            )
        } else {
            "true".into()
        }
    }

    pub fn operand(&self, q: &mut Q, o: &Operand) -> String {
        match o {
            Operand::Column(c) => self.col(c),
            Operand::Literal(v, t) => q.literal(v, *t),
            Operand::Now(0) => NOW.into(),
            Operand::Now(off) => format!("({NOW} + {})", q.duration(*off)),
            Operand::Path(p, t) => {
                let path = q.path(p);
                match t {
                    FieldType::Json => format!("({}.body #> {path})", self.alias),
                    FieldType::TextSet => {
                        format!("fireweed.text_set({}.body #> {path})", self.alias)
                    }
                    FieldType::Text => format!("({}.body #>> {path})", self.alias),
                    other => format!("(({}.body #>> {path})::{})", self.alias, cast(*other)),
                }
            }
        }
    }

    pub fn pred(&self, q: &mut Q, p: &RPred) -> String {
        match p {
            RPred::And(ps) if ps.is_empty() => "true".into(),
            RPred::Or(ps) if ps.is_empty() => "false".into(),
            RPred::And(ps) => format!(
                "({})",
                ps.iter()
                    .map(|p| self.pred(q, p))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            RPred::Or(ps) => format!(
                "({})",
                ps.iter()
                    .map(|p| self.pred(q, p))
                    .collect::<Vec<_>>()
                    .join(" OR ")
            ),
            RPred::Not(p) => format!("(NOT {})", self.pred(q, p)),
            RPred::Cmp(op, a, b) => {
                let op = match op {
                    CmpOp::Eq => "=",
                    CmpOp::Ne => "<>",
                    CmpOp::Lt => "<",
                    CmpOp::Le => "<=",
                    CmpOp::Gt => ">",
                    CmpOp::Ge => ">=",
                };
                format!("({} {op} {})", self.operand(q, a), self.operand(q, b))
            }
            RPred::In(_, vs) if vs.is_empty() => "false".into(),
            RPred::In(a, vs) => {
                let a = self.operand(q, a);
                let vs: Vec<String> = vs.iter().map(|v| self.operand(q, v)).collect();
                format!("({a} IN ({}))", vs.join(", "))
            }
            RPred::Contains(a, v) => {
                format!("({} @> ARRAY[{}])", self.operand(q, a), self.operand(q, v))
            }
            RPred::IsNull(a) => format!("({} IS NULL)", self.operand(q, a)),
            RPred::NotNull(a) => format!("({} IS NOT NULL)", self.operand(q, a)),
            RPred::Exists {
                obj,
                matches,
                filter,
            } => {
                // An indexed lookup into another object of the same space.
                let inner = Obj::new(
                    &self.schema_name,
                    &self.schema,
                    obj,
                    &format!("{}_x", self.alias),
                );
                let mut conds = vec![inner.live()];
                for (c, v) in matches {
                    conds.push(format!("{} = {}", inner.col(c), self.operand(q, v)));
                }
                if let Some(f) = filter {
                    conds.push(inner.pred(q, f));
                }
                format!(
                    "EXISTS (SELECT 1 FROM {} {} WHERE {})",
                    inner.table,
                    inner.alias,
                    conds.join(" AND ")
                )
            }
        }
    }

    /// `"f_x" = <expr>` for an assignment.
    pub fn assign(&self, q: &mut Q, c: &Column, e: &Expr) -> String {
        let me = self.col(c);
        let rhs = match e {
            Expr::Operand(o) => self.operand(q, o),
            Expr::Add(a, b) => format!("({} + {})", self.operand(q, a), self.operand(q, b)),
            Expr::Sub(a, b) => format!("({} - {})", self.operand(q, a), self.operand(q, b)),
            Expr::Coalesce(a, b) => {
                format!("coalesce({}, {})", self.operand(q, a), self.operand(q, b))
            }
            Expr::SetAdd(v) => format!(
                "ARRAY(SELECT DISTINCT x FROM unnest(coalesce({me}, '{{}}'::text[]) || {}) x ORDER BY x)",
                self.operand(q, v)
            ),
            Expr::SetRemove(v) => format!(
                "ARRAY(SELECT x FROM unnest(coalesce({me}, '{{}}'::text[])) x WHERE x <> ALL(coalesce({}, '{{}}'::text[])) ORDER BY x)",
                self.operand(q, v)
            ),
        };
        format!("{} = {rhs}", Self::col_name(c))
    }

    /// `body = <patched>` for a list of patches.
    pub fn patches(&self, q: &mut Q, ps: &[RPatch]) -> Option<String> {
        if ps.is_empty() {
            return None;
        }
        let mut e = "o.body".to_string();
        for p in ps {
            e = match p {
                RPatch::Set(path, v) => format!(
                    "fireweed.json_set({e}, {}, {})",
                    q.path(path),
                    self.operand(q, v)
                ),
                RPatch::Remove(path) => {
                    format!("(coalesce({e}, '{{}}'::jsonb) #- {})", q.path(path))
                }
                RPatch::Append(path, v, keep) => {
                    let keep = keep.map_or("NULL::int".to_string(), |k| {
                        format!("{}::int", q.bind_i64(k as i64))
                    });
                    format!(
                        "fireweed.json_append({e}, {}, {}, {keep})",
                        q.path(path),
                        self.operand(q, v)
                    )
                }
                RPatch::Replace(v) => self.operand(q, v),
            };
        }
        Some(format!("body = {e}"))
    }

    /// A JSON object of the given columns, as `jsonb`.
    pub fn json_of(&self, cols: &[(String, String)]) -> String {
        if cols.is_empty() {
            return "'{}'::jsonb".into();
        }
        // jsonb_build_object takes at most 100 arguments.
        let parts: Vec<String> = cols
            .chunks(40)
            .map(|ch| {
                let args: Vec<String> = ch.iter().map(|(k, v)| format!("'{k}', {v}")).collect();
                format!("jsonb_build_object({})", args.join(", "))
            })
            .collect();
        format!("({})", parts.join(" || "))
    }
}
