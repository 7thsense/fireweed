//! Fireweed space schemas as UMF core 0.10 documents (API-009, ADR-027
//! question 39).
//!
//! A space is one UMF module. Each queue and table is a core Record whose
//! members are its system columns and declared fields. Its physical table is
//! a `umf.tables` declaration (UMF CONTRACT-081). The queue, table and read
//! semantics that UMF core doesn't have are the module's `fireweed.space`
//! extension payload, registered through `umf-codegen`'s registry.
//!
//! [`export`] writes the canonical document for an API-008 [`Schema`].
//! [`load`] reads a document back: UMF validates it, including this
//! extension's semantics; the [`Schema`] is derived from it; and the document
//! must match the canonical export of that schema, so its `umf.tables`
//! declaration always describes the tables the executor creates.
//! [`deployment`] compiles UMF's PostgreSQL 18 DDL for those tables.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use fireweed_dsl::check::TABLE_COLUMNS;
use fireweed_dsl::{
    Duration, FieldDecl, FieldType, MaxAttempts, MergeRule, PriorityOrder, QueueDecl, ReadDecl,
    Schema, TableDecl,
};
use serde_json::{Map, Value as Json, json};
use umf_codegen::{CodegenEngine, CodegenResources};
use umf_core::{Capabilities, Context, Diagnostic, SemanticContext, Severity, Utf16};

/// The extension's identity and version.
pub const EXTENSION_ID: &str = "fireweed.space";
pub const EXTENSION_VERSION: &str = "0.1.0";
/// The extension package registered with UMF.
pub const PACKAGE: &str = include_str!("../../../schemas/umf/fireweed-space-0.1.0.package.json");
/// The registry token Fireweed's host capabilities answer for.
pub const TOKEN: u64 = 0x0f17_ee0d;

/// A queue table's fill factor (API-008 §Queues).
pub const QUEUE_FILLFACTOR: u32 = 80;

/// One problem in a document: a code, a JSON pointer and a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problem {
    pub code: String,
    pub path: String,
    pub message: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    /// The document isn't a valid Fireweed space document.
    Invalid(Vec<Problem>),
    /// UMF refused the operation itself (resources, limits).
    Umf(String),
}

impl Error {
    /// The problem codes, for tests and logs.
    pub fn codes(&self) -> Vec<&str> {
        match self {
            Error::Invalid(ps) => ps.iter().map(|p| p.code.as_str()).collect(),
            Error::Umf(_) => vec!["UMF"],
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Invalid(ps) => {
                write!(f, "invalid space document:")?;
                for p in ps {
                    write!(f, " [{} at {:?}: {}]", p.code, p.path, p.message)?;
                }
                Ok(())
            }
            Error::Umf(m) => write!(f, "umf: {m}"),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

fn problem(code: &str, path: impl Into<String>, message: impl Into<String>) -> Error {
    Error::Invalid(vec![Problem {
        code: code.into(),
        path: path.into(),
        message: message.into(),
    }])
}

fn umf_error(e: umf_core::EngineError) -> Error {
    Error::Umf(format!(
        "{}: {}",
        e.code,
        e.message.to_utf8().unwrap_or_default()
    ))
}

/// The identities a document gives its space. They aren't part of the
/// [`Schema`], so a loaded document reports them for its re-export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Names {
    /// The UMF document id.
    pub document: String,
    /// The module id, which is also its namespace.
    pub module: String,
}

impl Default for Names {
    fn default() -> Self {
        Names {
            document: "fireweed-space".into(),
            module: "space".into(),
        }
    }
}

// ------------------------------------------------------------------ export

/// A Fireweed field type's core meaning and native column type.
struct Mapping {
    scalar: Option<&'static str>,
    facets: Option<Json>,
    native: Json,
    set: bool,
}

fn mapping(t: FieldType) -> Mapping {
    let plain = |scalar, native: Json| Mapping {
        scalar: Some(scalar),
        facets: None,
        native,
        set: false,
    };
    match t {
        FieldType::Text => plain("string", json!({"kind": "text"})),
        FieldType::Int => Mapping {
            scalar: Some("integer"),
            facets: Some(json!({"integerWidth": {"bits": 64, "signed": true}})),
            native: json!({"kind": "int8"}),
            set: false,
        },
        FieldType::Float => plain("float", json!({"kind": "float8"})),
        FieldType::Bool => plain("boolean", json!({"kind": "boolean"})),
        FieldType::Timestamp => plain("timestamp", json!({"kind": "timestamptz"})),
        FieldType::Json => plain("json", json!({"kind": "jsonb"})),
        FieldType::TextSet => Mapping {
            scalar: None,
            facets: None,
            native: json!({"kind": "array", "element": {"kind": "text"}}),
            set: true,
        },
    }
}

fn type_name(t: FieldType) -> &'static str {
    match t {
        FieldType::Text => "text",
        FieldType::Int => "int",
        FieldType::Float => "float",
        FieldType::Bool => "bool",
        FieldType::Timestamp => "timestamp",
        FieldType::Json => "json",
        FieldType::TextSet => "text_set",
    }
}

/// One column of an object's table.
struct Column {
    /// The field name programs use.
    name: String,
    /// The native column name.
    column: String,
    ty: FieldType,
    required: bool,
    /// Assigned by the executor: an identity column.
    identity: bool,
    merge: Option<MergeRule>,
    allowed: Option<&'static [&'static str]>,
}

const QUEUE_STATES: &[&str] = &["pending", "leased", "complete", "failed"];

/// Queue system columns as the executor creates them (API-008 §Queues),
/// including the internal `lease_token`.
fn queue_columns() -> Vec<Column> {
    let sys = |name: &str, ty, required| Column {
        name: name.into(),
        column: name.into(),
        ty,
        required,
        identity: name == "id",
        merge: None,
        allowed: (name == "state").then_some(QUEUE_STATES),
    };
    vec![
        sys("id", FieldType::Int, true),
        sys("key", FieldType::Text, false),
        sys("state", FieldType::Text, true),
        sys("priority", FieldType::Int, true),
        sys("due_at", FieldType::Timestamp, true),
        sys("attempts", FieldType::Int, true),
        sys("version", FieldType::Int, true),
        sys("lease_owner", FieldType::Text, false),
        sys("lease_token", FieldType::Text, false),
        sys("created_at", FieldType::Timestamp, true),
        sys("updated_at", FieldType::Timestamp, true),
        sys("finished_at", FieldType::Timestamp, false),
        sys("body", FieldType::Json, false),
    ]
}

/// Table system columns (API-008 §Tables).
fn table_columns() -> Vec<Column> {
    TABLE_COLUMNS
        .iter()
        .map(|(name, ty)| Column {
            name: (*name).into(),
            column: (*name).into(),
            ty: *ty,
            required: *name != "expires_at",
            identity: *name == "id",
            merge: None,
            allowed: None,
        })
        .collect()
}

fn declared(fields: &BTreeMap<String, FieldDecl>, key: &[String]) -> Vec<Column> {
    fields
        .iter()
        .map(|(n, f)| Column {
            name: n.clone(),
            column: format!("f_{n}"),
            ty: f.ty,
            required: key.contains(n),
            identity: false,
            merge: f.merge,
            allowed: None,
        })
        .collect()
}

fn element_id(object: &str, field: &str) -> String {
    format!("{object}.{field}")
}

fn item_id(object: &str, field: &str) -> String {
    format!("{object}.{field}.item")
}

/// The index name for `suffix` on `table`, within PostgreSQL's 63 bytes.
fn index_name(table: &str, suffix: &str) -> String {
    use sha2::Digest;
    let name = format!("{table}_{suffix}");
    if name.len() <= 63 {
        return name;
    }
    let hex: String = sha2::Sha256::digest(name.as_bytes())
        .iter()
        .take(20)
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("fw_ix_{hex}")
}

fn reference(module: &str, element: &str) -> Json {
    json!({"module": module, "element": element})
}

struct Object<'a> {
    name: &'a str,
    queue: bool,
    columns: Vec<Column>,
    /// Declared fields with an index of their own.
    indexed: Vec<(&'a str, FieldType)>,
    key: &'a [String],
    priority: PriorityOrder,
}

fn elements_of(module: &str, o: &Object<'_>, elements: &mut Vec<Json>) {
    for c in &o.columns {
        let m = mapping(c.ty);
        let mut e = Map::new();
        e.insert("id".into(), json!(element_id(o.name, &c.name)));
        e.insert("name".into(), json!(c.name));
        e.insert("kind".into(), json!("field"));
        if let Some(s) = m.scalar {
            e.insert("scalarType".into(), json!(s));
        }
        if m.set {
            e.insert("cardinality".into(), json!("set"));
            e.insert(
                "itemType".into(),
                reference(module, &item_id(o.name, &c.name)),
            );
        } else {
            e.insert("cardinality".into(), json!("one"));
        }
        e.insert(
            "nullability".into(),
            json!(if c.required {
                "required"
            } else {
                "absent-allowed"
            }),
        );
        if let Some(f) = m.facets {
            e.insert("facets".into(), f);
        }
        if let Some(values) = c.allowed {
            e.insert(
                "allowedValues".into(),
                Json::Array(values.iter().map(|v| json!({"string": v})).collect()),
            );
        }
        let mut ext = Map::new();
        if let Some(rule) = c.merge {
            ext.insert(
                EXTENSION_ID.into(),
                json!({"kind": "field", "merge": merge_name(rule)}),
            );
        }
        e.insert("extensions".into(), Json::Object(ext));
        elements.push(Json::Object(e));
        if m.set {
            elements.push(json!({
                "id": item_id(o.name, &c.name),
                "name": format!("{} item", c.name),
                "kind": "field",
                "scalarType": "string",
                "cardinality": "one",
                "nullability": "required",
                "extensions": {}
            }));
        }
    }
    let mut keys = vec![json!({
        "id": "id",
        "name": "id",
        "primary": true,
        "fields": [reference(module, &element_id(o.name, "id"))]
    })];
    if !o.key.is_empty() {
        keys.push(json!({
            "id": "key",
            "name": "key",
            "fields": o
                .key
                .iter()
                .map(|k| reference(module, &element_id(o.name, k)))
                .collect::<Vec<_>>()
        }));
    }
    elements.push(json!({
        "id": o.name,
        "name": o.name,
        "kind": "record",
        "members": o
            .columns
            .iter()
            .map(|c| reference(module, &element_id(o.name, &c.name)))
            .collect::<Vec<_>>(),
        "keys": keys,
        "extensions": {}
    }));
}

fn table_of(module: &str, o: &Object<'_>) -> Json {
    let field = |n: &str| reference(module, &element_id(o.name, n));
    let table = format!("{}_{}", if o.queue { "q" } else { "t" }, o.name);
    let columns: Vec<Json> = o
        .columns
        .iter()
        .map(|c| {
            let mut col = json!({
                "field": field(&c.name),
                "name": c.column,
                "nativeType": mapping(c.ty).native,
            });
            if c.identity {
                col["generated"] = json!({"kind": "identity", "generation": "byDefault"});
            }
            col
        })
        .collect();
    let index = |suffix: &str, fields: Vec<Json>| {
        json!({
            "id": suffix,
            "name": index_name(&table, suffix),
            "fields": fields,
            "unique": false,
            "include": []
        })
    };
    let not_null = |n: &str| json!({"op": "not", "arg": {"op": "isNull", "arg": {"op": "field", "field": field(n)}}});
    let mut indexes = Vec::new();
    if o.queue {
        let mut key = index("key", vec![field("key")]);
        key["unique"] = json!(true);
        indexes.push(key);
        let mut claim = index(
            "claim",
            vec![field("due_at"), field("priority"), field("id")],
        );
        if o.priority == PriorityOrder::Desc {
            claim["directions"] = json!(["asc", "desc", "asc"]);
        }
        claim["where"] = json!({
            "op": "in",
            "arg": {"op": "field", "field": field("state")},
            "values": [{"string": "pending"}, {"string": "leased"}]
        });
        indexes.push(claim);
        let mut finished = index("finished", vec![field("finished_at")]);
        finished["where"] = not_null("finished_at");
        indexes.push(finished);
        indexes.push(index(
            "state",
            vec![field("state"), field("due_at"), field("attempts")],
        ));
    } else {
        let mut expires = index("expires", vec![field("expires_at")]);
        expires["where"] = not_null("expires_at");
        indexes.push(expires);
    }
    for (n, ty) in &o.indexed {
        let suffix = format!("f_{n}");
        if *ty == FieldType::TextSet {
            let mut gin = index(&suffix, vec![field(n)]);
            gin["method"] = json!("gin");
            indexes.push(gin);
        } else {
            indexes.push(index(&suffix, vec![field(n), field("id")]));
        }
    }
    let rules = |kind: &str| {
        let r = json!({"kind": kind});
        json!({"select": r, "insert": r, "update": r, "delete": r})
    };
    let mut t = json!({
        "id": o.name,
        "record": reference(module, o.name),
        "name": table,
        "columns": columns,
        "indexes": indexes,
        "checks": [],
        "foreignKeys": [],
        "policy": {"mode": "native-rls", "person": rules("deny"), "batch": rules("allow")}
    });
    if o.queue {
        t["fillfactor"] = json!(QUEUE_FILLFACTOR);
    }
    t
}

fn merge_name(r: MergeRule) -> &'static str {
    match r {
        MergeRule::AddOnly => "add_only",
        MergeRule::NonDecreasing => "non_decreasing",
    }
}

/// The canonical UMF document for a schema. The schema should pass
/// `fireweed_dsl::check::schema`; [`load`] of the result returns it.
pub fn export(s: &Schema, names: &Names) -> Json {
    let module = names.module.as_str();
    let mut objects = Vec::new();
    for (n, q) in &s.queues {
        let mut columns = queue_columns();
        columns.extend(declared(&q.fields, &[]));
        objects.push(Object {
            name: n,
            queue: true,
            columns,
            indexed: q
                .fields
                .iter()
                .filter(|(_, f)| f.index)
                .map(|(n, f)| (n.as_str(), f.ty))
                .collect(),
            key: &[],
            priority: q.priority,
        });
    }
    for (n, t) in &s.tables {
        let mut columns = table_columns();
        columns.extend(declared(&t.fields, &t.key));
        objects.push(Object {
            name: n,
            queue: false,
            columns,
            // The first key field leads the key's unique index already.
            indexed: t
                .fields
                .iter()
                .filter(|(n, f)| f.index && t.key.first() != Some(*n))
                .map(|(n, f)| (n.as_str(), f.ty))
                .collect(),
            key: &t.key,
            priority: PriorityOrder::Asc,
        });
    }
    let mut elements = Vec::new();
    let mut tables = Vec::new();
    for o in &objects {
        elements_of(module, o, &mut elements);
        tables.push(table_of(module, o));
    }
    let queues: Map<String, Json> = s
        .queues
        .iter()
        .map(|(n, q)| {
            (
                n.clone(),
                json!({
                    "record": reference(module, n),
                    "maxAttempts": match q.max_attempts {
                        MaxAttempts::Count(c) => json!(c),
                        MaxAttempts::Unlimited => json!("unlimited"),
                    },
                    "retention": q.retention.0,
                    "priority": match q.priority {
                        PriorityOrder::Asc => "asc",
                        PriorityOrder::Desc => "desc",
                    },
                }),
            )
        })
        .collect();
    let table_semantics: Map<String, Json> = s
        .tables
        .iter()
        .map(|(n, t)| {
            let mut v = json!({"record": reference(module, n), "sequenced": t.sequenced});
            if let Some(d) = t.expire_after_write {
                v["expireAfterWrite"] = json!(d.0);
            }
            (n.clone(), v)
        })
        .collect();
    let reads: Vec<Json> = s
        .reads
        .iter()
        .map(|r| {
            json!({
                "name": r.name,
                "version": r.version,
                "params": r
                    .params
                    .iter()
                    .map(|(k, t)| (k.clone(), json!(type_name(*t))))
                    .collect::<Map<_, _>>(),
                "select": serde_json::to_value(&r.select).expect("a select serializes"),
            })
        })
        .collect();
    json!({
        "umf": "0.10.0",
        "id": names.document,
        "vocabularies": {
            "umf.tables": {"version": "0.1.0"},
            EXTENSION_ID: {"version": EXTENSION_VERSION}
        },
        "modules": [{
            "id": module,
            "namespace": module,
            "elements": elements,
            "extensions": {
                "umf.tables": {
                    "profile": "postgresql18/0.1",
                    "tables": tables,
                    "seedSets": [],
                    "commands": []
                },
                EXTENSION_ID: {
                    "kind": "space",
                    "version": s.version,
                    "requestRetention": s.request_retention.0,
                    "queues": queues,
                    "tables": table_semantics,
                    "reads": reads
                }
            }
        }]
    })
}

// ------------------------------------------------------------------ derive

fn pointer_escape(s: &str) -> String {
    s.replace('~', "~0").replace('/', "~1")
}

fn extension_pointer() -> String {
    format!("/extensions/{}", pointer_escape(EXTENSION_ID))
}

fn module_of(doc: &Json) -> Result<(usize, &Json)> {
    let modules = doc
        .get("modules")
        .and_then(Json::as_array)
        .ok_or_else(|| problem("FIREWEED_SHAPE", "/modules", "expected modules"))?;
    let found: Vec<_> = modules
        .iter()
        .enumerate()
        .filter(|(_, m)| m.pointer(&extension_pointer()).is_some())
        .collect();
    match found.as_slice() {
        [one] => Ok(*one),
        _ => Err(problem(
            "FIREWEED_SPACE",
            "/modules",
            "a space document has exactly one module with a fireweed.space payload",
        )),
    }
}

fn element<'a>(module: &'a Json, id: &str) -> Option<&'a Json> {
    module
        .get("elements")?
        .as_array()?
        .iter()
        .find(|e| e.get("id").and_then(Json::as_str) == Some(id))
}

fn field_type_of(e: &Json, module: &Json, path: &str) -> Result<FieldType> {
    if e.get("cardinality").and_then(Json::as_str) == Some("set") {
        let item = e
            .pointer("/itemType/element")
            .and_then(Json::as_str)
            .and_then(|id| element(module, id))
            .ok_or_else(|| problem("FIREWEED_FIELD", path, "a set needs its item Field"))?;
        return match item.get("scalarType").and_then(Json::as_str) {
            Some("string") => Ok(FieldType::TextSet),
            _ => Err(problem(
                "FIREWEED_FIELD",
                path,
                "only sets of strings are Fireweed fields",
            )),
        };
    }
    Ok(match e.get("scalarType").and_then(Json::as_str) {
        Some("string") => FieldType::Text,
        Some("integer") => FieldType::Int,
        Some("float") => FieldType::Float,
        Some("boolean") => FieldType::Bool,
        Some("timestamp") => FieldType::Timestamp,
        Some("json") => FieldType::Json,
        other => {
            return Err(problem(
                "FIREWEED_FIELD",
                path,
                format!("no Fireweed field type for scalarType {other:?}"),
            ));
        }
    })
}

fn ms(v: Option<&Json>, path: &str) -> Result<Duration> {
    v.and_then(Json::as_i64)
        .filter(|n| *n >= 0)
        .map(Duration)
        .ok_or_else(|| problem("FIREWEED_SHAPE", path, "expected milliseconds"))
}

fn object<'a>(v: Option<&'a Json>, path: &str) -> Result<&'a Map<String, Json>> {
    v.and_then(Json::as_object)
        .ok_or_else(|| problem("FIREWEED_SHAPE", path, "expected an object"))
}

/// An object's declared fields: its Record's members after the system
/// columns, with their types, merge rules and (from `umf.tables`) indexes.
fn declared_fields(
    module: &Json,
    object: &str,
    system: usize,
    path: &str,
) -> Result<BTreeMap<String, FieldDecl>> {
    let record = element(module, object)
        .ok_or_else(|| problem("FIREWEED_RECORD", path, format!("no Record {object:?}")))?;
    let members = record
        .get("members")
        .and_then(Json::as_array)
        .ok_or_else(|| problem("FIREWEED_RECORD", path, "a Record lists its members"))?;
    let table = module
        .pointer("/extensions/umf.tables/tables")
        .and_then(Json::as_array)
        .and_then(|ts| {
            ts.iter()
                .find(|t| t.get("id").and_then(Json::as_str) == Some(object))
        });
    let indexed = |field_element: &str| {
        table
            .and_then(|t| t.get("indexes"))
            .and_then(Json::as_array)
            .is_some_and(|ixs| {
                ixs.iter().any(|ix| {
                    ix.get("id")
                        .and_then(Json::as_str)
                        .is_some_and(|id| id.starts_with("f_"))
                        && ix.pointer("/fields/0/element").and_then(Json::as_str)
                            == Some(field_element)
                })
            })
    };
    let mut fields = BTreeMap::new();
    for (i, m) in members.iter().enumerate().skip(system) {
        let p = format!("{path}/members/{i}");
        let id = m
            .get("element")
            .and_then(Json::as_str)
            .ok_or_else(|| problem("FIREWEED_RECORD", &p, "a member names its Field"))?;
        let name = id.strip_prefix(&format!("{object}.")).ok_or_else(|| {
            problem(
                "FIREWEED_RECORD",
                &p,
                format!("member {id:?} isn't {object}.<field>"),
            )
        })?;
        let e = element(module, id)
            .ok_or_else(|| problem("FIREWEED_RECORD", &p, format!("no Field {id:?}")))?;
        let ty = field_type_of(e, module, &p)?;
        let merge = match e.pointer(&format!("{}/merge", extension_pointer())) {
            None => None,
            Some(Json::String(s)) if s == "add_only" => Some(MergeRule::AddOnly),
            Some(Json::String(s)) if s == "non_decreasing" => Some(MergeRule::NonDecreasing),
            Some(_) => return Err(problem("FIREWEED_FIELD", &p, "unknown merge rule")),
        };
        fields.insert(
            name.to_string(),
            FieldDecl {
                ty,
                index: indexed(id),
                merge,
            },
        );
    }
    Ok(fields)
}

/// Derives the API-008 schema a document declares, without checking it or
/// that the document is canonical. [`load`] does all three.
pub fn derive(doc: &Json) -> Result<(Schema, Names)> {
    let (mi, module) = module_of(doc)?;
    let base = format!("/modules/{mi}{}", extension_pointer());
    let s = object(module.pointer(&extension_pointer()), &base)?;
    let version = s.get("version").and_then(Json::as_i64).ok_or_else(|| {
        problem(
            "FIREWEED_SHAPE",
            format!("{base}/version"),
            "expected a version",
        )
    })?;
    let mut queues = BTreeMap::new();
    for (n, q) in object(s.get("queues"), &format!("{base}/queues"))? {
        let p = format!("{base}/queues/{}", pointer_escape(n));
        let max_attempts = match q.get("maxAttempts") {
            Some(Json::String(u)) if u == "unlimited" => MaxAttempts::Unlimited,
            v => MaxAttempts::Count(
                v.and_then(Json::as_u64)
                    .and_then(|n| u32::try_from(n).ok())
                    .filter(|n| *n > 0)
                    .ok_or_else(|| {
                        problem(
                            "FIREWEED_SHAPE",
                            format!("{p}/maxAttempts"),
                            "expected a positive count or \"unlimited\"",
                        )
                    })?,
            ),
        };
        let priority = match q.get("priority").and_then(Json::as_str) {
            Some("asc") => PriorityOrder::Asc,
            Some("desc") => PriorityOrder::Desc,
            _ => {
                return Err(problem(
                    "FIREWEED_SHAPE",
                    format!("{p}/priority"),
                    "expected asc or desc",
                ));
            }
        };
        queues.insert(
            n.clone(),
            QueueDecl {
                max_attempts,
                retention: ms(q.get("retention"), &format!("{p}/retention"))?,
                priority,
                fields: declared_fields(module, n, queue_columns().len(), &p)?,
            },
        );
    }
    let mut tables = BTreeMap::new();
    for (n, t) in object(s.get("tables"), &format!("{base}/tables"))? {
        let p = format!("{base}/tables/{}", pointer_escape(n));
        let prefix = format!("{n}.");
        let key: Vec<String> = element(module, n)
            .and_then(|r| r.get("keys"))
            .and_then(Json::as_array)
            .and_then(|ks| {
                ks.iter()
                    .find(|k| k.get("id").and_then(Json::as_str) == Some("key"))
            })
            .and_then(|k| k.get("fields"))
            .and_then(Json::as_array)
            .map(|fs| {
                fs.iter()
                    .filter_map(|f| f.get("element").and_then(Json::as_str))
                    .map(|id| id.strip_prefix(&prefix).unwrap_or(id).to_string())
                    .collect()
            })
            .unwrap_or_default();
        let mut fields = declared_fields(module, n, table_columns().len(), &p)?;
        // The key's unique index leads with its first field, so that field is
        // indexed even though it has no index of its own.
        if let Some(first) = key.first().and_then(|k| fields.get_mut(k)) {
            first.index = true;
        }
        tables.insert(
            n.clone(),
            TableDecl {
                key,
                sequenced: t.get("sequenced").and_then(Json::as_bool).unwrap_or(false),
                expire_after_write: match t.get("expireAfterWrite") {
                    None => None,
                    v => Some(ms(v, &format!("{p}/expireAfterWrite"))?),
                },
                fields,
            },
        );
    }
    let mut reads = Vec::new();
    for (i, r) in s
        .get("reads")
        .and_then(Json::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .enumerate()
    {
        let read: ReadDecl = serde_json::from_value(r.clone())
            .map_err(|e| problem("FIREWEED_READ", format!("{base}/reads/{i}"), e.to_string()))?;
        reads.push(read);
    }
    let schema = Schema {
        version,
        request_retention: ms(
            s.get("requestRetention"),
            &format!("{base}/requestRetention"),
        )?,
        queues,
        tables,
        reads,
    };
    let text = |v: Option<&Json>| v.and_then(Json::as_str).unwrap_or_default().to_string();
    let names = Names {
        document: text(doc.get("id")),
        module: text(module.get("id")),
    };
    Ok((schema, names))
}

/// Members a document may add without changing its meaning.
const DESCRIPTIVE: &[&str] = &["description", "title", "aliases", "examples"];

fn interpreted(v: &Json) -> Json {
    match v {
        Json::Object(m) => Json::Object(
            m.iter()
                .filter(|(k, _)| !DESCRIPTIVE.contains(&k.as_str()))
                .map(|(k, v)| (k.clone(), interpreted(v)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(a.iter().map(interpreted).collect()),
        other => other.clone(),
    }
}

/// The first JSON pointer at which two values differ.
fn first_difference(a: &Json, b: &Json, path: &str) -> Option<String> {
    match (a, b) {
        (Json::Object(x), Json::Object(y)) => x.keys().chain(y.keys()).find_map(|k| {
            let p = format!("{path}/{}", pointer_escape(k));
            match (x.get(k), y.get(k)) {
                (Some(l), Some(r)) => first_difference(l, r, &p),
                _ => Some(p),
            }
        }),
        (Json::Array(x), Json::Array(y)) => x
            .iter()
            .zip(y)
            .enumerate()
            .find_map(|(i, (l, r))| first_difference(l, r, &format!("{path}/{i}")))
            .or_else(|| (x.len() != y.len()).then(|| format!("{path}/{}", x.len().min(y.len())))),
        (l, r) => (l != r).then(|| path.to_string()),
    }
}

// -------------------------------------------------------------- UMF bridge

fn to_umf(v: &Json) -> Result<umf_core::Value> {
    let text = serde_json::to_string(v).map_err(|e| Error::Umf(e.to_string()))?;
    umf_core::serialization::read_json_value(
        &text.encode_utf16().collect::<Vec<_>>(),
        umf_core::serialization::Format::Json,
        &mut Context::catalog(),
    )
    .map_err(umf_error)
}

fn from_umf(v: &umf_core::Value) -> Result<Json> {
    let text = umf_core::serialization::stringify_compact_metered(v, &mut Context::catalog())
        .map_err(umf_error)?
        .to_utf8()
        .map_err(|_| Error::Umf("invalid Unicode".into()))?;
    serde_json::from_str(&text).map_err(|e| Error::Umf(e.to_string()))
}

/// The `fireweed.space` semantics as UMF host capabilities: derive the
/// API-008 schema from the whole document and run its checks. Field payloads
/// are checked with the space that declares them.
pub struct Semantics;

impl Capabilities for Semantics {
    fn validate_extension(
        &mut self,
        token: u64,
        _payload: &umf_core::Value,
        context: &SemanticContext,
    ) -> std::result::Result<Vec<Diagnostic>, Utf16> {
        if token != TOKEN || context.scope != "module" {
            return Ok(Vec::new());
        }
        let diag = |code: &str, path: &str, message: &str| Diagnostic {
            code: Utf16::from(code),
            path: Utf16::from(path),
            python_source_path: false,
            message: Utf16::from(message),
            severity: Severity::Error,
            python_text_origin: None,
        };
        let doc = from_umf(&context.document).map_err(|e| Utf16::from(e.to_string()))?;
        let path = context.path.to_utf8().unwrap_or_default();
        match derive(&doc) {
            Err(Error::Invalid(ps)) => Ok(ps
                .iter()
                .map(|p| diag(&p.code, &p.path, &p.message))
                .collect()),
            Err(Error::Umf(m)) => Err(Utf16::from(m)),
            Ok((schema, _)) => Ok(match fireweed_dsl::check::schema(&schema) {
                Ok(()) => Vec::new(),
                Err(e) => vec![diag("FIREWEED_SCHEMA", &path, &e.to_string())],
            }),
        }
    }
}

/// UMF's release resources with the `fireweed.space` extension registered.
pub fn resources() -> Result<CodegenResources> {
    let mut ctx = Context::catalog();
    let mut r = CodegenResources::release(&mut ctx).map_err(umf_error)?;
    let package: Json = serde_json::from_str(PACKAGE).expect("the package is JSON");
    r.register_host_extension(&to_umf(&package)?, TOKEN, &mut ctx)
        .map_err(umf_error)?;
    Ok(r)
}

fn diagnostics(v: &Json) -> Vec<Problem> {
    let text = |d: &Json, k: &str| d.get(k).and_then(Json::as_str).unwrap_or_default().into();
    v.get("diagnostics")
        .and_then(Json::as_array)
        .map(|ds| {
            ds.iter()
                .map(|d| Problem {
                    code: text(d, "code"),
                    path: text(d, "path"),
                    message: text(d, "message"),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A loaded space document.
#[derive(Clone, Debug, PartialEq)]
pub struct Loaded {
    pub schema: Schema,
    pub names: Names,
    /// The document as read.
    pub document: Json,
}

/// Reads a UMF space document (YAML or JSON). UMF validates it, including
/// the `fireweed.space` semantics; the API-008 schema is derived and checked;
/// and the document must equal that schema's canonical export apart from
/// descriptive members (`description`, `title`, `aliases`, `examples`).
pub fn load(resources: &CodegenResources, text: &str) -> Result<Loaded> {
    let doc: Json =
        fireweed_dsl::from_yaml(text).map_err(|e| problem("FIREWEED_SYNTAX", "", e.to_string()))?;
    load_value(resources, doc)
}

/// [`load`] for an already parsed document.
pub fn load_value(resources: &CodegenResources, doc: Json) -> Result<Loaded> {
    let engine = CodegenEngine::new(resources);
    let inspection = engine
        .inspect_table_lifecycle_with_host(&to_umf(&doc)?, &mut Semantics, &mut Context::ordinary())
        .map_err(umf_error)?;
    let inspection = from_umf(&inspection)?;
    if inspection.get("valid") != Some(&Json::Bool(true))
        || inspection.get("complete") != Some(&Json::Bool(true))
    {
        let mut ps = diagnostics(&inspection);
        if ps.is_empty() {
            ps.push(Problem {
                code: "FIREWEED_INCOMPLETE".into(),
                path: String::new(),
                message: "UMF inspection is incomplete".into(),
            });
        }
        return Err(Error::Invalid(ps));
    }
    let (schema, names) = derive(&doc)?;
    fireweed_dsl::check::schema(&schema)
        .map_err(|e| problem("FIREWEED_SCHEMA", "", e.to_string()))?;
    let canonical = export(&schema, &names);
    if let Some(at) = first_difference(&interpreted(&doc), &interpreted(&canonical), "") {
        return Err(problem(
            "FIREWEED_NOT_CANONICAL",
            at,
            "the document differs from the canonical form of the schema it declares",
        ));
    }
    Ok(Loaded {
        schema,
        names,
        document: doc,
    })
}

/// Trusted deployment configuration (UMF CONTRACT-081): the PostgreSQL
/// schema, and the roles UMF's row-level security policies name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Deployment {
    pub schema: String,
    pub owner_role: String,
    pub person_role: String,
    pub batch_role: String,
}

/// UMF's PostgreSQL 18 deployment bundle for a loaded document's tables.
/// Its `steps[].sql` are the statements, run in order as the owner role.
pub fn deployment(resources: &CodegenResources, loaded: &Loaded, d: &Deployment) -> Result<Json> {
    let request = json!({
        "text": serde_json::to_string(&loaded.document).map_err(|e| Error::Umf(e.to_string()))?,
        "format": "json",
        "configuration": {
            "module": loaded.names.module,
            "schema": d.schema,
            "ownerRole": d.owner_role,
            "personRole": d.person_role,
            "batchRole": d.batch_role,
            "groupRoles": {},
            "independentExtensions": [{"id": EXTENSION_ID, "version": EXTENSION_VERSION}]
        }
    });
    let engine = CodegenEngine::new(resources);
    let result = engine
        .compile_table_deployment_with_host(
            &to_umf(&request)?,
            &mut Semantics,
            &mut Context::ordinary(),
        )
        .map_err(umf_error)?;
    let result = from_umf(&result)?;
    match result.get("status").and_then(Json::as_str) {
        Some("compiled") => Ok(result["bundle"].clone()),
        _ => Err(Error::Invalid(diagnostics(&result))),
    }
}
