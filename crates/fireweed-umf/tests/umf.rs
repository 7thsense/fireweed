//! Space schemas as UMF documents (API-009): export and load round-trip,
//! UMF validation with the `fireweed.space` semantics, rejections, and, with
//! `FIREWEED_PG_TEST_URL`, UMF's DDL creating the same PostgreSQL 18 catalog
//! as the executor.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use fireweed_dsl::{Schema, from_yaml};
use fireweed_umf::{Deployment, Names, deployment, export, load, load_value, resources};
use serde_json::{Value, json};
use umf_codegen::CodegenResources;

fn umf() -> &'static CodegenResources {
    static R: OnceLock<CodegenResources> = OnceLock::new();
    R.get_or_init(|| resources().expect("UMF resources with fireweed.space"))
}

/// The schema example in API-008 §Schema documents.
fn api_example() -> Schema {
    let doc = include_str!("../../../docs/helix/02-design/contracts/API-008-data-plane-dsl.md");
    let start = doc.find("## Schema documents").unwrap();
    let block = &doc[start..];
    let yaml = &block[block.find("```yaml\n").unwrap() + 8..];
    from_yaml(&yaml[..yaml.find("```").unwrap()]).unwrap()
}

/// Every field type, both priority orders, unlimited attempts, an indexed
/// field of each indexable type, merge rules, a sequenced expiring table and
/// a read with parameters.
const EVERYTHING: &str = r#"
version: 3
request_retention: 2d
queues:
  work:
    max_attempts: unlimited
    retention: 31d
    priority: desc
    fields:
      label:  {type: text, index: true}
      count:  {type: int, index: true}
      weight: {type: float, index: true}
      done:   {type: bool, index: true}
      at:     {type: timestamp, index: true}
      doc:    {type: json}
      tags:   {type: text_set, index: true}
  plain:
    fields: {}
tables:
  counters:
    key: [subject, metric]
    fields:
      subject: {type: text, index: true}
      metric:  {type: text, index: true}
      total:   {type: int, merge: non_decreasing}
      seen:    {type: timestamp, merge: non_decreasing}
      score:   {type: float, merge: non_decreasing}
      reasons: {type: text_set, merge: add_only, index: true}
  log:
    sequenced: true
    expire_after_write: 7d
    fields:
      instance: {type: int, index: true}
      event:    {type: json}
reads:
  - name: by_tag
    version: 1
    params: {tag: text}
    select:
      from: work
      where: {contains: [tags, {param: tag}]}
      fields: [id, label]
      limit: 100
"#;

fn schemas() -> Vec<(&'static str, Schema)> {
    vec![
        ("api-008", api_example()),
        (
            "site",
            from_yaml(include_str!("../../../docs/site/examples/src/schema.yaml")).unwrap(),
        ),
        ("everything", from_yaml(EVERYTHING).unwrap()),
    ]
}

/// A schema as a document reports it: a table's first key field is indexed
/// by the key's unique index, so it loads with `index: true`.
fn as_loaded(mut s: Schema) -> Schema {
    for t in s.tables.values_mut() {
        if let Some(f) = t.key.first().and_then(|k| t.fields.get_mut(k)) {
            f.index = true;
        }
    }
    s
}

fn codes(e: &fireweed_umf::Error) -> Vec<String> {
    e.codes().into_iter().map(String::from).collect()
}

#[test]
fn export_then_load_returns_the_schema() {
    for (name, s) in schemas() {
        fireweed_dsl::check::schema(&s).unwrap();
        let doc = export(&s, &Names::default());
        let loaded = load_value(umf(), doc.clone()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(loaded.schema, as_loaded(s.clone()), "{name}");
        assert_eq!(loaded.names, Names::default());
        // The loaded schema exports to the same document.
        assert_eq!(export(&loaded.schema, &loaded.names), doc, "{name}");
        // YAML text of the same document loads too.
        let text = serde_json::to_string_pretty(&doc).unwrap();
        assert_eq!(load(umf(), &text).unwrap().schema, as_loaded(s), "{name}");
    }
}

#[test]
fn names_are_kept_and_descriptive_members_are_allowed() {
    let names = Names {
        document: "acme-campaign-7".into(),
        module: "campaign".into(),
    };
    let s = api_example();
    let mut doc = export(&s, &names);
    doc["title"] = json!("Campaign 7");
    doc["modules"][0]["elements"][0]["description"] = json!("Assigned at insert");
    doc["modules"][0]["elements"][0]["title"] = json!("Item id");
    let loaded = load_value(umf(), doc).unwrap();
    assert_eq!(loaded.schema, as_loaded(s));
    assert_eq!(loaded.names, names);
}

#[test]
fn the_document_is_plain_umf_with_umf_tables_and_the_extension() {
    let doc = export(&from_yaml::<Schema>(EVERYTHING).unwrap(), &Names::default());
    let m = &doc["modules"][0];
    let el = |id: &str| {
        m["elements"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["id"] == id)
            .unwrap()
            .clone()
    };
    assert_eq!(el("work.doc")["scalarType"], "json");
    assert_eq!(el("work.tags")["cardinality"], "set");
    assert_eq!(el("work.tags.item")["scalarType"], "string");
    assert_eq!(
        el("work.state")["allowedValues"].as_array().unwrap().len(),
        4
    );
    assert_eq!(
        el("counters.reasons")["extensions"]["fireweed.space"]["merge"],
        "add_only"
    );
    assert_eq!(
        el("counters")["keys"][1]["fields"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let tables = m["extensions"]["umf.tables"]["tables"].as_array().unwrap();
    let work = tables.iter().find(|t| t["id"] == "work").unwrap();
    assert_eq!(work["name"], "q_work");
    assert_eq!(work["fillfactor"], 80);
    let claim = work["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"] == "claim")
        .unwrap();
    assert_eq!(claim["directions"], json!(["asc", "desc", "asc"]));
    assert_eq!(claim["where"]["op"], "in");
    let tags = work["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"] == "f_tags")
        .unwrap();
    assert_eq!(tags["method"], "gin");
}

fn load_err(doc: Value) -> Vec<String> {
    codes(&load_value(umf(), doc).expect_err("the document must be refused"))
}

fn schema_err(yaml: &str) -> Vec<String> {
    let s: Schema = from_yaml(yaml).unwrap();
    load_err(export(&s, &Names::default()))
}

#[test]
fn invalid_spaces_are_refused_by_the_extension_semantics() {
    // A merge rule on a type it doesn't apply to.
    assert!(
        schema_err("version: 1\ntables:\n  t:\n    key: [k]\n    fields:\n      k: {type: text}\n      n: {type: text, merge: non_decreasing}\n")
            .contains(&"FIREWEED_SCHEMA".to_string())
    );
    // A sequenced table with a key.
    assert!(
        schema_err("version: 1\ntables:\n  t:\n    sequenced: true\n    key: [k]\n    fields:\n      k: {type: int}\n")
            .contains(&"FIREWEED_SCHEMA".to_string())
    );
    // A read that references an undeclared field.
    assert!(
        schema_err("version: 1\nqueues:\n  q:\n    fields: {}\nreads:\n  - name: r\n    version: 1\n    select: {from: q, where: {eq: [nope, 1]}, fields: [id], limit: 1}\n")
            .contains(&"FIREWEED_SCHEMA".to_string())
    );
    // An authored field that shadows a system field.
    assert!(
        !schema_err("version: 1\nqueues:\n  q:\n    fields:\n      state: {type: text}\n")
            .is_empty()
    );
}

#[test]
fn a_document_that_isnt_the_canonical_form_is_refused() {
    let base = export(&api_example(), &Names::default());
    let tables = "/modules/0/extensions/umf.tables/tables";

    // A missing claim index.
    let mut doc = base.clone();
    let instances = doc
        .pointer_mut(tables)
        .unwrap()
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|t| t["id"] == "instances")
        .unwrap();
    instances["indexes"]
        .as_array_mut()
        .unwrap()
        .retain(|i| i["id"] != "claim");
    assert!(load_err(doc).contains(&"FIREWEED_NOT_CANONICAL".to_string()));

    // Another native type for a declared field.
    let mut doc = base.clone();
    for t in doc.pointer_mut(tables).unwrap().as_array_mut().unwrap() {
        for c in t["columns"].as_array_mut().unwrap() {
            if c["name"] == "f_campaign" {
                c["nativeType"] = json!({"kind": "varchar", "length": 40});
            }
        }
    }
    assert!(load_err(doc).contains(&"FIREWEED_NOT_CANONICAL".to_string()));

    // An unknown fireweed.space member is a structural error.
    let mut doc = base.clone();
    doc["modules"][0]["extensions"]["fireweed.space"]["cohorts"] = json!([]);
    assert!(load_err(doc).contains(&"EXTENSION_STRUCTURE".to_string()));

    // Another extension on a space Field has no Fireweed meaning.
    let mut doc = base;
    doc["vocabularies"]["acme.audit"] = json!({"version": "1.0.0"});
    doc["modules"][0]["elements"][0]["extensions"]["acme.audit"] = json!({});
    assert!(!load_err(doc).is_empty());
}

#[test]
fn without_fireweed_semantics_umf_reports_the_space_unchecked() {
    let doc = export(&api_example(), &Names::default());
    let mut ctx = umf_core::Context::ordinary();
    let text = serde_json::to_string(&doc).unwrap();
    let value = umf_core::serialization::read_json_value(
        &text.encode_utf16().collect::<Vec<_>>(),
        umf_core::serialization::Format::Json,
        &mut umf_core::Context::catalog(),
    )
    .unwrap();
    let inspection = umf_codegen::CodegenEngine::new(umf())
        .inspect_table_lifecycle(&value, &mut ctx)
        .unwrap();
    let complete = umf_codegen::value::member(&inspection, "complete");
    assert_eq!(complete, Some(&umf_core::Value::Bool(false)));
}

fn config(schema: &str) -> Deployment {
    Deployment {
        schema: schema.into(),
        owner_role: "fw_umf_owner".into(),
        person_role: "fw_umf_person".into(),
        batch_role: "fw_umf_batch".into(),
    }
}

#[test]
fn umf_compiles_the_space_ddl() {
    let loaded = load_value(
        umf(),
        export(&from_yaml::<Schema>(EVERYTHING).unwrap(), &Names::default()),
    )
    .unwrap();
    let bundle = deployment(umf(), &loaded, &config("fw_umf_space")).unwrap();
    let sql: Vec<&str> = bundle["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["sql"].as_str().unwrap())
        .collect();
    let all = sql.join("\n");
    for expected in [
        "CREATE TABLE \"fw_umf_space\".\"q_work\"",
        ") WITH (fillfactor = 80)",
        "\"f_weight\" pg_catalog.\"float8\"",
        "\"f_tags\" pg_catalog.\"text\"[] COLLATE pg_catalog.\"C\"",
        "USING btree (\"due_at\", \"priority\" DESC, \"id\") WHERE",
        "USING gin (\"f_tags\")",
        "GENERATED BY DEFAULT AS IDENTITY",
    ] {
        assert!(all.contains(expected), "missing {expected}\n{all}");
    }
}

// -------------------------------------------------- PostgreSQL 18 catalogs

mod pg {
    use super::*;
    use tokio_postgres::{Client, NoTls};

    async fn connect() -> Option<Client> {
        let url = std::env::var("FIREWEED_PG_TEST_URL").ok()?;
        let (c, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connect");
        tokio::spawn(conn);
        Some(c)
    }

    /// Columns, indexes and storage of a schema's `q_`/`t_` tables, without
    /// object names.
    async fn catalog(c: &Client, schema: &str) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for r in c
            .query(
                "SELECT c.relname, a.attnum, a.attname, format_type(a.atttypid, a.atttypmod),
                        a.attnotnull, coalesce(co.collname, ''), a.attidentity::text
                   FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
                   JOIN pg_namespace n ON n.oid = c.relnamespace
                   LEFT JOIN pg_collation co ON co.oid = a.attcollation AND a.attcollation <> 0
                  WHERE n.nspname = $1 AND c.relkind = 'r' AND c.relname ~ '^[qt]_'
                    AND a.attnum > 0 AND NOT a.attisdropped",
                &[&schema],
            )
            .await
            .unwrap()
        {
            out.insert(format!(
                "column {}.{:02} {} {} notnull={} collation={} identity={}",
                r.get::<_, String>(0),
                r.get::<_, i16>(1),
                r.get::<_, String>(2),
                r.get::<_, String>(3),
                r.get::<_, bool>(4),
                r.get::<_, String>(5),
                r.get::<_, String>(6),
            ));
        }
        for r in c
            .query(
                "SELECT t.relname, i.indisunique,
                        regexp_replace(pg_get_indexdef(i.indexrelid), '^.* USING ', 'USING ')
                   FROM pg_index i JOIN pg_class t ON t.oid = i.indrelid
                   JOIN pg_namespace n ON n.oid = t.relnamespace
                  WHERE n.nspname = $1 AND t.relname ~ '^[qt]_'",
                &[&schema],
            )
            .await
            .unwrap()
        {
            out.insert(format!(
                "index {} unique={} {}",
                r.get::<_, String>(0),
                r.get::<_, bool>(1),
                r.get::<_, String>(2),
            ));
        }
        for r in c
            .query(
                "SELECT c.relname, coalesce(array_to_string(c.reloptions, ','), '')
                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname = $1 AND c.relkind = 'r' AND c.relname ~ '^[qt]_'",
                &[&schema],
            )
            .await
            .unwrap()
        {
            out.insert(format!(
                "storage {} {}",
                r.get::<_, String>(0),
                r.get::<_, String>(1)
            ));
        }
        out
    }

    /// The check constraints on a schema's tables.
    async fn checks(c: &Client, schema: &str) -> Vec<String> {
        c.query(
            "SELECT con.conname FROM pg_constraint con JOIN pg_class t ON t.oid = con.conrelid
               JOIN pg_namespace n ON n.oid = t.relnamespace
              WHERE n.nspname = $1 AND con.contype = 'c' AND t.relname ~ '^[qt]_'",
            &[&schema],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
    }

    async fn install_umf(c: &Client, schema: &str, bundle: &Value) {
        c.batch_execute(&format!(
            "DO $$ BEGIN
               CREATE ROLE fw_umf_owner NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             DO $$ BEGIN
               CREATE ROLE fw_umf_person NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             DO $$ BEGIN
               CREATE ROLE fw_umf_batch NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
             DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;
             CREATE SCHEMA \"{schema}\" AUTHORIZATION fw_umf_owner;"
        ))
        .await
        .unwrap();
        let mut sql = String::from(
            "BEGIN; SET LOCAL ROLE fw_umf_owner; SET LOCAL search_path = pg_catalog;\n",
        );
        for step in bundle["steps"].as_array().unwrap() {
            sql.push_str(step["sql"].as_str().unwrap());
            sql.push_str(";\n");
        }
        sql.push_str("COMMIT;");
        c.batch_execute(&sql).await.unwrap();
    }

    #[tokio::test]
    async fn umf_ddl_creates_the_executor_catalog() {
        let Some(mut c) = connect().await else {
            return;
        };
        c.batch_execute(fireweed_dsl_postgres::CATALOG)
            .await
            .unwrap();
        for (i, (name, s)) in schemas().into_iter().enumerate() {
            let space = format!("umf-conformance-{name}-{}", std::process::id());
            fireweed_dsl_postgres::apply_schema(&mut c, "umf", &space, &s, None)
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let id: i64 = c
                .query_one(
                    "SELECT id FROM fireweed.spaces WHERE tenant = 'umf' AND space = $1",
                    &[&space],
                )
                .await
                .unwrap()
                .get(0);
            let executor = format!("fws_{id}");

            let umf_schema = format!("fw_umf_{i}_{}", std::process::id());
            let loaded = load_value(umf(), export(&s, &Names::default())).unwrap();
            let bundle = deployment(umf(), &loaded, &config(&umf_schema)).unwrap();
            install_umf(&c, &umf_schema, &bundle).await;

            let ours = catalog(&c, &executor).await;
            let theirs = catalog(&c, &umf_schema).await;
            let only_executor: Vec<_> = ours.difference(&theirs).collect();
            let only_umf: Vec<_> = theirs.difference(&ours).collect();
            assert!(
                only_executor.is_empty() && only_umf.is_empty(),
                "{name}: catalogs differ\nexecutor only: {only_executor:#?}\nUMF only: {only_umf:#?}"
            );
            // UMF adds only its domain checks; the executor declares none.
            assert!(checks(&c, &executor).await.is_empty(), "{name}");
            assert!(
                checks(&c, &umf_schema)
                    .await
                    .iter()
                    .all(|n| n.starts_with("umf_d_")),
                "{name}"
            );

            // The executor's claim shape uses UMF's claim index.
            for q in s.queues.keys() {
                let desc = if s.queues[q].priority == fireweed_dsl::PriorityOrder::Desc {
                    " DESC"
                } else {
                    ""
                };
                let tx = c.transaction().await.unwrap();
                tx.batch_execute("SET LOCAL enable_seqscan = off")
                    .await
                    .unwrap();
                let plan: Vec<String> = tx
                    .query(
                        &format!(
                            "EXPLAIN SELECT o.id FROM \"{umf_schema}\".\"q_{q}\" o
                              WHERE o.state IN ('pending', 'leased') AND o.due_at <= now()
                              ORDER BY o.due_at, o.priority{desc}, o.id LIMIT 10"
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| r.get(0))
                    .collect();
                tx.rollback().await.unwrap();
                assert!(
                    plan.iter().any(|l| l.contains(&format!("q_{q}_claim"))),
                    "{name}.{q}: {plan:#?}"
                );
            }
            // Under UMF's row-level security the batch role inserts through
            // the identity column with its table grant alone; people can't.
            for q in s.queues.keys() {
                let insert = format!(
                    "INSERT INTO \"{umf_schema}\".\"q_{q}\"
                       (state, priority, due_at, attempts, version, created_at, updated_at)
                     VALUES ('pending', 0, now(), 0, 1, now(), now()) RETURNING id"
                );
                let tx = c.transaction().await.unwrap();
                tx.batch_execute("SET LOCAL ROLE fw_umf_batch")
                    .await
                    .unwrap();
                let id: i64 = tx.query_one(&insert, &[]).await.unwrap().get(0);
                assert!(id >= 1, "{name}.{q}");
                tx.rollback().await.unwrap();
                let tx = c.transaction().await.unwrap();
                tx.batch_execute("SET LOCAL ROLE fw_umf_person")
                    .await
                    .unwrap();
                assert!(tx.query_one(&insert, &[]).await.is_err(), "{name}.{q}");
                tx.rollback().await.unwrap();
            }
            c.batch_execute(&format!("DROP SCHEMA \"{umf_schema}\" CASCADE"))
                .await
                .unwrap();
        }
    }
}
