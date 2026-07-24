//! Entity schema compilation and validation utilities (ADR-011).
//!
//! Backends call these BEFORE log append, idempotency recording, SQL mutation, or projection apply,
//! so a rejection leaves no trace. Schema-less queues pass through untouched (`validate_entity`
//! returns `Ok(())` when `schema` is `None`). Typed queues validate the entity document against the
//! compiled JSON Schema (Draft 2020-12); a violation returns `EngineError::EntitySchemaViolation`.

use std::sync::Arc;

use axon_esf::CompiledSchema;
use serde_json::Value;

use crate::{EngineError, EngineResult};

/// Compile a raw JSON Schema value (from `EntitySchemaDocument.entity_schema`) into a cached
/// validator. Returns `Err(EngineError::Storage(...))` if the schema itself is malformed.
pub fn compile_entity_schema(schema_value: &Value) -> EngineResult<Arc<CompiledSchema>> {
    CompiledSchema::compile(schema_value)
        .map(Arc::new)
        .map_err(|e| EngineError::Storage(format!("entity_schema compile error: {e}")))
}

/// Validate a single entity document against a compiled schema.
///
/// - `schema = None` → schema-less queue; always `Ok(())`.
/// - `schema = Some(_), entity = None` → the item carries no entity document; allowed on typed
///   queues (the entity document is optional even when a schema is declared).
/// - `schema = Some(cs), entity = Some(doc)` → validate `doc` against `cs`; on mismatch return
///   `Err(EngineError::EntitySchemaViolation(...))` with a human-readable error summary.
pub fn validate_entity(
    schema: Option<&Arc<CompiledSchema>>,
    entity: Option<&Value>,
) -> EngineResult<()> {
    let (Some(cs), Some(doc)) = (schema, entity) else {
        return Ok(());
    };
    cs.validate(doc)
        .map_err(|e| EngineError::EntitySchemaViolation(e.to_string()))
}
