use std::error::Error;
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcurrencyRegistry {
    pub schema_version: u64,
    pub reviewer: String,
    pub reviewed_at: String,
    pub workspace_scope: String,
    pub audits: Vec<ConcurrencyAudit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConcurrencyAudit {
    pub crate_name: String,
    pub no_custom_concurrency: bool,
    pub source_globs_checked: Vec<String>,
    pub dependency_primitives: Vec<String>,
    pub custom_structures: Vec<String>,
    pub loom_tests: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryError {
    message: String,
}

impl RegistryError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for RegistryError {}

pub fn load_registry(path: impl AsRef<Path>) -> Result<ConcurrencyRegistry, RegistryError> {
    let text = std::fs::read_to_string(path.as_ref())
        .map_err(|err| RegistryError::new(format!("failed to read registry: {err}")))?;
    parse_registry(&text)
}

pub fn parse_registry(text: &str) -> Result<ConcurrencyRegistry, RegistryError> {
    let mut schema_version = None;
    let mut reviewer = None;
    let mut reviewed_at = None;
    let mut workspace_scope = None;
    let mut audits = Vec::new();
    let mut current_audit = None;

    for raw_line in text.lines() {
        let line = raw_line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line == "[[audits]]" {
            if let Some(audit) = current_audit.take() {
                audits.push(audit);
            }
            current_audit = Some(ConcurrencyAudit::default());
            continue;
        }

        let (key, value) = line
            .split_once('=')
            .map(|(key, value)| (key.trim(), value.trim()))
            .ok_or_else(|| RegistryError::new(format!("invalid registry line `{line}`")))?;

        if let Some(audit) = current_audit.as_mut() {
            match key {
                "crate" => audit.crate_name = parse_string(value, key)?,
                "no_custom_concurrency" => {
                    audit.no_custom_concurrency = parse_bool(value, key)?;
                }
                "source_globs_checked" => {
                    audit.source_globs_checked = parse_string_array(value, key)?;
                }
                "dependency_primitives" => {
                    audit.dependency_primitives = parse_string_array(value, key)?;
                }
                "custom_structures" => {
                    audit.custom_structures = parse_string_array(value, key)?;
                }
                "loom_tests" => {
                    audit.loom_tests = parse_string_array(value, key)?;
                }
                other => {
                    return Err(RegistryError::new(format!("unknown audit field `{other}`")));
                }
            }
        } else {
            match key {
                "schema_version" => {
                    schema_version = Some(value.parse::<u64>().map_err(|_| {
                        RegistryError::new("schema_version must be an unsigned integer")
                    })?);
                }
                "reviewer" => reviewer = Some(parse_string(value, key)?),
                "reviewed_at" => reviewed_at = Some(parse_string(value, key)?),
                "workspace_scope" => workspace_scope = Some(parse_string(value, key)?),
                other => {
                    return Err(RegistryError::new(format!(
                        "unknown top-level field `{other}`"
                    )));
                }
            }
        }
    }
    if let Some(audit) = current_audit {
        audits.push(audit);
    }

    let registry = ConcurrencyRegistry {
        schema_version: schema_version
            .ok_or_else(|| RegistryError::new("missing schema_version"))?,
        reviewer: reviewer.ok_or_else(|| RegistryError::new("missing reviewer"))?,
        reviewed_at: reviewed_at.ok_or_else(|| RegistryError::new("missing reviewed_at"))?,
        workspace_scope: workspace_scope
            .ok_or_else(|| RegistryError::new("missing workspace_scope"))?,
        audits,
    };
    validate_registry(&registry)?;
    Ok(registry)
}

pub fn validate_registry(registry: &ConcurrencyRegistry) -> Result<(), RegistryError> {
    if registry.schema_version != 1 {
        return Err(RegistryError::new("schema_version must be 1"));
    }
    require_non_empty("reviewer", &registry.reviewer)?;
    require_non_empty("reviewed_at", &registry.reviewed_at)?;
    require_non_empty("workspace_scope", &registry.workspace_scope)?;
    if registry.audits.is_empty() {
        return Err(RegistryError::new("at least one audit is required"));
    }

    for audit in &registry.audits {
        require_non_empty("audits.crate", &audit.crate_name)?;
        if audit.source_globs_checked.is_empty() {
            return Err(RegistryError::new(format!(
                "{} must list source_globs_checked",
                audit.crate_name
            )));
        }
        if audit.no_custom_concurrency && !audit.custom_structures.is_empty() {
            return Err(RegistryError::new(format!(
                "{} cannot set no_custom_concurrency with custom_structures",
                audit.crate_name
            )));
        }
        if !audit.no_custom_concurrency && audit.custom_structures.is_empty() {
            return Err(RegistryError::new(format!(
                "{} must list custom_structures or set no_custom_concurrency",
                audit.crate_name
            )));
        }
        if audit.loom_tests.len() < audit.custom_structures.len() {
            return Err(RegistryError::new(format!(
                "{} must list a loom_tests entry for every custom structure",
                audit.crate_name
            )));
        }
    }
    Ok(())
}

fn parse_string(value: &str, field: &str) -> Result<String, RegistryError> {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| RegistryError::new(format!("{field} must be a non-empty string")))
}

fn parse_bool(value: &str, field: &str) -> Result<bool, RegistryError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(RegistryError::new(format!("{field} must be a bool"))),
    }
}

fn parse_string_array(value: &str, field: &str) -> Result<Vec<String>, RegistryError> {
    let inner = value
        .strip_prefix('[')
        .and_then(|value| value.strip_suffix(']'))
        .ok_or_else(|| RegistryError::new(format!("{field} must be a string array")))?;
    let trimmed = inner.trim();
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }
    trimmed
        .split(',')
        .map(|item| parse_string(item.trim(), field))
        .collect()
}

fn require_non_empty(field: &str, value: &str) -> Result<(), RegistryError> {
    if value.trim().is_empty() {
        return Err(RegistryError::new(format!("{field} must not be empty")));
    }
    Ok(())
}
