use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, PartialEq)]
pub struct VerificationLedger {
    pub rows: Vec<LedgerRow>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LedgerRow {
    pub ac_ids: Vec<String>,
    pub inv_ids: Vec<String>,
    pub command: String,
    pub exit_status: i64,
    pub backend_profile: String,
    pub scale: String,
    pub seed: u64,
    pub environment: BTreeMap<String, JsonValue>,
    pub suite: String,
    pub measurements: BTreeMap<String, JsonValue>,
    pub pass_bar: BTreeMap<String, JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerError {
    pub line: Option<usize>,
    pub field: Option<String>,
    pub message: String,
}

impl LedgerError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            line: None,
            field: None,
            message: message.into(),
        }
    }

    fn with_line(mut self, line: usize) -> Self {
        self.line = Some(line);
        self
    }

    fn with_field(mut self, field: impl Into<String>) -> Self {
        self.field = Some(field.into());
        self
    }

    fn missing_field(field: &str) -> Self {
        Self::new("missing required field").with_field(field)
    }

    fn invalid_field(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::new(message).with_field(field)
    }
}

impl fmt::Display for LedgerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(line) = self.line {
            write!(f, "line {line}: ")?;
        }
        if let Some(field) = &self.field {
            write!(f, "field `{field}`: ")?;
        }
        f.write_str(&self.message)
    }
}

impl Error for LedgerError {}

#[derive(Debug, Clone, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<JsonValue>),
    Object(BTreeMap<String, JsonValue>),
}

impl JsonValue {
    fn kind(&self) -> &'static str {
        match self {
            JsonValue::Null => "null",
            JsonValue::Bool(_) => "boolean",
            JsonValue::Number(_) => "number",
            JsonValue::String(_) => "string",
            JsonValue::Array(_) => "array",
            JsonValue::Object(_) => "object",
        }
    }
}

pub fn validate_ledger_file(path: impl AsRef<Path>) -> Result<VerificationLedger, LedgerError> {
    let text = fs::read_to_string(path.as_ref())
        .map_err(|err| LedgerError::new(format!("failed to read ledger: {err}")))?;
    validate_ledger_text(&text)
}

pub fn validate_ledger_text(text: &str) -> Result<VerificationLedger, LedgerError> {
    let mut rows = Vec::new();

    for (index, line) in text.lines().enumerate() {
        let line_number = index + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let mut parser = JsonParser::new(trimmed);
        let value = parser
            .parse_value()
            .map_err(|err| err.with_line(line_number))?;
        parser
            .expect_end()
            .map_err(|err| err.with_line(line_number))?;
        let row = LedgerRow::from_json(value).map_err(|err| err.with_line(line_number))?;
        rows.push(row);
    }

    if rows.is_empty() {
        return Err(LedgerError::new("ledger does not contain any rows"));
    }

    Ok(VerificationLedger { rows })
}

pub fn run_from_args<I, S>(args: I) -> Result<usize, LedgerError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let parsed = CliArgs::parse(args)?;
    if !parsed.strict {
        return Err(LedgerError::new(
            "missing required `--strict` flag for strict ledger validation",
        ));
    }

    let ledger = validate_ledger_file(&parsed.ledger)?;
    Ok(ledger.rows.len())
}

struct CliArgs {
    strict: bool,
    ledger: std::path::PathBuf,
}

impl CliArgs {
    fn parse<I, S>(args: I) -> Result<Self, LedgerError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut strict = false;
        let mut ledger = None;
        let mut iter = args.into_iter().map(Into::into).peekable();

        let _program = iter.next();
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--strict" => {
                    strict = true;
                }
                "--ledger" => {
                    let value = iter
                        .next()
                        .ok_or_else(|| LedgerError::new("missing value for `--ledger`"))?;
                    ledger = Some(std::path::PathBuf::from(value));
                }
                "--help" | "-h" => {
                    return Err(LedgerError::new(
                        "usage: pqueue-verify-ledger --strict --ledger <path>",
                    ));
                }
                other => {
                    return Err(LedgerError::new(format!("unrecognized argument `{other}`")));
                }
            }
        }

        let ledger = ledger.ok_or_else(|| LedgerError::new("missing required `--ledger` path"))?;
        Ok(Self { strict, ledger })
    }
}

impl LedgerRow {
    fn from_json(value: JsonValue) -> Result<Self, LedgerError> {
        let object = match value {
            JsonValue::Object(object) => object,
            other => {
                return Err(LedgerError::new(format!(
                    "ledger rows must be JSON objects, found {}",
                    other.kind()
                )));
            }
        };

        let row = Self {
            ac_ids: required_string_array(&object, "ac_ids")?,
            inv_ids: required_string_array(&object, "inv_ids")?,
            command: required_string_field(&object, "command")?,
            exit_status: required_i64_field(&object, "exit_status")?,
            backend_profile: required_string_field(&object, "backend_profile")?,
            scale: required_string_field(&object, "scale")?,
            seed: required_u64_field(&object, "seed")?,
            environment: required_object_field(&object, "environment")?,
            suite: required_string_field(&object, "suite")?,
            measurements: required_object_field(&object, "measurements")?,
            pass_bar: required_object_field(&object, "pass_bar")?,
        };
        row.validate_semantics()?;
        Ok(row)
    }

    fn validate_semantics(&self) -> Result<(), LedgerError> {
        if self.suite.starts_with("performance_") {
            validate_performance_row(self)?;
        }
        if self.suite == "object_log_commit_recovery_tests" {
            validate_object_log_e3_row(self)?;
        }
        Ok(())
    }
}

fn validate_performance_row(row: &LedgerRow) -> Result<(), LedgerError> {
    required_nested_string_field(&row.environment, "environment", "instance_class")?;
    required_nested_string_field(&row.measurements, "measurements", "deployment_shape")?;
    required_nested_string_field(&row.measurements, "measurements", "workload_envelope")?;
    required_nested_string_field(&row.measurements, "measurements", "query_plan")?;
    let evidence_ids =
        required_nested_string_array(&row.measurements, "measurements", "tp002_evidence_ids")?;
    if !evidence_ids.iter().any(|id| id == "E0") {
        return Err(LedgerError::invalid_field(
            "measurements.tp002_evidence_ids",
            "performance rows must cite TP-002 E0",
        ));
    }

    required_nested_u64_field(&row.measurements, "measurements", "items_per_hour")?;
    required_nested_u64_field(&row.measurements, "measurements", "p95_ms")?;
    required_nested_u64_field(&row.measurements, "measurements", "p99_ms")?;
    required_nested_u64_field(&row.pass_bar, "pass_bar", "e0_floor_items_per_hour")?;
    required_nested_u64_field(&row.pass_bar, "pass_bar", "p95_ms_lt")?;
    required_nested_u64_field(&row.pass_bar, "pass_bar", "p99_ms_lt")?;
    if row.suite == "performance_multi_shard_scale_out_tests" {
        validate_multi_shard_scale_out_row(row, &evidence_ids)?;
    }
    Ok(())
}

fn validate_multi_shard_scale_out_row(
    row: &LedgerRow,
    evidence_ids: &[String],
) -> Result<(), LedgerError> {
    if row.backend_profile != "object_log_sqlite_projection" {
        return Err(LedgerError::invalid_field(
            "backend_profile",
            "E2 scale-out headline rows must use object_log_sqlite_projection",
        ));
    }
    if !evidence_ids.iter().any(|id| id == "E2") {
        return Err(LedgerError::invalid_field(
            "measurements.tp002_evidence_ids",
            "E2 scale-out rows must cite TP-002 E2",
        ));
    }

    let shard_counts =
        required_nested_u64_array(&row.measurements, "measurements", "shard_counts")?;
    if shard_counts != [2, 4, 8] {
        return Err(LedgerError::invalid_field(
            "measurements.shard_counts",
            "E2 scale-out rows must cover shard counts 2, 4, and 8",
        ));
    }

    let throughputs = required_nested_u64_array(
        &row.measurements,
        "measurements",
        "items_per_hour_by_shard_count",
    )?;
    if throughputs.len() != shard_counts.len() {
        return Err(LedgerError::invalid_field(
            "measurements.items_per_hour_by_shard_count",
            "throughput series must align with shard_counts",
        ));
    }
    if !throughputs.windows(2).all(|pair| pair[1] >= pair[0]) {
        return Err(LedgerError::invalid_field(
            "measurements.items_per_hour_by_shard_count",
            "E2 scale-out throughput must be monotonic non-decreasing",
        ));
    }

    let single_deployment_ceiling = required_nested_u64_field(
        &row.measurements,
        "measurements",
        "single_deployment_ceiling_items_per_hour",
    )?;
    let eight_shard = *throughputs
        .last()
        .ok_or_else(|| LedgerError::missing_field("measurements.items_per_hour_by_shard_count"))?;
    let required_at_eight =
        required_nested_u64_field(&row.pass_bar, "pass_bar", "eight_shard_min_items_per_hour")?;
    if required_at_eight < single_deployment_ceiling.saturating_mul(4) {
        return Err(LedgerError::invalid_field(
            "pass_bar.eight_shard_min_items_per_hour",
            "E2 pass bar must require at least 4x the single-deployment ceiling",
        ));
    }
    if eight_shard < required_at_eight {
        return Err(LedgerError::invalid_field(
            "measurements.items_per_hour_by_shard_count",
            "8-shard throughput must satisfy the E2 4x pass bar",
        ));
    }

    let scale_out_multiple = required_nested_u64_field(
        &row.measurements,
        "measurements",
        "scale_out_multiple_at_8_shards_x100",
    )?;
    let minimum_multiple = required_nested_u64_field(
        &row.pass_bar,
        "pass_bar",
        "minimum_scale_out_multiple_at_8_shards_x100",
    )?;
    if scale_out_multiple < minimum_multiple {
        return Err(LedgerError::invalid_field(
            "measurements.scale_out_multiple_at_8_shards_x100",
            "E2 scale-out multiple must satisfy the pass bar",
        ));
    }

    let progress_violations = required_nested_u64_field(
        &row.measurements,
        "measurements",
        "progress_bound_violations",
    )?;
    if progress_violations != 0 {
        return Err(LedgerError::invalid_field(
            "measurements.progress_bound_violations",
            "E2 scale-out rows must report zero progress-bound violations",
        ));
    }
    if !required_nested_bool_field(
        &row.measurements,
        "measurements",
        "independent_storage_units",
    )? {
        return Err(LedgerError::invalid_field(
            "measurements.independent_storage_units",
            "E2 scale-out rows must use independent storage units",
        ));
    }
    if !required_nested_bool_field(
        &row.measurements,
        "measurements",
        "queue_global_progress_checked",
    )? {
        return Err(LedgerError::invalid_field(
            "measurements.queue_global_progress_checked",
            "E2 scale-out rows must check queue-global progress",
        ));
    }
    if !required_nested_bool_field(
        &row.pass_bar,
        "pass_bar",
        "monotonic_non_decreasing_required",
    )? {
        return Err(LedgerError::invalid_field(
            "pass_bar.monotonic_non_decreasing_required",
            "E2 pass bar must require monotonic non-decreasing throughput",
        ));
    }
    Ok(())
}

fn validate_object_log_e3_row(row: &LedgerRow) -> Result<(), LedgerError> {
    if row.backend_profile != "object_log_sqlite_projection" {
        return Err(LedgerError::invalid_field(
            "backend_profile",
            "object-log E3 rows must use object_log_sqlite_projection",
        ));
    }
    required_nested_string_field(&row.environment, "environment", "instance_class")?;
    required_nested_string_field(&row.measurements, "measurements", "deployment_shape")?;
    required_nested_string_field(&row.measurements, "measurements", "workload_envelope")?;
    let evidence_ids =
        required_nested_string_array(&row.measurements, "measurements", "tp002_evidence_ids")?;
    for required in ["E0", "E3"] {
        if !evidence_ids.iter().any(|id| id == required) {
            return Err(LedgerError::invalid_field(
                "measurements.tp002_evidence_ids",
                format!("object-log E3 rows must cite TP-002 {required}"),
            ));
        }
    }

    let items_per_hour =
        required_nested_u64_field(&row.measurements, "measurements", "items_per_hour")?;
    let p95_ms = required_nested_u64_field(&row.measurements, "measurements", "p95_ms")?;
    let p99_ms = required_nested_u64_field(&row.measurements, "measurements", "p99_ms")?;
    required_nested_u64_field(&row.measurements, "measurements", "segment_size_commands")?;
    required_nested_u64_field(&row.measurements, "measurements", "segment_max_latency_ms")?;
    let object_log_cost = required_nested_u64_field(
        &row.measurements,
        "measurements",
        "durable_commit_cost_per_billion_commands_usd",
    )?;
    let postgres_cost = required_nested_u64_field(
        &row.measurements,
        "measurements",
        "postgres_native_cost_per_billion_commands_usd",
    )?;
    let recovery_items =
        required_nested_u64_field(&row.measurements, "measurements", "recovery_items")?;
    let recovery_ms = required_nested_u64_field(&row.measurements, "measurements", "recovery_ms")?;
    required_nested_u64_field(&row.measurements, "measurements", "acked_commands")?;
    required_nested_u64_field(
        &row.measurements,
        "measurements",
        "manifest_fence_rejections",
    )?;
    required_nested_u64_field(
        &row.measurements,
        "measurements",
        "fallback_fence_rejections",
    )?;

    let e0_floor = required_nested_u64_field(&row.pass_bar, "pass_bar", "e0_floor_items_per_hour")?;
    let p95_lt = required_nested_u64_field(&row.pass_bar, "pass_bar", "p95_ms_lt")?;
    let p99_lt = required_nested_u64_field(&row.pass_bar, "pass_bar", "p99_ms_lt")?;
    let recovery_budget =
        required_nested_u64_field(&row.pass_bar, "pass_bar", "recovery_window_budget_ms")?;

    if items_per_hour < e0_floor {
        return Err(LedgerError::invalid_field(
            "measurements.items_per_hour",
            "object-log E3 throughput must meet the E0 floor",
        ));
    }
    if p95_ms >= p95_lt {
        return Err(LedgerError::invalid_field(
            "measurements.p95_ms",
            "object-log E3 p95 must be below the pass bar",
        ));
    }
    if p99_ms >= p99_lt {
        return Err(LedgerError::invalid_field(
            "measurements.p99_ms",
            "object-log E3 p99 must be below the pass bar",
        ));
    }
    if object_log_cost >= postgres_cost {
        return Err(LedgerError::invalid_field(
            "measurements.durable_commit_cost_per_billion_commands_usd",
            "object-log E3 cost must beat postgres_native at high sustained volume",
        ));
    }
    if recovery_items < 10_000_000 {
        return Err(LedgerError::invalid_field(
            "measurements.recovery_items",
            "object-log E3 recovery must cover a 10M-item projection",
        ));
    }
    if recovery_ms > recovery_budget {
        return Err(LedgerError::invalid_field(
            "measurements.recovery_ms",
            "object-log E3 recovery must fit within the recovery-window budget",
        ));
    }
    Ok(())
}

fn required_field<'a>(
    object: &'a BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<&'a JsonValue, LedgerError> {
    object
        .get(field)
        .ok_or_else(|| LedgerError::missing_field(field))
}

fn required_string_field(
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<String, LedgerError> {
    let value = required_field(object, field)?;
    match value {
        JsonValue::String(text) if !text.trim().is_empty() => Ok(text.clone()),
        JsonValue::String(_) => Err(LedgerError::invalid_field(
            field,
            "string field must not be empty",
        )),
        other => Err(LedgerError::invalid_field(
            field,
            format!("expected string, found {}", other.kind()),
        )),
    }
}

fn required_string_array(
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<Vec<String>, LedgerError> {
    let value = required_field(object, field)?;
    let items = match value {
        JsonValue::Array(items) => items,
        other => {
            return Err(LedgerError::invalid_field(
                field,
                format!("expected array, found {}", other.kind()),
            ));
        }
    };

    if items.is_empty() {
        return Err(LedgerError::invalid_field(
            field,
            "array field must not be empty",
        ));
    }

    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonValue::String(text) if !text.trim().is_empty() => values.push(text.clone()),
            JsonValue::String(_) => {
                return Err(LedgerError::invalid_field(
                    field,
                    "array entries must not be empty",
                ));
            }
            other => {
                return Err(LedgerError::invalid_field(
                    field,
                    format!("expected string entries, found {}", other.kind()),
                ));
            }
        }
    }

    Ok(values)
}

fn required_i64_field(
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<i64, LedgerError> {
    let value = required_field(object, field)?;
    match value {
        JsonValue::Number(number) => number
            .parse::<i64>()
            .map_err(|_| LedgerError::invalid_field(field, "expected 64-bit signed integer")),
        other => Err(LedgerError::invalid_field(
            field,
            format!("expected number, found {}", other.kind()),
        )),
    }
}

fn required_u64_field(
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<u64, LedgerError> {
    let value = required_field(object, field)?;
    match value {
        JsonValue::Number(number) => number
            .parse::<u64>()
            .map_err(|_| LedgerError::invalid_field(field, "expected 64-bit unsigned integer")),
        other => Err(LedgerError::invalid_field(
            field,
            format!("expected number, found {}", other.kind()),
        )),
    }
}

fn required_object_field(
    object: &BTreeMap<String, JsonValue>,
    field: &str,
) -> Result<BTreeMap<String, JsonValue>, LedgerError> {
    let value = required_field(object, field)?;
    match value {
        JsonValue::Object(entries) if !entries.is_empty() => Ok(entries.clone()),
        JsonValue::Object(_) => Err(LedgerError::invalid_field(
            field,
            "object field must not be empty",
        )),
        other => Err(LedgerError::invalid_field(
            field,
            format!("expected object, found {}", other.kind()),
        )),
    }
}

fn required_nested_string_field(
    object: &BTreeMap<String, JsonValue>,
    parent: &str,
    field: &str,
) -> Result<String, LedgerError> {
    let full_field = format!("{parent}.{field}");
    match object.get(field) {
        Some(JsonValue::String(text)) if !text.trim().is_empty() => Ok(text.clone()),
        Some(JsonValue::String(_)) => Err(LedgerError::invalid_field(
            full_field,
            "string field must not be empty",
        )),
        Some(other) => Err(LedgerError::invalid_field(
            full_field,
            format!("expected string, found {}", other.kind()),
        )),
        None => Err(LedgerError::missing_field(&full_field)),
    }
}

fn required_nested_string_array(
    object: &BTreeMap<String, JsonValue>,
    parent: &str,
    field: &str,
) -> Result<Vec<String>, LedgerError> {
    let full_field = format!("{parent}.{field}");
    let items = match object.get(field) {
        Some(JsonValue::Array(items)) => items,
        Some(other) => {
            return Err(LedgerError::invalid_field(
                full_field,
                format!("expected array, found {}", other.kind()),
            ));
        }
        None => return Err(LedgerError::missing_field(&full_field)),
    };

    if items.is_empty() {
        return Err(LedgerError::invalid_field(
            full_field,
            "array field must not be empty",
        ));
    }

    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonValue::String(text) if !text.trim().is_empty() => values.push(text.clone()),
            JsonValue::String(_) => {
                return Err(LedgerError::invalid_field(
                    full_field,
                    "array entries must not be empty",
                ));
            }
            other => {
                return Err(LedgerError::invalid_field(
                    full_field,
                    format!("expected string entries, found {}", other.kind()),
                ));
            }
        }
    }

    Ok(values)
}

fn required_nested_u64_field(
    object: &BTreeMap<String, JsonValue>,
    parent: &str,
    field: &str,
) -> Result<u64, LedgerError> {
    let full_field = format!("{parent}.{field}");
    match object.get(field) {
        Some(JsonValue::Number(number)) => number.parse::<u64>().map_err(|_| {
            LedgerError::invalid_field(full_field, "expected 64-bit unsigned integer")
        }),
        Some(other) => Err(LedgerError::invalid_field(
            full_field,
            format!("expected number, found {}", other.kind()),
        )),
        None => Err(LedgerError::missing_field(&full_field)),
    }
}

fn required_nested_bool_field(
    object: &BTreeMap<String, JsonValue>,
    parent: &str,
    field: &str,
) -> Result<bool, LedgerError> {
    let full_field = format!("{parent}.{field}");
    match object.get(field) {
        Some(JsonValue::Bool(value)) => Ok(*value),
        Some(other) => Err(LedgerError::invalid_field(
            full_field,
            format!("expected boolean, found {}", other.kind()),
        )),
        None => Err(LedgerError::missing_field(&full_field)),
    }
}

fn required_nested_u64_array(
    object: &BTreeMap<String, JsonValue>,
    parent: &str,
    field: &str,
) -> Result<Vec<u64>, LedgerError> {
    let full_field = format!("{parent}.{field}");
    let items = match object.get(field) {
        Some(JsonValue::Array(items)) => items,
        Some(other) => {
            return Err(LedgerError::invalid_field(
                full_field,
                format!("expected array, found {}", other.kind()),
            ));
        }
        None => return Err(LedgerError::missing_field(&full_field)),
    };

    if items.is_empty() {
        return Err(LedgerError::invalid_field(
            full_field,
            "array field must not be empty",
        ));
    }

    let mut values = Vec::with_capacity(items.len());
    for item in items {
        match item {
            JsonValue::Number(number) => values.push(number.parse::<u64>().map_err(|_| {
                LedgerError::invalid_field(&full_field, "expected 64-bit unsigned integers")
            })?),
            other => {
                return Err(LedgerError::invalid_field(
                    &full_field,
                    format!("expected number entries, found {}", other.kind()),
                ));
            }
        }
    }

    Ok(values)
}

struct JsonParser<'a> {
    input: &'a [u8],
    index: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            index: 0,
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, LedgerError> {
        self.skip_ws();
        let Some(byte) = self.peek() else {
            return Err(LedgerError::new("unexpected end of input"));
        };

        match byte {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => self.parse_string().map(JsonValue::String),
            b't' => self.parse_literal("true", JsonValue::Bool(true)),
            b'f' => self.parse_literal("false", JsonValue::Bool(false)),
            b'n' => self.parse_literal("null", JsonValue::Null),
            b'-' | b'0'..=b'9' => self.parse_number(),
            other => Err(LedgerError::new(format!(
                "unexpected character `{}`",
                other as char
            ))),
        }
    }

    fn expect_end(&mut self) -> Result<(), LedgerError> {
        self.skip_ws();
        if self.peek().is_some() {
            return Err(LedgerError::new("unexpected trailing data"));
        }
        Ok(())
    }

    fn parse_object(&mut self) -> Result<JsonValue, LedgerError> {
        self.expect_byte(b'{')?;
        self.skip_ws();
        let mut entries = BTreeMap::new();

        if self.consume_if(b'}') {
            return Ok(JsonValue::Object(entries));
        }

        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            entries.insert(key, value);
            self.skip_ws();
            if self.consume_if(b'}') {
                break;
            }
            self.expect_byte(b',')?;
        }

        Ok(JsonValue::Object(entries))
    }

    fn parse_array(&mut self) -> Result<JsonValue, LedgerError> {
        self.expect_byte(b'[')?;
        self.skip_ws();
        let mut items = Vec::new();

        if self.consume_if(b']') {
            return Ok(JsonValue::Array(items));
        }

        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_ws();
            if self.consume_if(b']') {
                break;
            }
            self.expect_byte(b',')?;
        }

        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, LedgerError> {
        self.expect_byte(b'"')?;
        let mut output = String::new();

        while let Some(byte) = self.next() {
            match byte {
                b'"' => return Ok(output),
                b'\\' => {
                    let escaped = self
                        .next()
                        .ok_or_else(|| LedgerError::new("unterminated escape sequence"))?;
                    match escaped {
                        b'"' => output.push('"'),
                        b'\\' => output.push('\\'),
                        b'/' => output.push('/'),
                        b'b' => output.push('\u{0008}'),
                        b'f' => output.push('\u{000c}'),
                        b'n' => output.push('\n'),
                        b'r' => output.push('\r'),
                        b't' => output.push('\t'),
                        b'u' => {
                            return Err(LedgerError::new(
                                "unicode escape sequences are not supported in ledger fixtures",
                            ));
                        }
                        other => {
                            return Err(LedgerError::new(format!(
                                "unsupported escape sequence `\\{}`",
                                other as char
                            )));
                        }
                    }
                }
                other => output.push(other as char),
            }
        }

        Err(LedgerError::new("unterminated string"))
    }

    fn parse_number(&mut self) -> Result<JsonValue, LedgerError> {
        let start = self.index;

        if self.consume_if(b'-') && !self.peek_is_digit() {
            return Err(LedgerError::new("invalid number"));
        }

        self.consume_digits();
        if self.consume_if(b'.') || self.consume_if(b'e') || self.consume_if(b'E') {
            return Err(LedgerError::new(
                "floating-point and exponent numbers are not supported in ledger fixtures",
            ));
        }

        let end = self.index;
        if end == start {
            return Err(LedgerError::new("invalid number"));
        }

        let token = std::str::from_utf8(&self.input[start..end])
            .map_err(|_| LedgerError::new("invalid UTF-8 in number"))?;
        Ok(JsonValue::Number(token.to_string()))
    }

    fn parse_literal(&mut self, literal: &str, value: JsonValue) -> Result<JsonValue, LedgerError> {
        for expected in literal.bytes() {
            self.expect_byte(expected)?;
        }
        Ok(value)
    }

    fn consume_digits(&mut self) {
        while self.peek_is_digit() {
            self.index += 1;
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.index += 1;
        }
    }

    fn consume_if(&mut self, expected: u8) -> bool {
        if self.peek() == Some(expected) {
            self.index += 1;
            true
        } else {
            false
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), LedgerError> {
        match self.next() {
            Some(found) if found == expected => Ok(()),
            Some(found) => Err(LedgerError::new(format!(
                "expected `{}` but found `{}`",
                expected as char, found as char
            ))),
            None => Err(LedgerError::new("unexpected end of input")),
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.index).copied()
    }

    fn peek_is_digit(&self) -> bool {
        matches!(self.peek(), Some(b'0'..=b'9'))
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.index += 1;
        Some(byte)
    }
}
