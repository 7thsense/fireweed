//! Values, value domains, canonical JSON and RFC 3339 (API-006 §Values,
//! §Canonical JSON, §Casts).

use std::collections::BTreeSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::ids::UtcMillis;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ScalarType {
    Bool,
    Int,
    Float,
    Text,
    Bytes,
    Timestamp,
    Json,
    TextSet,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    /// A null carries its type.
    Null(ScalarType),
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Bytes),
    Timestamp(UtcMillis),
    Json(serde_json::Value),
    TextSet(BTreeSet<String>),
}

/// Largest magnitude of a `Float` value.
pub const FLOAT_MAX_ABS: f64 = 1e290;
/// Earliest `Timestamp`: 0000-01-01T00:00:00.000Z.
pub const TIMESTAMP_MIN: i64 = -62_167_219_200_000;
/// Latest `Timestamp`: 9999-12-31T23:59:59.999Z.
pub const TIMESTAMP_MAX: i64 = 253_402_300_799_999;

impl Value {
    pub fn ty(&self) -> ScalarType {
        match self {
            Value::Null(t) => *t,
            Value::Bool(_) => ScalarType::Bool,
            Value::Int(_) => ScalarType::Int,
            Value::Float(_) => ScalarType::Float,
            Value::Text(_) => ScalarType::Text,
            Value::Bytes(_) => ScalarType::Bytes,
            Value::Timestamp(_) => ScalarType::Timestamp,
            Value::Json(_) => ScalarType::Json,
            Value::TextSet(_) => ScalarType::TextSet,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null(_))
    }

    pub fn text(s: impl Into<String>) -> Self {
        Value::Text(s.into())
    }

    /// Approximate stored size, used for `max_value_bytes` and `max_body_bytes`.
    pub fn byte_size(&self) -> u64 {
        match self {
            Value::Null(_) | Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) | Value::Timestamp(_) => 8,
            Value::Text(s) => s.len() as u64,
            Value::Bytes(b) => b.len() as u64,
            Value::Json(j) => serde_json::to_vec(j)
                .map(|v| v.len() as u64)
                .unwrap_or(u64::MAX),
            Value::TextSet(s) => s.iter().map(|x| x.len() as u64 + 1).sum(),
        }
    }
}

pub fn float_in_domain(f: f64) -> bool {
    f.is_finite() && f.abs() <= FLOAT_MAX_ABS
}

pub fn timestamp_in_domain(ms: i64) -> bool {
    (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&ms)
}

fn text_ok(s: &str) -> bool {
    !s.contains('\0')
}

/// Why a value is outside its domain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DomainError {
    FloatOutOfRange,
    TimestampOutOfRange,
    NulCharacter,
    JsonNumber,
}

impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            DomainError::FloatOutOfRange => "float outside the Float domain",
            DomainError::TimestampOutOfRange => "timestamp outside years 0000 to 9999",
            DomainError::NulCharacter => "text contains U+0000",
            DomainError::JsonNumber => "JSON number outside the canonical domain",
        };
        f.write_str(s)
    }
}

/// Checks a value against its domain (API-006 §Values).
pub fn check_domain(v: &Value) -> Result<(), DomainError> {
    match v {
        Value::Float(f) if !float_in_domain(*f) => Err(DomainError::FloatOutOfRange),
        Value::Timestamp(t) if !timestamp_in_domain(t.0) => Err(DomainError::TimestampOutOfRange),
        Value::Text(s) if !text_ok(s) => Err(DomainError::NulCharacter),
        Value::TextSet(s) if s.iter().any(|x| !text_ok(x)) => Err(DomainError::NulCharacter),
        Value::Json(j) => normalize_json(j).map(|_| ()),
        _ => Ok(()),
    }
}

/// Normalizes a JSON value (API-006 §Canonical JSON rule 2) and checks its
/// strings and keys for U+0000.
pub fn normalize_json(v: &serde_json::Value) -> Result<serde_json::Value, DomainError> {
    use serde_json::Value as J;
    Ok(match v {
        J::Null | J::Bool(_) => v.clone(),
        J::String(s) => {
            if !text_ok(s) {
                return Err(DomainError::NulCharacter);
            }
            v.clone()
        }
        J::Number(n) => J::Number(normalize_number(n)?),
        J::Array(a) => J::Array(a.iter().map(normalize_json).collect::<Result<_, _>>()?),
        J::Object(o) => {
            let mut out = serde_json::Map::new();
            for (k, x) in o {
                if !text_ok(k) {
                    return Err(DomainError::NulCharacter);
                }
                out.insert(k.clone(), normalize_json(x)?);
            }
            J::Object(out)
        }
    })
}

fn normalize_number(n: &serde_json::Number) -> Result<serde_json::Number, DomainError> {
    if let Some(i) = n.as_i64() {
        return Ok(i.into());
    }
    if let Some(u) = n.as_u64() {
        // Above i64::MAX: keep as f64 only if exact.
        let f = u as f64;
        if f < 18_446_744_073_709_551_616.0 && f as u64 == u && float_in_domain(f) {
            return serde_json::Number::from_f64(f).ok_or(DomainError::JsonNumber);
        }
        return Err(DomainError::JsonNumber);
    }
    let f = n.as_f64().ok_or(DomainError::JsonNumber)?;
    if f.fract() == 0.0 && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&f)
    {
        return Ok((f as i64).into());
    }
    if !float_in_domain(f) {
        return Err(DomainError::JsonNumber);
    }
    serde_json::Number::from_f64(f).ok_or(DomainError::JsonNumber)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 15) as usize] as char);
    }
    s
}

/// Lowercase hex of bytes.
pub fn to_hex(bytes: &[u8]) -> String {
    hex(bytes)
}

/// Encodes a value as JSON (API-006 §Canonical JSON, last paragraph).
pub fn value_to_json(v: &Value) -> serde_json::Value {
    use base64::Engine as _;
    use serde_json::Value as J;
    match v {
        Value::Null(_) => J::Null,
        Value::Bool(b) => J::Bool(*b),
        Value::Int(i) => J::from(*i),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(J::Number)
            .unwrap_or(J::Null),
        Value::Text(s) => J::String(s.clone()),
        Value::Bytes(b) => J::String(base64::engine::general_purpose::STANDARD.encode(b)),
        Value::Timestamp(t) => J::String(format_rfc3339(t.0)),
        Value::Json(j) => j.clone(),
        Value::TextSet(s) => J::Array(s.iter().cloned().map(J::String).collect()),
    }
}

// ---- civil time (proleptic Gregorian), after H. Hinnant's algorithms ----

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `YYYY-MM-DDTHH:MM:SS.sssZ` for a timestamp in the `Timestamp` domain.
pub fn format_rfc3339(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let (h, mi, s, milli) = (
        rem / 3_600_000,
        (rem / 60_000) % 60,
        (rem / 1000) % 60,
        rem % 1000,
    );
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{milli:03}Z")
}

fn digits(s: &[u8], n: usize) -> Option<i64> {
    if s.len() < n || !s[..n].iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(&s[..n]).ok()?.parse().ok()
}

/// Parses RFC 3339 (API-006 §Casts): any offset, converted to UTC;
/// fractional seconds truncated to milliseconds; `:60` and out-of-domain
/// results yield `None`.
pub fn parse_rfc3339(text: &str) -> Option<i64> {
    let b = text.as_bytes();
    if b.len() < 20 {
        return None;
    }
    let y = digits(b, 4)?;
    if b[4] != b'-' {
        return None;
    }
    let mo = digits(&b[5..], 2)?;
    if b[7] != b'-' {
        return None;
    }
    let d = digits(&b[8..], 2)?;
    if !matches!(b[10], b'T' | b't' | b' ') {
        return None;
    }
    let h = digits(&b[11..], 2)?;
    if b[13] != b':' {
        return None;
    }
    let mi = digits(&b[14..], 2)?;
    if b[16] != b':' {
        return None;
    }
    let s = digits(&b[17..], 2)?;
    let mut i = 19;
    let mut millis = 0i64;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
        let frac = &text[start..i];
        let first3: String = frac.chars().chain("000".chars()).take(3).collect();
        millis = first3.parse().ok()?;
    }
    let offset_min = match b.get(i)? {
        b'Z' | b'z' if i + 1 == b.len() => 0,
        sign @ (b'+' | b'-') if i + 6 == b.len() && b[i + 3] == b':' => {
            let oh = digits(&b[i + 1..], 2)?;
            let om = digits(&b[i + 4..], 2)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = oh * 60 + om;
            if *sign == b'+' { o } else { -o }
        }
        _ => return None,
    };
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || s > 59 {
        return None;
    }
    let dim = match mo {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
    };
    if d > dim {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    let ms = ((days * 24 + h) * 60 + mi - offset_min) * 60_000 + s * 1000 + millis;
    timestamp_in_domain(ms).then_some(ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_round_trip() {
        for ms in [
            0,
            1,
            -1,
            86_399_999,
            TIMESTAMP_MIN,
            TIMESTAMP_MAX,
            1_700_000_000_123,
        ] {
            assert_eq!(parse_rfc3339(&format_rfc3339(ms)), Some(ms), "{ms}");
        }
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(parse_rfc3339("1970-01-01T01:00:00.9999+01:00"), Some(999));
        assert_eq!(parse_rfc3339("2016-12-31T23:59:60Z"), None);
    }

    #[test]
    fn json_numbers_normalize() {
        let j: serde_json::Value = serde_json::from_str("{\"a\":3.0,\"b\":[1.5,-0.0]}").unwrap();
        assert_eq!(
            normalize_json(&j).unwrap(),
            serde_json::json!({"a":3,"b":[1.5,0]})
        );
        let big: serde_json::Value = serde_json::from_str("18446744073709551615").unwrap();
        assert_eq!(normalize_json(&big), Err(DomainError::JsonNumber));
    }
}
