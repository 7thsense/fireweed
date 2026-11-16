//! Validate nested ordered-key bytes without allocating decoded scalars.
use fireweed_core::queue_contract::{FieldType, QueueError, QueueResult};

pub(super) fn validate(
    mut bytes: impl Iterator<Item = u8>,
    types: &[FieldType],
    missing: bool,
) -> QueueResult<()> {
    if bytes.next() != Some(1) {
        return Err(unknown("ordered codec version differs"));
    }
    for kind in types {
        match bytes.next() {
            Some(0) if missing => continue,
            Some(1) => (),
            _ => return Err(unknown("invalid ordered presence")),
        }
        let expected = match kind {
            FieldType::Integer => 0,
            FieldType::Float => 1,
            FieldType::Boolean => 2,
            FieldType::Text => 3,
            FieldType::Bytes => 4,
        };
        if bytes.next() != Some(expected) {
            return Err(unknown("ordered component type differs"));
        }
        match kind {
            FieldType::Integer => {
                take::<8>(&mut bytes)?;
            }
            FieldType::Float => {
                let ordered = u64::from_be_bytes(take::<8>(&mut bytes)?);
                let bits = if ordered >> 63 == 1 {
                    ordered ^ (1 << 63)
                } else {
                    !ordered
                };
                let value = f64::from_bits(bits);
                if !value.is_finite() || (value == 0.0 && bits != 0) {
                    return Err(unknown("noncanonical ordered float"));
                }
            }
            FieldType::Boolean => {
                if !matches!(bytes.next(), Some(0 | 1)) {
                    return Err(unknown("invalid ordered boolean"));
                }
            }
            FieldType::Text | FieldType::Bytes => {
                let mut utf8 = Utf8::default();
                loop {
                    let byte = match bytes.next() {
                        Some(0) => match bytes.next() {
                            Some(0) => break,
                            Some(255) => 0,
                            _ => return Err(unknown("invalid ordered literal escape")),
                        },
                        Some(byte) => byte,
                        None => return Err(unknown("unterminated ordered literal")),
                    };
                    if *kind == FieldType::Text {
                        utf8.push(byte)?;
                    }
                }
                if *kind == FieldType::Text && utf8.length != 0 {
                    return Err(unknown("truncated ordered UTF-8"));
                }
            }
        }
    }
    if bytes.next().is_some() {
        return Err(unknown("ordered tuple arity differs"));
    }
    Ok(())
}
fn take<const N: usize>(bytes: &mut impl Iterator<Item = u8>) -> QueueResult<[u8; N]> {
    let mut output = [0; N];
    for byte in &mut output {
        *byte = bytes
            .next()
            .ok_or_else(|| unknown("truncated ordered number"))?;
    }
    Ok(output)
}
#[derive(Default)]
struct Utf8 {
    bytes: [u8; 4],
    length: usize,
}
impl Utf8 {
    fn push(&mut self, byte: u8) -> QueueResult<()> {
        if self.length == self.bytes.len() {
            return Err(unknown("invalid ordered UTF-8"));
        }
        self.bytes[self.length] = byte;
        self.length += 1;
        match std::str::from_utf8(&self.bytes[..self.length]) {
            Ok(_) => self.length = 0,
            Err(error) if error.error_len().is_none() => (),
            Err(_) => return Err(unknown("invalid ordered UTF-8")),
        }
        Ok(())
    }
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source posting: {reason}"))
}
