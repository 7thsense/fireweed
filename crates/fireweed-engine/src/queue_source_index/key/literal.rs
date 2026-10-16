//! Canonical borrowed source-key literals. Parsing never allocates; callers fund
//! decoded copies before requesting them. A literal is not source authority.
use fireweed_core::queue_contract::{QueueError, QueueResult};

#[derive(Clone, Copy)]
pub(super) struct Literal<'a> {
    encoded: &'a [u8],
    decoded_len: usize,
}
impl<'a> Literal<'a> {
    /// Consume one escaped literal, leaving any following key components intact.
    pub(super) fn parse(input: &'a [u8]) -> QueueResult<(Self, &'a [u8])> {
        let mut position = 0;
        let mut decoded_len = 0;
        while let Some(&byte) = input.get(position) {
            if byte != 0 {
                position += 1;
                decoded_len += 1;
                continue;
            }
            match input.get(position + 1) {
                Some(255) => {
                    position += 2;
                    decoded_len += 1;
                }
                Some(0) => {
                    return Ok((
                        Self {
                            encoded: &input[..position],
                            decoded_len,
                        },
                        &input[position + 2..],
                    ));
                }
                _ => return Err(unknown("invalid literal escape or terminator")),
            }
        }
        Err(unknown("unterminated literal"))
    }
    pub(super) fn len(&self) -> usize {
        self.decoded_len
    }
    pub(super) fn bytes(&self) -> impl Iterator<Item = u8> + Clone + 'a {
        Bytes {
            remaining: self.encoded,
        }
    }
    pub(super) fn matches(&self, expected: &[u8]) -> bool {
        self.bytes().eq(expected.iter().copied())
    }
    pub(super) fn validate_text(&self) -> QueueResult<()> {
        // NUL cannot occur inside a UTF-8 code point. Each zero escape therefore
        // separates independently valid UTF-8 segments, without a decoded copy.
        let mut remaining = self.encoded;
        while let Some(position) = remaining.iter().position(|byte| *byte == 0) {
            std::str::from_utf8(&remaining[..position])
                .map_err(|_| unknown("invalid literal UTF-8"))?;
            remaining = &remaining[position + 2..];
        }
        std::str::from_utf8(remaining).map_err(|_| unknown("invalid literal UTF-8"))?;
        Ok(())
    }
    pub(super) fn validate_identifier(&self) -> QueueResult<()> {
        self.validate_text()?;
        // Every encoded zero here is an escaped NUL, which is not whitespace.
        if !self.encoded.contains(&0)
            && std::str::from_utf8(self.encoded)
                .map_err(|_| unknown("invalid literal UTF-8"))?
                .trim()
                .is_empty()
        {
            return Err(unknown("blank record identifier"));
        }
        Ok(())
    }
    fn copy_text(&self) -> QueueResult<String> {
        self.validate_text()?;
        let mut decoded = Vec::with_capacity(self.decoded_len);
        let mut position = 0;
        while let Some(&byte) = self.encoded.get(position) {
            decoded.push(byte);
            position += if byte == 0 { 2 } else { 1 };
        }
        String::from_utf8(decoded).map_err(|_| unknown("invalid literal UTF-8"))
    }
}
#[derive(Clone)]
struct Bytes<'a> {
    remaining: &'a [u8],
}
impl Iterator for Bytes<'_> {
    type Item = u8;
    fn next(&mut self) -> Option<u8> {
        let (&byte, rest) = self.remaining.split_first()?;
        self.remaining = if byte == 0 { &rest[1..] } else { rest };
        Some(byte)
    }
}
/// Caller must admit at least `input.len()` bytes plus String/container overhead
/// before calling. Reject trailing components rather than accepting a key prefix.
pub(in crate::queue_source_index) fn text(input: &[u8]) -> QueueResult<String> {
    let (literal, rest) = Literal::parse(input)?;
    if !rest.is_empty() {
        return Err(unknown("trailing literal components"));
    }
    literal.copy_text()
}
fn unknown(reason: &str) -> QueueError {
    QueueError::OutcomeUnknown(format!("source key: {reason}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_literals_preserve_unicode_nul_and_component_boundaries() {
        for value in ["", "a", "a\0b", "\0\0", "é\0🦊", "\u{10ffff}"] {
            let mut encoded = Vec::new();
            crate::queue_index::escaped(&mut encoded, value.as_bytes());
            assert_eq!(text(&encoded).unwrap(), value);
            let end = encoded.len();
            encoded.extend_from_slice(&[7, 0, 0]);
            let (literal, rest) = Literal::parse(&encoded).unwrap();
            assert_eq!(literal.decoded_len, value.len());
            assert_eq!(literal.copy_text().unwrap(), value);
            assert_eq!(rest, &encoded[end..]);
            assert!(text(&encoded).is_err());
        }
    }
    #[test]
    fn malformed_or_truncated_literals_never_become_text() {
        for value in ["", "a", "\0", "é\0🦊"] {
            let mut encoded = Vec::new();
            crate::queue_index::escaped(&mut encoded, value.as_bytes());
            for end in 0..encoded.len() {
                assert!(text(&encoded[..end]).is_err(), "{value:?} at {end}");
            }
        }
        for input in [
            &[0, 1][..],
            &[0, 254, 0, 0],
            &[255, 0, 0],
            &[0xc3, 0, 255, 0xa9, 0, 0],
            &[0, 0, 0],
        ] {
            assert!(text(input).is_err(), "{input:?}");
        }
    }
}
