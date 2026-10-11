//! Versioned API-007 full-address framing. Product callers admit the returned
//! peak scratch before calling: raw framing plus Base64 output on encode;
//! decoded framing plus canonical re-encoding on decode. Namespace components
//! belong only in a separate
//! API-007 log/metadata root; they never reinterpret existing legacy partitions.
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fireweed_core::queue_contract::{QueueAddress, QueueError, QueueResult};

const MAGIC: &[u8; 8] = b"FWQADDR1";
const PREFIX: &str = "fwq7-";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddressRef<'a> {
    pub tenant: &'a str,
    pub queue: &'a str,
}

pub fn framed_address_len(address: &QueueAddress) -> QueueResult<usize> {
    24usize
        .checked_add(address.tenant.len())
        .and_then(|n| n.checked_add(address.queue.len()))
        .ok_or(QueueError::TooLarge)
}

/// Exact byte framing, with no normalization, delimiter escaping or hashing.
pub fn encode_address(address: &QueueAddress) -> QueueResult<Vec<u8>> {
    address.validate()?;
    let mut out = Vec::with_capacity(framed_address_len(address)?);
    out.extend_from_slice(MAGIC);
    for name in [&address.tenant, &address.queue] {
        out.extend_from_slice(
            &u64::try_from(name.len())
                .map_err(|_| QueueError::TooLarge)?
                .to_le_bytes(),
        );
        out.extend_from_slice(name.as_bytes());
    }
    Ok(out)
}

pub fn view_address(mut bytes: &[u8]) -> QueueResult<AddressRef<'_>> {
    fn invalid() -> QueueError {
        QueueError::OutcomeUnknown("invalid API-007 address framing".into())
    }
    fn name<'a>(rest: &mut &'a [u8]) -> QueueResult<&'a str> {
        let header = rest.get(..8).ok_or_else(invalid)?;
        let len = usize::try_from(u64::from_le_bytes(
            header.try_into().map_err(|_| invalid())?,
        ))
        .map_err(|_| invalid())?;
        *rest = &rest[8..];
        let value =
            std::str::from_utf8(rest.get(..len).ok_or_else(invalid)?).map_err(|_| invalid())?;
        *rest = &rest[len..];
        Ok(value)
    }
    if !bytes.starts_with(MAGIC) {
        return Err(invalid());
    }
    bytes = &bytes[MAGIC.len()..];
    let tenant = name(&mut bytes)?;
    let queue = name(&mut bytes)?;
    if tenant.trim().is_empty() || queue.trim().is_empty() {
        return Err(invalid());
    }
    if !bytes.is_empty() {
        return Err(invalid());
    }
    Ok(AddressRef { tenant, queue })
}

/// One ASCII component suitable for the existing log transport. Full framing
/// remains the identity; the fixed prefix/version prevents format confusion.
pub fn namespace_component(address: &QueueAddress) -> QueueResult<String> {
    let raw = encode_address(address)?;
    let mut out = String::from(PREFIX);
    URL_SAFE_NO_PAD.encode_string(raw, &mut out);
    Ok(out)
}

/// Callers pre-admit decoded bytes plus canonical re-encoding scratch, then retain
/// the decoded allocation while inspecting its borrowed address. Re-encoding
/// rejects alternate spellings and padding.
pub fn decode_namespace(component: &str) -> QueueResult<Vec<u8>> {
    let invalid = || QueueError::OutcomeUnknown("invalid API-007 namespace component".into());
    let encoded = component.strip_prefix(PREFIX).ok_or_else(invalid)?;
    let raw = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
    if URL_SAFE_NO_PAD.encode(&raw) != encoded {
        return Err(invalid());
    }
    view_address(&raw)?;
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_full_identity_is_injective_and_borrowed() {
        let names = [
            "a", "a\0b", "b\0c", "c", "a/b", "a%2fb", "β", "β/\0", ".", "..", "é", "e\u{301}",
        ];
        let mut components = std::collections::BTreeSet::new();
        for tenant in names {
            for queue in names {
                let address = QueueAddress {
                    tenant: tenant.into(),
                    queue: queue.into(),
                };
                let raw = encode_address(&address).unwrap();
                assert_eq!(raw.len(), framed_address_len(&address).unwrap());
                assert_eq!(view_address(&raw).unwrap(), AddressRef { tenant, queue });
                let component = namespace_component(&address).unwrap();
                assert!(components.insert(component.clone()));
                assert!(!component.contains(['\0', '/', '=']));
                assert_eq!(decode_namespace(&component).unwrap(), raw);
            }
        }
    }
    #[test]
    fn literal_fixture_and_complete_validation() {
        let address = QueueAddress {
            tenant: "a\0b".into(),
            queue: "β/".into(),
        };
        let raw = encode_address(&address).unwrap();
        assert_eq!(
            raw,
            b"FWQADDR1\x03\0\0\0\0\0\0\0a\0b\x03\0\0\0\0\0\0\0\xce\xb2/".to_vec()
        );
        assert_eq!(
            namespace_component(&address).unwrap(),
            "fwq7-RldRQUREUjEDAAAAAAAAAGEAYgMAAAAAAAAAzrIv"
        );
        assert!(
            encode_address(&QueueAddress {
                tenant: " ".into(),
                queue: "q".into()
            })
            .is_err()
        );
        let padded = format!("{}=", namespace_component(&address).unwrap());
        assert!(decode_namespace(&padded).is_err());
        for len in 0..raw.len() {
            assert!(view_address(&raw[..len]).is_err());
        }
        let mut extra = raw.clone();
        extra.push(0);
        assert!(view_address(&extra).is_err());
        let mut invalid = raw.clone();
        invalid[16] = 255;
        assert!(view_address(&invalid).is_err());
        let mut overflow = raw;
        overflow[8..16].fill(255);
        assert!(view_address(&overflow).is_err());
        assert!(decode_namespace("fwq7-====").is_err());
        let long = QueueAddress {
            tenant: "β\0/".repeat(20_000),
            queue: "q".into(),
        };
        let component = namespace_component(&long).unwrap();
        let decoded = decode_namespace(&component).unwrap();
        assert_eq!(view_address(&decoded).unwrap().tenant, long.tenant);
    }
}
