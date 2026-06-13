//! ApiVersions handler (API Key 18) — producer-only advertisement.
//!
//! Only ApiVersions (18), Metadata (3), and Produce (0) are advertised.
//! Consumer-side APIs are permanently out of scope (ADR-005).

use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::ApiVersionsResponse;

/// Produce: v0-v9, Metadata: v0-v12, ApiVersions: v0-v3
pub const PRODUCER_APIS: &[(i16, i16, i16)] = &[
    (0, 0, 9),  // Produce
    (3, 0, 12), // Metadata
    (18, 0, 3), // ApiVersions
];

pub fn handle(_api_version: i16) -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    for &(api_key, min, max) in PRODUCER_APIS {
        let mut api = ApiVersion::default();
        api.api_key = api_key;
        api.min_version = min;
        api.max_version = max;
        response.api_keys.push(api);
    }
    response
}
