//! Field naming shared by every model.

/// A field name for index `i`, e.g. `f007`. Width is fixed at 3 digits
/// (covers the 200-field case) so every name in one run is the same length,
/// which keeps JSON-key overhead comparable across configs.
pub fn field_name(i: usize) -> String {
    format!("f{i:03}")
}
