# Hot Projection Queries

Verified:
- `cargo test -p pqueue --test hot_projection_queries hourly_distribution_by_status -- --nocapture`
- `cargo test -p pqueue --test hot_projection_queries recycling_preview_by_hour -- --nocapture`
- `cargo test -p pqueue --test hot_projection_queries engagement_probability_segments -- --nocapture`
- `cargo fmt --check`

Result:
- All three acceptance tests passed on both memory and sqlite backends.
- `cargo fmt --check` passed cleanly.

