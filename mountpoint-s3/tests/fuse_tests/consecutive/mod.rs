// Tests in this module should not be run in parallel as they are using the shared state.
// Use `--test-threads=1` parameter, e.g. `cargo test -- --test-threads=1`
mod cache_test;
