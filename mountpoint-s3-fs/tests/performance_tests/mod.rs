/// Tests should be run sequentially:
/// cargo test --features=performance_tests -- --test-threads=1
#[cfg(feature = "manifest")]
mod manifest_test;
