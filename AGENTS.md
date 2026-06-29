sccache is available. you should use it.

should build with

export RUSTC_WRAPPER=sccache
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
export CARGO_TARGET_DIR=/tmp/codex-target
