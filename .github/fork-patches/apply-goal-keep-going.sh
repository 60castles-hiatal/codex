#!/usr/bin/env bash
set -euo pipefail

patch_commit="b084a1c941"
patch_paths=(
  codex-rs/ext/goal/src/extension.rs
  codex-rs/ext/goal/src/runtime.rs
  codex-rs/ext/goal/src/spec.rs
  codex-rs/ext/goal/src/tool.rs
  codex-rs/ext/goal/templates/goals/continuation.md
  codex-rs/ext/goal/tests/goal_extension_backend.rs
)

if ! git show --format= "${patch_commit}" -- "${patch_paths[@]}" | git apply; then
  echo "Direct goal keep-going patch did not apply; using release-tag fallback." >&2

  fallback_paths=(
    codex-rs/ext/goal/src/runtime.rs
    codex-rs/ext/goal/src/spec.rs
    codex-rs/ext/goal/src/tool.rs
    codex-rs/ext/goal/templates/goals/continuation.md
    codex-rs/ext/goal/tests/goal_extension_backend.rs
  )
  git show --format= "${patch_commit}" -- "${fallback_paths[@]}" | git apply

  perl -0pi -e 's/use crate::runtime::ActiveGoalStopReason;\n//' \
    codex-rs/ext/goal/src/extension.rs
  perl -0pi -e 's/\n([ ]*)let reason = match input\.error \{\n\1    CodexErrorInfo::UsageLimitExceeded => ActiveGoalStopReason::UsageLimit,\n(?:\1    .*\n)*?\1    _ => ActiveGoalStopReason::TurnError,\n\1\};\n\1if let Err\(err\) = runtime\n\1    \.stop_active_goal_for_turn\(input\.turn_id, reason\)\n\1    \.await\n\1\{\n\1    tracing::warn!\(\n\1        error = \?input\.error,\n\1        "failed to stop active goal after turn error: \{err\}"\n\1    \);\n\1\}/\n${1}let result = match input.error {\n${1}    CodexErrorInfo::UsageLimitExceeded => {\n${1}        runtime.stop_active_goal_for_turn(input.turn_id).await\n${1}    }\n${1}    _ => {\n${1}        runtime\n${1}            .continue_active_goal_after_turn_error(input.turn_id)\n${1}            .await\n${1}    }\n${1}};\n${1}if let Err(err) = result {\n${1}    tracing::warn!(\n${1}        error = ?input.error,\n${1}        "failed to process active goal after turn error: {err}"\n${1}    );\n${1}}/' \
    codex-rs/ext/goal/src/extension.rs
fi

if grep -q 'json!("blocked")' codex-rs/ext/goal/src/spec.rs; then
  echo "Failed to remove blocked goal status from update_goal schema." >&2
  exit 1
fi
if grep -q 'ActiveGoalStopReason' \
  codex-rs/ext/goal/src/extension.rs \
  codex-rs/ext/goal/src/runtime.rs; then
  echo "Failed to remove blocked goal turn-error stop path." >&2
  exit 1
fi
if ! grep -q 'continue_active_goal_after_turn_error' \
  codex-rs/ext/goal/src/runtime.rs; then
  echo "Failed to apply non-blocking turn-error goal patch." >&2
  exit 1
fi
if ! grep -q 'continue_active_goal_after_turn_error(input.turn_id)' \
  codex-rs/ext/goal/src/extension.rs; then
  echo "Failed to route non-usage turn errors through goal continuation." >&2
  exit 1
fi
if grep -q 'Blocked audit:' codex-rs/ext/goal/templates/goals/continuation.md; then
  echo "Failed to replace goal blocked-audit continuation instructions." >&2
  exit 1
fi
