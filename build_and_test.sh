#!/usr/bin/env bash
# Build + verify chanbus: install deps, typecheck, run the full test suite.
# Used locally and by CI (.github/workflows/ci.yml).
set -euo pipefail

cd "$(dirname "$0")"

echo "==> bun install (frozen lockfile)"
bun install --frozen-lockfile

echo "==> typecheck (tsc --noEmit)"
bunx tsc --noEmit

echo "==> test (bun test)"
bun test

echo "==> OK: build_and_test passed"
