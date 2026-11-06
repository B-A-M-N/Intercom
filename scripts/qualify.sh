#!/usr/bin/env bash
# Intercom release qualification: repeatable, zero-inference-spend.
#
# Runs the full Go + Python suites, the live no-spend harness protocol
# tests (Muse echo, Codex thread-start, OpenCode session-new), and the
# adversarial conformance tests. It does NOT run model turns, does NOT
# touch global harness configs, and starts everything in temp dirs.
#
# Usage: scripts/qualify.sh [--quick]
#   --quick  skip the live protocol tests (still runs both suites' unit tests)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== go build/vet =="
go build ./...
go vet ./...

echo "== go test ./... =="
go test ./...

echo "== python adapter tests =="
cd adapters/python
if [[ "${1:-}" == "--quick" ]]; then
  python3 -m unittest tests.test_adapter_host tests.test_harness_adapters tests.test_real_acp_client -v 2>&1 | tail -n 5
else
  python3 -m unittest discover -s tests 2>&1 | tail -n 5
fi
cd ../..

echo "QUALIFY: PASS (no inference spent)"
