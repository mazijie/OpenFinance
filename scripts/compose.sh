#!/usr/bin/env bash
# Run the OpenFinance Compose stack.
set -euo pipefail
cd "$(dirname "$0")/.."
exec docker compose "$@"
