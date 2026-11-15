#!/usr/bin/env bash
# Select the embedding runtime; forward all arguments to Compose.
set -euo pipefail
cd "$(dirname "$0")/.."
mode="${OPENFINANCE_COMPUTE_MODE:-auto}"
if [[ "$mode" == auto ]]; then
  if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi -L >/dev/null 2>&1 \
      && docker info --format '{{json .Runtimes}}' 2>/dev/null | grep -q '"nvidia"'; then
    mode=gpu
  else
    mode=cpu
  fi
fi
if [[ "$mode" != cpu && "$mode" != gpu ]]; then
  echo "OPENFINANCE_COMPUTE_MODE must be auto, cpu, or gpu" >&2
  exit 2
fi
echo "OpenFinance embedding mode: $mode" >&2
if [[ "$mode" == gpu ]]; then
  export OPENFINANCE_OLLAMA_RUNTIME=nvidia
  export OLLAMA_NUM_PARALLEL="${OLLAMA_NUM_PARALLEL:-2}"
else
  export OPENFINANCE_OLLAMA_RUNTIME=runc
  export OLLAMA_NUM_PARALLEL="${OLLAMA_NUM_PARALLEL:-1}"
fi
exec docker compose "$@"
