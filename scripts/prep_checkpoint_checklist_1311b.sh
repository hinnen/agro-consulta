#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="418fb789bb88ad8098cada93fc254a6fcc4be272"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
echo "producao=$ATUAL"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: a loja andou"; exit 2; }
echo "PREP_CHECKPOINT_OK 1311b $ATUAL"
