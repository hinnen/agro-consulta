#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="e3745faef6d1767026b6de7526354f23c2c08d20"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
echo "producao=$ATUAL"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: a loja andou"; exit 2; }
echo "PREP_CHECKPOINT_OK 1311d $ATUAL"
