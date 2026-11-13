#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="8cc412a936a6e49099ed27bbc2a86f0b53b93442"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
echo "producao=$ATUAL"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: a loja andou"; exit 2; }
echo "PREP_CHECKPOINT_OK 1311c $ATUAL"
