#!/usr/bin/env bash
# Confere se a loja ainda está no commit anotado. Não cria branch e não faz push.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="ee3304332d77f0d91f6565bb84cfcfd1e81263b0"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
echo "producao=$ATUAL"
echo "checkpoint=$ESPERADO"
[[ "$ATUAL" == "$ESPERADO" ]] || {
  echo "FAIL: a loja andou. Não usar este corte até refazer o checkpoint."
  exit 2
}
echo "PREP_CHECKPOINT_OK 1311 $ATUAL"
