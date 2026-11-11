#!/usr/bin/env bash
# Confere se a loja ainda está no commit anotado. Não cria branch e não faz push.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
ESPERADO="c8256c2664d86d5d9c79cc974d5bc10b104666d7"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
echo "producao=$ATUAL"
echo "checkpoint=$ESPERADO"
[[ "$ATUAL" == "$ESPERADO" ]] || {
  echo "FAIL: a loja andou. Não usar este corte até refazer o checkpoint."
  exit 2
}
echo "PREP_CHECKPOINT_OK 1111c $ATUAL"
