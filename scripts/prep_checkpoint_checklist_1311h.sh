#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ESPERADO="3b9fc063612d72754a0ac3a657b68a78fdb56bba"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: a loja andou ($ATUAL)"; exit 2; }
echo "PREP_CHECKPOINT_OK 1311h $ATUAL"
