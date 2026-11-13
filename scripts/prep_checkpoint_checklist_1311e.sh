#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
ESPERADO="eb34cf2aeef01b228483a1e97b5e455eaeed656d"
git fetch origin producao
ATUAL="$(git rev-parse origin/producao)"
[[ "$ATUAL" == "$ESPERADO" ]] || { echo "FAIL: a loja andou"; exit 2; }
echo "PREP_CHECKPOINT_OK 1311e $ATUAL"
