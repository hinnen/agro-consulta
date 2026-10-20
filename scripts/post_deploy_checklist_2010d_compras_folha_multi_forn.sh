#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_compras_folha_multi_forn_path.py
"$PY" manage.py test produtos.tests_compras_relatorio_fornecedor_multi -v 0
echo "POST_DEPLOY_OK 2010d-compras-folha-multi-forn"
