#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_etq_filtro_nf_entrada_path.py
"$PY" manage.py test produtos.tests_pdv_cadastro_rapido.ArquivosPathTests.test_filtro_parse_numero_nf_entrada -v 0
echo "POST_DEPLOY_OK 2010b-etq-nf-sem-exato"
