#!/usr/bin/env bash
# Pós-deploy loja — CHECKLIST 18/10f · migrate 0147 + provas
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" manage.py migrate --noinput
"$PY" manage.py backfill_busca_palavras_overlay 2>/dev/null || true
"$PY" scripts/verify_nfe_saida_path.py
"$PY" scripts/verify_cadastro_busca_med_path.py
"$PY" manage.py test produtos.tests_nfe_saida_util produtos.tests_cadastro_busca_pdv_alinhada -v 0
echo "POST_DEPLOY_OK 1810f-nfe-sefaz-danfe-pdv"
