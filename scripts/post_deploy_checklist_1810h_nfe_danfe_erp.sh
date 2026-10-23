#!/usr/bin/env bash
# Pós-deploy loja — CHECKLIST 18/10h · DANFE layout ERP · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" manage.py migrate --noinput
"$PY" scripts/verify_nfe_saida_path.py
"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaDanfeLayoutTests \
  produtos.tests_nfe_saida_util.NfeSaidaXmlMontagemTests \
  -v 0
echo "POST_DEPLOY_OK 1810h-nfe-danfe-erp"
