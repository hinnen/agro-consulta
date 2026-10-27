#!/usr/bin/env bash
# Pós-deploy loja — CHECKLIST 27/10h · NFE-EMITENTE-CFOP · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_nfe_saida_path.py
"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaChecklistEmitenteTests \
  produtos.tests_nfe_saida_util.NfeSaidaJsStaticTests \
  produtos.tests_nfce_loja \
  -v 0
echo "POST_DEPLOY_OK 2710h-nfe-emitente-cfop · sem migrate · PIN 9973 só smoke · NÃO emitir NF-e"
echo "Smoke loja: Ctrl+F5 /nfe/saida/ · Centro e Vila · checklist SP 5102"
