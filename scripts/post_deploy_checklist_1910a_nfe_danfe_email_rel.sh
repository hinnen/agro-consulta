#!/usr/bin/env bash
# Pós-deploy loja — CHECKLIST 19/10a · DANFE + e-mail NF-e + REL cat/sub · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" manage.py migrate --noinput
"$PY" scripts/verify_nfe_saida_path.py
"$PY" scripts/verify_rel_cat_sub_excel_path.py
"$PY" manage.py test \
  produtos.tests_nfe_saida_util.NfeSaidaDanfeLayoutTests \
  produtos.tests_nfe_saida_util.NfeSaidaXmlMontagemTests \
  produtos.tests_nfe_saida_util.NfeSaidaEmailTests \
  produtos.tests_relatorios_central_filtros \
  -v 0
echo "POST_DEPLOY_OK 1910a-nfe-danfe-email-rel"
