#!/usr/bin/env bash
# Pós-deploy loja — CHECKLIST 19/10b · REL-GRUPO-MARGEM-COMP · sem migrate
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" manage.py migrate --noinput
"$PY" scripts/verify_rel_grupo_margem_path.py
"$PY" scripts/verify_rel_cat_sub_excel_path.py
"$PY" manage.py test produtos.tests_relatorios_central_filtros -v 0
echo "POST_DEPLOY_OK 1910b-rel-grupo-margem-comp"
