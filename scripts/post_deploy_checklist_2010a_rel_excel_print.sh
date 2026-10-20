#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
"$PY" scripts/verify_rel_grupo_margem_path.py
"$PY" scripts/verify_rel_cat_sub_excel_path.py
"$PY" manage.py test produtos.tests_relatorios_central_filtros -v 0
echo "POST_DEPLOY_OK 2010a-rel-excel-print"
