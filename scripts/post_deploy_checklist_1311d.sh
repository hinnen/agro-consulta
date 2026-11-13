#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" -m py_compile produtos/views_atendimento_whatsapp.py base/wa_split_middleware.py
echo "POST_DEPLOY_OK 1311d-zap-ligacao"
echo "Sem migrate. Ctrl+F5 no SistApp e abra o Zap. A lista tem que aparecer."
