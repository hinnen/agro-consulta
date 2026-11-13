#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_vendas_lojas_pwa_install.py
echo "POST_DEPLOY_OK 1311b-botao-zap"
echo "Sem migrate. Ctrl+F5 no SistApp. O botão Zap abre o atendimento. Vendas, Tarefas, Fotos, Etiquetas e Ajuste continuam."
