#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 11/10k (restante) · sem migrate
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== migrate (nenhuma obrigatória neste pacote; 0144 já na loja v27.01) =="
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate --check 2>/dev/null || "$PY" manage.py migrate --plan | head -5

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
else
  echo "(skip healthz local — abra /healthz no browser após deploy)"
fi

"$PY" scripts/verify_vendas_lojas_pwa_install.py | tail -3

echo "OK pós-deploy 11/10k — SistApp · hub Ajuste · painel Atualizar · Zap enxuto"
