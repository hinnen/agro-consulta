#!/usr/bin/env bash
# Pós-deploy Render — CHECKLIST 11/10 (rodar no Shell logo após badge v26.94)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "== migrate produtos 0143 =="
python manage.py migrate produtos 0143 --noinput

echo "== healthz =="
if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
else
  echo "(skip healthz — PORT não definido; abra /healthz no browser)"
fi

echo "== smoke paths (grep estático) =="
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_etq_solicitacao_vl_pin_path.py | tail -3
[[ -f scripts/verify_gm_lojas_pin_runtime.py ]] && "$PY" scripts/verify_gm_lojas_pin_runtime.py | tail -3

echo "OK pós-deploy 11/10 — testar celular: /vendas/lojas/ PIN · Etiquetas · Gestão LOG"
