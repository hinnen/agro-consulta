#!/usr/bin/env bash
# Pós-deploy — CADASTRO-BUSCA-GM-PDV (sem migrate)
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ -n "${PORT:-}" ]]; then
  curl -sf "http://127.0.0.1:${PORT}/healthz" && echo " healthz OK"
fi

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_cadastro_busca_gm_path.py | tail -2
"$PY" scripts/verify_pdv_busca_coerente_path.py | tail -2
echo "OK pós-deploy 13/10 — busca cadastro alinhada PDV"
