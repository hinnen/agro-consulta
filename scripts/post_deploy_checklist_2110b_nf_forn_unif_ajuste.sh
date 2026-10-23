#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate --noinput
"$PY" scripts/verify_nf_forn_unif_ajuste_path.py
"$PY" scripts/verify_nf_ajuste_manual_rateio_path.py
"$PY" scripts/verify_fornecedor_entrada_nfe_alias_path.py | tee /tmp/post-2110b-alias.log
grep -q 'VERIFY_OK fornecedor_entrada_nfe_alias' /tmp/post-2110b-alias.log
echo "POST_DEPLOY_OK 2110b-nf-forn-unif-ajuste"
