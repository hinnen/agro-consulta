#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" manage.py migrate produtos 0155_pdv_operacao_log_agro --noinput
"$PY" scripts/verify_nf_forn_unif_ajuste_path.py
"$PY" scripts/verify_fornecedor_entrada_nfe_alias_path.py | tee /tmp/post-1111-alias.log
grep -q 'VERIFY_OK fornecedor_entrada_nfe_alias' /tmp/post-1111-alias.log
"$PY" scripts/verify_mp_point_orfao_pago_pdv_log_path.py | tee /tmp/post-1111-mp.log
grep -q VERIFY_OK /tmp/post-1111-mp.log
"$PY" scripts/verify_pin_operador_saida_nathan_path.py | tee /tmp/post-1111-pin.log
grep -q VERIFY_OK /tmp/post-1111-pin.log
echo "POST_DEPLOY_OK 1111-loja-3"
