#!/usr/bin/env bash
# Prova consolidada — Live v27.20 + código em teste (incl. CAIXA-FECHAR-PERF-2).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
export AGRO_PIN_TESTE="${AGRO_PIN_TESTE:-9973}"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

ver="$(tr -d '[:space:]' < VERSION)"
if [[ "$ver" == *"<<<<<<"* ]] || [[ "$ver" == *"======="* ]] || [[ "$ver" == *">>>>>>"* ]]; then
  echo "FAIL: VERSION com marcadores de merge: $ver"
  exit 2
fi
echo "VERSION=$ver"

run() {
  echo "== $1 =="
  "$PY" "$2"
}

run "caixa_fechar_loja" scripts/verify_caixa_fechar_loja_path.py | tee /tmp/v2720-loja.log
grep -q VERIFY_FECHAR_LOJA_OK /tmp/v2720-loja.log
run "caixa_fechar_repasse" scripts/verify_caixa_fechar_repasse_path.py | tee /tmp/v2720-rep.log
grep -q VERIFY_FECHAR_REPASSE_OK /tmp/v2720-rep.log
run "caixa_devolucao_mp" scripts/verify_caixa_devolucao_dinheiro_mp_path.py | tee /tmp/v2720-dev.log
grep -q VERIFY_CAIXA_DEVOL_DINHEIRO_MP_OK /tmp/v2720-dev.log
run "credito_limite_revisao" scripts/verify_credito_limite_revisao_path.py | tee /tmp/v2720-cred.log
grep -q '0 FAIL' /tmp/v2720-cred.log
"$PY" manage.py test produtos.tests_credito_limite_revisao -v0 2>&1 | tee /tmp/v2720-cred-t.log | grep -qE '^OK$|OK \('
run "cliente_desc_pdv" scripts/verify_cliente_lista_desc_pdv_path.py | tee /tmp/v2720-cli.log
grep -qE '18/18|0 FAIL' /tmp/v2720-cli.log
run "cliente_planilha" scripts/verify_cliente_planilha_path.py | tee /tmp/v2720-plan.log
grep -qE '61/61|0 FAIL' /tmp/v2720-plan.log

echo "OK: verify_checklist_v2720_path (caixa+credito+cliente)"
