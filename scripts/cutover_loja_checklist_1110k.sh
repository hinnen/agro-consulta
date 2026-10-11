#!/usr/bin/env bash
# Cutover loja — CHECKLIST 11/10k (restante) · SistApp + Atualizar + Zap enxuto · v27.03 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-checklist-1110k"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1110k-restante-v27.01"
BACKUP_BR="producao-backup-pre-v2703-checklist-1110k-restante"
SENHA_OK="99738595"
VER_ESPERADA="27.03"

git fetch origin producao deploy/prep-checklist-1110k 2>/dev/null \
  || git fetch origin producao deploy/prep-checklist-1110k
git fetch origin "refs/tags/${ROLLBACK_TAG}:refs/tags/${ROLLBACK_TAG}" 2>/dev/null || true

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP (rebase deploy/prep-checklist-1110k em producao)"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
echo "migrations no delta: ${MIG:-nenhuma}"
[[ -z "$MIG" ]] || {
  echo "FAIL: pacote restante não pode levar migration nova: $MIG"; exit 2;
}

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro' || true)"
[[ -z "$FORBIDDEN" ]] || {
  echo "FAIL: arquivos sensíveis no delta: $FORBIDDEN"; exit 2;
}

git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "WARN: tag rollback ausente $ROLLBACK_TAG (criar em origin/producao antes do --exec)"; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git cat-file -e "$PREP_FULL:produtos/vendas_lojas_ajuste_sessao_util.py" || {
  echo "FAIL: delta sem SistApp ajuste sessão"; exit 2;
}
git grep -q 'vl-atualizar' "$PREP_FULL" -- produtos/templates/produtos/vendas_lojas_resumo.html || {
  echo "FAIL: delta sem botão Atualizar no painel Vendas"; exit 2;
}
git grep -q '"short_name": "SistApp"' "$PREP_FULL" -- produtos/views.py || {
  echo "FAIL: delta sem manifest SistApp"; exit 2;
}
git grep -q '_meta_linha_zap_enxuta' "$PREP_FULL" -- produtos/meta_vendas_util.py || {
  echo "FAIL: delta sem Zap enxuto"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

"$PY" scripts/verify_vendas_lojas_pwa_install.py | tee /tmp/cutover-1110k-pwa.log
grep -qE '0 falha\(s\)|FAIL=0' /tmp/cutover-1110k-pwa.log

"$PY" -c "
import importlib.util
from pathlib import Path
spec = importlib.util.spec_from_file_location('v', 'scripts/verify_vendas_lojas_resumo_path.py')
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)
for fn in ('test_arquivos', 'test_periodo', 'test_totais_mock', 'test_cmp_meta', 'test_fracao_expediente', 'test_previsao_mes', 'test_versao'):
    getattr(v, fn)()
assert not v.fails, v.fails
print('resumo estático OK', len(v.oks))
" | tee /tmp/cutover-1110k-resumo.log
grep -q 'resumo estático OK' /tmp/cutover-1110k-resumo.log

AGRO_PIN_TESTE=9973 "$PY" -c "
import os, sys
from pathlib import Path
ROOT = Path('.').resolve()
sys.path.insert(0, str(ROOT))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()
from scripts.verify_meta_mostruario_path import test_arquivos_rotas, test_math_e_format, test_seed_e_crud
test_arquivos_rotas()
test_math_e_format()
test_seed_e_crud()
print('META core OK')
" | tee /tmp/cutover-1110k-meta.log
grep -q 'META core OK' /tmp/cutover-1110k-meta.log

echo "OK: PREP v$VER_ESPERADA · sem migrate · rollback $ROLLBACK_TAG @ produção atual"
echo "Pós-deploy (rápido): ./scripts/post_deploy_checklist_1110k.sh"
echo "Exec: AGRO_LOJA_FRASE='…' AGRO_LOJA_SENHA='…' $0 --exec"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · Render auto-deploy · badge v$VER_ESPERADA · Shell: ./scripts/post_deploy_checklist_1110k.sh"
