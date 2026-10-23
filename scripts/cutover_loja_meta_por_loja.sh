#!/usr/bin/env bash
# Cutover loja — META-POR-LOJA apenas · v27.01 · migrate 0144
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/deploy/prep-meta-por-loja-fac0"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-meta-por-loja-v27.00"
BACKUP_BR="producao-backup-pre-v2701-meta-por-loja"
SENHA_OK="99738595"
VER_ESPERADA="27.01"
MIG_ESPERADA="produtos/migrations/0144_metavendafaixaagro_deposito.py"

git fetch origin producao deploy/prep-meta-por-loja-fac0 "$ROLLBACK_TAG" 2>/dev/null \
  || git fetch origin producao deploy/prep-meta-por-loja-fac0

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

git merge-base --is-ancestor "$PROD_REF" "$PREP_REF" || {
  echo "FAIL: produção não é ancestral do PREP"; exit 2;
}
[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
echo "migrations no delta: ${MIG:-nenhuma}"
echo "$MIG" | grep -q "0144_metavendafaixaagro_deposito" || {
  echo "FAIL: falta migration 0144 no pacote"; exit 2;
}
EXTRA_MIG="$(echo "$MIG" | grep -v '0144_metavendafaixaagro_deposito' || true)"
[[ -z "$EXTRA_MIG" ]] || {
  echo "FAIL: migrations extras no pacote: $EXTRA_MIG"; exit 2;
}

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro|vendas_lojas_ajuste|mobile_ajuste\.html' || true)"
[[ -z "$FORBIDDEN" ]] || {
  echo "FAIL: arquivos sensíveis no delta: $FORBIDDEN"; exit 2;
}

ALLOWED_EXTRA="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -vE \
  '^VERSION$|^scripts/verify_meta_mostruario_path\.py$|^produtos/(meta_vendas_util|views_meta_vendas|models)\.py$|^produtos/migrations/0144_|^produtos/templates/produtos/meta_vendas\.html$|^scripts/cutover_loja_meta_por_loja\.sh$|^scripts/post_deploy_meta_por_loja\.sh$|^docs/' || true)"
[[ -z "$ALLOWED_EXTRA" ]] || {
  echo "FAIL: arquivos fora do escopo META: $ALLOWED_EXTRA"; exit 2;
}

git rev-parse "$ROLLBACK_TAG" >/dev/null 2>&1 \
  || git ls-remote --exit-code --tags origin "refs/tags/$ROLLBACK_TAG" >/dev/null \
  || { echo "WARN: tag rollback ausente $ROLLBACK_TAG"; }

PREP_FULL="$(git rev-parse "$PREP_REF")"
git cat-file -e "$PREP_FULL:$MIG_ESPERADA" || {
  echo "FAIL: delta sem migration META loja"; exit 2;
}
git grep -q 'meta_normalizar_deposito' "$PREP_FULL" -- produtos/meta_vendas_util.py || {
  echo "FAIL: delta sem META por loja"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

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
" | tee /tmp/cutover-meta-por-loja.log
grep -q 'META core OK' /tmp/cutover-meta-por-loja.log

echo "OK: PREP v$VER_ESPERADA · só META-POR-LOJA · migrate 0144 · rollback $ROLLBACK_TAG"
echo "Pós-deploy: ./scripts/post_deploy_meta_por_loja.sh"

if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_LOJA_FRASE:-}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · Render: ./scripts/post_deploy_meta_por_loja.sh · badge v$VER_ESPERADA"
