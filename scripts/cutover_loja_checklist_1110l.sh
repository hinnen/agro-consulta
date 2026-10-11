#!/usr/bin/env bash
# Cutover loja — CHECKLIST 11/10l · META Centro+Vila unificado + ritmo hora · v27.04 · SEM migrate
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PREP_REF="origin/teste"
PROD_REF="origin/producao"
ROLLBACK_TAG="rollback/pre-checklist-1110l-v27.03"
BACKUP_BR="producao-backup-pre-v2704-checklist-1110l"
SENHA_OK="99738595"
VER_ESPERADA="27.04"

git fetch origin producao teste "$ROLLBACK_TAG" 2>/dev/null || git fetch origin producao teste

PROD="$(git rev-parse --short "$PROD_REF")"
PREP="$(git rev-parse --short "$PREP_REF")"
VER="$(git show "$PREP_REF:VERSION" | tr -d '[:space:]')"
echo "producao=$PROD PREP=$PREP VERSION=$VER"

[[ "$VER" == "$VER_ESPERADA" ]] || {
  echo "FAIL: VERSION=$VER (esperado $VER_ESPERADA)"; exit 2;
}

MIG="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E '/migrations/[0-9]+_' || true)"
[[ -z "$MIG" ]] || { echo "FAIL: migration no pacote: $MIG"; exit 2; }

FORBIDDEN="$(git diff --name-only "$PROD_REF...$PREP_REF" | grep -E \
  'pdv_wizard\.js|busca_filtro_pdv|motor_busca_unificado|caixa_|mongo_financeiro' || true)"
[[ -z "$FORBIDDEN" ]] || { echo "FAIL: sensível: $FORBIDDEN"; exit 2; }

git grep -q 'meta_salvar_faixa_todas' "$PREP_REF" -- produtos/meta_vendas_util.py || {
  echo "FAIL: falta META Centro+Vila unificado"; exit 2;
}
git cat-file -e "$PREP_REF:produtos/vendas_lojas_ritmo_util.py" || {
  echo "FAIL: falta ritmo hora vendas lojas"; exit 2;
}

PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python

AGRO_PIN_TESTE=9973 "$PY" -c "
import os, sys
from pathlib import Path
sys.path.insert(0, str(Path('.').resolve()))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
import django
django.setup()
from scripts.verify_meta_mostruario_path import test_arquivos_rotas, test_math_e_format, test_seed_e_crud
test_arquivos_rotas()
test_math_e_format()
test_seed_e_crud()
print('META core OK')
" | tee /tmp/cutover-1110l-meta.log
grep -q 'META core OK' /tmp/cutover-1110l-meta.log

"$PY" manage.py test produtos.tests_vendas_lojas_ritmo -v0 2>&1 | tee /tmp/cutover-1110l-ritmo.log
grep -qE 'OK|passed' /tmp/cutover-1110l-ritmo.log

echo "OK: PREP v$VER_ESPERADA · 11/10l · rollback $ROLLBACK_TAG"
if [[ "${1:-}" != "--exec" ]]; then exit 0; fi

FRASE="${AGRO_PIN_FRASE:-${AGRO_LOJA_FRASE:-}}"
SENHA="${AGRO_LOJA_SENHA:-}"
[[ "$SENHA" == "$SENHA_OK" ]] || { echo "FAIL: senha"; exit 3; }
echo "$FRASE" | grep -Eiq 'pode.*(produ[cç][aã]o|producao)|sobe.*(produ[cç][aã]o|producao)|subir.*(produ[cç][aã]o|producao)' \
  || { echo "FAIL: autorização"; exit 3; }

git push origin "$PROD_REF:refs/heads/$BACKUP_BR" || true
git push origin "$PREP_REF:producao"
echo "PUSH OK · badge v$VER_ESPERADA · Ctrl+F5 /meta/ e Vendas lojas"
