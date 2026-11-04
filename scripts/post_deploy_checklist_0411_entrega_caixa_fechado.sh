#!/usr/bin/env bash
# Conferência depois do deploy. Não grava entrega nem abre caixa.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
grep -q 'apenas_caixas_abertos: bool = False' produtos/entrega_pdv_pendente_util.py
grep -q 'sessao_caixa__fechado_em__isnull=False' produtos/entrega_pdv_pendente_util.py
grep -q 'PONTO_CAIXA_TESTE' produtos/entrega_pdv_pendente_util.py
grep -q '27.26.50' VERSION
echo "POST_DEPLOY_OK 0411-entrega-caixa-fechado"
