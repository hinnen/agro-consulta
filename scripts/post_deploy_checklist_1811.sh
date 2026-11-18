#!/usr/bin/env bash
# Depois que a v27.26.77 estiver no ar. Cria a tabela do cartão (0156).
set -euo pipefail
cd "$(dirname "$0")/.."
python manage.py migrate produtos
python manage.py showmigrations produtos | grep 0156
echo "OK: tabela do cartão. Ctrl+F5. Badge 27.26.77."
echo "Caixa: folha do cartão. PDV: bip 981 abre o cliente. Nota: imposto entra no boleto."
