#!/usr/bin/env bash
# Bootstrap idempotente do ambiente de desenvolvimento (Cloud Agent).
# App: Django 6 (config/). Banco: SQLite por padrão (sem DATABASE_URL); Mongo é opcional.
set -euo pipefail

# Raiz do repositório (este script fica em .cursor/scripts/).
cd "$(dirname "$0")/../.."

# 1. Dependência de sistema para criar virtualenv (não vem na imagem padrão).
if ! dpkg -s python3.12-venv >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3.12-venv
fi

# 2. Virtualenv (reutiliza se já existir).
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
# shellcheck disable=SC1091
. .venv/bin/activate

# 3. Dependências Python (lockfile do requirements.txt).
pip install --upgrade pip -q
pip install -r requirements.txt

# 4. .env de desenvolvimento (não commitado; ver .gitignore).
#    SECRET_KEY dev + SQLite. Para BI/financeiro/PDV completos, defina no .env:
#    VENDA_ERP_MONGO_URL / VENDA_ERP_MONGO_DB (Mongo) e/ou DATABASE_URL (Postgres).
if [ ! -f .env ]; then
  cat > .env <<'ENVEOF'
# Gerado pelo install do Cloud Agent — ambiente de desenvolvimento.
DEBUG=True
SECRET_KEY=dev-cloud-agent-insecure-key-change-me
ALLOWED_HOSTS=127.0.0.1,localhost,0.0.0.0
ENVEOF
fi

# 5. Banco (migrações) e arquivos estáticos.
python manage.py migrate --noinput
python manage.py collectstatic --noinput

echo "Ambiente pronto. Servidor: .venv/bin/python manage.py runserver 0.0.0.0:8000"
