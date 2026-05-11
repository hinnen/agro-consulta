#!/bin/bash
set -e

echo "Rodando migrações do banco de dados..."
python manage.py migrate --noinput

echo "Coletando arquivos estáticos..."
python manage.py collectstatic --noinput

echo "Iniciando o servidor Gunicorn..."
exec gunicorn config.wsgi:application --bind 0.0.0.0:8000 --workers 3 --timeout 120
