web: gunicorn config.wsgi:application --bind 0.0.0.0:$PORT --workers 1 --preload --timeout 120 --graceful-timeout 30 --no-control-socket --access-logfile - --error-logfile -
