web: gunicorn config.wsgi:application --bind 0.0.0.0:$PORT --workers 1 --preload --timeout 180 --graceful-timeout 45 --no-control-socket --access-logfile - --error-logfile -
