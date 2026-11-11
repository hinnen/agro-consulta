#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_sistapp_troca_camera_path.py
echo "POST_DEPLOY_OK 1111d-sistapp-camera-traseira"
echo "Sem migrate. Atualize a página do SistApp (Ajuste, Fotos, Etiquetas) antes de abrir a câmera."
