#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_sistapp_troca_camera_path.py
echo "POST_DEPLOY_OK 1111c-sistapp-troca-camera"
echo "Sem migrate. Ctrl+F5 no Ajuste, nas Fotos e nas Etiquetas. Botão Trocar câmera."
