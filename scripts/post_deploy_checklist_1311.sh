#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PY=python3
[[ -x .venv/bin/python ]] && PY=.venv/bin/python
"$PY" scripts/verify_sistapp_troca_camera_path.py
"$PY" - <<'PY'
from pathlib import Path
root = Path(".")
js = (root / "produtos/static/produtos/js/pdv_topbar_whatsapp.js").read_text(encoding="utf-8")
view = (root / "produtos/views_atendimento_whatsapp.py").read_text(encoding="utf-8")
assert "window.open" in js and "zap-entrada" in js
assert "emitir_entrada_pin_zap" in view and "AGRO_WA_PIN_HANDOFF_KEY" in (root / "config/settings.py").read_text(encoding="utf-8")
print("PIN_OK")
PY
echo "POST_DEPLOY_OK 1311"
echo "Sem migrate. Ctrl+F5 no Ajuste, Fotos, Etiquetas e no Zap."
