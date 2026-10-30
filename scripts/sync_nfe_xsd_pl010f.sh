#!/usr/bin/env bash
# Republica o pacote PL 010f (espelho PL_010_V1.30) em produtos/schemas/nfe/PL_010f/
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEST="$ROOT/produtos/schemas/nfe/PL_010f"
API="https://api.github.com/repos/nfephp-org/sped-nfe/contents/schemes/PL_010_V1.30?ref=master"
BASE="https://raw.githubusercontent.com/nfephp-org/sped-nfe/master/schemes/PL_010_V1.30"
rm -rf "$DEST"
mkdir -p "$DEST"
curl -fsSL -o /tmp/pl010_index.json "$API"
python3 << PY
import json, subprocess, os
dest = "$DEST"
with open("/tmp/pl010_index.json") as f:
    items = json.load(f)
for item in items:
    if item.get("type") != "file":
        continue
    out = os.path.join(dest, item["name"])
    subprocess.run(["curl", "-fsSL", "-o", out, item["download_url"]], check=True)
print(len(os.listdir(dest)), "arquivos em", dest)
PY
echo "OK sync PL_010f"
