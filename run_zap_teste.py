"""Sobe o Zap Teste neste PC, no disco E:. Nao mexe na loja nem na sessao dela."""
import os
import sys
from pathlib import Path
from urllib.parse import urlparse


def _ler_env(path: Path) -> dict:
    out = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def main() -> None:
    base = Path(__file__).resolve().parent
    if base.drive.upper() != "E:":
        print("O teste so sobe no disco E:.")
        sys.exit(1)
    os.chdir(base)
    wa = _ler_env(base / ".env.wa-servidor")
    url = (os.environ.get("ZAP_TESTE_DATABASE_URL") or wa.get("DATABASE_URL") or "").strip()
    if not url:
        print("Falta o Postgres local deste PC. Nao inventei endereco.")
        sys.exit(1)
    host = (urlparse(url).hostname or "").lower()
    if host not in {"127.0.0.1", "localhost", "::1"}:
        print("O teste so usa o Postgres deste PC. Nao liguei em banco de fora.")
        sys.exit(1)
    if "search_path" not in url:
        url += ("&" if "?" in url else "?") + "options=-csearch_path%3Dzapteste"
    media = base / "var" / "wa-teste-media"
    tmp = base / "var" / "tmp"
    media.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)
    os.environ["DATABASE_URL"] = url
    os.environ["DJANGO_CONN_MAX_AGE"] = "0"
    os.environ["AGRO_WA_TESTE"] = "1"
    os.environ["AGRO_WA_DJANGO_URL"] = "http://127.0.0.1:8011"
    os.environ["AGRO_WA_MEDIA_ROOT"] = str(media)
    os.environ["AGRO_WA_AUTH_DIR"] = str(base / "whatsapp_atendimento" / "auth-zap-teste")
    os.environ["PDV_ENTREGA_WHATSAPP"] = ""
    os.environ.pop("AGRO_LOJA_DATABASE_URL", None)
    os.environ["TEMP"] = str(tmp)
    os.environ["TMP"] = str(tmp)
    token = (wa.get("AGRO_WA_BRIDGE_TOKEN") or "").strip()
    if token:
        os.environ["AGRO_WA_BRIDGE_TOKEN"] = token
    os.execv(
        sys.executable,
        [sys.executable, "manage.py", "runserver", "127.0.0.1:8011", "--noreload"],
    )


if __name__ == "__main__":
    main()
