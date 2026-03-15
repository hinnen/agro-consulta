import re
import unicodedata

def normalizar(txt: str) -> str:
    txt = (txt or "").lower()
    txt = unicodedata.normalize("NFD", txt)
    txt = "".join(c for c in txt if unicodedata.category(c) != "Mn")
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt

def tokens(txt: str):
    base = normalizar(txt)
    return [t for t in base.split(" ") if t]