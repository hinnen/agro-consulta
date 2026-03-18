import re
import unicodedata
from difflib import SequenceMatcher


STOPWORDS = {
    "de", "da", "do", "das", "dos",
    "e", "em", "para", "com", "a", "o", "as", "os",
    "kg", "g", "gr", "ml", "lt", "l",
}

SINONIMOS = {
    "cao": ["cachorro", "caes", "dog", "cão"],
    "cachorro": ["cao", "dog", "cão", "caes"],
    "dog": ["cao", "cachorro", "cão"],
    "gato": ["felino"],
    "racao": ["ração"],
    "vermifugo": ["vermífugo", "vermifugo", "vermifugacao", "vermifugação"],
    "antipulgas": ["anti pulgas", "pulgas", "pulga"],
    "milho": ["milho"],
    "quebrado": ["partido", "quirera"],
}

GRANEL_TERMOS = {
    "granel",
}


def remover_acentos(txt: str) -> str:
    txt = txt or ""
    txt = unicodedata.normalize("NFD", txt)
    return "".join(c for c in txt if unicodedata.category(c) != "Mn")


def normalizar(txt: str) -> str:
    txt = (txt or "").lower()
    txt = remover_acentos(txt)
    txt = txt.replace("ç", "c")
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def tokens(txt: str, remover_stopwords: bool = True) -> list[str]:
    base = normalizar(txt)
    if not base:
        return []

    itens = [t for t in base.split(" ") if t]
    if remover_stopwords:
        itens = [t for t in itens if t not in STOPWORDS]
    return itens


def expandir_tokens(txt_ou_tokens) -> list[str]:
    if isinstance(txt_ou_tokens, str):
        base_tokens = tokens(txt_ou_tokens)
    else:
        base_tokens = [normalizar(t) for t in (txt_ou_tokens or []) if normalizar(t)]

    expandidos = set(base_tokens)

    for token in base_tokens:
        if token in SINONIMOS:
            for equivalente in SINONIMOS[token]:
                equivalente_norm = normalizar(equivalente)
                if equivalente_norm:
                    expandidos.add(equivalente_norm)

        for chave, valores in SINONIMOS.items():
            valores_norm = {normalizar(v) for v in valores}
            if token in valores_norm:
                expandidos.add(chave)

    return list(expandidos)


def similaridade(a: str, b: str) -> float:
    a = normalizar(a)
    b = normalizar(b)
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def eh_granel(categoria: str = "", subcategoria: str = "", nome: str = "") -> bool:
    texto = " ".join([
        normalizar(categoria),
        normalizar(subcategoria),
        normalizar(nome),
    ]).strip()

    if not texto:
        return False

    return any(termo in texto for termo in GRANEL_TERMOS)


def montar_busca_texto(nome: str = "", marca: str = "", categoria: str = "", subcategoria: str = "") -> str:
    partes = [
        normalizar(nome),
        normalizar(marca),
        normalizar(categoria),
        normalizar(subcategoria),
    ]
    return " ".join([p for p in partes if p]).strip()
