"""Configuração NFC-e (SP, Simples Nacional) via .env / settings."""
from __future__ import annotations

import base64
import os
import re
import tempfile
from typing import Any

from decouple import config

_cert_temp_cache: str | None = None


def _cfg(name: str, default: str = "") -> str:
    return (config(name, default=default) or default).strip()


def nfce_resolve_cert_path() -> str:
    """Caminho do .pfx: arquivo no disco ou temporário a partir de NFC_E_CERT_BASE64 (Render)."""
    global _cert_temp_cache
    path = _cfg("NFC_E_CERT_PATH") or _cfg("NFE_DIST_DFE_CERT_PATH")
    if path and os.path.isfile(path):
        return path
    b64 = _cfg("NFC_E_CERT_BASE64") or _cfg("NFE_DIST_DFE_CERT_BASE64")
    if b64:
        if not _cert_temp_cache or not os.path.isfile(_cert_temp_cache):
            raw = base64.b64decode(re.sub(r"\s", "", b64))
            f = tempfile.NamedTemporaryFile(delete=False, suffix=".pfx")
            f.write(raw)
            f.close()
            _cert_temp_cache = f.name
        return _cert_temp_cache
    return path


def nfce_emissao_automatica() -> bool:
    modo = (_cfg("NFC_E_MODO", "manual") or "manual").strip().lower()
    return modo in ("auto", "automatico", "automatica", "automatic")


def nfce_emissao_solicitada(data: dict | None) -> bool:
    """True se a NFC-e deve ser emitida nesta venda (auto ou flag manual do PDV)."""
    if nfce_emissao_automatica():
        return True
    if not isinstance(data, dict):
        return False
    raw = data.get("nfce_emitir")
    if raw is None:
        raw = data.get("nfce_solicitar")
    if isinstance(raw, str):
        return raw.strip().lower() in ("1", "true", "sim", "yes", "on")
    return bool(raw)


def nfce_cfg() -> dict[str, Any]:
    cert_path = nfce_resolve_cert_path()
    cert_password = _cfg("NFC_E_CERT_PASSWORD") or _cfg("NFE_DIST_DFE_CERT_PASSWORD")
    cnpj = re.sub(r"\D", "", _cfg("NFC_E_CNPJ") or _cfg("NFE_DIST_DFE_CNPJ"))[:14]
    try:
        tp_amb = int(_cfg("NFC_E_TP_AMB", "2") or 2)
    except (TypeError, ValueError):
        tp_amb = 2
    if tp_amb not in (1, 2):
        tp_amb = 2
    try:
        serie = int(_cfg("NFC_E_SERIE", "20") or 20)
    except (TypeError, ValueError):
        serie = 20
    try:
        proximo_numero_inicial = int(_cfg("NFC_E_PROXIMO_NUMERO", "1") or 1)
    except (TypeError, ValueError):
        proximo_numero_inicial = 1
    try:
        csc_id = int(re.sub(r"\D", "", _cfg("NFC_E_CSC_ID", "1") or "1") or 1)
    except (TypeError, ValueError):
        csc_id = 1
    cidade = _cfg("NFC_E_CIDADE")[:60]
    cmun = re.sub(r"\D", "", _cfg("NFC_E_CMUN"))[:7]
    # Jacupiranga/SP = 3524600 (3521900 é Guaiçara — valor errado no setup inicial)
    if cidade.strip().lower() == "jacupiranga":
        cmun = "3524600"
    return {
        "ativo": config("NFC_E_ENABLED", default=False, cast=bool),
        "cert_path": cert_path,
        "cert_password": cert_password,
        "cnpj": cnpj,
        "ie": re.sub(r"\D", "", _cfg("NFC_E_IE"))[:14],
        "razao_social": _cfg("NFC_E_RAZAO_SOCIAL")[:150],
        "fantasia": (_cfg("NFC_E_FANTASIA") or _cfg("NFC_E_RAZAO_SOCIAL"))[:60],
        "logradouro": _cfg("NFC_E_LOGRADOURO")[:60],
        "numero": _cfg("NFC_E_NUMERO", "S/N")[:60],
        "bairro": _cfg("NFC_E_BAIRRO")[:60],
        "cmun": cmun,
        "cidade": cidade,
        "uf": (_cfg("NFC_E_UF", "SP") or "SP").upper()[:2],
        "cep": re.sub(r"\D", "", _cfg("NFC_E_CEP"))[:8],
        "fone": re.sub(r"\D", "", _cfg("NFC_E_FONE"))[:14],
        "csc_id": csc_id,
        "csc_token": _cfg("NFC_E_CSC_TOKEN"),
        "serie": max(1, min(serie, 999)),
        "proximo_numero_inicial": max(1, proximo_numero_inicial),
        "tp_amb": tp_amb,
    }


def nfce_configurada() -> bool:
    c = nfce_cfg()
    if not c["ativo"]:
        return False
    cert_ok = bool(c["cert_path"] and os.path.isfile(c["cert_path"]) and c["cert_password"])
    return bool(
        cert_ok
        and len(c["cnpj"]) == 14
        and c["ie"]
        and c["razao_social"]
        and c["logradouro"]
        and len(c["cmun"]) == 7
        and len(c["cep"]) == 8
        and c["csc_token"]
        and c["uf"] == "SP"
    )


def nfce_config_resumo() -> dict[str, Any]:
    c = nfce_cfg()
    return {
        "ativo": nfce_configurada(),
        "modo": "auto" if nfce_emissao_automatica() else "manual",
        "tp_amb": c["tp_amb"],
        "serie": c["serie"],
        "cnpj": c["cnpj"][:8] + "…" if len(c["cnpj"]) == 14 else "",
        "uf": c["uf"],
        "cmun": c["cmun"],
        "cidade": c["cidade"],
    }
