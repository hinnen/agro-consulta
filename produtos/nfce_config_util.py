"""Configuração NFC-e (SP, Simples Nacional) via .env / settings."""
from __future__ import annotations

import base64
import logging
import os
import re
import tempfile
import time
from typing import Any

from decouple import config

logger = logging.getLogger(__name__)

_cert_temp_cache: str | None = None

NFCE_FORMAS_PAGAMENTO_AUTO: frozenset[str] = frozenset(
    {
        "PIX",
        "Cartão de débito",
        "Cartão de crédito",
        "Cartão de crédito parcelado",
    }
)

# Maquininha manual «Mercado Pago Renan» — nunca dispara NFC-e automática
# (débito/crédito/parcelado/Pix). Demais máquinas e Point automático não mudam.
NFCE_MAQUINAS_SEM_EMISSAO_AUTO: frozenset[str] = frozenset(
    {
        "mp_renan",
        "pix_mp_renan",
    }
)


def nfce_formas_pagamento_auto() -> list[str]:
    return sorted(NFCE_FORMAS_PAGAMENTO_AUTO)


def nfce_maquina_ids_no_payload(data: dict | None) -> list[str]:
    """IDs de maquininha nos lançamentos do payload PDV."""
    if not isinstance(data, dict):
        return []
    out: list[str] = []
    pag = data.get("pagamentos")
    if isinstance(pag, list):
        for row in pag:
            if not isinstance(row, dict):
                continue
            mid = str(
                row.get("maquinaId") or row.get("maquina_id") or row.get("maquina") or ""
            ).strip()
            if mid:
                out.append(mid)
    mid_top = str(
        data.get("maquinaId") or data.get("maquina_id") or data.get("maquina") or ""
    ).strip()
    if mid_top:
        out.append(mid_top)
    return out


def nfce_venda_usa_maquina_sem_auto(data: dict | None) -> bool:
    """True se a venda usou Mercado Pago Renan (cartão ou Pix)."""
    sem = NFCE_MAQUINAS_SEM_EMISSAO_AUTO
    return any(mid in sem for mid in nfce_maquina_ids_no_payload(data))


def _cfg(name: str, default: str = "") -> str:
    return (config(name, default=default) or default).strip()


def nfce_garantir_certificado(*, force: bool = False) -> str:
    """Materializa o .pfx no disco (Render / base64). Retorna caminho ou ''."""
    global _cert_temp_cache
    path = _cfg("NFC_E_CERT_PATH") or _cfg("NFE_DIST_DFE_CERT_PATH")
    if path and os.path.isfile(path):
        return path
    b64 = _cfg("NFC_E_CERT_BASE64") or _cfg("NFE_DIST_DFE_CERT_BASE64")
    if not b64:
        return path if path and os.path.isfile(path) else ""
    if force:
        _cert_temp_cache = None
    if _cert_temp_cache and os.path.isfile(_cert_temp_cache):
        return _cert_temp_cache
    try:
        raw = base64.b64decode(re.sub(r"\s", "", b64))
    except Exception:
        logger.exception("NFC-e: falha ao decodificar certificado base64")
        return ""
    try:
        f = tempfile.NamedTemporaryFile(delete=False, suffix=".pfx")
        f.write(raw)
        f.close()
        _cert_temp_cache = f.name
        return _cert_temp_cache
    except Exception:
        logger.exception("NFC-e: falha ao gravar certificado temporário")
        return ""


def nfce_resolve_cert_path() -> str:
    """Caminho do .pfx: arquivo no disco ou temporário a partir de NFC_E_CERT_BASE64 (Render)."""
    return nfce_garantir_certificado()


def _formas_pagamento_no_payload(data: dict) -> list[str]:
    from produtos.caixa_util import normalizar_forma_pagamento_caixa

    formas: list[str] = []
    pag = data.get("pagamentos")
    if isinstance(pag, list):
        for row in pag:
            if not isinstance(row, dict):
                continue
            raw = str(
                row.get("formaPagamento")
                or row.get("forma_pagamento")
                or row.get("forma")
                or ""
            ).strip()
            if raw:
                formas.append(normalizar_forma_pagamento_caixa(raw))
    if not formas:
        fp = str(data.get("forma_pagamento") or data.get("formaPagamento") or "").strip()
        if fp:
            for part in re.split(r"\s*\+\s*", fp):
                part = part.strip()
                if part:
                    formas.append(normalizar_forma_pagamento_caixa(part))
    return formas


def nfce_venda_tem_forma_pagamento_auto(data: dict | None) -> bool:
    if not isinstance(data, dict):
        return False
    auto = NFCE_FORMAS_PAGAMENTO_AUTO
    return any(f in auto for f in _formas_pagamento_no_payload(data))


def nfce_emissao_automatica() -> bool:
    modo = (_cfg("NFC_E_MODO", "manual") or "manual").strip().lower()
    return modo in ("auto", "automatico", "automatica", "automatic")


def _nfce_emitir_explicito(data: dict | None) -> bool:
    if not isinstance(data, dict):
        return False
    raw = data.get("nfce_emitir")
    if raw is None:
        raw = data.get("nfce_solicitar")
    if isinstance(raw, str):
        return raw.strip().lower() in ("1", "true", "sim", "yes", "on")
    return bool(raw)


def nfce_emissao_solicitada(data: dict | None) -> bool:
    """True se a NFC-e deve ser emitida nesta venda (global auto, forma auto ou checkbox PDV).

    Mercado Pago Renan: **nunca** emite na finalização da venda (débito/crédito/parcelado/Pix).
    Se precisar de cupom fiscal depois: Consultar vendas → Reemitir NFC-e.
    """
    if nfce_venda_usa_maquina_sem_auto(data):
        return False
    if nfce_emissao_automatica():
        return True
    if nfce_venda_tem_forma_pagamento_auto(data):
        return True
    return _nfce_emitir_explicito(data)


# Filial Vila Elias (GM Agro) — defaults; sobrescreve com NFC_E_VILA_* no .env
_VILA_CNPJ_DEFAULT = "48900774000286"
_VILA_IE_DEFAULT = "394051450113"
_VILA_LOGRADOURO_DEFAULT = "Joaquim Mauricio Grothe"
_VILA_NUMERO_DEFAULT = "173"
_VILA_BAIRRO_DEFAULT = "Vila Elias"
_VILA_CEP_DEFAULT = "11940000"
_VILA_FANTASIA_DEFAULT = "Agro Mais Vila Elias"


def nfce_normalizar_loja(loja: str | None) -> str:
    """centro | vila — depósito do caixa / venda."""
    d = str(loja or "").strip().lower()
    if d in ("vila", "2", "vila_elias", "vila-elias"):
        return "vila"
    return "centro"


def nfce_loja_de_venda(venda: Any) -> str:
    """Loja fiscal da NFC-e = depósito da venda (herdado do caixa)."""
    dep = ""
    if venda is not None:
        dep = str(getattr(venda, "deposito", "") or "").strip().lower()
        if not dep:
            sessao = getattr(venda, "sessao_caixa", None)
            if sessao is not None:
                try:
                    from produtos.caixa_util import deposito_de_ponto_caixa

                    dep = deposito_de_ponto_caixa(getattr(sessao, "ponto_caixa", None))
                except Exception:
                    dep = ""
    return nfce_normalizar_loja(dep)


def nfce_loja_de_cnpj(cnpj: str | None) -> str:
    c = re.sub(r"\D", "", str(cnpj or ""))[:14]
    vila = re.sub(r"\D", "", _cfg("NFC_E_VILA_CNPJ") or _VILA_CNPJ_DEFAULT)[:14]
    if c and vila and c == vila:
        return "vila"
    return "centro"


def nfce_cnpj_da_chave(chave: str | None) -> str:
    """CNPJ emitente na chave NFC-e (posições 6–20)."""
    ch = re.sub(r"\D", "", str(chave or ""))
    if len(ch) < 20:
        return ""
    return ch[6:20]


def nfce_cfg(loja: str | None = "centro") -> dict[str, Any]:
    """Config NFC-e. ``loja`` = centro|vila — mesmo cert/CSC; emitente muda."""
    loja_n = nfce_normalizar_loja(loja)
    cert_path = nfce_resolve_cert_path()
    cert_password = _cfg("NFC_E_CERT_PASSWORD") or _cfg("NFE_DIST_DFE_CERT_PASSWORD")
    try:
        tp_amb = int(_cfg("NFC_E_TP_AMB", "2") or 2)
    except (TypeError, ValueError):
        tp_amb = 2
    if tp_amb not in (1, 2):
        tp_amb = 2
    try:
        serie_centro = int(_cfg("NFC_E_SERIE", "20") or 20)
    except (TypeError, ValueError):
        serie_centro = 20
    try:
        serie_vila = int(_cfg("NFC_E_VILA_SERIE") or _cfg("NFC_E_SERIE", "20") or 20)
    except (TypeError, ValueError):
        serie_vila = serie_centro
    try:
        proximo_centro = int(_cfg("NFC_E_PROXIMO_NUMERO", "1") or 1)
    except (TypeError, ValueError):
        proximo_centro = 1
    try:
        proximo_vila = int(_cfg("NFC_E_VILA_PROXIMO_NUMERO", "1") or 1)
    except (TypeError, ValueError):
        proximo_vila = 1
    try:
        csc_id = int(re.sub(r"\D", "", _cfg("NFC_E_CSC_ID", "1") or "1") or 1)
    except (TypeError, ValueError):
        csc_id = 1
    cidade = _cfg("NFC_E_CIDADE")[:60]
    cmun = re.sub(r"\D", "", _cfg("NFC_E_CMUN"))[:7]
    # Jacupiranga/SP = 3524600 (3521900 é Guaiçara — valor errado no setup inicial)
    if cidade.strip().lower() == "jacupiranga":
        cmun = "3524600"
    razao = _cfg("NFC_E_RAZAO_SOCIAL")[:150]
    base = {
        "ativo": config("NFC_E_ENABLED", default=False, cast=bool),
        "loja": loja_n,
        "cert_path": cert_path,
        "cert_password": cert_password,
        "razao_social": razao,
        "cmun": cmun,
        "cidade": cidade,
        "uf": (_cfg("NFC_E_UF", "SP") or "SP").upper()[:2],
        "fone": re.sub(r"\D", "", _cfg("NFC_E_FONE"))[:14],
        "csc_id": csc_id,
        "csc_token": _cfg("NFC_E_CSC_TOKEN"),
        "tp_amb": tp_amb,
    }
    if loja_n == "vila":
        base.update(
            {
                "cnpj": re.sub(r"\D", "", _cfg("NFC_E_VILA_CNPJ") or _VILA_CNPJ_DEFAULT)[:14],
                "ie": re.sub(r"\D", "", _cfg("NFC_E_VILA_IE") or _VILA_IE_DEFAULT)[:14],
                "fantasia": (
                    _cfg("NFC_E_VILA_FANTASIA")
                    or _VILA_FANTASIA_DEFAULT
                    or _cfg("NFC_E_FANTASIA")
                    or razao
                )[:60],
                "logradouro": (_cfg("NFC_E_VILA_LOGRADOURO") or _VILA_LOGRADOURO_DEFAULT)[:60],
                "numero": (_cfg("NFC_E_VILA_NUMERO") or _VILA_NUMERO_DEFAULT or "S/N")[:60],
                "bairro": (_cfg("NFC_E_VILA_BAIRRO") or _VILA_BAIRRO_DEFAULT)[:60],
                "cep": re.sub(r"\D", "", _cfg("NFC_E_VILA_CEP") or _VILA_CEP_DEFAULT)[:8],
                "serie": max(1, min(serie_vila, 999)),
                "proximo_numero_inicial": max(1, proximo_vila),
            }
        )
    else:
        base.update(
            {
                "cnpj": re.sub(r"\D", "", _cfg("NFC_E_CNPJ") or _cfg("NFE_DIST_DFE_CNPJ"))[:14],
                "ie": re.sub(r"\D", "", _cfg("NFC_E_IE"))[:14],
                "fantasia": (_cfg("NFC_E_FANTASIA") or razao)[:60],
                "logradouro": _cfg("NFC_E_LOGRADOURO")[:60],
                "numero": _cfg("NFC_E_NUMERO", "S/N")[:60],
                "bairro": _cfg("NFC_E_BAIRRO")[:60],
                "cep": re.sub(r"\D", "", _cfg("NFC_E_CEP"))[:8],
                "serie": max(1, min(serie_centro, 999)),
                "proximo_numero_inicial": max(1, proximo_centro),
            }
        )
    return base


def _nfce_configurada_once(loja: str | None = "centro") -> bool:
    c = nfce_cfg(loja)
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


def nfce_configurada(
    *,
    warmup: bool = True,
    tentativas: int = 1,
    loja: str | None = "centro",
) -> bool:
    """
    True se NFC-e está pronta para emitir na loja (centro|vila).
    warmup: recria .pfx temporário se sumiu (Render / cold start).
    tentativas: repete após re-materializar certificado (transiente).
    """
    tries = max(1, int(tentativas or 1))
    for idx in range(tries):
        if warmup:
            nfce_garantir_certificado(force=idx > 0)
        if _nfce_configurada_once(loja):
            return True
        if idx + 1 < tries:
            time.sleep(0.15 * (idx + 1))
            nfce_garantir_certificado(force=True)
    return False


def nfce_config_resumo(loja: str | None = "centro") -> dict[str, Any]:
    loja_n = nfce_normalizar_loja(loja)
    c = nfce_cfg(loja_n)
    vila = nfce_cfg("vila")
    return {
        "ativo": nfce_configurada(loja=loja_n),
        "ativo_centro": nfce_configurada(loja="centro", warmup=False),
        "ativo_vila": nfce_configurada(loja="vila", warmup=False),
        "loja": loja_n,
        "modo": "auto" if nfce_emissao_automatica() else "por_forma",
        "formas_auto": nfce_formas_pagamento_auto(),
        "tp_amb": c["tp_amb"],
        "serie": c["serie"],
        "cnpj": c["cnpj"][:8] + "…" if len(c["cnpj"]) == 14 else "",
        "cnpj_centro": (nfce_cfg("centro")["cnpj"][:8] + "…")
        if len(nfce_cfg("centro")["cnpj"]) == 14
        else "",
        "cnpj_vila": vila["cnpj"][:8] + "…" if len(vila["cnpj"]) == 14 else "",
        "uf": c["uf"],
        "cmun": c["cmun"],
        "cidade": c["cidade"],
    }
