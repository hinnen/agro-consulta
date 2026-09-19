"""Cadastro rápido de produto no PDV — checagem EAN, lookup internet, flag pendente."""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from django.db.models import Q
from django.utils import timezone as dj_tz

logger = logging.getLogger(__name__)

_EAN_DIGITS = re.compile(r"^\d{8,14}$")


def normalizar_ean(raw: str) -> str:
    return re.sub(r"\D", "", str(raw or "").strip())


def ean_parece_valido(ean: str) -> bool:
    return bool(_EAN_DIGITS.match(str(ean or "").strip()))


def buscar_produto_por_codigo(codigo: str) -> dict[str, Any] | None:
    """Retorna resumo do produto se EAN/GM/código já existir (Postgres + overlay)."""
    from produtos.catalogo_agro import obter_produto_model, produto_agro_para_row
    from produtos.models import Produto, ProdutoGestaoOverlayAgro

    cb = str(codigo or "").strip()
    if not cb:
        return None
    ean = normalizar_ean(cb)
    keys = [cb]
    if ean and ean != cb:
        keys.append(ean)

    p = None
    for k in keys:
        p = (
            Produto.objects.filter(
                Q(codigo_barras__iexact=k)
                | Q(codigo_interno__iexact=k)
                | Q(codigo_nfe__iexact=k)
            )
            .order_by("id")
            .first()
        )
        if p is not None:
            break
    if p is None and ean:
        # Overlay com barras sem linha Produto (raro)
        ov = (
            ProdutoGestaoOverlayAgro.objects.filter(codigo_barras__iexact=ean)
            .order_by("id")
            .first()
        )
        if ov is not None:
            p = obter_produto_model(ov.produto_externo_id)
            if p is None:
                return {
                    "id": ov.produto_externo_id,
                    "nome": (ov.nome or ov.produto_externo_id)[:200],
                    "codigo": "",
                    "codigo_nfe": (ov.codigo_nfe or "")[:64],
                    "codigo_barras": (ov.codigo_barras or ean)[:50],
                    "preco_venda": float(ov.preco_venda or 0) if ov.preco_venda is not None else 0.0,
                }
    if p is None:
        return None
    row = produto_agro_para_row(p)
    return {
        "id": str(row.get("id") or p.produto_externo_id),
        "nome": str(row.get("nome") or "")[:200],
        "codigo": str(row.get("codigo") or "")[:50],
        "codigo_nfe": str(row.get("codigo_nfe") or "")[:64],
        "codigo_barras": str(row.get("codigo_barras") or "")[:50],
        "preco_venda": float(row.get("preco_venda") or 0),
    }


def _ncm_so_digitos(raw: Any) -> str:
    from produtos.agro_produto_fiscal_defaults import normalizar_ncm_somente_digitos

    return normalizar_ncm_somente_digitos(raw)


def formatar_ncm_exibicao(raw: Any) -> str:
    """Ex.: 22072019 → 2207.20.19 (só exibição)."""
    d = _ncm_so_digitos(raw)
    if len(d) == 8:
        return f"{d[:4]}.{d[4:6]}.{d[6:]}"
    return d


def _extrair_ncm_de_payload(data: dict[str, Any]) -> str:
    ncm_obj = data.get("ncm")
    if isinstance(ncm_obj, dict):
        return _ncm_so_digitos(ncm_obj.get("code") or ncm_obj.get("codigo") or "")
    if isinstance(ncm_obj, str):
        return _ncm_so_digitos(ncm_obj)
    return _ncm_so_digitos(data.get("ncm_code") or data.get("codigo_ncm") or "")


def _extrair_thumb_cosmos(data: dict[str, Any]) -> str:
    for key in ("thumbnail", "thumbnail_url", "image", "picture", "img"):
        u = str(data.get(key) or "").strip()
        if u.startswith("http"):
            return u[:500]
    return ""


def _extrair_thumb_off(prod: dict[str, Any]) -> str:
    for key in (
        "image_front_url",
        "image_url",
        "image_front_small_url",
        "image_small_url",
        "image_thumb_url",
    ):
        u = str(prod.get(key) or "").strip()
        if u.startswith("http"):
            return u[:500]
    return ""


def _consultar_ean_cosmos(digits: str, *, timeout: float = 4.0) -> dict[str, Any] | None:
    """Bluesoft Cosmos (BR) — precisa ``AGRO_COSMOS_TOKEN`` no .env (cadastro gratuito no site)."""
    import json
    import urllib.error
    import urllib.request

    from django.conf import settings

    token = str(getattr(settings, "AGRO_COSMOS_TOKEN", "") or "").strip()
    if not token:
        return None
    ua = str(getattr(settings, "AGRO_COSMOS_USER_AGENT", "") or "").strip() or (
        "SisVale-AgroConsulta/1.0"
    )
    url = f"https://api.cosmos.bluesoft.com.br/gtins/{digits}.json"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": ua,
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Cosmos-Token": token,
            },
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
        data = json.loads(raw) if raw else {}
        if not isinstance(data, dict):
            return None
        nome = str(data.get("description") or data.get("description_html") or "").strip()
        marca = ""
        brand = data.get("brand")
        if isinstance(brand, dict):
            marca = str(brand.get("name") or "").strip()
        elif isinstance(brand, str):
            marca = brand.strip()
        if len(nome) < 2:
            return None
        ncm = _extrair_ncm_de_payload(data)
        return {
            "ok": True,
            "achou": True,
            "fonte": "cosmos",
            "nome": nome[:300],
            "marca": marca[:120],
            "ean": digits,
            "ncm": ncm,
            "ncm_exibicao": formatar_ncm_exibicao(ncm) if ncm else "",
            "foto_url": _extrair_thumb_cosmos(data),
        }
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
        logger.info("cosmos EAN %s: %s", digits, e)
        return None
    except Exception:
        logger.warning("cosmos EAN falhou", exc_info=True)
        return None


def consultar_ean_internet(ean: str, *, timeout: float = 4.0) -> dict[str, Any]:
    """
    Ordem: Bluesoft Cosmos (se token) → Open Food Facts / Products Facts.
    Sem achado → sugestao vazia (ok=True, achou=False). Rede nunca trava o cadastro.
    """
    import json
    import urllib.error
    import urllib.request

    digits = normalizar_ean(ean)
    out: dict[str, Any] = {
        "ok": True,
        "achou": False,
        "fonte": "",
        "nome": "",
        "marca": "",
        "ean": digits,
        "ncm": "",
        "ncm_exibicao": "",
        "foto_url": "",
        "motivo": "",
    }
    if not ean_parece_valido(digits):
        out["motivo"] = "codigo_invalido"
        return out

    cosmos = _consultar_ean_cosmos(digits, timeout=timeout)
    if cosmos and cosmos.get("achou"):
        return cosmos

    urls = [
        f"https://world.openfoodfacts.org/api/v2/product/{digits}.json",
        f"https://br.openfoodfacts.org/api/v2/product/{digits}.json",
        f"https://world.openproductsfacts.org/api/v2/product/{digits}.json",
    ]
    for url in urls:
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "SisVale-AgroConsulta/1.0 (cadastro-rapido-pdv; loja)",
                    "Accept": "application/json",
                },
                method="GET",
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw else {}
            if not isinstance(data, dict):
                continue
            status = data.get("status")
            prod = data.get("product") if isinstance(data.get("product"), dict) else {}
            if status not in (1, "1") or not prod:
                continue
            nome = (
                str(prod.get("product_name_pt") or "").strip()
                or str(prod.get("product_name") or "").strip()
                or str(prod.get("generic_name_pt") or "").strip()
                or str(prod.get("generic_name") or "").strip()
            )
            marca = str(prod.get("brands") or "").strip().split(",")[0].strip()
            if len(nome) < 2:
                continue
            ncm = _ncm_so_digitos(prod.get("ncm") or prod.get("code_ncm") or "")
            out.update(
                {
                    "achou": True,
                    "fonte": "openfoodfacts",
                    "nome": nome[:300],
                    "marca": marca[:120],
                    "ean": digits,
                    "ncm": ncm,
                    "ncm_exibicao": formatar_ncm_exibicao(ncm) if ncm else "",
                    "foto_url": _extrair_thumb_off(prod),
                    "motivo": "",
                }
            )
            return out
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as e:
            logger.info("consultar_ean_internet %s: %s", url[:60], e)
            continue
        except Exception:
            logger.warning("consultar_ean_internet falhou", exc_info=True)
            continue

    from django.conf import settings

    tem_cosmos = bool(str(getattr(settings, "AGRO_COSMOS_TOKEN", "") or "").strip())
    out["motivo"] = "sem_cosmos" if not tem_cosmos else "nao_encontrado"
    return out


def overlays_pendentes_pdv_qs():
    from produtos.models import ProdutoGestaoOverlayAgro

    return ProdutoGestaoOverlayAgro.objects.filter(
        cadastro_extras__pendente_conferencia=True,
        cadastro_extras__origem_pdv=True,
    )


def contar_pendentes_pdv() -> int:
    try:
        return int(overlays_pendentes_pdv_qs().count())
    except Exception:
        logger.warning("contar_pendentes_pdv", exc_info=True)
        return 0


def ids_pendentes_pdv(limit: int = 500) -> list[str]:
    lim = max(1, min(int(limit or 500), 2000))
    try:
        return list(
            overlays_pendentes_pdv_qs()
            .order_by("-atualizado_em")
            .values_list("produto_externo_id", flat=True)[:lim]
        )
    except Exception:
        logger.warning("ids_pendentes_pdv", exc_info=True)
        return []


def marcar_extras_origem_pdv(ex: dict | None) -> dict:
    """Garante flags de origem PDV + pendente conferência no cadastro_extras."""
    out = dict(ex) if isinstance(ex, dict) else {}
    out["origem_pdv"] = True
    out["pendente_conferencia"] = True
    if not out.get("criado_em_pdv"):
        try:
            out["criado_em_pdv"] = dj_tz.now().isoformat()
        except Exception:
            out["criado_em_pdv"] = datetime.now(timezone.utc).isoformat()
    return out


def limpar_pendente_conferencia(ex: dict | None) -> dict:
    out = dict(ex) if isinstance(ex, dict) else {}
    if out.get("pendente_conferencia"):
        out["pendente_conferencia"] = False
        try:
            out["conferido_em"] = dj_tz.now().isoformat()
        except Exception:
            out["conferido_em"] = datetime.now(timezone.utc).isoformat()
    return out


def alocar_gm_preview() -> tuple[str | None, str, str]:
    """
    Pré-visualiza próximo código sistema + GM (sem gravar).
    Retorna (erro, codigo_sistema, codigo_gm).
    """
    from produtos.cadastro_codigo_sequencial_util import alocar_codigo_sequencial_novo_cadastro

    err, c_sys, c_gm = alocar_codigo_sequencial_novo_cadastro(None, None)
    if err is not None:
        return str(err.get("erro") or "Erro ao gerar código."), "", ""
    return None, str(c_sys or "").strip(), str(c_gm or "").strip()
