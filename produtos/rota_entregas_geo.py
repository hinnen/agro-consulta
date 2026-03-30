"""
Geocodificação e ordenação de paradas de entrega (vizinho mais próximo / Haversine).

Usa Nominatim (OpenStreetMap): política de uso — 1 requisição/s, User-Agent identificável.
Resultados em cache Django para reduzir chamadas.
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from typing import Any

import requests
from django.core.cache import cache
from openlocationcode import openlocationcode as olc

NOMINATIM_SEARCH = "https://nominatim.openstreetmap.org/search"
# Plus Codes (OLC) no texto — captura código completo (587HCX4J+8R) ou curto (CX4J+8R).
_PLUS_CODE_IN_TEXT_RE = re.compile(
    r"(?:\d{2,8})?[2-9CFGHJMPQRVWX]{2,8}\+[2-9CFGHJMPQRVWX]{2,3}",
    re.IGNORECASE,
)
USER_AGENT = "AgroConsulta/1.0 (rota-entregas; contato via administrador do sistema)"

_last_nom_mono: list[float] = [0.0]


def _throttle_nominatim() -> None:
    elapsed = time.monotonic() - _last_nom_mono[0]
    if elapsed < 1.08:
        time.sleep(1.08 - elapsed)
    _last_nom_mono[0] = time.monotonic()


def normalize_q(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())[:500]


def parse_lat_lng_text(s: str) -> tuple[float, float] | None:
    s = str(s or "").strip()
    m = re.match(r"^(-?\d{1,3}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)$", s)
    if not m:
        return None
    try:
        lat, lon = float(m.group(1)), float(m.group(2))
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return None
        return lat, lon
    except ValueError:
        return None


def find_valid_plus_codes_in_text(s: str) -> list[str]:
    """Retorna códigos OLC válidos encontrados no texto (maiúsculos)."""
    out: list[str] = []
    for m in _PLUS_CODE_IN_TEXT_RE.finditer(str(s or "")):
        c = m.group(0).upper()
        if olc.isValid(c):
            out.append(c)
    return out


def try_plus_code_latlng(
    texto: str,
    ref_ll: tuple[float, float] | None,
) -> tuple[tuple[float, float] | None, str | None]:
    """
    Converte Plus Code do Google (OLC) em lat/lng sem Nominatim.
    Códigos curtos precisam de um ponto de referência (ex.: coordenadas da loja).
    """
    found = find_valid_plus_codes_in_text(texto)
    if not found:
        return None, None
    found.sort(key=lambda x: (olc.isFull(x), len(x)), reverse=True)
    code = found[0]
    try:
        if olc.isFull(code):
            area = olc.decode(code)
            return (area.latitudeCenter, area.longitudeCenter), "plus_code_full"
        if olc.isShort(code):
            if ref_ll is None:
                return None, "plus_code_curto_sem_ref"
            full = olc.recoverNearest(code, ref_ll[0], ref_ll[1])
            area = olc.decode(full)
            return (area.latitudeCenter, area.longitudeCenter), "plus_code_short"
    except Exception as exc:
        return None, str(exc)[:120]
    return None, None


def extract_latlng_from_google_maps_url(url: str) -> tuple[float, float] | None:
    t = str(url or "")
    m = re.search(r"@(-?\d+\.?\d*),(-?\d+\.?\d*)", t)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    m = re.search(r"[!&?]3d(-?\d+\.?\d*)[!&?]4d(-?\d+\.?\d*)", t, re.I)
    if m:
        try:
            return float(m.group(1)), float(m.group(2))
        except ValueError:
            pass
    return None


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1 = a
    lat2, lon2 = b
    r_km = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r_km * math.asin(min(1.0, math.sqrt(x)))


def nominatim_geocode(query: str) -> tuple[tuple[float, float] | None, str | None]:
    nq = normalize_q(query)
    if not nq:
        return None, "texto vazio"
    ck = "nom_ent_" + hashlib.sha256(nq.encode("utf-8")).hexdigest()[:40]
    cached = cache.get(ck)
    if isinstance(cached, dict) and "lat" in cached:
        return (float(cached["lat"]), float(cached["lng"])), None
    if cached == "miss":
        return None, "não encontrado (cache)"

    _throttle_nominatim()
    try:
        r = requests.get(
            NOMINATIM_SEARCH,
            params={"q": nq, "format": "json", "limit": 1},
            headers={"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt,en"},
            timeout=18,
        )
        if r.status_code != 200:
            cache.set(ck, "miss", 300)
            return None, f"HTTP {r.status_code}"
        data = r.json()
        if not data:
            cache.set(ck, "miss", 900)
            return None, "endereço não encontrado"
        lat = float(data[0]["lat"])
        lon = float(data[0]["lon"])
        cache.set(ck, {"lat": lat, "lng": lon}, 86400)
        return (lat, lon), None
    except Exception as exc:
        cache.set(ck, "miss", 120)
        return None, str(exc)[:220]


def resolve_parada_latlng(
    texto_busca: str,
    maps_url_manual: str,
    ref_ll: tuple[float, float] | None = None,
) -> tuple[tuple[float, float] | None, str]:
    mu = str(maps_url_manual or "").strip()
    if mu.lower().startswith("http"):
        ll = extract_latlng_from_google_maps_url(mu)
        if ll:
            return ll, "coord_maps"
    nq = normalize_q(texto_busca)
    if not nq:
        return None, "sem texto de busca"
    ll_pc, fonte_pc = try_plus_code_latlng(nq, ref_ll)
    if ll_pc:
        return ll_pc, fonte_pc or "plus_code"
    ll, err = nominatim_geocode(nq)
    if ll:
        return ll, "geocode"
    return None, err or "falha geocode"


def ordenar_entregas_por_proximidade(
    origem_texto: str,
    paradas: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    origem_texto: lat,lng ou endereço (geocodifica se necessário).
    paradas: [{ "id", "texto", "label", "maps_url_manual"? }, ...]
    """
    avisos: list[str] = []
    ot = str(origem_texto or "").strip()
    if not ot:
        return {"ok": False, "erro": "Origem vazia. Configure LOJA_MAPS_ORIGEM_* ou escolha a loja no painel."}

    origin_ll = parse_lat_lng_text(ot)
    if origin_ll:
        origem_fonte = "coord"
    else:
        origin_ll, oerr = nominatim_geocode(ot)
        if not origin_ll:
            return {"ok": False, "erro": f"Não foi possível localizar a origem: {oerr or '?'}"}
        origem_fonte = "geocode"
        avisos.append("Origem geocodificada por texto — confira se o ponto está correto.")

    if not paradas or not isinstance(paradas, list):
        return {"ok": False, "erro": "Informe ao menos uma parada."}
    if len(paradas) > 23:
        return {"ok": False, "erro": "Máximo de 23 paradas por rota (limite do Maps)."}

    resolved: list[dict[str, Any]] = []
    for i, p in enumerate(paradas):
        pid = p.get("id")
        texto = str(p.get("texto") or "").strip()
        label = str(p.get("label") or texto or f"Parada {i + 1}")[:200]
        mum = str(p.get("maps_url_manual") or "").strip()
        ll, fonte = resolve_parada_latlng(texto, mum, ref_ll=origin_ll)
        item = {
            "id": pid,
            "label": label,
            "texto": texto or label,
            "maps_url_manual": mum,
            "lat": ll[0] if ll else None,
            "lng": ll[1] if ll else None,
            "fonte_coord": fonte if ll else None,
            "geocode_erro": None if ll else fonte,
        }
        resolved.append(item)

    ok_pts = [x for x in resolved if x["lat"] is not None]
    bad_pts = [x for x in resolved if x["lat"] is None]
    if not ok_pts:
        return {
            "ok": False,
            "erro": "Nenhuma parada pôde ser localizada. Inclua Plus Code, endereço ou link do Maps com local.",
            "paradas": resolved,
        }

    if bad_pts:
        avisos.append(
            f"{len(bad_pts)} parada(s) sem coordenadas — ficaram por último na lista (confira manualmente)."
        )

    ordered: list[dict[str, Any]] = []
    remaining = list(ok_pts)
    cur: tuple[float, float] = origin_ll
    ordem = 0
    total_km = 0.0
    while remaining:
        best_i = min(
            range(len(remaining)),
            key=lambda j: haversine_km(cur, (remaining[j]["lat"], remaining[j]["lng"])),
        )
        p = remaining.pop(best_i)
        d = haversine_km(cur, (p["lat"], p["lng"]))
        total_km += d
        ordem += 1
        ordered.append(
            {
                "id": p["id"],
                "ordem": ordem,
                "label": p["label"],
                "texto": p["texto"],
                "maps_url_manual": p.get("maps_url_manual") or "",
                "km_da_anterior": round(d, 2),
                "lat": p["lat"],
                "lng": p["lng"],
                "fonte_coord": p.get("fonte_coord"),
            }
        )
        cur = (p["lat"], p["lng"])

    for p in bad_pts:
        ordem += 1
        ordered.append(
            {
                "id": p["id"],
                "ordem": ordem,
                "label": p["label"],
                "texto": p["texto"],
                "maps_url_manual": p.get("maps_url_manual") or "",
                "km_da_anterior": None,
                "lat": None,
                "lng": None,
                "fonte_coord": None,
                "geocode_erro": p.get("geocode_erro"),
            }
        )

    return {
        "ok": True,
        "origem": {
            "lat": origin_ll[0],
            "lng": origin_ll[1],
            "fonte": origem_fonte,
            "texto": ot,
        },
        "paradas_ordenadas": ordered,
        "km_total_estimado": round(total_km, 2),
        "avisos": avisos,
    }
