"""Lat/lng → Plus Code + endereço (checkout catálogo delivery)."""
from __future__ import annotations

from openlocationcode import openlocationcode as olc

from produtos.models import compor_endereco_resumo_cliente
from produtos.rota_entregas_geo import reverse_geocode_endereco


def _validar_latlng(lat: float, lng: float) -> None:
    if not (-90 <= lat <= 90 and -180 <= lng <= 180):
        raise ValueError("Coordenadas inválidas.")


def localizacao_de_latlng(lat: float, lng: float) -> dict:
    _validar_latlng(lat, lng)
    codigo_full = olc.encode(lat, lng)
    endereco = reverse_geocode_endereco(lat, lng)
    cidade = (endereco.get("cidade") or "").strip()
    uf = (endereco.get("uf") or "").strip().upper()
    plus_code = codigo_full
    if cidade:
        plus_code = f"{codigo_full} {cidade}" + (f", {uf}" if uf else "")
    endereco_linha = compor_endereco_resumo_cliente(
        cep=(endereco.get("cep") or "").strip(),
        uf=uf,
        cidade=cidade,
        bairro=(endereco.get("bairro") or "").strip(),
        logradouro=(endereco.get("logradouro") or "").strip(),
        numero=(endereco.get("numero") or "").strip(),
        complemento=(endereco.get("complemento") or "").strip(),
    )
    if not endereco_linha:
        endereco_linha = f"{lat:.6f}, {lng:.6f}"
    return {
        "plus_code": plus_code[:120],
        "plus_code_full": codigo_full,
        "lat": lat,
        "lng": lng,
        "endereco_linha": endereco_linha[:500],
        "logradouro": (endereco.get("logradouro") or "")[:300],
        "numero": (endereco.get("numero") or "")[:30],
        "bairro": (endereco.get("bairro") or "")[:120],
        "cidade": cidade[:120],
        "uf": uf[:2],
        "cep": (endereco.get("cep") or "")[:12],
        "maps_url": f"https://www.google.com/maps?q={lat},{lng}",
    }
