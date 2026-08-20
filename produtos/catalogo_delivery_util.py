"""Catálogo delivery GM Agro — produtos marcados no cadastro → vitrine pública."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from django.db import transaction

from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp, extrair_whatsapp_digits
from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro
from produtos.models import (
    CatalogoDeliveryCategoria,
    CatalogoDeliveryConfig,
    ClienteAgro,
    PedidoEntrega,
    Produto,
    ProdutoGestaoOverlayAgro,
    compor_endereco_resumo_cliente,
)
from produtos.pdv_racoes_util import parse_peso_racoes

# Árvore delivery: raiz + até 4 níveis filhos (total 5).
CATALOGO_MAX_NIVEIS = 5

PESOS_GRADE_CATALOGO: list[dict[str, str]] = [
    {"key": "kg:1", "label": "Granel"},
    {"key": "kg:2.5", "label": "Saco 2,5 kg"},
    {"key": "kg:5", "label": "Saco 5 kg"},
    {"key": "kg:10", "label": "Saco 10 kg"},
    {"key": "kg:15", "label": "Saco 15 kg"},
    {"key": "kg:20", "label": "Saco 20 kg"},
    {"key": "kg:25", "label": "Saco 25 kg"},
]


def profundidade_categoria(cat: CatalogoDeliveryCategoria | None) -> int:
    """1 = raiz · 5 = folha máxima."""
    if cat is None:
        return 0
    n = 1
    seen: set[int] = set()
    cur: CatalogoDeliveryCategoria | None = cat
    while cur is not None and cur.parent_id:
        if cur.pk in seen:
            break
        seen.add(cur.pk)
        nxt = getattr(cur, "parent", None)
        if nxt is None:
            n += 1
            break
        cur = nxt
        n += 1
        if n > CATALOGO_MAX_NIVEIS + 2:
            break
    return n


def profundidade_por_parent_id(parent: CatalogoDeliveryCategoria | None) -> int:
    """Profundidade do *filho* se criado sob ``parent`` (None → nível 1)."""
    if parent is None:
        return 1
    return profundidade_categoria(parent) + 1


def _int_id(d: dict, key: str) -> int:
    try:
        v = int(d.get(key) or 0)
    except (TypeError, ValueError):
        return 0
    return v if v > 0 else 0


def _cadeia_de_folha(
    cats: dict[int, CatalogoDeliveryCategoria],
    *ids: int,
) -> list[CatalogoDeliveryCategoria | None]:
    """Resolve até 5 slots [raiz…folha] a partir dos ids informados (prefere o mais profundo)."""
    leaf = None
    for cid in ids:
        if cid and cid in cats:
            leaf = cats[cid]
            break
    chain: list[CatalogoDeliveryCategoria] = []
    seen: set[int] = set()
    cur = leaf
    while cur is not None and cur.pk not in seen:
        seen.add(cur.pk)
        chain.append(cur)
        pid = cur.parent_id
        cur = cats.get(pid) if pid else None
    chain.reverse()
    # Se o 1º não é raiz (parent_id set), tenta subir pelo mapa
    while chain and chain[0].parent_id and chain[0].parent_id in cats:
        pai = cats[chain[0].parent_id]
        if pai.pk in {c.pk for c in chain}:
            break
        chain.insert(0, pai)
    out: list[CatalogoDeliveryCategoria | None] = list(chain[:CATALOGO_MAX_NIVEIS])
    while len(out) < CATALOGO_MAX_NIVEIS:
        out.append(None)
    return out


def pesos_keys_do_item(item: dict) -> list[str]:
    """Chaves kg:* presentes no card (peso próprio + embalagens)."""
    keys: list[str] = []
    seen: set[str] = set()
    candidatos = [item.get("peso_texto") or ""]
    for e in item.get("embalagens") or []:
        candidatos.append(e.get("peso_texto") or "")
        candidatos.append(e.get("rotulo") or "")
    for raw in candidatos:
        k = parse_peso_racoes(raw)
        if k and k.startswith("kg:") and k not in seen:
            seen.add(k)
            keys.append(k)
    return keys


def path_slugs_do_item(item: dict) -> list[str]:
    path: list[str] = []
    for key in (
        "categoria_slug",
        "subcategoria_slug",
        "subcategoria2_slug",
        "subcategoria3_slug",
        "subcategoria4_slug",
    ):
        s = (item.get(key) or "").strip()
        if not s:
            break
        path.append(s)
    return path or ["_sem"]


class ErroPedidoCatalogo(Exception):
    def __init__(self, mensagem: str, status: int = 400):
        super().__init__(mensagem)
        self.mensagem = mensagem
        self.status = status


def _bool(v: Any, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on", "sim", "s")


def _strip_data_url(raw: str) -> tuple[str, str]:
    """Retorna (base64, mime). Aceita data:image/...;base64,XXX ou base64 puro."""
    s = (raw or "").strip()
    mime = "image/jpeg"
    if s.startswith("data:") and ";base64," in s:
        head, _, b64 = s.partition(";base64,")
        mime = head.replace("data:", "").strip() or mime
        return b64.strip(), mime
    return s, mime


def comprimir_imagem_upload(
    raw: bytes,
    *,
    max_lado: int = 1200,
    qualidade: int = 82,
) -> tuple[bytes, str]:
    """
    Redimensiona e grava JPEG (mais leve no Postgres).
    Sem Pillow: devolve o original.
    """
    if not raw:
        return b"", "image/jpeg"
    try:
        from io import BytesIO

        from PIL import Image

        im = Image.open(BytesIO(raw))
        if im.mode in ("RGBA", "P", "LA"):
            fundo = Image.new("RGB", im.size, (255, 255, 255))
            if im.mode == "P":
                im = im.convert("RGBA")
            if im.mode in ("RGBA", "LA"):
                fundo.paste(im, mask=im.split()[-1])
            else:
                fundo.paste(im)
            im = fundo
        else:
            im = im.convert("RGB")
        im.thumbnail((max_lado, max_lado), Image.Resampling.LANCZOS)
        buf = BytesIO()
        im.save(buf, format="JPEG", quality=qualidade, optimize=True)
        return buf.getvalue(), "image/jpeg"
    except Exception:
        return raw, "image/jpeg"


def _hex_rgb(valor: str, padrao: tuple[int, int, int] = (236, 253, 245)) -> tuple[int, int, int]:
    v = (valor or "").strip()
    if len(v) == 7 and v.startswith("#"):
        try:
            return int(v[1:3], 16), int(v[3:5], 16), int(v[5:7], 16)
        except ValueError:
            return padrao
    return padrao


def montar_imagem_og_preview(
    logo_bytes: bytes,
    *,
    cor_fundo: str = "#ecfdf5",
    faixa_texto: str = "Delivery de ração",
) -> bytes | None:
    """
    Cartão 1200×630 (WhatsApp/Facebook): logo centralizada sem cortar (contain)
    + faixa inferior com texto (ex.: Delivery de ração) para ficar óbvio no preview.
    """
    if not logo_bytes:
        return None
    try:
        from io import BytesIO
        from pathlib import Path

        from PIL import Image, ImageDraw, ImageFont

        w, h = 1200, 630
        faixa_h = 110
        canvas = Image.new("RGB", (w, h), _hex_rgb(cor_fundo))
        logo = Image.open(BytesIO(logo_bytes)).convert("RGBA")
        pad_x, pad_top = 96, 48
        max_w = w - 2 * pad_x
        max_h = h - faixa_h - pad_top - 36
        logo_fit = logo.copy()
        logo_fit.thumbnail((max_w, max_h), Image.Resampling.LANCZOS)
        x = (w - logo_fit.width) // 2
        y = pad_top + (max_h - logo_fit.height) // 2
        canvas.paste(logo_fit, (x, y), logo_fit)

        # Faixa inferior escura/esmeralda — texto grande legível no Zap
        draw = ImageDraw.Draw(canvas)
        draw.rectangle((0, h - faixa_h, w, h), fill=(4, 120, 87))  # emerald-700
        texto = (faixa_texto or "Delivery de ração").strip()[:48] or "Delivery de ração"
        font = None
        for fp in (
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            str(Path("C:/Windows/Fonts/arialbd.ttf")),
            str(Path("C:/Windows/Fonts/segoeuib.ttf")),
        ):
            try:
                font = ImageFont.truetype(fp, 52)
                break
            except OSError:
                continue
        if font is None:
            font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), texto, font=font)
        tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
        tx = (w - tw) // 2
        ty = h - faixa_h + (faixa_h - th) // 2 - 4
        draw.text((tx, ty), texto, fill=(255, 255, 255), font=font)

        buf = BytesIO()
        canvas.save(buf, format="JPEG", quality=88, optimize=True)
        return buf.getvalue()
    except Exception:
        return None


def normalizar_embalagens(raw: Any) -> list[dict]:
    """Lista de {produto_id, rotulo} — máx. 6, sem duplicar id."""
    out: list[dict] = []
    seen: set[str] = set()
    if not isinstance(raw, list):
        return out
    for row in raw[:8]:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("produto_id") or row.get("id") or "").strip()
        if not pid or pid in seen:
            continue
        seen.add(pid)
        rotulo = str(row.get("rotulo") or "").strip()[:40]
        out.append({"produto_id": pid, "rotulo": rotulo})
        if len(out) >= 6:
            break
    return out


def rotulo_embalagem_padrao(
    *,
    rotulo: str = "",
    peso_texto: str = "",
    unidade: str = "",
    nome: str = "",
) -> str:
    r = (rotulo or "").strip()
    if r:
        return r[:40]
    peso = (peso_texto or "").strip()
    if peso:
        return peso[:40]
    un = (unidade or "").strip().upper()
    nome_u = (nome or "").upper()
    if un in ("KG", "G", "GR") or "GRANEL" in nome_u or un == "GRANEL":
        return "Granel"
    return (un or "UN")[:40]


def _comprimir_imagem_base64_delivery(b64: str, mime: str) -> tuple[str, str]:
    """Reencoda JPEG compacto para caber no JSON do overlay (e no PDV)."""
    import base64

    b64 = str(b64 or "").strip().replace("\n", "").replace(" ", "")
    mime = str(mime or "image/jpeg").strip()[:80] or "image/jpeg"
    if not b64:
        return "", "image/jpeg"
    try:
        raw = base64.b64decode(b64, validate=False)
    except Exception:
        return "", "image/jpeg"
    if not raw:
        return "", "image/jpeg"

    # Já leve — mantém (PNG transparente etc.).
    if len(raw) <= 180_000 and len(b64) <= 240_000:
        return b64, mime

    for lado, qual in ((900, 78), (720, 72), (560, 68)):
        out, mime_out = comprimir_imagem_upload(raw, max_lado=lado, qualidade=qual)
        if not out:
            continue
        b64_out = base64.b64encode(out).decode("ascii")
        if len(b64_out) <= 900_000:
            return b64_out, str(mime_out or "image/jpeg")
    return "", "image/jpeg"


def data_url_imagem_delivery_de_overlay(ov) -> str:
    """URL data: da foto Delivery no overlay (PDV / catálogo / APIs)."""
    if ov is None:
        return ""
    return _imagem_data_url(delivery_de_extras(getattr(ov, "cadastro_extras", None) or {}))


def aplicar_imagem_delivery_no_row(row: dict, ov) -> None:
    """Se o overlay tiver foto Delivery, usa no campo ``imagem`` do produto."""
    if not isinstance(row, dict):
        return
    url = data_url_imagem_delivery_de_overlay(ov)
    if url:
        row["imagem"] = url


def normalizar_delivery(raw: Any, *, processar_imagem: bool = False) -> dict:
    """Sanitiza delivery do overlay.

    ``processar_imagem=True`` só no save (comprime). Na leitura não apaga foto grande.
    """
    d = raw if isinstance(raw, dict) else {}
    titulo = str(d.get("titulo") or "").strip()[:200]
    descricao = str(d.get("descricao") or "").strip()[:2000]
    try:
        ordem = int(d.get("ordem") or 0)
    except (TypeError, ValueError):
        ordem = 0
    ordem = max(0, min(ordem, 9999))
    peso = str(d.get("peso_texto") or "").strip()[:40]
    b64_in = str(d.get("imagem_base64") or "").strip()
    b64, mime_guess = _strip_data_url(b64_in)
    mime = str(d.get("imagem_mime") or mime_guess or "image/jpeg").strip()[:40] or "image/jpeg"
    if processar_imagem and b64:
        b64, mime = _comprimir_imagem_base64_delivery(b64, mime)
    cat_id = _int_id(d, "categoria_id")
    sub_id = _int_id(d, "subcategoria_id")
    sub2_id = _int_id(d, "subcategoria2_id")
    sub3_id = _int_id(d, "subcategoria3_id")
    sub4_id = _int_id(d, "subcategoria4_id")
    embalagens = normalizar_embalagens(d.get("embalagens"))
    return {
        "ativo": _bool(d.get("ativo")),
        "titulo": titulo,
        "descricao": descricao,
        "ordem": ordem,
        "destaque": _bool(d.get("destaque")),
        "permitir_estoque_negativo": _bool(d.get("permitir_estoque_negativo")),
        "peso_texto": peso,
        "imagem_base64": b64,
        "imagem_mime": mime,
        "categoria_id": cat_id,
        "subcategoria_id": sub_id,
        "subcategoria2_id": sub2_id,
        "subcategoria3_id": sub3_id,
        "subcategoria4_id": sub4_id,
        "embalagens": embalagens,
    }


def delivery_de_extras(cadastro_extras: Any) -> dict:
    if not isinstance(cadastro_extras, dict):
        return normalizar_delivery({})
    return normalizar_delivery(cadastro_extras.get("delivery"))


def obter_config_catalogo() -> CatalogoDeliveryConfig:
    cfg, _ = CatalogoDeliveryConfig.objects.get_or_create(pk=1)
    return cfg


def _fmt_total(valor: Decimal) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _imagem_data_url(d: dict) -> str:
    b64 = (d.get("imagem_base64") or "").strip()
    if not b64:
        return ""
    mime = (d.get("imagem_mime") or "image/jpeg").strip() or "image/jpeg"
    return f"data:{mime};base64,{b64}"


def listar_itens_catalogo(*, incluir_ocultos_estoque: bool = False) -> list[dict]:
    """Produtos com delivery.ativo no overlay; aplica regra de estoque + famílias de embalagem."""
    overlays = list(
        ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={})[:5000]
    )
    ativos: list[tuple[ProdutoGestaoOverlayAgro, dict]] = []
    emb_ids_extra: set[str] = set()
    for ov in overlays:
        ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        d = delivery_de_extras(ex)
        if not d.get("ativo"):
            continue
        ativos.append((ov, d))
        for emb in d.get("embalagens") or []:
            pid_e = str(emb.get("produto_id") or "").strip()
            if pid_e:
                emb_ids_extra.add(pid_e)

    if not ativos:
        return []

    cats = {
        c.pk: c
        for c in CatalogoDeliveryCategoria.objects.filter(ativo=True).select_related(
            "parent",
            "parent__parent",
            "parent__parent__parent",
            "parent__parent__parent__parent",
        )
    }

    pids_ativos = [str(ov.produto_externo_id) for ov, _ in ativos]
    pids_all = list({*pids_ativos, *emb_ids_extra})
    overlays_by_pid = {
        str(ov.produto_externo_id): ov
        for ov, _ in ativos
    }
    # Overlays dos irmãos que não estão em ativos
    missing = [p for p in emb_ids_extra if p not in overlays_by_pid]
    if missing:
        for ov in ProdutoGestaoOverlayAgro.objects.filter(produto_externo_id__in=missing):
            overlays_by_pid[str(ov.produto_externo_id)] = ov

    produtos_pg = {
        str(p.produto_externo_id): p
        for p in Produto.objects.filter(produto_externo_id__in=pids_all)
    }
    try:
        saldos = mapa_saldos_operacionais_agro(pids_all)
    except Exception:
        saldos = {}

    def _meta_produto(pid: str) -> dict:
        ov = overlays_by_pid.get(pid)
        pg = produtos_pg.get(pid)
        d_ov = delivery_de_extras(ov.cadastro_extras if ov else {})
        nome = (
            (d_ov.get("titulo") or "")
            or (ov.nome if ov else "")
            or (pg.nome if pg else "")
            or "Produto"
        ).strip()
        unidade = (
            (ov.unidade if ov else "") or (pg.unidade if pg else "") or "UN"
        ).strip() or "UN"
        preco = ov.preco_venda if ov is not None else None
        if preco is None and pg is not None:
            preco = pg.preco_venda
        try:
            preco_f = float(preco or 0)
        except (TypeError, ValueError):
            preco_f = 0.0
        saldo_map = saldos.get(pid) or {}
        centro = float(saldo_map.get("centro") or 0)
        vila = float(saldo_map.get("vila") or 0)
        return {
            "id": pid,
            "nome": nome,
            "preco": round(preco_f, 2),
            "unidade": unidade,
            "peso_texto": (d_ov.get("peso_texto") or "").strip(),
            "saldo_total": round(centro + vila, 3),
        }

    itens: list[dict] = []
    for ov, d in ativos:
        pid = str(ov.produto_externo_id)
        meta = _meta_produto(pid)
        pg = produtos_pg.get(pid)
        nome = meta["nome"]
        desc = (d.get("descricao") or ov.descricao or (pg.descricao if pg else "") or "").strip()
        marca = (ov.marca or (pg.marca if pg else "") or "").strip()
        unidade = meta["unidade"]
        preco_f = meta["preco"]

        saldo_total = meta["saldo_total"]
        forcar = bool(d.get("permitir_estoque_negativo"))
        if not incluir_ocultos_estoque and not forcar and saldo_total <= 0:
            continue

        cat_id = int(d.get("categoria_id") or 0)
        sub_id = int(d.get("subcategoria_id") or 0)
        sub2_id = int(d.get("subcategoria2_id") or 0)
        sub3_id = int(d.get("subcategoria3_id") or 0)
        sub4_id = int(d.get("subcategoria4_id") or 0)
        cadeia = _cadeia_de_folha(cats, sub4_id, sub3_id, sub2_id, sub_id, cat_id)
        cat, sub, sub2, sub3, sub4 = cadeia
        cat_id = cat.pk if cat else 0
        sub_id = sub.pk if sub else 0
        sub2_id = sub2.pk if sub2 else 0
        sub3_id = sub3.pk if sub3 else 0
        sub4_id = sub4.pk if sub4 else 0

        emb_raw = list(d.get("embalagens") or [])
        if not emb_raw:
            emb_raw = [{"produto_id": pid, "rotulo": ""}]
        elif not any(str(e.get("produto_id") or "") == pid for e in emb_raw):
            emb_raw = [{"produto_id": pid, "rotulo": ""}] + emb_raw
            emb_raw = emb_raw[:6]

        embalagens: list[dict] = []
        seen_e: set[str] = set()
        for e in emb_raw:
            eid = str(e.get("produto_id") or "").strip()
            if not eid or eid in seen_e:
                continue
            seen_e.add(eid)
            m = _meta_produto(eid)
            rotulo = rotulo_embalagem_padrao(
                rotulo=str(e.get("rotulo") or ""),
                peso_texto=m["peso_texto"] or (d.get("peso_texto") if eid == pid else ""),
                unidade=m["unidade"],
                nome=m["nome"],
            )
            embalagens.append(
                {
                    "id": eid,
                    "produto_id": eid,
                    "rotulo": rotulo,
                    "preco": m["preco"],
                    "nome": m["nome"],
                    "unidade": m["unidade"],
                    "peso_texto": m["peso_texto"],
                    "saldo_total": m["saldo_total"],
                }
            )

        if not embalagens:
            embalagens = [
                {
                    "id": pid,
                    "produto_id": pid,
                    "rotulo": rotulo_embalagem_padrao(
                        peso_texto=d.get("peso_texto") or "",
                        unidade=unidade,
                        nome=nome,
                    ),
                    "preco": preco_f,
                    "nome": nome,
                    "unidade": unidade,
                    "peso_texto": d.get("peso_texto") or "",
                    "saldo_total": saldo_total,
                }
            ]

        item_row = {
            "id": pid,
            "nome": nome,
            "descricao": desc,
            "preco": round(preco_f, 2),
            "marca": marca,
            "categoria_id": cat_id,
            "categoria_nome": (cat.nome if cat else "") or "",
            "categoria_slug": (cat.slug if cat else "") or "",
            "subcategoria_id": sub_id,
            "subcategoria_nome": (sub.nome if sub else "") or "",
            "subcategoria_slug": (sub.slug if sub else "") or "",
            "subcategoria2_id": sub2_id,
            "subcategoria2_nome": (sub2.nome if sub2 else "") or "",
            "subcategoria2_slug": (sub2.slug if sub2 else "") or "",
            "subcategoria3_id": sub3_id,
            "subcategoria3_nome": (sub3.nome if sub3 else "") or "",
            "subcategoria3_slug": (sub3.slug if sub3 else "") or "",
            "subcategoria4_id": sub4_id,
            "subcategoria4_nome": (sub4.nome if sub4 else "") or "",
            "subcategoria4_slug": (sub4.slug if sub4 else "") or "",
            "peso_texto": d.get("peso_texto") or "",
            "destaque": bool(d.get("destaque")),
            "ordem": int(d.get("ordem") or 0),
            "saldo_total": round(saldo_total, 3),
            "saldo_centro": round(float((saldos.get(pid) or {}).get("centro") or 0), 3),
            "saldo_vila": round(float((saldos.get(pid) or {}).get("vila") or 0), 3),
            "permitir_estoque_negativo": forcar,
            "imagem": _imagem_data_url(d),
            "unidade": unidade,
            "embalagens": embalagens,
        }
        item_row["path_slugs"] = path_slugs_do_item(item_row)
        item_row["path"] = "/".join(item_row["path_slugs"])
        item_row["peso_keys"] = pesos_keys_do_item(item_row)
        itens.append(item_row)

    # Dedupe: irmãos listados em família de outro âncora não geram card próprio
    itens.sort(
        key=lambda x: (
            not x["destaque"],
            x["ordem"],
            x["nome"].lower(),
        )
    )
    hide_ids: set[str] = set()
    for item in itens:
        emb = item.get("embalagens") or []
        others = [e["id"] for e in emb if e.get("id") and e["id"] != item["id"]]
        if not others:
            continue
        if item["id"] in hide_ids:
            continue
        for oid in others:
            hide_ids.add(oid)

    itens = [i for i in itens if i["id"] not in hide_ids]

    itens.sort(
        key=lambda x: (
            not x["destaque"],
            x.get("categoria_nome") or "ÿ",
            x.get("subcategoria_nome") or "",
            x.get("subcategoria2_nome") or "",
            x.get("subcategoria3_nome") or "",
            x.get("subcategoria4_nome") or "",
            x["ordem"],
            x["nome"].lower(),
        )
    )
    return itens


def listar_produtos_delivery_para_vinculo(*, q: str = "", limite: int = 40) -> list[dict]:
    """Busca produtos com delivery.ativo para montar família de embalagens (gestão)."""
    termo = (q or "").strip().lower()
    overlays = list(
        ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={})[:5000]
    )
    out: list[dict] = []
    for ov in overlays:
        d = delivery_de_extras(ov.cadastro_extras)
        if not d.get("ativo"):
            continue
        pid = str(ov.produto_externo_id)
        nome = (d.get("titulo") or ov.nome or "").strip() or pid
        if termo and termo not in nome.lower() and termo not in pid.lower():
            continue
        preco = ov.preco_venda
        try:
            preco_f = float(preco or 0)
        except (TypeError, ValueError):
            preco_f = 0.0
        out.append(
            {
                "id": pid,
                "nome": nome,
                "preco": round(preco_f, 2),
                "peso_texto": (d.get("peso_texto") or "").strip(),
                "unidade": (ov.unidade or "UN").strip() or "UN",
            }
        )
        if len(out) >= max(1, min(int(limite or 40), 80)):
            break
    out.sort(key=lambda x: x["nome"].lower())
    return out


def listar_categorias_arvore(*, so_ativas: bool = True) -> list[dict]:
    """Categorias raiz → filhos recursivos (até 5 níveis) para gestão e selects."""
    qs = CatalogoDeliveryCategoria.objects.all().order_by("ordem", "nome")
    if so_ativas:
        qs = qs.filter(ativo=True)
    by_parent: dict[int | None, list] = {}
    for c in qs:
        by_parent.setdefault(c.parent_id, []).append(c)

    def _node(c, *, com_imagem: bool = False, profundidade: int = 1) -> dict:
        filhos = []
        if profundidade < CATALOGO_MAX_NIVEIS:
            filhos = [
                _node(f, com_imagem=False, profundidade=profundidade + 1)
                for f in by_parent.get(c.pk, [])
            ]
        out = {
            "id": c.pk,
            "nome": c.nome,
            "slug": c.slug,
            "ordem": c.ordem,
            "ativo": c.ativo,
            "nivel": profundidade,
            "filhos": filhos,
        }
        if com_imagem:
            img = ""
            b64 = (c.imagem_base64 or "").strip()
            if b64:
                mime = (c.imagem_mime or "image/jpeg").strip() or "image/jpeg"
                img = f"data:{mime};base64,{b64}"
            out["imagem"] = img
        return out

    return [_node(c, com_imagem=True, profundidade=1) for c in by_parent.get(None, [])]


def opcoes_pai_categoria(*, so_ativas: bool = True) -> list[dict]:
    """Opções do select «pai» na gestão: até nível 4 (filho vira nível 5)."""
    out: list[dict] = []

    def _walk(nodes: list[dict], prefix: str, nivel: int) -> None:
        for n in nodes:
            if nivel >= CATALOGO_MAX_NIVEIS:
                continue
            label = f"Nível {nivel + 1} de: {prefix}{n['nome']}" if prefix else f"Sub de: {n['nome']}"
            if nivel == 1:
                label = f"Sub de: {n['nome']}"
            elif nivel == 2:
                label = f"Nível 3 de: {prefix}{n['nome']}"
            elif nivel == 3:
                label = f"Nível 4 de: {prefix}{n['nome']}"
            elif nivel == 4:
                label = f"Nível 5 de: {prefix}{n['nome']}"
            out.append({"id": n["id"], "label": label})
            filhos = n.get("filhos") or []
            if filhos and nivel < CATALOGO_MAX_NIVEIS - 1:
                _walk(filhos, f"{prefix}{n['nome']} › ", nivel + 1)

    _walk(listar_categorias_arvore(so_ativas=so_ativas), "", 1)
    return out


def salvar_foto_categoria(cat: CatalogoDeliveryCategoria, raw_b64: str, mime: str = "") -> None:
    """Grava foto do card (limite ~1,2 MB de arquivo ≈ ~1,6 MB base64)."""
    b64, mime_guess = _strip_data_url(raw_b64 or "")
    mime_final = (mime or mime_guess or "image/jpeg").strip()[:40] or "image/jpeg"
    if len(b64) > 1_700_000:
        b64 = ""
        mime_final = "image/jpeg"
    cat.imagem_base64 = b64
    cat.imagem_mime = mime_final if b64 else "image/jpeg"
    cat.save(update_fields=["imagem_base64", "imagem_mime"])


def salvar_logo_loja(cfg: CatalogoDeliveryConfig, raw_b64: str, mime: str = "") -> None:
    """Grava logotipo da loja (limite ~1,2 MB de arquivo ≈ ~1,6 MB base64)."""
    b64, mime_guess = _strip_data_url(raw_b64 or "")
    mime_final = (mime or mime_guess or "image/png").strip()[:40] or "image/png"
    if len(b64) > 1_700_000:
        b64 = ""
        mime_final = "image/png"
    cfg.logo_base64 = b64
    cfg.logo_mime = mime_final if b64 else "image/png"
    cfg.save(update_fields=["logo_base64", "logo_mime"])


def cards_home_catalogo(itens: list[dict]) -> list[dict]:
    """
    Cards da tela inicial: categorias raiz ativas + opcional «Outros».
    Contagem de produtos por categoria.
    """
    contagem: dict[str, int] = {}
    for it in itens:
        ck = it.get("categoria_slug") or "_sem"
        contagem[ck] = contagem.get(ck, 0) + 1
    cards = []
    for c in listar_categorias_arvore(so_ativas=True):
        cards.append(
            {
                "id": c["id"],
                "slug": c["slug"],
                "nome": c["nome"],
                "imagem": c.get("imagem") or "",
                "qtd": contagem.get(c["slug"], 0),
            }
        )
    q_sem = contagem.get("_sem", 0)
    if q_sem:
        cards.append(
            {
                "id": 0,
                "slug": "_sem",
                "nome": "Outros",
                "imagem": "",
                "qtd": q_sem,
            }
        )
    return cards


def arvore_navegacao_catalogo(itens: list[dict]) -> list[dict]:
    """
    Árvore mobile recursiva (até 5 níveis).
    Contagens por prefixo de path + ``qtd_exata`` (produto para neste nó).
    """
    paths = [path_slugs_do_item(it) for it in itens]

    def _conta_prefixo(prefix: tuple[str, ...]) -> int:
        n = 0
        plen = len(prefix)
        for p in paths:
            if len(p) >= plen and tuple(p[:plen]) == prefix:
                n += 1
        return n

    def _conta_exata(prefix: tuple[str, ...]) -> int:
        n = 0
        for p in paths:
            if tuple(p) == prefix:
                n += 1
        return n

    def _montar_no(node: dict, prefix: tuple[str, ...]) -> dict:
        slug = node["slug"]
        aqui = prefix + (slug,)
        filhos_out = []
        for f in node.get("filhos") or []:
            filhos_out.append(_montar_no(f, aqui))
        # Slugs de produtos mais profundos que não estão na árvore cadastrada
        conhecidos = {x["slug"] for x in filhos_out}
        extras: dict[str, int] = {}
        plen = len(aqui)
        for p in paths:
            if len(p) > plen and tuple(p[:plen]) == aqui:
                nxt = p[plen]
                if nxt not in conhecidos:
                    extras[nxt] = extras.get(nxt, 0) + 1
        for slug_e, q in sorted(extras.items()):
            filhos_out.append(
                {
                    "id": 0,
                    "slug": slug_e,
                    "nome": slug_e,
                    "qtd": q,
                    "qtd_exata": _conta_exata(aqui + (slug_e,)),
                    "filhos": [],
                }
            )
        return {
            "id": node.get("id") or 0,
            "slug": slug,
            "nome": node["nome"],
            "qtd": _conta_prefixo(aqui),
            "qtd_exata": _conta_exata(aqui),
            "filhos": filhos_out,
        }

    out = []
    for c in listar_categorias_arvore(so_ativas=True):
        out.append(_montar_no(c, ()))

    # Produtos sem categoria cadastrada
    q_sem = _conta_prefixo(("_sem",))
    if q_sem:
        filhos_sem = []
        extras_sem: dict[str, int] = {}
        for p in paths:
            if p and p[0] == "_sem" and len(p) > 1:
                extras_sem[p[1]] = extras_sem.get(p[1], 0) + 1
        for slug_e, q in sorted(extras_sem.items()):
            filhos_sem.append(
                {
                    "id": 0,
                    "slug": slug_e,
                    "nome": slug_e,
                    "qtd": q,
                    "qtd_exata": _conta_exata(("_sem", slug_e)),
                    "filhos": [],
                }
            )
        out.append(
            {
                "id": 0,
                "slug": "_sem",
                "nome": "Outros",
                "qtd": q_sem,
                "qtd_exata": _conta_exata(("_sem",)),
                "filhos": filhos_sem,
            }
        )
    return out


def slugify_categoria(nome: str, *, exclude_pk: int | None = None) -> str:
    from django.utils.text import slugify

    base = slugify(nome)[:80] or "categoria"
    slug = base
    n = 2
    while True:
        qs = CatalogoDeliveryCategoria.objects.filter(slug=slug)
        if exclude_pk:
            qs = qs.exclude(pk=exclude_pk)
        if not qs.exists():
            return slug
        slug = f"{base}-{n}"[:90]
        n += 1


def agrupar_itens_por_categoria(itens: list[dict]) -> list[dict]:
    """Seções da vitrine: uma por categoria raiz, produtos flat (filtro via data-path)."""
    ordem_cat: list[str] = []
    mapa: dict[str, dict] = {}
    for it in itens:
        ck = it.get("categoria_slug") or "_sem"
        cn = it.get("categoria_nome") or "Outros"
        if ck not in mapa:
            mapa[ck] = {
                "slug": ck,
                "nome": cn,
                "produtos": [],
            }
            ordem_cat.append(ck)
        mapa[ck]["produtos"].append(it)
    return [
        {
            "slug": mapa[ck]["slug"],
            "nome": mapa[ck]["nome"],
            "produtos": mapa[ck]["produtos"],
        }
        for ck in ordem_cat
    ]


def _montar_endereco_linha(
    *,
    logradouro: str,
    numero: str,
    bairro: str,
    cidade: str,
    uf: str,
    cep: str = "",
) -> str:
    return compor_endereco_resumo_cliente(cep, uf, cidade, bairro, logradouro, numero, "")


def _upsert_cliente_catalogo(
    *,
    nome: str,
    telefone: str,
    endereco: str,
    plus_code: str,
    logradouro: str,
    numero: str,
    bairro: str,
    cidade: str,
    uf: str,
    cep: str,
    maps_url: str = "",
) -> ClienteAgro | None:
    digits = extrair_whatsapp_digits(telefone)
    if len(digits) < 10:
        return None
    cli = cliente_agro_por_whatsapp(digits)
    if cli is None:
        cli = ClienteAgro(whatsapp=digits, nome=nome[:300])
    else:
        if nome:
            cli.nome = nome[:300]
    if logradouro:
        cli.logradouro = logradouro[:200]
    if numero:
        cli.numero = numero[:30]
    if bairro:
        cli.bairro = bairro[:120]
    if cidade:
        cli.cidade = cidade[:120]
    if uf:
        cli.uf = uf[:2]
    if cep:
        cli.cep = cep[:12]
    if plus_code:
        cli.plus_code = plus_code[:120]
    if maps_url:
        cli.maps_url_manual = maps_url[:600]
    if endereco:
        cli.endereco = endereco[:500]
    cli.save()
    return cli


def criar_pedido_catalogo_delivery(payload: dict[str, Any]) -> PedidoEntrega:
    cliente_nome = (payload.get("cliente_nome") or "").strip()[:300]
    telefone = "".join(c for c in (payload.get("telefone") or "") if c.isdigit() or c in "+ ")[:40].strip()
    plus_code = (payload.get("plus_code") or "").strip()[:120]
    logradouro = (payload.get("logradouro") or "").strip()[:300]
    numero = (payload.get("numero") or "").strip()[:30]
    bairro = (payload.get("bairro") or "").strip()[:120]
    cidade = (payload.get("cidade") or "").strip()[:120]
    uf = (payload.get("uf") or "SP").strip()[:2].upper() or "SP"
    cep = (payload.get("cep") or "").strip()[:12]
    endereco = (payload.get("endereco_linha") or "").strip()[:500]
    if not endereco:
        endereco = _montar_endereco_linha(
            logradouro=logradouro,
            numero=numero,
            bairro=bairro,
            cidade=cidade,
            uf=uf,
            cep=cep,
        )[:500]
    maps_url = (payload.get("maps_url") or "").strip()[:600]
    forma = (payload.get("forma_pagamento") or "").strip()[:40]
    obs = (payload.get("observacoes") or "").strip()[:2000]
    raw_itens = payload.get("itens")

    if not cliente_nome:
        raise ErroPedidoCatalogo("Informe seu nome.")
    if not telefone or len("".join(c for c in telefone if c.isdigit())) < 10:
        raise ErroPedidoCatalogo("Informe um telefone com DDD.")
    if not plus_code and not (cidade and logradouro and numero):
        raise ErroPedidoCatalogo("Informe cidade, logradouro e número — ou use a localização.")
    if not endereco and not plus_code:
        raise ErroPedidoCatalogo("Informe o endereço ou use a localização do celular.")
    if not forma:
        raise ErroPedidoCatalogo("Escolha a forma de pagamento.")
    if not isinstance(raw_itens, list) or not raw_itens:
        raise ErroPedidoCatalogo("Carrinho vazio.")

    troco_precisa = None
    if forma == "Dinheiro":
        tp = payload.get("troco_precisa")
        if tp is True or tp in ("true", "1"):
            troco_precisa = True
        else:
            troco_precisa = False

    qtd_map: dict[str, int] = {}
    for linha in raw_itens:
        if not isinstance(linha, dict):
            continue
        pid = str(linha.get("produto_id") or "").strip()
        try:
            qtd = int(linha.get("qtd") or 1)
        except (TypeError, ValueError):
            continue
        if not pid or qtd <= 0 or qtd > 99:
            continue
        qtd_map[pid] = qtd_map.get(pid, 0) + qtd

    if not qtd_map:
        raise ErroPedidoCatalogo("Itens inválidos no carrinho.")

    catalogo = {i["id"]: i for i in listar_itens_catalogo(incluir_ocultos_estoque=False)}
    for pid in qtd_map:
        if pid not in catalogo:
            raise ErroPedidoCatalogo(
                "Algum item saiu do catálogo ou está sem estoque. Atualize a página."
            )

    itens_json: list[dict] = []
    total = Decimal("0")
    for pid, qtd in qtd_map.items():
        item = catalogo[pid]
        try:
            preco = Decimal(str(item["preco"]))
        except (InvalidOperation, KeyError):
            preco = Decimal("0")
        linha_total = (preco * qtd).quantize(Decimal("0.01"))
        total += linha_total
        itens_json.append(
            {
                "produto_id": pid,
                "nome": item["nome"],
                "qtd": qtd,
                "preco": float(preco),
                "total": float(linha_total),
                "unidade": item.get("unidade") or "UN",
            }
        )

    with transaction.atomic():
        cli = _upsert_cliente_catalogo(
            nome=cliente_nome,
            telefone=telefone,
            endereco=endereco,
            plus_code=plus_code,
            logradouro=logradouro,
            numero=numero,
            bairro=bairro,
            cidade=cidade,
            uf=uf,
            cep=cep,
            maps_url=maps_url,
        )
        if cli is not None:
            if not plus_code and cli.plus_code:
                plus_code = cli.plus_code[:120]
            if not maps_url and getattr(cli, "maps_url_manual", None):
                maps_url = (cli.maps_url_manual or "")[:600]
        pedido = PedidoEntrega.objects.create(
            status=PedidoEntrega.Status.PENDENTE,
            origem="catalogo",
            cliente_agro=cli,
            cliente_nome=cliente_nome,
            telefone=telefone,
            endereco_linha=endereco,
            plus_code=plus_code,
            maps_url_manual=maps_url,
            itens_json=itens_json,
            total_texto=_fmt_total(total),
            operador="Catálogo delivery",
            observacoes=obs,
            forma_pagamento=forma,
            troco_precisa=troco_precisa,
            aguarda_pagamento_pdv=True,
        )
    return pedido


def cliente_catalogo_json(cli: ClienteAgro) -> dict:
    end = (cli.endereco or "").strip() or compor_endereco_resumo_cliente(
        cli.cep, cli.uf, cli.cidade, cli.bairro, cli.logradouro, cli.numero, cli.complemento
    )
    return {
        "nome": cli.nome,
        "telefone": cli.whatsapp,
        "endereco_linha": end,
        "plus_code": (cli.plus_code or "").strip(),
        "maps_url": (cli.maps_url_manual or "").strip(),
        "logradouro": (cli.logradouro or "").strip(),
        "numero": (cli.numero or "").strip(),
        "bairro": (cli.bairro or "").strip(),
        "cidade": (cli.cidade or "").strip(),
        "uf": (cli.uf or "SP").strip() or "SP",
        "cep": (cli.cep or "").strip(),
    }
