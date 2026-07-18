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


def normalizar_delivery(raw: Any) -> dict:
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
    if len(b64) > 900_000:
        b64 = ""
        mime = "image/jpeg"
    cat_id = 0
    sub_id = 0
    sub2_id = 0
    try:
        cat_id = int(d.get("categoria_id") or 0)
    except (TypeError, ValueError):
        cat_id = 0
    try:
        sub_id = int(d.get("subcategoria_id") or 0)
    except (TypeError, ValueError):
        sub_id = 0
    try:
        sub2_id = int(d.get("subcategoria2_id") or 0)
    except (TypeError, ValueError):
        sub2_id = 0
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
        "categoria_id": cat_id if cat_id > 0 else 0,
        "subcategoria_id": sub_id if sub_id > 0 else 0,
        "subcategoria2_id": sub2_id if sub2_id > 0 else 0,
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
    """Produtos com delivery.ativo no overlay; aplica regra de estoque."""
    overlays = list(
        ProdutoGestaoOverlayAgro.objects.exclude(cadastro_extras={})[:5000]
    )
    ativos: list[tuple[ProdutoGestaoOverlayAgro, dict]] = []
    for ov in overlays:
        ex = ov.cadastro_extras if isinstance(ov.cadastro_extras, dict) else {}
        d = delivery_de_extras(ex)
        if not d.get("ativo"):
            continue
        ativos.append((ov, d))

    if not ativos:
        return []

    cats = {
        c.pk: c
        for c in CatalogoDeliveryCategoria.objects.filter(ativo=True).select_related(
            "parent", "parent__parent"
        )
    }

    pids = [ov.produto_externo_id for ov, _ in ativos]
    produtos_pg = {
        str(p.produto_externo_id): p
        for p in Produto.objects.filter(produto_externo_id__in=pids)
    }
    try:
        saldos = mapa_saldos_operacionais_agro(pids)
    except Exception:
        saldos = {}

    itens: list[dict] = []
    for ov, d in ativos:
        pid = str(ov.produto_externo_id)
        pg = produtos_pg.get(pid)
        nome = (d.get("titulo") or ov.nome or (pg.nome if pg else "") or "Produto").strip()
        desc = (d.get("descricao") or ov.descricao or (pg.descricao if pg else "") or "").strip()
        marca = (ov.marca or (pg.marca if pg else "") or "").strip()
        unidade = (ov.unidade or (pg.unidade if pg else "") or "UN").strip() or "UN"
        preco = ov.preco_venda
        if preco is None and pg is not None:
            preco = pg.preco_venda
        try:
            preco_f = float(preco or 0)
        except (TypeError, ValueError):
            preco_f = 0.0

        saldo_map = saldos.get(pid) or {}
        centro = float(saldo_map.get("centro") or 0)
        vila = float(saldo_map.get("vila") or 0)
        saldo_total = centro + vila
        forcar = bool(d.get("permitir_estoque_negativo"))
        if not incluir_ocultos_estoque and not forcar and saldo_total <= 0:
            continue

        cat_id = int(d.get("categoria_id") or 0)
        sub_id = int(d.get("subcategoria_id") or 0)
        sub2_id = int(d.get("subcategoria2_id") or 0)
        cat = cats.get(cat_id) if cat_id else None
        sub = cats.get(sub_id) if sub_id else None
        sub2 = cats.get(sub2_id) if sub2_id else None
        # Normaliza árvore: sub2 → sub → cat
        if sub2:
            if sub2.parent_id:
                if not sub or sub.pk != sub2.parent_id:
                    sub = cats.get(sub2.parent_id)
                    sub_id = sub.pk if sub else 0
            else:
                sub2 = None
                sub2_id = 0
        if sub and sub.parent_id:
            if not cat or cat.pk != sub.parent_id:
                cat = cats.get(sub.parent_id)
                cat_id = cat.pk if cat else 0
        elif sub and not sub.parent_id:
            # sub sem pai = tratar como categoria
            if not cat:
                cat = sub
                cat_id = sub.pk
            sub = None
            sub_id = 0
        if sub2 and sub and sub2.parent_id != sub.pk:
            sub2 = None
            sub2_id = 0

        itens.append(
            {
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
                "peso_texto": d.get("peso_texto") or "",
                "destaque": bool(d.get("destaque")),
                "ordem": int(d.get("ordem") or 0),
                "saldo_total": round(saldo_total, 3),
                "saldo_centro": round(centro, 3),
                "saldo_vila": round(vila, 3),
                "permitir_estoque_negativo": forcar,
                "imagem": _imagem_data_url(d),
                "unidade": unidade,
            }
        )

    itens.sort(
        key=lambda x: (
            not x["destaque"],
            x.get("categoria_nome") or "ÿ",
            x.get("subcategoria_nome") or "",
            x.get("subcategoria2_nome") or "",
            x["ordem"],
            x["nome"].lower(),
        )
    )
    return itens


def listar_categorias_arvore(*, so_ativas: bool = True) -> list[dict]:
    """Categorias raiz → sub → sub2 (até 3 níveis) para gestão e selects."""
    qs = CatalogoDeliveryCategoria.objects.all().order_by("ordem", "nome")
    if so_ativas:
        qs = qs.filter(ativo=True)
    by_parent: dict[int | None, list] = {}
    for c in qs:
        by_parent.setdefault(c.parent_id, []).append(c)

    def _node(c, *, com_imagem: bool = False) -> dict:
        filhos = [
            _node(f, com_imagem=False)
            for f in by_parent.get(c.pk, [])
        ]
        out = {
            "id": c.pk,
            "nome": c.nome,
            "slug": c.slug,
            "ordem": c.ordem,
            "ativo": c.ativo,
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

    return [_node(c, com_imagem=True) for c in by_parent.get(None, [])]


def opcoes_pai_categoria(*, so_ativas: bool = True) -> list[dict]:
    """Opções do select «pai» na gestão: raiz ou sub (para criar nível 2 ou 3)."""
    out = []
    for c in listar_categorias_arvore(so_ativas=so_ativas):
        out.append({"id": c["id"], "label": f"Sub de: {c['nome']}"})
        for f in c.get("filhos") or []:
            out.append(
                {
                    "id": f["id"],
                    "label": f"Sub-sub de: {c['nome']} › {f['nome']}",
                }
            )
    return out


def salvar_foto_categoria(cat: CatalogoDeliveryCategoria, raw_b64: str, mime: str = "") -> None:
    """Grava foto do card (limite ~700 KB de base64)."""
    b64, mime_guess = _strip_data_url(raw_b64 or "")
    mime_final = (mime or mime_guess or "image/jpeg").strip()[:40] or "image/jpeg"
    if len(b64) > 900_000:
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
    Árvore mobile: categoria → sub → sub2 → produtos.
    Contagens por nível + «Geral» quando há produto sem nível inferior.
    """
    # (cat) -> (sub) -> (sub2) -> count
    por3: dict[str, dict[str, dict[str, int]]] = {}
    sem_sub2: dict[str, dict[str, int]] = {}  # cat -> sub -> count
    sem_sub: dict[str, int] = {}  # cat -> count

    for it in itens:
        ck = it.get("categoria_slug") or "_sem"
        sk = (it.get("subcategoria_slug") or "").strip()
        s2 = (it.get("subcategoria2_slug") or "").strip()
        if sk and s2:
            por3.setdefault(ck, {}).setdefault(sk, {})
            por3[ck][sk][s2] = por3[ck][sk].get(s2, 0) + 1
        elif sk:
            sem_sub2.setdefault(ck, {})
            sem_sub2[ck][sk] = sem_sub2[ck].get(sk, 0) + 1
        else:
            sem_sub[ck] = sem_sub.get(ck, 0) + 1

    def _qtd_sub(ck: str, sk: str) -> int:
        n = (sem_sub2.get(ck) or {}).get(sk, 0)
        for q in (por3.get(ck) or {}).get(sk, {}).values():
            n += q
        return n

    out = []
    for c in listar_categorias_arvore(so_ativas=True):
        filhos = []
        for f in c.get("filhos") or []:
            netos = []
            for n in f.get("filhos") or []:
                netos.append(
                    {
                        "id": n["id"],
                        "slug": n["slug"],
                        "nome": n["nome"],
                        "qtd": ((por3.get(c["slug"]) or {}).get(f["slug"]) or {}).get(n["slug"], 0),
                    }
                )
            conhecidos = {x["slug"] for x in netos}
            for s2, q in ((por3.get(c["slug"]) or {}).get(f["slug"]) or {}).items():
                if s2 not in conhecidos:
                    netos.append({"id": 0, "slug": s2, "nome": s2, "qtd": q})
            filhos.append(
                {
                    "id": f["id"],
                    "slug": f["slug"],
                    "nome": f["nome"],
                    "qtd": _qtd_sub(c["slug"], f["slug"]),
                    "filhos": netos,
                    "qtd_sem_sub2": (sem_sub2.get(c["slug"]) or {}).get(f["slug"], 0),
                }
            )
        conhecidos_sub = {x["slug"] for x in filhos}
        for sk, q in (sem_sub2.get(c["slug"]) or {}).items():
            if sk not in conhecidos_sub:
                filhos.append(
                    {
                        "id": 0,
                        "slug": sk,
                        "nome": sk,
                        "qtd": _qtd_sub(c["slug"], sk),
                        "filhos": [
                            {"id": 0, "slug": s2, "nome": s2, "qtd": qq}
                            for s2, qq in ((por3.get(c["slug"]) or {}).get(sk) or {}).items()
                        ],
                        "qtd_sem_sub2": q,
                    }
                )
        for sk, mapa_s2 in (por3.get(c["slug"]) or {}).items():
            if sk not in conhecidos_sub and sk not in (sem_sub2.get(c["slug"]) or {}):
                filhos.append(
                    {
                        "id": 0,
                        "slug": sk,
                        "nome": sk,
                        "qtd": _qtd_sub(c["slug"], sk),
                        "filhos": [
                            {"id": 0, "slug": s2, "nome": s2, "qtd": qq}
                            for s2, qq in mapa_s2.items()
                        ],
                        "qtd_sem_sub2": 0,
                    }
                )
        out.append(
            {
                "slug": c["slug"],
                "nome": c["nome"],
                "filhos": filhos,
                "qtd_sem_sub": sem_sub.get(c["slug"], 0),
            }
        )
    if sem_sub.get("_sem") or sem_sub2.get("_sem") or por3.get("_sem"):
        filhos_sem = []
        for sk in set(list((sem_sub2.get("_sem") or {}).keys()) + list((por3.get("_sem") or {}).keys())):
            filhos_sem.append(
                {
                    "id": 0,
                    "slug": sk,
                    "nome": sk,
                    "qtd": _qtd_sub("_sem", sk),
                    "filhos": [
                        {"id": 0, "slug": s2, "nome": s2, "qtd": qq}
                        for s2, qq in ((por3.get("_sem") or {}).get(sk) or {}).items()
                    ],
                    "qtd_sem_sub2": (sem_sub2.get("_sem") or {}).get(sk, 0),
                }
            )
        out.append(
            {
                "slug": "_sem",
                "nome": "Outros",
                "filhos": filhos_sem,
                "qtd_sem_sub": sem_sub.get("_sem", 0),
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
    """Monta seções para a vitrine (categoria → sub → sub2 → produtos)."""
    ordem_cat: list[str] = []
    mapa: dict[str, dict] = {}
    for it in itens:
        ck = it.get("categoria_slug") or "_sem"
        cn = it.get("categoria_nome") or "Outros"
        if ck not in mapa:
            mapa[ck] = {
                "slug": ck,
                "nome": cn,
                "subs": {},
                "produtos_sem_sub": [],
            }
            ordem_cat.append(ck)
        sk = it.get("subcategoria_slug") or ""
        s2 = it.get("subcategoria2_slug") or ""
        if not sk:
            mapa[ck]["produtos_sem_sub"].append(it)
            continue
        if sk not in mapa[ck]["subs"]:
            mapa[ck]["subs"][sk] = {
                "slug": sk,
                "nome": it.get("subcategoria_nome") or sk,
                "subs2": {},
                "produtos_sem_sub2": [],
            }
        bloco_sub = mapa[ck]["subs"][sk]
        if s2:
            if s2 not in bloco_sub["subs2"]:
                bloco_sub["subs2"][s2] = {
                    "slug": s2,
                    "nome": it.get("subcategoria2_nome") or s2,
                    "produtos": [],
                }
            bloco_sub["subs2"][s2]["produtos"].append(it)
        else:
            bloco_sub["produtos_sem_sub2"].append(it)

    secoes = []
    for ck in ordem_cat:
        bloco = mapa[ck]
        subs_list = []
        for sub in bloco["subs"].values():
            subs_list.append(
                {
                    "slug": sub["slug"],
                    "nome": sub["nome"],
                    "subs2": list(sub["subs2"].values()),
                    "produtos_sem_sub2": sub["produtos_sem_sub2"],
                }
            )
        secoes.append(
            {
                "slug": bloco["slug"],
                "nome": bloco["nome"],
                "subs": subs_list,
                "produtos_sem_sub": bloco["produtos_sem_sub"],
            }
        )
    return secoes


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
        raise ErroPedidoCatalogo("Informe cidade, logradouro e número.")
    if not endereco and not plus_code:
        raise ErroPedidoCatalogo("Informe o endereço de entrega.")
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
        )
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
        "logradouro": (cli.logradouro or "").strip(),
        "numero": (cli.numero or "").strip(),
        "bairro": (cli.bairro or "").strip(),
        "cidade": (cli.cidade or "").strip(),
        "uf": (cli.uf or "SP").strip() or "SP",
        "cep": (cli.cep or "").strip(),
    }
