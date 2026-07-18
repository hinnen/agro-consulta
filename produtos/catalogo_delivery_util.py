"""Catálogo delivery GM Agro — produtos marcados no cadastro → vitrine pública."""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any

from django.db import transaction

from produtos.cliente_whatsapp_util import cliente_agro_por_whatsapp, extrair_whatsapp_digits
from produtos.estoque_saldo_agro_util import mapa_saldos_operacionais_agro
from produtos.models import (
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
        categoria = (ov.categoria or (pg.categoria if pg else "") or "").strip()
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

        itens.append(
            {
                "id": pid,
                "nome": nome,
                "descricao": desc,
                "preco": round(preco_f, 2),
                "marca": marca,
                "categoria": categoria,
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

    itens.sort(key=lambda x: (not x["destaque"], x["ordem"], x["nome"].lower()))
    return itens


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
