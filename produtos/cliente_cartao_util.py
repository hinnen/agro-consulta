"""Cartão do cliente: barras 981 no PDV e QR com token longo no celular."""
from __future__ import annotations

import hashlib
import re
import secrets
from datetime import timedelta

from django.db import transaction
from django.utils import timezone

from produtos.models import ClienteAgro, ClienteCartaoAgro, ItemVendaAgro, VendaAgro

PREFIXO = "981"
SESSAO_OK = "cliente_cartao_ok"


def digito_ean13(corpo12: str) -> str:
    soma = 0
    for i, ch in enumerate(corpo12):
        n = int(ch)
        soma += n if i % 2 == 0 else n * 3
    return str((10 - (soma % 10)) % 10)


def montar_ean13(seq: int) -> str:
    corpo = PREFIXO + f"{int(seq):09d}"
    return corpo + digito_ean13(corpo)


def normalizar_codigo(bruto: str) -> str:
    return re.sub(r"\D", "", bruto or "")


def eh_codigo_cartao(bruto: str) -> bool:
    codigo = normalizar_codigo(bruto)
    if len(codigo) != 13 or not codigo.startswith(PREFIXO):
        return False
    return digito_ean13(codigo[:12]) == codigo[12]


def _proximo_seq() -> int:
    ultimo = 0
    for codigo in ClienteCartaoAgro.objects.values_list("codigo", flat=True):
        if codigo and codigo.startswith(PREFIXO) and len(codigo) == 13:
            try:
                ultimo = max(ultimo, int(codigo[3:12]))
            except ValueError:
                continue
    return ultimo + 1


def emitir_cartoes(quantidade: int) -> list[ClienteCartaoAgro]:
    qtd = max(1, min(int(quantidade or 8), 40))
    criados: list[ClienteCartaoAgro] = []
    with transaction.atomic():
        seq = _proximo_seq()
        for _ in range(qtd):
            codigo = montar_ean13(seq)
            while ClienteCartaoAgro.objects.filter(codigo=codigo).exists():
                seq += 1
                codigo = montar_ean13(seq)
            criados.append(
                ClienteCartaoAgro.objects.create(
                    codigo=codigo,
                    token=secrets.token_urlsafe(32),
                )
            )
            seq += 1
    return criados


def cartao_por_codigo(bruto: str) -> ClienteCartaoAgro | None:
    codigo = normalizar_codigo(bruto)
    if not eh_codigo_cartao(codigo):
        return None
    return ClienteCartaoAgro.objects.select_related("cliente").filter(codigo=codigo).first()


def cartao_por_token(token: str) -> ClienteCartaoAgro | None:
    tok = (token or "").strip()
    if len(tok) < 20:
        return None
    return ClienteCartaoAgro.objects.select_related("cliente").filter(token=tok).first()


def vincular(cartao: ClienteCartaoAgro, cliente: ClienteAgro) -> None:
    agora = timezone.now()
    with transaction.atomic():
        ClienteCartaoAgro.objects.filter(cliente=cliente, ativo=True).exclude(pk=cartao.pk).update(
            ativo=False
        )
        if cartao.cliente_id and cartao.cliente_id != cliente.pk:
            cartao.ativo = False
            cartao.save(update_fields=["ativo"])
            raise ValueError("este cartão já é de outra pessoa")
        cartao.cliente = cliente
        cartao.ativo = True
        cartao.vinculado_em = agora
        cartao.codigo_whatsapp_hash = ""
        cartao.codigo_whatsapp_expira = None
        cartao.save(
            update_fields=[
                "cliente",
                "ativo",
                "vinculado_em",
                "codigo_whatsapp_hash",
                "codigo_whatsapp_expira",
            ]
        )


def mesmo_whatsapp(cadastro: str, digitado: str) -> bool:
    a = re.sub(r"\D", "", cadastro or "")
    b = re.sub(r"\D", "", digitado or "")
    if len(a) < 10 or len(b) < 10:
        return False
    return a[-10:] == b[-10:]


def _hash_codigo(token: str, codigo: str) -> str:
    return hashlib.sha256(f"{token}:{codigo}".encode()).hexdigest()


def pedir_codigo_whatsapp(cartao: ClienteCartaoAgro, whatsapp: str) -> tuple[str, str]:
    """Devolve (codigo, situacao). situacao: enviado | sem_conversa | falha | sem_whatsapp | numero_diferente."""
    cliente = cartao.cliente
    if cliente is None or not cartao.ativo:
        return "", "invalido"
    if not (cliente.whatsapp or "").strip():
        return "", "sem_whatsapp"
    if not mesmo_whatsapp(cliente.whatsapp, whatsapp):
        return "", "numero_diferente"
    codigo = f"{secrets.randbelow(1000000):06d}"
    cartao.codigo_whatsapp_hash = _hash_codigo(cartao.token, codigo)
    cartao.codigo_whatsapp_expira = timezone.now() + timedelta(minutes=10)
    cartao.save(update_fields=["codigo_whatsapp_hash", "codigo_whatsapp_expira"])
    ok, _motivo = _enviar_codigo(cliente, codigo)
    if ok:
        return codigo, "enviado"
    return codigo, "sem_conversa"


def _enviar_codigo(cliente: ClienteAgro, codigo: str) -> tuple[bool, str]:
    try:
        from produtos.atendimento_whatsapp_recursos import enviar_texto_cliente
        from produtos.models import WhatsAppConversaAgro

        digits = re.sub(r"\D", "", cliente.whatsapp or "")
        cauda = digits[-11:] if len(digits) >= 11 else digits[-10:]
        if len(cauda) < 10:
            return False, "sem_conversa"
        conv = (
            WhatsAppConversaAgro.objects.filter(telefone__icontains=cauda)
            .order_by("-ultima_em")
            .first()
        )
        if conv is None:
            return False, "sem_conversa"
        texto = f"GM Agro: código do seu cartão {codigo}. Vale por 10 minutos."
        return enviar_texto_cliente(conv.pk, texto, autor="Cartão")
    except Exception:
        return False, "sem_conversa"


def confirmar_codigo(cartao: ClienteCartaoAgro, codigo: str) -> bool:
    bruto = re.sub(r"\D", "", codigo or "")
    if len(bruto) != 6 or not cartao.codigo_whatsapp_hash or not cartao.codigo_whatsapp_expira:
        return False
    if timezone.now() > cartao.codigo_whatsapp_expira:
        return False
    ok = secrets.compare_digest(cartao.codigo_whatsapp_hash, _hash_codigo(cartao.token, bruto))
    if ok:
        cartao.codigo_whatsapp_hash = ""
        cartao.codigo_whatsapp_expira = None
        cartao.save(update_fields=["codigo_whatsapp_hash", "codigo_whatsapp_expira"])
    return ok


def compras_do_cliente(cliente: ClienteAgro, limite: int = 20) -> list[dict]:
    refs = [str(cliente.pk), f"local:{cliente.pk}", f"agro:{cliente.pk}"]
    externo = (cliente.externo_id or "").strip()
    if externo:
        refs.append(externo)
    vendas = (
        VendaAgro.objects.filter(cliente_id_erp__in=refs, devolvida_em__isnull=True)
        .order_by("-criado_em")
        .prefetch_related("itens")[:limite]
    )
    saida = []
    for venda in vendas:
        itens = []
        for item in venda.itens.all():
            if not isinstance(item, ItemVendaAgro):
                continue
            itens.append(
                {
                    "descricao": item.descricao,
                    "quantidade": item.quantidade,
                    "total": item.valor_total,
                }
            )
        saida.append(
            {
                "quando": venda.criado_em,
                "total": venda.total,
                "forma": venda.forma_pagamento,
                "itens": itens,
            }
        )
    return saida
