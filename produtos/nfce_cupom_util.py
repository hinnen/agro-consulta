"""DANFE NFC-e 80mm — impressão térmica após autorização SEFAZ."""
from __future__ import annotations

from typing import Any

from django.utils import timezone

from produtos.caixa_util import format_moeda_br, pagamentos_lista_de_venda
from produtos.models import NfceDocumentoAgro, VendaAgro
from produtos.nfce_config_util import nfce_cfg
from produtos.venda_cupom_util import _formatar_data_venda, _forma_pagamento_cupom


def serializar_nfce_cupom_80mm(venda: VendaAgro, nfce: NfceDocumentoAgro, *, segunda_via: bool = False) -> dict[str, Any]:
    cfg = nfce_cfg()
    itens = []
    for it in venda.itens.all().order_by("pk"):
        itens.append(
            {
                "nome": it.descricao,
                "qtd": float(it.quantidade or 0),
                "preco": float(it.valor_unitario or 0),
                "subtotal": float(it.valor_total or 0),
            }
        )
    cpf_fmt = ""
    if nfce.dest_cpf and len(nfce.dest_cpf) == 11:
        c = nfce.dest_cpf
        cpf_fmt = f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"
    return {
        "tipo": "nfce",
        "segunda_via": bool(segunda_via),
        "emitente_fantasia": cfg.get("fantasia") or cfg.get("razao_social") or "",
        "emitente_cnpj": cfg.get("cnpj") or "",
        "emitente_ie": cfg.get("ie") or "",
        "emitente_endereco": ", ".join(
            x
            for x in (
                cfg.get("logradouro"),
                cfg.get("numero"),
                cfg.get("bairro"),
                cfg.get("cidade"),
                cfg.get("uf"),
            )
            if x
        ),
        "venda_id": venda.pk,
        "numero_nf": nfce.numero,
        "serie_nf": nfce.serie,
        "chave": nfce.chave,
        "protocolo": nfce.protocolo,
        "qr_code_url": nfce.qr_code_url,
        "cliente_nome": (venda.cliente_nome or "CONSUMIDOR")[:120],
        "cliente_cpf": cpf_fmt,
        "consumidor_sem_identificacao": nfce.consumidor_sem_identificacao,
        "criado_em": _formatar_data_venda(venda.criado_em),
        "itens": itens,
        "total": float(venda.total or 0),
        "total_texto": format_moeda_br(venda.total),
        "forma_pagamento": _forma_pagamento_cupom(venda),
        "pagamentos": pagamentos_lista_de_venda(venda),
        "tp_amb": nfce.tp_amb,
        "homologacao": int(nfce.tp_amb or 2) == 2,
    }
