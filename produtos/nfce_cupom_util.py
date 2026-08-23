"""DANFE NFC-e 80mm — impressão térmica após autorização SEFAZ."""
from __future__ import annotations

import re
from typing import Any

from produtos.caixa_util import format_moeda_br, pagamentos_lista_de_venda
from produtos.models import NfceDocumentoAgro, VendaAgro
from produtos.nfce_config_util import nfce_cfg, nfce_loja_de_venda
from produtos.nfce_ibpt_util import calcular_ibpt_venda_itens
from produtos.venda_cupom_util import _formatar_data_venda, _forma_pagamento_cupom

URL_CONSULTA_CHAVE = {
    1: "https://www.nfce.fazenda.sp.gov.br/consulta",
    2: "https://homologacao.nfce.fazenda.sp.gov.br/consulta",
}


def _fmt_cnpj(cnpj: str) -> str:
    d = re.sub(r"\D", "", cnpj or "")
    if len(d) != 14:
        return cnpj or ""
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _fmt_cpf(cpf: str) -> str:
    c = re.sub(r"\D", "", cpf or "")
    if len(c) != 11:
        return cpf or ""
    return f"{c[:3]}.{c[3:6]}.{c[6:9]}-{c[9:]}"


def _fmt_cep(cep: str) -> str:
    d = re.sub(r"\D", "", cep or "")
    if len(d) != 8:
        return cep or ""
    return f"{d[:5]}-{d[5:]}"


def serializar_nfce_cupom_80mm(
    venda: VendaAgro,
    nfce: NfceDocumentoAgro,
    *,
    segunda_via: bool = False,
    db=None,
    col_p: str | None = None,
) -> dict[str, Any]:
    cfg = nfce_cfg(nfce_loja_de_venda(venda))
    # Preferir CNPJ gravado no documento (autorizado) se houver
    if getattr(nfce, "emitente_cnpj", None):
        from produtos.nfce_config_util import nfce_loja_de_cnpj

        cfg = nfce_cfg(nfce_loja_de_cnpj(nfce.emitente_cnpj))
    itens_qs = list(venda.itens.all().order_by("pk"))
    itens = []
    for it in itens_qs:
        itens.append(
            {
                "nome": it.descricao,
                "qtd": float(it.quantidade or 0),
                "preco": float(it.valor_unitario or 0),
                "subtotal": float(it.valor_total or 0),
            }
        )
    frete = float(getattr(venda, "frete", 0) or 0)
    if frete > 0.009:
        itens.append(
            {
                "nome": "Taxa de entrega",
                "qtd": 1.0,
                "preco": frete,
                "subtotal": frete,
                "eh_frete": True,
            }
        )
    ibpt = calcular_ibpt_venda_itens(itens_qs, db=db, col_p=col_p, uf=cfg.get("uf") or "SP")
    tp_amb = int(nfce.tp_amb or 2)
    endereco_partes = [
        cfg.get("logradouro"),
        cfg.get("numero"),
        cfg.get("bairro"),
        cfg.get("cidade"),
        cfg.get("uf"),
    ]
    cep_fmt = _fmt_cep(cfg.get("cep") or "")
    if cep_fmt:
        endereco_partes.append(f"CEP {cep_fmt}")
    pagamentos = pagamentos_lista_de_venda(venda)
    total = float(venda.total or 0)
    valor_pago = sum(float(p.get("valor") or 0) for p in pagamentos) or total
    troco = max(0.0, round(valor_pago - total, 2))
    return {
        "tipo": "nfce",
        "segunda_via": bool(segunda_via),
        "emitente_razao_social": cfg.get("razao_social") or "",
        "emitente_fantasia": cfg.get("fantasia") or cfg.get("razao_social") or "",
        "emitente_cnpj": _fmt_cnpj(cfg.get("cnpj") or ""),
        "emitente_ie": cfg.get("ie") or "",
        "emitente_endereco": ", ".join(x for x in endereco_partes if x),
        "venda_id": venda.pk,
        "numero_nf": nfce.numero,
        "serie_nf": nfce.serie,
        "chave": nfce.chave,
        "protocolo": nfce.protocolo,
        "qr_code_url": nfce.qr_code_url,
        "url_consulta_chave": URL_CONSULTA_CHAVE.get(tp_amb, URL_CONSULTA_CHAVE[2]),
        "cliente_nome": (venda.cliente_nome or "CONSUMIDOR")[:120],
        "cliente_cpf": _fmt_cnpj(nfce.dest_cpf) if len(re.sub(r"\D", "", nfce.dest_cpf or "")) == 14 else _fmt_cpf(nfce.dest_cpf),
        "cliente_doc_rotulo": "CNPJ" if len(re.sub(r"\D", "", nfce.dest_cpf or "")) == 14 else "CPF",
        "consumidor_sem_identificacao": nfce.consumidor_sem_identificacao,
        "criado_em": _formatar_data_venda(venda.criado_em),
        "itens": itens,
        "qtd_itens": len(itens),
        "frete": frete,
        "frete_texto": format_moeda_br(frete),
        "total": total,
        "total_texto": format_moeda_br(venda.total),
        "valor_pago": valor_pago,
        "valor_pago_texto": format_moeda_br(valor_pago),
        "troco": troco,
        "troco_texto": format_moeda_br(troco),
        "desconto": 0.0,
        "desconto_texto": format_moeda_br(0),
        "forma_pagamento": _forma_pagamento_cupom(venda),
        "pagamentos": pagamentos,
        "ibpt_texto": ibpt["ibpt_texto"],
        "tp_amb": tp_amb,
        "homologacao": tp_amb == 2,
        "emissao_normal": True,
    }
