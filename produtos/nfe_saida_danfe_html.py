"""DANFE NF-e modelo 55 — layout A4 padrão (HTML para impressão)."""
from __future__ import annotations

import html
import re
from decimal import Decimal
from typing import Any

from decouple import config

from produtos.nfe_saida_util import total_itens


def _esc(val: Any) -> str:
    return html.escape(str(val if val is not None else ""))


def _digits(s: Any, n: int = 20) -> str:
    return re.sub(r"\D", "", str(s or ""))[:n]


def _fmt_cnpj(cnpj: str) -> str:
    d = _digits(cnpj, 14)
    if len(d) != 14:
        return d or "—"
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:]}"


def _fmt_cpf_cnpj(doc: str) -> str:
    d = _digits(doc, 14)
    if len(d) == 14:
        return _fmt_cnpj(d)
    if len(d) == 11:
        return f"{d[:3]}.{d[3:6]}.{d[6:9]}-{d[9:]}"
    return d or "—"


def _fmt_chave(chave: str) -> str:
    d = _digits(chave, 44)
    if len(d) != 44:
        return chave or "—"
    return " ".join(d[i : i + 4] for i in range(0, 44, 4))


def _fmt_moeda(v: Any) -> str:
    try:
        x = Decimal(str(v or "0").replace(",", "."))
    except Exception:
        x = Decimal("0")
    return f"{x.quantize(Decimal('0.01')):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def carregar_emitente_danfe() -> dict[str, str]:
    """Emitente a partir do mesmo .env da NFC-e / Dist DF-e."""
    cnpj_nfe = _digits(config("NFE_DIST_DFE_CNPJ", default="") or "", 14)
    try:
        from produtos.nfce_config_util import nfce_cfg, nfce_loja_de_cnpj

        loja = nfce_loja_de_cnpj(cnpj_nfe) if cnpj_nfe else "centro"
        cfg = nfce_cfg(loja)
    except Exception:
        cfg = {}
    razao = (cfg.get("razao_social") or config("NFC_E_RAZAO_SOCIAL", default="") or "GM Agro").strip()
    fantasia = (cfg.get("fantasia") or config("NFC_E_FANTASIA", default="") or razao).strip()
    return {
        "razao": razao[:150],
        "fantasia": fantasia[:60],
        "cnpj": _digits(cfg.get("cnpj") or cnpj_nfe, 14),
        "ie": _digits(cfg.get("ie") or config("NFC_E_IE", default=""), 14),
        "logradouro": str(cfg.get("logradouro") or config("NFC_E_LOGRADOURO", default="") or "")[:60],
        "numero": str(cfg.get("numero") or config("NFC_E_NUMERO", default="") or "S/N")[:20],
        "bairro": str(cfg.get("bairro") or config("NFC_E_BAIRRO", default="") or "")[:40],
        "cidade": str(cfg.get("cidade") or config("NFC_E_CIDADE", default="") or "")[:40],
        "uf": str(cfg.get("uf") or config("NFC_E_UF", default="SP") or "SP")[:2].upper(),
        "cep": _digits(cfg.get("cep") or config("NFC_E_CEP", default=""), 8),
        "fone": _digits(cfg.get("fone") or config("NFC_E_FONE", default=""), 14),
    }


def montar_html_danfe(doc: dict, *, emitente: dict[str, str] | None = None) -> str:
    """HTML DANFE retrato A4 — estrutura alinhada ao modelo nacional."""
    em = emitente or carregar_emitente_danfe()
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    itens = doc.get("itens") if isinstance(doc.get("itens"), list) else []
    tp_amb = int(doc.get("tp_amb") or 2)
    homolog = tp_amb == 2
    autorizada = str(doc.get("status") or "") == "autorizada"
    chave_raw = _digits(doc.get("chave"), 44)
    chave_fmt = _fmt_chave(chave_raw) if chave_raw else "—"
    nnf = doc.get("nNF") or "—"
    serie = fiscal.get("serie") or doc.get("serie") or "—"
    protocolo = str(doc.get("protocolo") or "")
    natureza = str(fiscal.get("natureza") or "VENDA")
    total = total_itens(itens)
    em_end = ", ".join(
        x
        for x in (
            f"{em.get('logradouro', '')} {em.get('numero', '')}".strip(),
            em.get("bairro"),
            f"{em.get('cidade', '')}-{em.get('uf', '')}",
            f"CEP {_digits(em.get('cep'), 8)}" if em.get("cep") else "",
        )
        if x
    )
    dest_end = ", ".join(
        x
        for x in (
            f"{dest.get('logradouro', '')} {dest.get('numero', '')}".strip(),
            dest.get("bairro"),
            f"{dest.get('cidade', '')}-{dest.get('uf', '')}",
            f"CEP {_digits(dest.get('cep'), 8)}" if dest.get("cep") else "",
        )
        if x
    )

    item_rows = ""
    for ln in itens:
        if not isinstance(ln, dict):
            continue
        csosn = _esc(ln.get("csosn") or "102")
        item_rows += (
            "<tr>"
            f"<td class='c'>{_esc(ln.get('codigo') or ln.get('produto_id') or '')}</td>"
            f"<td>{_esc(ln.get('descricao', ''))}</td>"
            f"<td class='c'>{_esc(ln.get('ncm', ''))}</td>"
            f"<td class='c'>{csosn}</td>"
            f"<td class='c'>{_esc(ln.get('cfop', ''))}</td>"
            f"<td class='c'>{_esc(ln.get('un', 'UN'))}</td>"
            f"<td class='r'>{_esc(ln.get('qtd', ''))}</td>"
            f"<td class='r'>{_fmt_moeda(ln.get('v_unit'))}</td>"
            f"<td class='r'>{_fmt_moeda(ln.get('v_total'))}</td>"
            "<td class='r'>0,00</td><td class='r'>0,00</td><td class='r'>0,00</td>"
            "<td class='r'>0,00</td><td class='r'>0,00</td>"
            "</tr>"
        )
    if not item_rows:
        item_rows = "<tr><td colspan='14' class='c'>—</td></tr>"

    aviso_homolog = ""
    if homolog:
        aviso_homolog = (
            "<div class='homolog'>SEM VALOR FISCAL — AMBIENTE DE HOMOLOGAÇÃO — "
            "SEM VALOR FISCAL</div>"
        )
    aviso_previa = ""
    if not autorizada:
        aviso_previa = "<div class='previa'>PRÉVIA — NOTA AINDA NÃO AUTORIZADA NA SEFAZ</div>"

    barcode_block = ""
    barcode_script = ""
    if len(chave_raw) == 44:
        barcode_block = "<svg id='danfe-barcode'></svg>"
        barcode_script = f"""
<script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.6/dist/JsBarcode.all.min.js"></script>
<script>
try {{
  JsBarcode("#danfe-barcode", "{chave_raw}", {{format: "CODE128", width: 1.1, height: 48, displayValue: false, margin: 0}});
}} catch (e) {{}}
</script>"""

    inf_compl = str(doc.get("inf_complementar") or fiscal.get("inf_compl") or "")
    if doc.get("simulado"):
        inf_compl = (inf_compl + " Emissão simulada homologação SisVale.").strip()

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DANFE NF-e {_esc(nnf)}</title>
<style>
@page {{ size: A4 portrait; margin: 5mm; }}
* {{ box-sizing: border-box; }}
body {{ font-family: Arial, Helvetica, sans-serif; font-size: 7pt; color: #000; margin: 0; padding: 4mm; }}
table.danfe {{ width: 100%; border-collapse: collapse; table-layout: fixed; }}
table.danfe td, table.danfe th {{ border: 1px solid #000; padding: 2px 3px; vertical-align: top; word-wrap: break-word; }}
.lbl {{ font-size: 5.5pt; text-transform: uppercase; display: block; margin-bottom: 1px; }}
.val {{ font-size: 7pt; font-weight: bold; }}
.r {{ text-align: right; }}
.c {{ text-align: center; }}
.nob {{ border: none !important; }}
.corte {{ border: none; border-top: 1px dashed #000; margin: 3mm 0; height: 0; }}
.danfe-box {{ text-align: center; vertical-align: middle; }}
.danfe-box .tit {{ font-size: 12pt; font-weight: bold; letter-spacing: 1px; }}
.danfe-box .sub {{ font-size: 6pt; font-weight: bold; line-height: 1.2; }}
.danfe-box .nf {{ font-size: 10pt; font-weight: bold; margin-top: 4px; }}
.chave {{ font-size: 7pt; font-weight: bold; letter-spacing: 0.5px; text-align: center; }}
#danfe-barcode {{ width: 100%; max-height: 52px; }}
.homolog {{ background: #000; color: #fff; text-align: center; font-weight: bold; font-size: 9pt; padding: 4px; margin-bottom: 2mm; }}
.previa {{ background: #fef3c7; border: 2px solid #d97706; text-align: center; font-weight: bold; padding: 3px; margin-bottom: 2mm; }}
.prod thead th {{ font-size: 5.5pt; text-align: center; background: #f3f3f3; }}
.prod td {{ font-size: 6.5pt; }}
@media print {{
  body {{ padding: 0; }}
  .no-print {{ display: none; }}
}}
</style>
</head>
<body>
{aviso_homolog}
{aviso_previa}
<p class="no-print" style="font-size:8pt;margin:0 0 4px;">Use <strong>Ctrl+P</strong> · papel A4 · margens padrão.</p>

<table class="danfe">
<tr>
  <td colspan="2" style="width:70%">
    <span class="lbl">Recebemos de {_esc(em.get('razao'))} os produtos e/ou serviços constantes da NF-e indicada ao lado.</span>
    <span class="val">Emissão: {_esc(doc.get('updated_at', '')[:10] if isinstance(doc.get('updated_at'), str) else '')}</span>
  </td>
  <td style="width:30%" class="c">
    <span class="lbl">NF-e</span>
    <span class="val">Nº {_esc(nnf)} · Série {_esc(serie)}</span>
  </td>
</tr>
<tr>
  <td colspan="2"><span class="lbl">Data do recebimento</span><span class="val">&nbsp;</span></td>
  <td><span class="lbl">Identificação e assinatura do recebedor</span><span class="val">&nbsp;</span></td>
</tr>
</table>
<hr class="corte">

<table class="danfe">
<tr>
  <td style="width:40%">
    <span class="lbl">Identificação do emitente</span>
    <span class="val">{_esc(em.get('fantasia') or em.get('razao'))}</span><br>
    <span style="font-size:6.5pt">{_esc(em.get('razao'))}<br>
    {_esc(em_end)}<br>
    Fone: {_esc(em.get('fone') or '—')}</span>
  </td>
  <td class="danfe-box" style="width:12%">
    <div class="tit">DANFE</div>
    <div class="sub">Documento Auxiliar da<br>Nota Fiscal Eletrônica</div>
    <div class="sub" style="margin-top:6px">0 - ENTRADA<br>1 - SAÍDA</div>
    <div class="nf">1</div>
    <div class="sub">Nº {_esc(nnf)}<br>Série {_esc(serie)}</div>
  </td>
  <td style="width:48%">
    {barcode_block}
    <div class="chave">{_esc(chave_fmt)}</div>
    <span class="lbl" style="margin-top:4px">Consulta de autenticidade no portal nacional da NF-e</span>
  </td>
</tr>
</table>

<table class="danfe">
<tr>
  <td colspan="2"><span class="lbl">Natureza da operação</span><span class="val">{_esc(natureza)}</span></td>
  <td><span class="lbl">Protocolo de autorização</span><span class="val">{_esc(protocolo or '—')}</span></td>
</tr>
<tr>
  <td><span class="lbl">Inscrição estadual</span><span class="val">{_esc(em.get('ie') or '—')}</span></td>
  <td><span class="lbl">CNPJ</span><span class="val">{_esc(_fmt_cnpj(em.get('cnpj', '')))}</span></td>
  <td><span class="lbl">Chave de acesso</span><span class="val" style="font-size:6pt">{_esc(chave_fmt)}</span></td>
</tr>
</table>

<table class="danfe">
<tr>
  <td colspan="4"><span class="lbl">Destinatário / Remetente</span></td>
</tr>
<tr>
  <td colspan="3"><span class="lbl">Nome / Razão social</span><span class="val">{_esc(dest.get('nome', ''))}</span></td>
  <td><span class="lbl">CNPJ/CPF</span><span class="val">{_esc(_fmt_cpf_cnpj(dest.get('documento', '')))}</span></td>
</tr>
<tr>
  <td colspan="2"><span class="lbl">Endereço</span><span class="val">{_esc(dest_end)}</span></td>
  <td><span class="lbl">Bairro</span><span class="val">{_esc(dest.get('bairro', ''))}</span></td>
  <td><span class="lbl">CEP</span><span class="val">{_esc(_digits(dest.get('cep'), 8))}</span></td>
</tr>
<tr>
  <td><span class="lbl">Município</span><span class="val">{_esc(dest.get('cidade', ''))}</span></td>
  <td><span class="lbl">UF</span><span class="val">{_esc(dest.get('uf', ''))}</span></td>
  <td><span class="lbl">Inscrição estadual</span><span class="val">{_esc(dest.get('ie') or '—')}</span></td>
  <td><span class="lbl">Fone</span><span class="val">—</span></td>
</tr>
</table>

<table class="danfe">
<tr>
  <td><span class="lbl">Base cálc. ICMS</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Valor ICMS</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Base cálc. ICMS ST</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Valor ICMS ST</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Valor total produtos</span><span class="val r">{_fmt_moeda(total)}</span></td>
</tr>
<tr>
  <td><span class="lbl">Valor frete</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Valor seguro</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Desconto</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Outras despesas</span><span class="val r">0,00</span></td>
  <td><span class="lbl">Valor total da nota</span><span class="val r">{_fmt_moeda(total)}</span></td>
</tr>
</table>

<table class="danfe prod">
<thead>
<tr>
  <th style="width:7%">Cód.</th>
  <th style="width:22%">Descrição</th>
  <th style="width:7%">NCM</th>
  <th style="width:5%">CST</th>
  <th style="width:5%">CFOP</th>
  <th style="width:4%">UN</th>
  <th style="width:5%">Qtd</th>
  <th style="width:7%">V.Unit</th>
  <th style="width:7%">V.Total</th>
  <th style="width:7%">BC ICMS</th>
  <th style="width:6%">V.ICMS</th>
  <th style="width:5%">V.IPI</th>
  <th style="width:6%">%ICMS</th>
  <th style="width:5%">%IPI</th>
</tr>
</thead>
<tbody>
{item_rows}
</tbody>
</table>

<table class="danfe">
<tr>
  <td style="height:48px">
    <span class="lbl">Dados adicionais</span>
    <span style="font-size:6.5pt">{_esc(inf_compl)}</span>
  </td>
</tr>
</table>

{barcode_script}
</body>
</html>"""


def montar_html_danfe_previa(doc: dict, *, emitente_nome: str = "", emitente_cnpj: str = "") -> str:
    """Compat — usa layout padrão; ignora nome mascarado legado."""
    em = carregar_emitente_danfe()
    if emitente_nome and not em.get("razao"):
        em["razao"] = emitente_nome
    if emitente_cnpj and not em.get("cnpj"):
        em["cnpj"] = _digits(emitente_cnpj, 14)
    return montar_html_danfe(doc, emitente=em)
