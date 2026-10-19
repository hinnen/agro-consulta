"""DANFE NF-e modelo 55 — layout A4 profissional (HTML impressão / prévia)."""
from __future__ import annotations

import base64
import html
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from decouple import config
from django.conf import settings

from produtos.nfe_saida_util import total_itens

_NS_NFE = "http://www.portalfiscal.inf.br/nfe"
_ITENS_POR_PAGINA = 22
_LOGO_REL = Path("produtos/static/produtos/img/logo_agro_mais_danfe.png")


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


def _fmt_cep(cep: str) -> str:
    d = _digits(cep, 8)
    if len(d) != 8:
        return d or "—"
    return f"{d[:5]}-{d[5:]}"


def _fmt_nnf(nnf: Any) -> str:
    try:
        n = int(nnf)
    except (TypeError, ValueError):
        return str(nnf or "—")
    s = str(n).zfill(9)
    return f"{s[0:3]}.{s[3:6]}.{s[6:9]}"


def _fmt_serie(serie: Any) -> str:
    try:
        return str(int(serie)).zfill(3)
    except (TypeError, ValueError):
        return str(serie or "—")


def _fmt_data_br(val: str) -> str:
    raw = (val or "").strip()
    if not raw:
        return "—"
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    if re.match(r"\d{2}/\d{2}/\d{4}", raw):
        return raw[:10]
    return raw[:10]


def _fmt_hora_br(val: str) -> str:
    raw = (val or "").strip()
    m = re.search(r"T(\d{2}:\d{2}:\d{2})", raw)
    if m:
        return m.group(1)
    m = re.search(r"(\d{2}:\d{2}:\d{2})", raw)
    return m.group(1) if m else "—"


def _dest_resumo_canhoto(dest: dict) -> str:
    nome = str(dest.get("nome") or "").strip()
    log = str(dest.get("logradouro") or "").strip()
    nro = str(dest.get("numero") or "").strip()
    bairro = str(dest.get("bairro") or "").strip()
    cid = str(dest.get("cidade") or "").strip()
    uf = str(dest.get("uf") or "").strip().upper()
    partes = [p for p in (nome, f"{log}, {nro}".strip(", "), bairro, f"{cid}-{uf}".strip("-")) if p]
    return " - ".join(partes) if partes else "—"


def cnpj_emitente_da_chave(chave: str | None) -> str:
    ch = _digits(chave, 44)
    if len(ch) >= 20:
        return ch[6:20]
    return ""


def _local_tag(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _parse_totais_xml(xml_raw: str) -> dict[str, str]:
    """Extrai ICMSTot do XML autorizado (sem inventar valores)."""
    out: dict[str, str] = {}
    raw = (xml_raw or "").strip()
    if not raw:
        return out
    try:
        root = ET.fromstring(raw.encode("utf-8") if isinstance(raw, str) else raw)
    except ET.ParseError:
        return out
    for el in root.iter():
        if _local_tag(el.tag) != "ICMSTot":
            continue
        for ch in el:
            ln = _local_tag(ch.tag)
            if ch.text is not None and ln.startswith("v"):
                out[ln] = ch.text.strip()
        break
    for el in root.iter():
        if _local_tag(el.tag) == "dhEmi" and el.text:
            out["dhEmi"] = el.text.strip()[:19]
            break
    for el in root.iter():
        if _local_tag(el.tag) == "nProt" and el.text:
            out["nProt"] = el.text.strip()
            break
    return out


def _logo_data_uri() -> str:
    path = Path(settings.BASE_DIR) / _LOGO_REL
    if not path.is_file():
        alt = Path(settings.BASE_DIR) / "produtos/static/produtos/img/logo_agro_mais.png"
        path = alt if alt.is_file() else path
    if not path.is_file():
        return ""
    try:
        b64 = base64.b64encode(path.read_bytes()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except OSError:
        return ""


def carregar_emitente_danfe(cnpj: str | None = None, *, doc: dict | None = None) -> dict[str, str]:
    """Emitente por CNPJ (chave da nota, XML ou .env) — Centro / Vila."""
    cnpj_res = _digits(cnpj, 14)
    if not cnpj_res and doc:
        cnpj_res = cnpj_emitente_da_chave(str(doc.get("chave") or ""))
        xml = str(doc.get("xml_autorizado") or "")
        if not cnpj_res and xml:
            try:
                root_xml = ET.fromstring(xml.encode("utf-8"))
                for el in root_xml.iter():
                    if _local_tag(el.tag) == "CNPJ" and el.text:
                        cnpj_res = _digits(el.text, 14)
                        break
            except ET.ParseError:
                pass
    if not cnpj_res:
        cnpj_res = _digits(config("NFE_DIST_DFE_CNPJ", default="") or "", 14)
    try:
        from produtos.nfce_config_util import nfce_cfg, nfce_loja_de_cnpj

        loja = nfce_loja_de_cnpj(cnpj_res) if cnpj_res else "centro"
        cfg = nfce_cfg(loja)
    except Exception:
        cfg = {}
    razao = (cfg.get("razao_social") or config("NFC_E_RAZAO_SOCIAL", default="") or "GM Agro").strip()
    fantasia = (cfg.get("fantasia") or config("NFC_E_FANTASIA", default="") or razao).strip()
    return {
        "razao": razao[:150],
        "fantasia": fantasia[:60],
        "cnpj": _digits(cfg.get("cnpj") or cnpj_res, 14),
        "ie": _digits(cfg.get("ie") or config("NFC_E_IE", default=""), 14),
        "logradouro": str(cfg.get("logradouro") or config("NFC_E_LOGRADOURO", default="") or "")[:60],
        "numero": str(cfg.get("numero") or config("NFC_E_NUMERO", default="") or "S/N")[:20],
        "bairro": str(cfg.get("bairro") or config("NFC_E_BAIRRO", default="") or "")[:40],
        "cidade": str(cfg.get("cidade") or config("NFC_E_CIDADE", default="") or "")[:40],
        "uf": str(cfg.get("uf") or config("NFC_E_UF", default="SP") or "SP")[:2].upper(),
        "cep": _digits(cfg.get("cep") or config("NFC_E_CEP", default=""), 8),
        "fone": _digits(cfg.get("fone") or config("NFC_E_FONE", default=""), 14),
    }


def _linha_produto(ln: dict) -> str:
    orig = _esc(str(ln.get("origem") or "0")[:1])
    csosn = _esc(re.sub(r"\D", "", str(ln.get("csosn") or "102"))[:3] or "102")
    qtd = ln.get("qtd", "")
    try:
        qtd_fmt = f"{Decimal(str(qtd or 0)).quantize(Decimal('0.0001')):,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        qtd_fmt = _esc(qtd)
    return (
        "<tr>"
        f"<td class='c cod'>{_esc(ln.get('codigo') or ln.get('produto_id') or '')}</td>"
        f"<td class='desc'>{_esc(ln.get('descricao', ''))}</td>"
        f"<td class='c'>{_esc(ln.get('ncm', ''))}</td>"
        f"<td class='c'>{orig}/{csosn}</td>"
        f"<td class='c'>{_esc(ln.get('cfop', ''))}</td>"
        f"<td class='c'>{_esc(ln.get('un', 'UN'))}</td>"
        f"<td class='r'>{qtd_fmt}</td>"
        f"<td class='r'>{_fmt_moeda(ln.get('v_unit'))}</td>"
        f"<td class='r'>{_fmt_moeda(ln.get('v_total'))}</td>"
        "<td class='r'>0,00</td><td class='r'>0,00</td><td class='r'>0,00</td>"
        "<td class='r'>0,00</td><td class='r'>0,00</td><td class='r'>0,00</td>"
        "</tr>"
    )


def _tabela_produtos_head() -> str:
    return """
<thead>
<tr>
  <th>Código produto</th>
  <th>Descrição do produto / serviço</th>
  <th>NCM/SH</th>
  <th>O/CSOSN</th>
  <th>CFOP</th>
  <th>UN</th>
  <th>Quant</th>
  <th>Valor unit</th>
  <th>Valor total</th>
  <th>Valor desc</th>
  <th>B.Cálc ICMS</th>
  <th>Valor ICMS</th>
  <th>Valor IPI</th>
  <th>Alíq. ICMS</th>
  <th>Alíq. IPI</th>
</tr>
</thead>"""


def _bloco_canhoto(
    em: dict,
    nnf: Any,
    serie: Any,
    razao: str,
    *,
    dest: dict,
    dh_emi: str,
    total_nf: Decimal,
) -> str:
    nnf_f = _fmt_nnf(nnf)
    ser_f = _fmt_serie(serie)
    texto = (
        f"RECEBEMOS DE {_esc(razao.upper())} OS PRODUTOS E/OU SERVIÇOS CONSTANTES DA NOTA FISCAL "
        f"ELETRÔNICA INDICADA ABAIXO. EMISSÃO: {_esc(_fmt_data_br(dh_emi))} "
        f"VALOR TOTAL: R$ {_esc(_fmt_moeda(total_nf))} DESTINATÁRIO: {_esc(_dest_resumo_canhoto(dest))}"
    )
    return f"""
<table class="danfe canhoto">
<tr>
  <td class="canhoto-txt" colspan="2">{texto}</td>
  <td class="canhoto-nf c" rowspan="2">
    <div class="canhoto-nfe-tit">NF-e</div>
    <div>Nº. {nnf_f}</div>
    <div>Série {ser_f}</div>
  </td>
</tr>
<tr>
  <td style="width:50%"><span class="lbl">Data de recebimento</span></td>
  <td style="width:50%"><span class="lbl">Identificação e assinatura do recebedor</span></td>
</tr>
</table>
<div class="corte" aria-hidden="true"></div>"""


def _bloco_cabecalho(
    em: dict,
    *,
    nnf: Any,
    serie: Any,
    chave_fmt: str,
    chave_raw: str,
    natureza: str,
    protocolo: str,
    logo_uri: str,
    folha: str,
    dh_emi: str,
) -> str:
    nnf_f = _fmt_nnf(nnf)
    ser_f = _fmt_serie(serie)
    logo_html = (
        f"<img src='{_esc(logo_uri)}' alt='Agro Mais' class='logo-emitente'/>"
        if logo_uri
        else ""
    )
    barcode = (
        f"<svg class='barcode' data-chave='{_esc(chave_raw)}'></svg>"
        if len(chave_raw) == 44
        else ""
    )
    prot_txt = _esc(protocolo or "—")
    if protocolo and dh_emi:
        prot_txt = _esc(f"{protocolo} - {_fmt_data_br(dh_emi)} {_fmt_hora_br(dh_emi)}")
    return f"""
<table class="danfe">
<tr><td colspan="3" class="sec-title">Identificação do emitente</td></tr>
<tr class="row-emit">
  <td class="col-emit">
    {logo_html}
    <div class="emit-razao">{_esc(em.get('razao') or em.get('fantasia'))}</div>
    <div class="emit-linha">{_esc(f"{em.get('logradouro', '')}, {em.get('numero', '')}".strip(', '))}</div>
    <div class="emit-linha">{_esc(f"{em.get('bairro', '')} - {_fmt_cep(em.get('cep', ''))}".strip(' -'))}</div>
    <div class="emit-linha">{_esc(f"{em.get('cidade', '')} - {em.get('uf', '')}".strip(' -'))} Fone/Fax: {_esc(em.get('fone') or '—')}</div>
  </td>
  <td class="col-danfe c">
    <div class="danfe-tit">DANFE</div>
    <div class="danfe-sub">Documento Auxiliar da Nota<br>Fiscal Eletrônica</div>
    <div class="danfe-ent-sai">
      <div>0 - ENTRADA</div>
      <div>1 - SAÍDA <span class="danfe-saida-mark">1</span></div>
    </div>
    <div class="danfe-nf">Nº. {nnf_f}<br>Série {ser_f}<br>Folha {folha}</div>
  </td>
  <td class="col-chave">
    {barcode}
    <div class="chave-lbl">CHAVE DE ACESSO</div>
    <div class="chave-val">{_esc(chave_fmt)}</div>
    <div class="chave-hint">Consulta de autenticidade no portal nacional da NF-e<br>
    www.nfe.fazenda.gov.br/portal ou no site da Sefaz Autorizadora</div>
  </td>
</tr>
</table>
<table class="danfe quad">
<tr>
  <td colspan="2"><span class="lbl">Natureza da operação</span><span class="val">{_esc(natureza.upper())}</span></td>
  <td colspan="2"><span class="lbl">Protocolo de autorização de uso</span><span class="val">{prot_txt}</span></td>
</tr>
<tr>
  <td><span class="lbl">Inscrição estadual</span><span class="val">{_esc(em.get('ie') or '—')}</span></td>
  <td><span class="lbl">Inscrição municipal</span><span class="val">—</span></td>
  <td><span class="lbl">Inscrição estadual do subst. trib.</span><span class="val">—</span></td>
  <td><span class="lbl">CNPJ / CPF</span><span class="val">{_esc(_fmt_cnpj(em.get('cnpj', '')))}</span></td>
</tr>
</table>"""


def _bloco_destinatario(dest: dict, dh_emi: str) -> str:
    dest_end = f"{dest.get('logradouro', '')}, {dest.get('numero', '')}".strip(", ")
    email = str(dest.get("email") or "").strip()
    return f"""
<table class="danfe quad">
<tr><td colspan="4" class="sec-title">Destinatário / Remetente</td></tr>
<tr>
  <td colspan="3"><span class="lbl">Nome / Razão social</span><span class="val">{_esc(dest.get('nome', ''))}</span></td>
  <td><span class="lbl">CNPJ / CPF</span><span class="val">{_esc(_fmt_cpf_cnpj(dest.get('documento', '')))}</span></td>
</tr>
<tr>
  <td colspan="2"><span class="lbl">Endereço</span><span class="val">{_esc(dest_end)}</span></td>
  <td><span class="lbl">Bairro / Distrito</span><span class="val">{_esc(dest.get('bairro', ''))}</span></td>
  <td><span class="lbl">CEP</span><span class="val">{_esc(_fmt_cep(dest.get('cep', '')))}</span></td>
</tr>
<tr>
  <td><span class="lbl">Município</span><span class="val">{_esc(dest.get('cidade', ''))}</span></td>
  <td><span class="lbl">UF</span><span class="val">{_esc(dest.get('uf', ''))}</span></td>
  <td><span class="lbl">Fone / Fax</span><span class="val">—</span></td>
  <td><span class="lbl">Inscrição estadual</span><span class="val">{_esc(dest.get('ie') or '—')}</span></td>
</tr>
<tr>
  <td><span class="lbl">Data da emissão</span><span class="val">{_esc(_fmt_data_br(dh_emi))}</span></td>
  <td><span class="lbl">Data da saída/entrada</span><span class="val">—</span></td>
  <td><span class="lbl">Hora da saída/entrada</span><span class="val">{_esc(_fmt_hora_br(dh_emi))}</span></td>
  <td><span class="lbl">Email</span><span class="val">{_esc(email) if email else '—'}</span></td>
</tr>
</table>"""


def _bloco_impostos(totais: dict[str, str], total_prod: Decimal, total_nf: Decimal) -> str:
    def t(key: str, fallback: Decimal | float | str) -> str:
        if key in totais and totais[key]:
            return _fmt_moeda(totais[key])
        if isinstance(fallback, Decimal):
            return _fmt_moeda(fallback)
        return _fmt_moeda(fallback)

    return f"""
<table class="danfe impostos quad">
<tr><td colspan="4" class="sec-title">Cálculo do imposto</td></tr>
<tr>
  <td><span class="lbl">Base de cálc. do ICMS</span><span class="val r">{t('vBC', 0)}</span></td>
  <td><span class="lbl">Valor do ICMS</span><span class="val r">{t('vICMS', 0)}</span></td>
  <td><span class="lbl">Base de cálc. ICMS S.T.</span><span class="val r">{t('vBCST', 0)}</span></td>
  <td><span class="lbl">Valor do ICMS subst.</span><span class="val r">{t('vST', 0)}</span></td>
</tr>
<tr>
  <td><span class="lbl">V. imp. importação</span><span class="val r">0,00</span></td>
  <td><span class="lbl">V. ICMS UF remet.</span><span class="val r">{t('vICMSUFRemet', 0)}</span></td>
  <td><span class="lbl">V. FCP UF dest.</span><span class="val r">{t('vFCPUFDest', 0)}</span></td>
  <td><span class="lbl">Valor do PIS</span><span class="val r">{t('vPIS', 0)}</span></td>
</tr>
<tr>
  <td><span class="lbl">V. total produtos</span><span class="val r">{t('vProd', total_prod)}</span></td>
  <td><span class="lbl">Valor do frete</span><span class="val r">{t('vFrete', 0)}</span></td>
  <td><span class="lbl">Valor do seguro</span><span class="val r">{t('vSeg', 0)}</span></td>
  <td><span class="lbl">Desconto</span><span class="val r">{t('vDesc', 0)}</span></td>
</tr>
<tr>
  <td><span class="lbl">Outras despesas</span><span class="val r">{t('vOutro', 0)}</span></td>
  <td><span class="lbl">Valor total IPI</span><span class="val r">{t('vIPI', 0)}</span></td>
  <td><span class="lbl">V. ICMS UF dest.</span><span class="val r">{t('vICMSUFDest', 0)}</span></td>
  <td><span class="lbl">V. tot. trib.</span><span class="val r">{t('vTotTrib', 0)}</span></td>
</tr>
<tr>
  <td><span class="lbl">Valor da COFINS</span><span class="val r">{t('vCOFINS', 0)}</span></td>
  <td colspan="3"><span class="lbl">V. total da nota</span><span class="val r destaque">{t('vNF', total_nf)}</span></td>
</tr>
</table>"""


def _bloco_transporte() -> str:
    return """
<table class="danfe quad">
<tr><td colspan="4" class="sec-title">Transportador / Volumes transportados</td></tr>
<tr>
  <td colspan="2"><span class="lbl">Nome / Razão social</span></td>
  <td><span class="lbl">Frete</span><span class="val">9-Sem Transporte</span></td>
  <td><span class="lbl">Código ANTT</span></td>
</tr>
<tr>
  <td colspan="2"><span class="lbl">Endereço</span></td>
  <td><span class="lbl">Município</span></td>
  <td><span class="lbl">UF</span></td>
</tr>
<tr>
  <td><span class="lbl">Placa do veículo</span></td>
  <td><span class="lbl">UF</span></td>
  <td colspan="2"><span class="lbl">CNPJ / CPF</span></td>
</tr>
<tr>
  <td><span class="lbl">Quantidade</span></td>
  <td><span class="lbl">Espécie</span></td>
  <td><span class="lbl">Marca</span></td>
  <td><span class="lbl">Numeração</span></td>
</tr>
</table>"""


def _bloco_dados_adicionais(inf_compl: str, *, impresso_em: str) -> str:
    return f"""
<table class="danfe quad">
<tr><td colspan="2" class="sec-title">Dados adicionais</td></tr>
<tr>
  <td class="inf-compl" style="width:65%">
    <span class="lbl">Informações complementares</span>
    <div class="inf-txt">{_esc(inf_compl) or '—'}</div>
  </td>
  <td class="inf-fisco">
    <span class="lbl">Reservado ao fisco</span>
    <div class="inf-txt">—</div>
  </td>
</tr>
</table>
<div class="rodape-impresso">Impresso em {_esc(impresso_em)}</div>"""


def montar_html_danfe(
    doc: dict,
    *,
    emitente: dict[str, str] | None = None,
    logo_url: str | None = None,
) -> str:
    """HTML DANFE retrato A4 — modelo 55, identidade Agro Mais."""
    em = emitente or carregar_emitente_danfe(doc=doc)
    dest = doc.get("destinatario") if isinstance(doc.get("destinatario"), dict) else {}
    fiscal = doc.get("fiscal") if isinstance(doc.get("fiscal"), dict) else {}
    itens_raw = [ln for ln in (doc.get("itens") or []) if isinstance(ln, dict)]
    tp_amb = int(doc.get("tp_amb") or 2)
    homolog = tp_amb == 2
    autorizada = str(doc.get("status") or "") == "autorizada"
    chave_raw = _digits(doc.get("chave"), 44)
    chave_fmt = _fmt_chave(chave_raw) if chave_raw else "—"
    nnf = doc.get("nNF") or "—"
    serie = fiscal.get("serie") or doc.get("serie") or "—"
    protocolo = str(doc.get("protocolo") or "")
    natureza = str(fiscal.get("natureza") or "VENDA")
    xml_totais = _parse_totais_xml(str(doc.get("xml_autorizado") or ""))
    if xml_totais.get("nProt") and not protocolo:
        protocolo = xml_totais["nProt"]
    dh_emi = xml_totais.get("dhEmi") or ""
    if not dh_emi and isinstance(doc.get("updated_at"), str):
        dh_emi = doc["updated_at"][:10]

    total_prod = total_itens(itens_raw)
    try:
        total_nf = Decimal(str(xml_totais.get("vNF") or total_prod))
    except Exception:
        total_nf = total_prod

    logo_uri = (logo_url or "").strip() or _logo_data_uri()
    razao_canhoto = em.get("razao") or em.get("fantasia") or ""

    paginas: list[list[dict]] = []
    if not itens_raw:
        paginas = [[]]
    else:
        for i in range(0, len(itens_raw), _ITENS_POR_PAGINA):
            paginas.append(itens_raw[i : i + _ITENS_POR_PAGINA])
    n_pag = len(paginas)

    inf_compl = str(doc.get("inf_complementar") or fiscal.get("inf_compl") or "")
    if doc.get("simulado"):
        inf_compl = (inf_compl + " Emissão simulada homologação SisVale.").strip()

    aviso_homolog = (
        "<div class='faixa homolog'>SEM VALOR FISCAL — AMBIENTE DE HOMOLOGAÇÃO — SEM VALOR FISCAL</div>"
        if homolog
        else ""
    )
    aviso_previa = (
        "<div class='faixa previa'>PRÉVIA — DOCUMENTO SEM AUTORIZAÇÃO DE USO NA SEFAZ</div>"
        if not autorizada
        else ""
    )

    pages_html = ""
    for idx, chunk in enumerate(paginas, start=1):
        folha = f"{idx}/{n_pag}"
        rows = "".join(_linha_produto(ln) for ln in chunk) or "<tr><td colspan='15' class='c'>—</td></tr>"
        cab = ""
        if idx == 1:
            cab = (
                _bloco_canhoto(
                    em,
                    nnf,
                    serie,
                    razao_canhoto,
                    dest=dest,
                    dh_emi=dh_emi,
                    total_nf=total_nf,
                )
                + _bloco_cabecalho(
                    em,
                    nnf=nnf,
                    serie=serie,
                    chave_fmt=chave_fmt,
                    chave_raw=chave_raw,
                    natureza=natureza,
                    protocolo=protocolo,
                    logo_uri=logo_uri,
                    folha=folha,
                    dh_emi=dh_emi,
                )
                + _bloco_destinatario(dest, dh_emi)
                + _bloco_impostos(xml_totais, total_prod, total_nf)
            )
        else:
            cab = f"""
<div class="continuacao">DANFE — Continuação · NF-e Nº {_esc(nnf)} · Série {_esc(serie)} · Folha {folha}</div>
<table class="danfe mini-chave"><tr>
<td><span class="lbl">Chave de acesso</span><span class="val chave-mini">{_esc(chave_fmt)}</span></td>
</tr></table>"""

        rodape = ""
        if idx == n_pag:
            try:
                tz = ZoneInfo("America/Sao_Paulo")
                agora = datetime.now(tz).strftime("%d/%m/%Y as %H:%M:%S")
            except Exception:
                agora = datetime.now().strftime("%d/%m/%Y as %H:%M:%S")
            rodape = _bloco_transporte() + _bloco_dados_adicionais(inf_compl, impresso_em=agora)

        pages_html += f"""
<section class="danfe-page" data-folha="{idx}">
{cab}
<table class="danfe prod">
<tr><td colspan="15" class="sec-title">Dados dos produtos / serviços</td></tr>
{_tabela_produtos_head()}
<tbody>{rows}</tbody>
</table>
{rodape}
</section>"""

    barcode_script = ""
    if chave_raw:
        barcode_script = """
<script src="https://cdn.jsdelivr.net/npm/jsbarcode@3.11.6/dist/JsBarcode.all.min.js"></script>
<script>
document.querySelectorAll('svg.barcode[data-chave]').forEach(function(el) {
  try {
    JsBarcode(el, el.getAttribute('data-chave'), {format: 'CODE128', width: 1.05, height: 44, displayValue: false, margin: 0});
  } catch (e) {}
});
</script>"""

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>DANFE NF-e {_esc(nnf)} — Agro Mais</title>
<style>
@page {{ size: A4 portrait; margin: 5mm; }}
* {{ box-sizing: border-box; }}
body {{ font-family: Arial, Helvetica, sans-serif; font-size: 7pt; color: #000; margin: 0; padding: 0; background: #e8e8e8; }}
.danfe-page {{ background: #fff; width: 200mm; min-height: 287mm; margin: 0 auto 8mm; padding: 3mm 4mm; box-shadow: 0 1px 4px rgba(0,0,0,.12); page-break-after: always; }}
.danfe-page:last-child {{ page-break-after: auto; margin-bottom: 0; }}
table.danfe {{ width: 100%; border-collapse: collapse; table-layout: fixed; margin-bottom: 1px; }}
table.danfe td, table.danfe th {{ border: 1px solid #000; padding: 1px 3px; vertical-align: top; word-wrap: break-word; }}
.lbl {{ font-size: 5pt; text-transform: uppercase; color: #000; display: block; line-height: 1.1; margin-bottom: 0; }}
.val {{ font-size: 7pt; font-weight: 700; color: #000; display: block; }}
.val.destaque {{ font-size: 8pt; }}
.sec-title {{ font-size: 5.5pt; font-weight: 700; text-transform: uppercase; background: #fff; padding: 2px 3px !important; }}
.r {{ text-align: right; }}
.c {{ text-align: center; }}
.corte {{ border: none; border-top: 1px dashed #000; margin: 2mm 0; height: 0; }}
.faixa {{ text-align: center; font-weight: 800; font-size: 9pt; padding: 5px 4px; margin-bottom: 3mm; letter-spacing: 0.3px; }}
.faixa.homolog {{ background: #000; color: #fff; }}
.faixa.previa {{ background: #fff7ed; color: #9a3412; border: 2px solid #ea580c; }}
.col-emit {{ width: 50%; }}
.col-danfe {{ width: 16%; border-left: 2px solid #000 !important; border-right: 2px solid #000 !important; }}
.col-chave {{ width: 34%; }}
.logo-emitente {{ max-width: 38mm; max-height: 26mm; display: block; margin: 0 auto 4px; object-fit: contain; }}
.emit-razao {{ font-size: 8pt; font-weight: 700; text-transform: uppercase; line-height: 1.2; margin-bottom: 2px; }}
.emit-linha {{ font-size: 6.5pt; line-height: 1.25; }}
.danfe-tit {{ font-size: 12pt; font-weight: 800; letter-spacing: 0.5px; }}
.danfe-sub {{ font-size: 5pt; font-weight: 700; line-height: 1.15; margin-top: 2px; }}
.danfe-ent-sai {{ font-size: 5pt; margin-top: 5px; line-height: 1.25; text-align: left; padding-left: 4px; }}
.danfe-saida-mark {{ font-size: 12pt; font-weight: 900; border: 1px solid #000; display: inline-block; padding: 0 8px; margin-left: 4px; }}
.danfe-nf {{ font-size: 6.5pt; font-weight: 700; margin-top: 6px; line-height: 1.3; }}
.chave-lbl {{ font-size: 5pt; font-weight: 700; text-align: center; margin-top: 2px; }}
.chave-val {{ font-size: 6.5pt; font-weight: 700; text-align: center; letter-spacing: 0.35px; line-height: 1.3; }}
.chave-hint {{ font-size: 4.5pt; text-align: center; margin-top: 2px; line-height: 1.2; }}
svg.barcode {{ width: 100%; max-height: 42px; display: block; margin-bottom: 2px; }}
table.prod thead th {{ font-size: 4.5pt; text-align: center; background: #fff; font-weight: 700; padding: 2px 1px; }}
table.prod td {{ font-size: 6pt; }}
table.prod td.desc {{ font-size: 6.5pt; line-height: 1.15; }}
table.prod td.cod {{ font-size: 5.5pt; }}
.continuacao {{ font-size: 7pt; font-weight: 700; text-align: center; margin-bottom: 2mm; padding: 2px; border: 1px solid #000; }}
.chave-mini {{ font-size: 6.5pt; letter-spacing: 0.3px; }}
.inf-compl, .inf-fisco {{ min-height: 16mm; vertical-align: top; }}
.inf-txt {{ font-size: 6pt; white-space: pre-wrap; line-height: 1.25; margin-top: 2px; }}
.canhoto-txt {{ font-size: 5.5pt; line-height: 1.25; text-transform: uppercase; font-weight: 600; }}
.canhoto-nf {{ font-size: 7pt; font-weight: 700; vertical-align: middle; }}
.canhoto-nfe-tit {{ font-size: 8pt; font-weight: 800; margin-bottom: 4px; }}
.rodape-impresso {{ font-size: 5.5pt; text-align: right; margin-top: 2mm; color: #333; }}
.no-print {{ font-size: 8pt; text-align: center; padding: 8px; color: #444; }}
@media print {{
  body {{ background: #fff; }}
  .danfe-page {{ box-shadow: none; margin: 0; width: auto; min-height: 0; padding: 0; }}
  .no-print {{ display: none !important; }}
  table.prod thead {{ display: table-header-group; }}
}}
</style>
</head>
<body>
{aviso_homolog}
{aviso_previa}
<p class="no-print">Agro Mais · DANFE NF-e · <strong>Ctrl+P</strong> para imprimir ou salvar em PDF (A4).</p>
{pages_html}
{barcode_script}
</body>
</html>"""


def montar_html_danfe_previa(doc: dict, *, emitente_nome: str = "", emitente_cnpj: str = "") -> str:
    em = carregar_emitente_danfe(_digits(emitente_cnpj, 14) or None, doc=doc)
    if emitente_nome and not em.get("razao"):
        em["razao"] = emitente_nome
    if emitente_cnpj and not em.get("cnpj"):
        em["cnpj"] = _digits(emitente_cnpj, 14)
    return montar_html_danfe(doc, emitente=em)


def gerar_html_danfe_exemplo() -> str:
    """Documento de demonstração para conferência visual (não é nota real)."""
    doc = {
        "status": "autorizada",
        "tp_amb": 1,
        "chave": "35261048900774000110550020000000011234567890",
        "nNF": 1,
        "serie": 2,
        "protocolo": "135260000000000",
        "fiscal": {"natureza": "VENDA", "serie": 2},
        "destinatario": {
            "nome": "CLIENTE EXEMPLO LTDA",
            "documento": "11222333000181",
            "ie": "123456789012",
            "uf": "SP",
            "cidade": "Jacupiranga",
            "bairro": "Centro",
            "logradouro": "Rua das Flores",
            "numero": "100",
            "cep": "11940000",
        },
        "itens": [
            {
                "codigo": "1001",
                "descricao": "Ração equina premium 20kg — linha Agro Mais",
                "qtd": 2,
                "v_unit": 89.9,
                "v_total": 179.8,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
                "un": "UN",
            },
            {
                "codigo": "2002",
                "descricao": "Suplemento mineral bovino 25kg",
                "qtd": 1,
                "v_unit": 120,
                "v_total": 120,
                "ncm": "23099020",
                "cfop": "5102",
                "csosn": "102",
                "un": "UN",
            },
        ],
        "inf_complementar": "Documento de exemplo para layout DANFE Agro Mais. Valores ilustrativos.",
    }
    return montar_html_danfe(doc, emitente=carregar_emitente_danfe(doc=doc))
