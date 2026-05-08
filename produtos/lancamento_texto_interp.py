"""Interpretação livre de texto para pré-preencher lote manual de lançamentos (pt-BR).

1) Heurísticas locais (sempre, sem rede).
2) Opcional (*com chave*): modelo extrai tipo/datas/plano textual; falhas caem só na parte local.

A escolha final do **nome de plano** entre cadastros reais faz-se em ``lancamento_plano_resolver``:
lista de planos ancorada ao Mongo + **modelo só escolhe um índice** (sem inventar conta). Sem chave, usa só roteiro de pontuação."""
from __future__ import annotations

import copy
import logging
import re
import unicodedata
from datetime import date
from typing import Any

from .lancamento_llm_client import gerar_json_llm, resolver_credencial_llm

logger = logging.getLogger(__name__)

# Palavras que não viram dica de plano/fornecedor (após remover valor e datas).
_STOP = frozenset(
    """
    lance lances lançe lança lançar lancamento lancamentos lote conta contas despesa desp
    pagar pg crédito credito débito debito receber já ja manual ap
    de da do das dos e em no na nos nas pelo pela pelos pelas por para pra pro
    com sem um uma uns umas o os as a ao aos à às que se esse essa esses essas esse
    registrar registro grava gravação
    """.split()
)


def _limpar_dica_plano(s: str) -> str:
    x = _nfc(s).strip()
    while True:
        stripped = re.sub(
            r"^(de|da|do|das|dos|e|em|no|na|nos|nas|pelo|pela|pelos|pelas|por|para|pra|pro|com|sem)\s+",
            "",
            x,
            flags=re.I,
        )
        if stripped == x:
            break
        x = stripped.strip()
    return x


def _nfc(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    return s.replace("\u00a0", " ").strip()


def _glue_pagar_receber(s: str) -> str:
    """Ex.: «conta a pagarenergia» → «conta a pagar energia»."""
    s = re.sub(r"(?i)pagar([a-záàâãéêíóôõúç])", r"pagar \1", s)
    s = re.sub(r"(?i)receber([a-záàâãéêíóôõúç])", r"receber \1", s)
    return " ".join(s.split())


_RE_DATA_BR = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
_RE_DATA_ISO = re.compile(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b")
_RE_VAL_BR = re.compile(r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b")
_RE_VAL_SIMP = re.compile(r"\b\d+,\d{2}\b")
_RE_VAL_DOT_DEC = re.compile(r"\b\d+\.\d{2}\b")


def _parse_data_br(m: re.Match) -> date | None:
    d, mes, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if y < 100:
        y += 2000 if y < 70 else 1900
    try:
        return date(y, mes, d)
    except ValueError:
        return None


def _parse_data_iso_from_str(s: str | None) -> date | None:
    if not s or not isinstance(s, str):
        return None
    s = s.strip()[:10]
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", s)
    if not m:
        return None
    y, mes, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return date(y, mes, d)
    except ValueError:
        return None


def _coletar_datas(s: str) -> list[tuple[int, int, date]]:
    out: list[tuple[int, int, date]] = []
    for m in _RE_DATA_BR.finditer(s):
        dt = _parse_data_br(m)
        if dt:
            out.append((m.start(), m.end(), dt))
    for m in _RE_DATA_ISO.finditer(s):
        dti = _parse_data_iso_from_str(m.group(0))
        if dti:
            out.append((m.start(), m.end(), dti))
    out.sort(key=lambda x: x[0])
    return out


def _extrair_valores(s: str) -> list[tuple[int, int, str]]:
    faixas: list[tuple[int, int, str]] = []
    seen: set[tuple[int, int]] = set()

    def push(st: int, en: int, display: str) -> None:
        if (st, en) in seen:
            return
        seen.add((st, en))
        faixas.append((st, en, display))

    for rx in (_RE_VAL_BR, _RE_VAL_SIMP):
        for m in rx.finditer(s):
            push(m.start(), m.end(), m.group(0))

    if not faixas:
        for m in _RE_VAL_DOT_DEC.finditer(s):
            tok = m.group(0)
            push(m.start(), m.end(), tok.replace(".", ","))

    faixas.sort(key=lambda x: x[0])
    return faixas


def _span_remove(s: str, spans: list[tuple[int, int]]) -> str:
    if not spans:
        return s
    spans = sorted(spans)
    chunks: list[str] = []
    pos = 0
    for a, b in spans:
        if a > pos:
            chunks.append(s[pos:a])
        pos = max(pos, b)
    chunks.append(s[pos:])
    return " ".join(" ".join(chunks).split())


def parse_data_qualquer_na_string(s: str) -> date | None:
    """Primeira data dd/mm/aaaa ou yyyy-mm-dd encontrada em ``s``."""
    dts = _coletar_datas(_nfc(s))
    return dts[0][2] if dts else None


def extrair_parcial_local(texto: str) -> dict[str, Any]:
    """Extrai tipo, texto livre útil para plano/descrição, datas e valor quando existirem (sem falhar cedo)."""
    raw = _nfc(texto)
    if not raw:
        return {"tipo": "pagar", "quitado_hint": False, "data_competencia": None, "data_vencimento": None, "valor": "", "plano_hint": "", "descricao": ""}
    s = _glue_pagar_receber(raw)
    low = s.lower()
    if re.search(r"\b(conta\s+a\s+)?receber\b", low) or re.search(
        r"\b(créditos?|creditos?|entrada\s+financeira)\b",
        low,
    ):
        tipo = "receber"
    else:
        tipo = "pagar"
    quitado_hint = bool(
        re.search(
            r"\b(quitad[oa]s?|quitei|quite|liquidad[oa]s?|baixad[oa]s?)"
            r"|\bjá\s+pago\b|\bja\s+pago\b|\bpago\s*$",
            low,
        )
    )
    datas = _coletar_datas(s)
    valores = _extrair_valores(s)
    dc_dt: date | None = None
    dv_dt: date | None = None
    if datas:
        if len(datas) >= 2:
            dc_dt, dv_dt = datas[0][2], datas[1][2]
        else:
            dc_dt = dv_dt = datas[0][2]
    valor_display = valores[-1][2] if valores else ""

    spans_val = [(vs, ve) for vs, ve, _ in valores]
    spans_dt = [(a, b) for a, b, _ in datas]
    resto = _span_remove(s, spans_val + spans_dt)
    tokens = [_nfc(tok) for tok in re.split(r"[\s,;:]+", resto) if len(_nfc(tok)) > 1]
    hints: list[str] = []
    for tok in tokens:
        tl = tok.lower()
        if tl in _STOP:
            continue
        if tl.isdigit():
            continue
        hints.append(tok)
    plano_hint = _limpar_dica_plano(" ".join(hints).strip())
    return {
        "tipo": tipo,
        "quitado_hint": quitado_hint,
        "data_competencia": dc_dt.isoformat() if dc_dt else None,
        "data_vencimento": dv_dt.isoformat() if dv_dt else None,
        "valor": valor_display,
        "plano_hint": plano_hint,
        "descricao": "",
    }


def interpretar_so_heuristica(texto: str) -> dict:
    """Só regex/local; resultado inclui ``fonte_interp: heuristica``."""
    raw = _nfc(texto)
    if not raw:
        return {"ok": False, "erro": "Digite uma frase com valor e data (ex.: «energia 479,03 22/04/2026»)."}

    par = extrair_parcial_local(texto)
    if not par.get("valor"):
        return {"ok": False, "erro": "Não achei valor monetário no texto (ex.: 479,03 ou 1.234,56)."}
    if not par.get("data_competencia"):
        return {"ok": False, "erro": "Não achei data (use DD/MM/AAAA ou AAAA-MM-DD)."}

    avisos: list[str] = []
    plano_hint = str(par.get("plano_hint") or "")
    if not plano_hint:
        avisos.append("Complete o plano de contas na primeira linha (escolha na lista).")

    return {
        "ok": True,
        "tipo": par["tipo"],
        "data_competencia": par["data_competencia"],
        "data_vencimento": par["data_vencimento"],
        "quitado_hint": bool(par.get("quitado_hint")),
        "linhas": [
            {
                "plano_hint": plano_hint,
                "valor": par["valor"],
                "descricao": None,
            }
        ],
        "avisos": avisos,
        "fonte_interp": "heuristica",
    }


def _normalizar_valor_display(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return ""
    if isinstance(v, (int, float)):
        n = float(v)
        s = f"{n:.2f}"
        i, frac = s.split(".")
        return f"{i},{frac}"
    s = _nfc(str(v)).replace("R$", "").replace("r$", "")
    if not s:
        return ""
    s = s.replace(" ", "")
    if "," in s and re.fullmatch(r"\d{1,3}(?:\.\d{3})*,\d{2}", s):
        return s
    if "," in s and re.fullmatch(r"\d+,\d{2}", s):
        return s
    if "." in s and "," not in s and re.fullmatch(r"\d+\.\d{2}", s):
        return s.replace(".", ",")
    return s


def _resultado_flat_llm(payload: dict[str, Any], *, fonte_interp: str) -> dict | None:
    tipo = str(payload.get("tipo") or "pagar").strip().lower()
    if tipo not in ("pagar", "receber"):
        tipo = "pagar"
    dc = _parse_data_iso_from_str(str(payload.get("data_competencia") or "").strip())
    dv = _parse_data_iso_from_str(str(payload.get("data_vencimento") or "").strip())
    if dc is None and dv is not None:
        dc = dv
    if dv is None and dc is not None:
        dv = dc
    if dc is None or dv is None:
        return None
    quitado_hint = payload.get("quitado_hint") is True or str(
        payload.get("quitado_hint") or ""
    ).strip().lower() in ("true", "1", "sim", "yes")

    vd = _normalizar_valor_display(payload.get("valor"))
    if not vd:
        return None

    plano_hint = _limpar_dica_plano(_nfc(str(payload.get("plano_hint") or "")))
    ds = _nfc(str(payload.get("descricao") or "")).strip()

    avisos: list[str] = []
    if not plano_hint:
        avisos.append("Complete o plano de contas na primeira linha (escolha na lista).")

    return {
        "ok": True,
        "tipo": tipo,
        "data_competencia": dc.isoformat(),
        "data_vencimento": dv.isoformat(),
        "quitado_hint": quitado_hint,
        "linhas": [
            {
                "plano_hint": plano_hint,
                "valor": vd,
                "descricao": ds or None,
            }
        ],
        "avisos": avisos,
        "fonte_interp": fonte_interp,
    }


def _prompt_extracao(frase: str) -> str:
    return (
        "Você extrai dados de lançamentos financeiros em português do Brasil para um sistema ERP.\n"
        "Responda somente um JSON com estas chaves (use null ou \"\" quando não inferir):\n"
        '- tipo: "pagar" ou "receber"\n'
        '- data_competencia: "AAAA-MM-DD" ou null\n'
        '- data_vencimento: "AAAA-MM-DD" ou null (se houver só uma data no texto, use a mesma em ambos)\n'
        '- quitado_hint: true ou false (título já pago ou baixado)\n'
        '- valor: string no formato brasileiro com vírgula decimal, exemplo "479,03"\n'
        '- plano_hint: termos próximos de plano de contas no Brasil (ex.: conta de luz → energia elétrica), '
        'sem artigo nem preposição sobrando;\n'
        "- descricao: histórico curto ou string vazia\n"
        "Não invente valor nem datas que não estejam no texto ou claramente implícitas pela frase "
        '("hoje"/"amanhã" não existem; retorne null nesses campos).\n'
        "Frase do usuário:\n"
        "---\n"
        f"{frase}\n"
        "---"
    )


def _extrair_via_llm(frase: str) -> tuple[dict[str, Any] | None, str]:
    raw, provedor = gerar_json_llm(_prompt_extracao(frase))
    return raw, provedor


def _misturar_plano_llm(heur_ok: dict, llm_flat: dict, provedor: str) -> dict:
    """Datas/valor da heurística; plano/descrição vindos do JSON cru do modelo."""
    out = copy.deepcopy(heur_ok)
    linhas = list(out.get("linhas") or [])
    if not linhas:
        return out
    h0 = dict(linhas[0])
    ph = _limpar_dica_plano(_nfc(str(llm_flat.get("plano_hint") or "")).strip())
    dh = _nfc(str(llm_flat.get("descricao") or "")).strip()
    antes_vazio_plano = not (h0.get("plano_hint") or "").strip()
    antes_vazio_desc = not (h0.get("descricao") or "").strip()

    enriched = False
    if antes_vazio_plano and ph:
        h0["plano_hint"] = ph
        enriched = True
    if antes_vazio_desc and dh:
        h0["descricao"] = dh

    linhas[0] = h0
    out["linhas"] = linhas

    avisos = list(out.get("avisos") or [])
    if antes_vazio_plano and enriched and ph:
        avisos = [a for a in avisos if "Complete o plano de contas" not in str(a)]

    if antes_vazio_plano:
        out["fonte_interp"] = "hibrido" if enriched else "heuristica"
        if not enriched and provedor:
            if not any("IA não sugeriu plano" in str(a) for a in avisos):
                avisos.append("A IA não sugeriu plano de contas; escolha na lista.")
    else:
        out["fonte_interp"] = "heuristica"

    out["avisos"] = avisos
    return out


def refinar_lancamento_pos_extracao(payload: dict[str, Any], texto: str, *, permitir_llm: bool) -> dict[str, Any]:
    """Se o grosso já veio OK mas falta dica de plano, opcionalmente chama LLM só para texto."""
    if not permitir_llm or not payload.get("ok"):
        return payload
    try:
        if resolver_credencial_llm() is None:
            return payload
    except Exception:
        return payload
    lin0 = ((payload.get("linhas") or [{}])[0] if isinstance(payload.get("linhas"), list) else {}) or {}
    if (lin0.get("plano_hint") or "").strip():
        return payload
    llm_flat, provedor = _extrair_via_llm(texto.strip())
    if llm_flat:
        return _misturar_plano_llm(payload, llm_flat, provedor or "")
    return payload


def interpretar_texto_lancamento_manual(
    texto: str,
    *,
    permitir_llm: bool = True,
    dados_parciais: dict[str, Any] | None = None,
    respostas_dialogo: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from . import lancamento_interpret_dialogo as lid

    return lid.rodada_interpretacao_inteligente(
        texto,
        permitir_llm=permitir_llm,
        dados_parciais=dados_parciais,
        respostas_dialogo=respostas_dialogo,
    )


def interpretar_texto_lancamento_manual_legacy_llm_fallback(
    texto: str,
    *,
    permitir_llm: bool = True,
) -> dict[str, Any]:
    """Fluxo anterior (heurística + LLM só p/ lacunas): usado apenas se ``rodada_*`` falhar."""
    heur = interpretar_so_heuristica(texto)
    if not permitir_llm:
        return heur

    try:
        if resolver_credencial_llm() is None:
            return heur
    except Exception:
        logger.debug("credencial LLM indisponível", exc_info=True)
        return heur

    precisa_llm = False
    if not heur.get("ok"):
        precisa_llm = True
    elif heur.get("ok"):
        lin0 = (heur.get("linhas") or [{}])[0] if isinstance(heur.get("linhas"), list) else {}
        if not ((lin0 or {}).get("plano_hint") or "").strip():
            precisa_llm = True

    if not precisa_llm:
        return heur

    llm_flat, provedor = _extrair_via_llm(texto.strip())
    if not llm_flat:
        extra = (
            "Refino por IA falhou (rede, cota ou resposta inválida). "
            "Verifique a chave (Gemini/Groq/OpenAI) ou tente de novo."
        )
        out = copy.deepcopy(heur)
        av = list(out.get("avisos") or [])
        av.append(extra)
        out["avisos"] = av
        out["fonte_interp"] = str(heur.get("fonte_interp") or "heuristica")
        return out

    if heur.get("ok"):
        return _misturar_plano_llm(heur, llm_flat, provedor)

    llm_ready = _resultado_flat_llm(llm_flat, fonte_interp=provedor or "llm")
    if llm_ready and llm_ready.get("ok"):
        return llm_ready

    out = copy.deepcopy(heur)
    av = list(out.get("avisos") or [])
    av.append("A IA não retornou valor e data válidos; use DD/MM/AAAA e valor com centavos na frase.")
    out["avisos"] = av
    out["fonte_interp"] = "heuristica"
    return out

