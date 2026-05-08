"""Interpretação guiada por diálogo: quando faltar dados ou há dúvida, devolve perguntas objetivas."""

from __future__ import annotations

import copy
import logging
from typing import Any

from .lancamento_llm_client import gerar_json_llm, resolver_credencial_llm
from .lancamento_texto_interp import (
    _limpar_dica_plano,
    _nfc,
    _normalizar_valor_display,
    _parse_data_iso_from_str,
    extrair_parcial_local,
    parse_data_qualquer_na_string,
    refinar_lancamento_pos_extracao,
)

logger = logging.getLogger(__name__)

_CHAVES_PARCIAIS = frozenset(
    {
        "tipo",
        "quitado_hint",
        "data_competencia",
        "data_vencimento",
        "valor",
        "plano_hint",
        "descricao",
        "eco_perguntas_llm",
        "tipo_ambiguo",
        "conf_ia",
    }
)


def _prompt_inteligencia_completa(frase: str) -> str:
    return (
        "Você interpreta comandos livres sobre lançamentos financeiros brasileiros (contas a pagar ou receber).\n"
        "Extraia apenas o que a frase deixa claro — use null, false ou \"\" quando não inferir.\n"
        'Responda somente um JSON com as chaves abaixo; inclua sempre "confianca" e "ambiguidade_tipo".\n\n'
        '- tipo: \"pagar\" | \"receber\" ou null quando realmente duvidoso\n'
        '- data_competencia: \"AAAA-MM-DD\" ou null\n'
        '- data_vencimento: \"AAAA-MM-DD\" ou null (se só uma data, replique ou null junto à competência)\n'
        "- valor: string pt-BR com vírgula decimal (ex.: \"150,75\") ou \"\"\n"
        "- plano_hint: síntese curta tipo plano DRE/coluna (sem artigos extras) ou \"\"\n"
        "- descricao: histórico curto livre ou \"\"\n"
        "- quitado_hint: booleano\n"
        '- confianca: \"alta\" | \"media\" | \"baixa\" — média ou baixa se houver importante em aberto ou ambiguidade\n'
        '- ambiguidade_tipo: booleano — true se não der para saber se é despesa ou receita sem perguntar\n'
        '- perguntas: lista (máx. 3) de objetos: '
        '{"id": identificador_curto_ascii, '
        '\"texto\": pergunta_muito_curta_PT, '
        '"tipo_input": "texto"|"opcoes", '
        '"opcoes": opcional [{"valor":"...","rotulo":"..."}, ...] }. '
        "Lista vazia [] se já estiver seguro sobre tudo importante.\n"
        "Faça apenas perguntas que realmente faltem pelo texto.\n\n"
        f"Frase do usuário:\n---\n{frase.strip()}\n---\n"
    )


def _merge_local_e_llm(parcial: dict[str, Any], llm: dict[str, Any] | None) -> dict[str, Any]:
    m = dict(parcial)
    if not llm:
        return m
    t = llm.get("tipo")
    if t in ("pagar", "receber"):
        m["tipo"] = t
    if llm.get("ambiguidade_tipo"):
        m["_pedir_tipo"] = True
    elif t in ("pagar", "receber"):
        m.pop("_pedir_tipo", None)
    for key in ("data_competencia", "data_vencimento"):
        raw = llm.get(key)
        if raw:
            sdt = _nfc(str(raw).strip())
            iso = _parse_data_iso_from_str(sdt) or parse_data_qualquer_na_string(sdt)
            if iso:
                m[key] = iso.isoformat()
    vv = _normalizar_valor_display(llm.get("valor"))
    if vv:
        m["valor"] = vv
    ph = _limpar_dica_plano(_nfc(str(llm.get("plano_hint") or "")))
    if ph:
        m["plano_hint"] = ph
    dh = _nfc(str(llm.get("descricao") or "")).strip()
    if dh:
        m["descricao"] = dh
    qh = llm.get("quitado_hint")
    if qh is True or str(qh or "").strip().lower() in ("true", "1", "sim", "yes"):
        m["quitado_hint"] = True
    if llm.get("confianca") in ("alta", "media", "baixa"):
        m["_conf_llm"] = llm["confianca"]
    return m


def _sanitize_dados_parciais(d: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not isinstance(d, dict):
        return out
    for k, v in d.items():
        if str(k).startswith("_"):
            continue
        if k in _CHAVES_PARCIAIS:
            out[k] = copy.deepcopy(v)
    return out


def _normalizar_respostas_dialogo(raw: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if not isinstance(raw, dict):
        return out
    if "tipo" in raw:
        tl = str(raw.get("tipo") or "").strip().lower()
        if tl in ("pagar", "receber"):
            out["tipo"] = tl
    if raw.get("quitado_hint") is True or str(raw.get("quitado_hint") or "").lower() in (
        "1",
        "true",
        "on",
        "sim",
        "yes",
    ):
        out["quitado_hint"] = True
    elif "quitado_hint" in raw:
        out["quitado_hint"] = False
    if "valor" in raw:
        vv = _normalizar_valor_display(raw.get("valor"))
        if vv:
            out["valor"] = vv
    if "descricao" in raw:
        d = _nfc(str(raw.get("descricao") or "")).strip()
        if d:
            out["descricao"] = d
    if "plano_hint" in raw:
        ph = _limpar_dica_plano(_nfc(str(raw.get("plano_hint") or "")))
        out["plano_hint"] = ph
    if "data_competencia" in raw:
        s = _nfc(str(raw.get("data_competencia") or ""))
        iso = None
        if s:
            iso = _parse_data_iso_from_str(s) or parse_data_qualquer_na_string(s)
            if iso:
                out["data_competencia"] = iso.isoformat()
    if "data_vencimento" in raw:
        s = _nfc(str(raw.get("data_vencimento") or ""))
        if s:
            ddt = _parse_data_iso_from_str(s) or parse_data_qualquer_na_string(s)
            if ddt:
                out["data_vencimento"] = ddt.isoformat()
    return out


def _dados_obrigatorios_ok(m: dict[str, Any]) -> bool:
    if not (str(m.get("valor") or "").strip()):
        return False
    if not m.get("data_competencia"):
        return False
    if m.get("tipo") not in ("pagar", "receber"):
        return False
    return True


def _montar_ok_de_merged(
    merged: dict[str, Any], *, fonte: str = "mesclado_dialogo"
) -> dict[str, Any] | None:
    if not _dados_obrigatorios_ok(merged):
        return None
    dc = str(merged.get("data_competencia") or "")
    dv = str(merged.get("data_vencimento") or "") or dc
    ph = str(merged.get("plano_hint") or "")
    avisos: list[str] = []
    if not ph.strip():
        avisos.append("Complete o plano de contas na primeira linha (escolha na lista).")
    return {
        "ok": True,
        "tipo": merged.get("tipo"),
        "data_competencia": dc,
        "data_vencimento": dv,
        "quitado_hint": bool(merged.get("quitado_hint")),
        "linhas": [
            {
                "plano_hint": ph,
                "valor": merged.get("valor"),
                "descricao": (merged.get("descricao") or None) or None,
            }
        ],
        "avisos": avisos,
        "fonte_interp": fonte,
    }


def _perguntas_faltantes_para_merged(
    merged: dict[str, Any], perguntas_llm: list[Any] | None
) -> list[dict[str, Any]]:
    perg: list[dict[str, Any]] = []
    cobre: set[str] = set()

    def add_item(p: dict[str, Any]) -> None:
        pid = str(p.get("id") or "").strip()
        if not pid or pid in cobre:
            return
        txt = str(p.get("texto") or "").strip()
        if not txt:
            return
        cobre.add(pid)
        item = {"id": pid, "texto": txt, "tipo_input": str(p.get("tipo_input") or "texto")}
        if p.get("tipo_input") == "opcoes" and isinstance(p.get("opcoes"), list):
            item["opcoes"] = [
                {
                    "valor": str(o.get("valor", "")).strip(),
                    "rotulo": str(o.get("rotulo", "")).strip() or str(o.get("valor", "")),
                }
                for o in p["opcoes"]
                if isinstance(o, dict)
            ]
        perg.append(item)

    if isinstance(perguntas_llm, list):
        for p in perguntas_llm[:3]:
            if isinstance(p, dict) and p.get("texto"):
                add_item(p)
            elif isinstance(p, str) and p.strip():
                add_item({"id": f"extra_{len(perg)}", "texto": p.strip(), "tipo_input": "texto"})

    if merged.get("_pedir_tipo") and "tipo" not in cobre:
        add_item(
            {
                "id": "tipo",
                "texto": "É conta a pagar ou a receber?",
                "tipo_input": "opcoes",
                "opcoes": [
                    {"valor": "pagar", "rotulo": "Pagar (despesa)"},
                    {"valor": "receber", "rotulo": "Receber (entrada)"},
                ],
            }
        )
    if not (str(merged.get("valor") or "").strip()) and "valor" not in cobre:
        add_item(
            {
                "id": "valor",
                "texto": "Qual o valor em reais?",
                "tipo_input": "texto",
            }
        )
    if not merged.get("data_competencia") and "data_competencia" not in cobre:
        add_item(
            {
                "id": "data_competencia",
                "texto": "Qual é a competência ou vencimento? (DD/MM/AAAA)",
                "tipo_input": "texto",
            }
        )
    if (
        (_dados_obrigatorios_ok(merged))
        and (not str(merged.get("plano_hint") or "").strip())
        and "plano_hint" not in cobre
    ):
        add_item(
            {
                "id": "plano_hint",
                "texto": "Para que gasto ou receita é? (uma palavra ou curta frase)",
                "tipo_input": "texto",
            }
        )

    return perg[:6]


def _dados_para_cliente(merged_interno: dict[str, Any]) -> dict[str, Any]:
    eco = merged_interno.get("_llm_perguntas_echo")
    out: dict[str, Any] = {
        "tipo": merged_interno.get("tipo"),
        "quitado_hint": bool(merged_interno.get("quitado_hint")),
        "data_competencia": merged_interno.get("data_competencia"),
        "data_vencimento": merged_interno.get("data_vencimento"),
        "valor": merged_interno.get("valor") or "",
        "plano_hint": merged_interno.get("plano_hint") or "",
        "descricao": merged_interno.get("descricao") or "",
        "eco_perguntas_llm": eco if isinstance(eco, list) else [],
        "tipo_ambiguo": bool(merged_interno.get("_pedir_tipo")),
    }
    cf = merged_interno.get("_conf_llm")
    if cf in ("alta", "media", "baixa"):
        out["conf_ia"] = cf
    return out


def _restaurar_flags_internas(dp: dict[str, Any], dest: dict[str, Any]) -> None:
    if dp.get("tipo_ambiguo"):
        dest["_pedir_tipo"] = True
    cf = dp.get("conf_ia")
    if cf in ("alta", "media", "baixa"):
        dest["_conf_llm"] = cf


def rodada_interpretacao_inteligente(
    texto: str,
    *,
    permitir_llm: bool = True,
    dados_parciais: dict[str, Any] | None = None,
    respostas_dialogo: dict[str, Any] | None = None,
) -> dict[str, Any]:
    texto = _nfc(texto).strip()

    # --- segunda rodada: usuário respondeu perguntas ---
    if isinstance(respostas_dialogo, dict) and len(respostas_dialogo):
        base = _sanitize_dados_parciais(dados_parciais)
        merged_prev = copy.deepcopy(base)
        _restaurar_flags_internas(base, merged_prev)
        eco = base.get("eco_perguntas_llm") if isinstance(base.get("eco_perguntas_llm"), list) else []
        merged_prev["_llm_perguntas_echo"] = eco

        atualizacoes = _normalizar_respostas_dialogo(respostas_dialogo)
        merged_prev.update(atualizacoes)
        if "tipo" in atualizacoes:
            merged_prev.pop("_pedir_tipo", None)

        chk = _montar_ok_de_merged(
            merged_prev,
            fonte="dialogo_segunda_rodada" if atualizacoes else "dialogo_merge",
        )
        if chk:
            texto_ref = texto or "."
            chk = refinar_lancamento_pos_extracao(chk, texto_ref, permitir_llm=permitir_llm)
            return chk

        prev_llm_pg = eco
        perg = _perguntas_faltantes_para_merged(merged_prev, prev_llm_pg if prev_llm_pg else None)
        if not perg:
            return {
                "ok": False,
                "erro": "Ainda faltam dados. Confira valor (ex.: 10,50) e data (DD/MM/AAAA).",
            }
        cli = _dados_para_cliente({**merged_prev, "_llm_perguntas_echo": eco})
        return {
            "ok": False,
            "precisa_esclarecimento": True,
            "texto_original": texto,
            "dados_parciais": cli,
            "perguntas": perg,
        }

    if not texto:
        return {"ok": False, "erro": "Digite algo no campo Por texto ou responda às perguntas."}

    parcial = extrair_parcial_local(texto)
    llm_raw: dict[str, Any] | None = None

    try:
        if permitir_llm and resolver_credencial_llm():
            llm_raw, _ = gerar_json_llm(_prompt_inteligencia_completa(texto))
    except Exception:
        logger.debug("inteligência completa llm falhou na origem", exc_info=True)

    merged = _merge_local_e_llm(parcial, llm_raw)
    if isinstance(llm_raw, dict):
        pll = llm_raw.get("perguntas")
        if isinstance(pll, list) and pll:
            merged["_llm_perguntas_echo"] = copy.deepcopy(pll[:3])

    tem_perguntas_modelo = (
        isinstance(llm_raw, dict)
        and isinstance(llm_raw.get("perguntas"), list)
        and len(llm_raw["perguntas"]) > 0
    )

    precisa = (
        not _dados_obrigatorios_ok(merged)
        or (not str(merged.get("plano_hint") or "").strip())
        or bool(merged.get("_pedir_tipo"))
        or tem_perguntas_modelo
    )

    if not precisa:
        base_ok = _montar_ok_de_merged(merged, fonte=("heuristica" if llm_raw is None else "ia_mescla"))
        if not base_ok:
            precisa = True
        else:
            base_ok = refinar_lancamento_pos_extracao(base_ok, texto, permitir_llm=permitir_llm)
            return base_ok

    perg_total = _perguntas_faltantes_para_merged(merged, llm_raw.get("perguntas") if llm_raw else None)
    cliente_base = _dados_para_cliente(merged)

    return {
        "ok": False,
        "precisa_esclarecimento": True,
        "motivo_curto": "Faltava informação clara ou há dúvida — responda abaixo para eu preencher com segurança.",
        "texto_original": texto,
        "dados_parciais": cliente_base,
        "perguntas": perg_total,
    }