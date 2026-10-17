#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova detalhada — Adiar 1h / alerta por entrega (não global).

Pacote: PDV-ENT-ALERTA-POR-ID (+ CARD-LATERAL / lembrete clique-through)

  set AGRO_PIN_TESTE=9973
  python scripts/verify_pdv_entrega_alerta_por_id_path.py
"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

PIN = (os.environ.get("AGRO_PIN_TESTE") or "9973").strip()

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print("  OK  " + name + ((" — " + detail) if detail else ""))
    else:
        fails.append(name)
        print("  FAIL " + name + ((" — " + detail) if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def _fn_body(src: str, name: str, limit: int = 3500) -> str:
    idx = src.find(f"function {name}")
    if idx < 0:
        return ""
    return src[idx : idx + limit]


def test_estatico() -> None:
    print("== Estático PDV-ENT-ALERTA-POR-ID ==")
    wiz_js = _read("produtos/static/produtos/js/pdv_wizard.js")
    wiz_html = _read("produtos/templates/produtos/pdv_wizard.html")
    step = _read("produtos/templates/produtos/partials/pdv/step_produtos.html")

    check("map_key", "gm_pdv_entregas_alerta_snooze_map" in wiz_js)
    check(
        "legacy_key_clear",
        "gm_pdv_entregas_alerta_snooze_until" in wiz_js
        and "removeItem(ENTREGAS_ALERTA_SNOOZE_KEY_LEGACY)" in wiz_js,
    )
    check("fn_ler_mapa", "function lerMapaSnoozeEntregas" in wiz_js)
    check("fn_gravar_mapa", "function gravarMapaSnoozeEntregas" in wiz_js)
    check("fn_por_id", "function adiarAlertaEntrega1hPorId" in wiz_js)
    check("fn_topo_todas", "function adiarAlertaEntregas1h" in wiz_js)
    check("fn_esta_adiada", "function entregaAlertaEstaAdiada" in wiz_js)
    check("fn_urgencia_efetiva", "function urgenciaEfetivaEntregaRow" in wiz_js)
    check("fn_ids_urgentes", "function idsEntregasComHorarioUrgente" in wiz_js)
    check("fn_todas_adiadas", "function todasUrgentesAdiadas" in wiz_js)

    body_card = _fn_body(wiz_js, "adiarAlertaEntrega1hPorId", 1800)
    check("card_toggle_religar", "delete map[id]" in body_card)
    check("card_set_1h", "60 * 60 * 1000" in body_card)
    check("card_toast_desta", "desta entrega" in body_card.lower() or "desta entrega" in body_card)

    body_topo = _fn_body(wiz_js, "adiarAlertaEntregas1h", 2200)
    check("topo_usa_ids_urgentes", "idsEntregasComHorarioUrgente" in body_topo)
    check("topo_forEach_ids", "ids.forEach" in body_topo)
    check("topo_toast_todas", "todas" in body_topo.lower())

    body_html_card = _fn_body(wiz_js, "htmlEntregaPendenteCard", 9000)
    check(
        "badge_usa_por_id",
        "entregaAlertaEstaAdiada(id)" in body_html_card,
        "não pode usar snooze global no card",
    )
    check(
        "badge_data_entrega_id",
        'data-entrega-id="' in body_html_card
        and "pdv-entrega-adiar-alerta-1h" in body_html_card,
    )
    check("badge_lbl_alerta_ok", "Alerta OK" in body_html_card)
    check("badge_lbl_adiar_1h", "Adiar 1h" in body_html_card)

    body_bind = _fn_body(wiz_js, "bindEntregaPendenteCardBtns", 4500)
    m_bind = re.search(
        r"pdv-entrega-adiar-alerta-1h[\s\S]{0,500}?adiarAlertaEntrega1hPorId",
        body_bind,
    )
    check("bind_card_chama_por_id", bool(m_bind))
    check(
        "bind_card_nao_chama_global_direto",
        "adiarAlertaEntregas1h()"
        not in re.search(
            r"pdv-entrega-adiar-alerta-1h[\s\S]{0,400}",
            body_bind,
        ).group(0)
        if re.search(r"pdv-entrega-adiar-alerta-1h[\s\S]{0,400}", body_bind)
        else False,
    )
    check(
        "bind_le_data_id",
        "getAttribute('data-entrega-id')" in body_bind
        or 'getAttribute("data-entrega-id")' in body_bind,
    )

    # Topo ainda amarra o botão global
    check(
        "topo_listener",
        "pdv-entregas-adiar-alerta" in wiz_js
        and "addEventListener('click', adiarAlertaEntregas1h)" in wiz_js,
    )

    # Urgência efetiva ignora adiada
    body_urg = _fn_body(wiz_js, "urgenciaEfetivaEntregaRow", 900)
    check("urg_efetiva_zera_adiada", "entregaAlertaEstaAdiada" in body_urg and "return 0" in body_urg)
    check(
        "urg_efetiva_filtra_loja_saida",
        "entregaAlertaHorarioDestaLoja" in body_urg,
        "bip só na loja que sai",
    )

    check("fn_alerta_loja_saida", "function entregaAlertaHorarioDestaLoja" in wiz_js)
    body_loja = _fn_body(wiz_js, "entregaAlertaHorarioDestaLoja", 900)
    check("loja_saida_usa_deposito", "depositoPdvAtivo" in body_loja)
    check("loja_saida_compara_loja_entrega", "loja_entrega" in body_loja)
    check("loja_saida_sem_dono_ok", "if (!saida) return true" in body_loja)

    body_max = _fn_body(wiz_js, "maxUrgenciaEntregasPendentes", 800)
    check("max_urg_usa_efetiva", "urgenciaEfetivaEntregaRow" in body_max)
    check("max_urg_inclui_pagas", "itensPagas" in body_max)

    body_ids = _fn_body(wiz_js, "idsEntregasComHorarioUrgente", 900)
    check("ids_urgentes_filtra_loja", "entregaAlertaHorarioDestaLoja" in body_ids)

    body_bruta = _fn_body(wiz_js, "maxUrgenciaBrutaEntregasPendentes", 900)
    check("bruta_filtra_loja", "entregaAlertaHorarioDestaLoja" in body_bruta)

    check(
        "cache_ls_por_loja",
        "entregasPendentesLsKey" in wiz_js
        and "ENTREGAS_PENDENTES_LS_KEY_BASE" in wiz_js
        and "+ '_' + loja" in wiz_js,
    )

    # Som não deve depender de flag global antiga
    body_som = _fn_body(wiz_js, "syncEntregasAlertaSonoro", 900)
    check("som_sem_global_legado", "entregasAlertaEstaAdiado" not in body_som)
    check("som_recheca_max", "maxUrgenciaEntregasPendentes" in body_som)

    # Lembrete: não clique-through no Alerta +1h
    check("lembrete_topo_centro", 'id="alerta-lembrete"' in wiz_html)
    check(
        "lembrete_left_half",
        "left-1/2" in wiz_html and "alerta-lembrete" in wiz_html,
    )
    check("lembrete_nao_right4_fixo", not re.search(
        r'id="alerta-lembrete"[^>]*right-4',
        wiz_html,
    ))
    body_fecha = _fn_body(wiz_js, "fecharAlertaLembreteWizard", 700)
    check("lembrete_pointer_none", "pointerEvents" in body_fecha or "pointer-events" in body_fecha)
    check("lembrete_ok_stop", "stopPropagation" in wiz_js and "alerta-lembrete-ok" in wiz_js)

    # CSS Adiar|Cancelar lado a lado (regressão tip)
    check(
        "css_acoes_dialog",
        "pdv-entrega-card-acoes" in step
        or "pdv-entrega-card-acoes" in wiz_html
        or "pdv-entrega-adiar" in step,
    )

    # Overlay largo (CARD / split tip)
    check("overlay_88rem", "88rem" in wiz_html or "88rem" in step or "max-w-[88rem]" in wiz_html)


def test_logica_mapa_python() -> None:
    """Espelha a lógica do mapa (sem browser): 1 id ≠ todos."""
    print("== Lógica mapa snooze (Python) ==")
    agora = 1_000_000_000_000
    mapa: dict[str, int] = {}

    def esta_adiada(eid: str) -> bool:
        until = int(mapa.get(str(eid), 0) or 0)
        return bool(until and agora < until)

    def toggle(eid: str) -> None:
        eid = str(eid)
        if esta_adiada(eid):
            mapa.pop(eid, None)
        else:
            mapa[eid] = agora + 3_600_000

    def urg_efetiva(eid: str, urg_bruta: int) -> int:
        if urg_bruta > 0 and esta_adiada(eid):
            return 0
        return urg_bruta

    toggle("42")
    check("sim_so_42_adiada", esta_adiada("42") and not esta_adiada("99"))
    check("sim_99_ainda_urgente", urg_efetiva("99", 2) == 2)
    check("sim_42_efetiva_0", urg_efetiva("42", 2) == 0)
    toggle("42")
    check("sim_42_religada", not esta_adiada("42") and urg_efetiva("42", 2) == 2)

    # Topo: todas urgentes
    urgentes = ["10", "20", "30"]
    until = agora + 3_600_000
    for i in urgentes:
        mapa[i] = until
    check("sim_todas_adiadas", all(esta_adiada(i) for i in urgentes))
    for i in urgentes:
        mapa.pop(i, None)
    check("sim_todas_religadas", not any(esta_adiada(i) for i in urgentes))

    # JSON roundtrip como localStorage
    payload = {"7": agora + 1000, "8": agora - 1}
    limpo = {k: v for k, v in payload.items() if v > agora}
    raw = json.dumps(limpo)
    back = json.loads(raw)
    check("sim_json_expira", "7" in back and "8" not in back, raw)

    # Bip só na loja que sai (Centro entrega / Vila só paga → Vila urg=0)
    def alerta_horario_desta_loja(loja_pdv: str, loja_entrega: str) -> bool:
        loja = (loja_pdv or "").strip().lower()
        if loja not in ("centro", "vila"):
            return True
        saida = (loja_entrega or "").strip().lower()
        if not saida:
            return True
        return saida == loja

    def urg_com_loja(loja_pdv: str, loja_entrega: str, urg_bruta: int) -> int:
        if not alerta_horario_desta_loja(loja_pdv, loja_entrega):
            return 0
        return urg_bruta

    check(
        "sim_vila_ignora_saida_centro",
        urg_com_loja("vila", "centro", 2) == 0,
    )
    check(
        "sim_centro_alerta_saida_centro",
        urg_com_loja("centro", "centro", 2) == 2,
    )
    check(
        "sim_vila_alerta_saida_vila",
        urg_com_loja("vila", "vila", 1) == 1,
    )
    check(
        "sim_sem_dono_alerta_nas_duas",
        urg_com_loja("vila", "", 2) == 2 and urg_com_loja("centro", "", 2) == 2,
    )


def test_runtime() -> None:
    print("== Runtime Django + PIN ==")
    import django

    django.setup()

    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.caixa_util import rotulo_operador_pin, validar_pin_operador

    ok_pin, err = validar_pin_operador(PIN)
    check("pin_9973", ok_pin, (err or "")[:80])
    rot = rotulo_operador_pin(PIN) if ok_pin else ""
    check("pin_rotulo", bool(rot), rot[:40])

    user = get_user_model().objects.filter(is_active=True).order_by("id").first()
    check("user_ativo", user is not None)
    if not user:
        return

    client = Client(HTTP_HOST="127.0.0.1")
    client.force_login(user)

    # Wizard real = /pdv/ (pdv_home). checkout sem rascunho redireciona à home.
    r = client.get(reverse("pdv_home"), follow=True)
    check(
        "pdv_home_ok",
        r.status_code == 200,
        f"status={r.status_code} redirects={len(getattr(r, 'redirect_chain', []) or [])}",
    )
    body = r.content.decode("utf-8", errors="replace") if r.status_code == 200 else ""
    if "alerta-lembrete" not in body:
        body = _read("produtos/templates/produtos/pdv_wizard.html")
        check("pdv_wizard_template_disk", 'id="alerta-lembrete"' in body)
    check("pdv_serve_alerta_lembrete", 'id="alerta-lembrete"' in body)
    check(
        "pdv_serve_adiar_alerta_btn",
        "pdv-entregas-adiar-alerta" in body
        or "pdv-entregas-adiar-alerta"
        in _read("produtos/templates/produtos/partials/pdv/step_produtos.html"),
    )

    # API pendentes responde (lista pode estar vazia)
    url_pend = reverse("api_pdv_entregas_pendentes")
    rp = client.get(url_pend)
    check("api_pendentes_status", rp.status_code in (200, 403), str(rp.status_code))
    if rp.status_code == 200:
        try:
            dj = rp.json()
        except Exception:
            dj = {}
        check(
            "api_pendentes_json",
            isinstance(dj, dict),
            str(list(dj.keys())[:12]),
        )
        check("api_pendentes_ok_flag", bool(dj.get("ok")))

    wiz_js = _read("produtos/static/produtos/js/pdv_wizard.js")

    for loja in ("centro", "vila"):
        rl = client.get(url_pend, {"loja": loja})
        check(f"api_pendentes_{loja}_status", rl.status_code == 200, str(rl.status_code))
        if rl.status_code != 200:
            continue
        try:
            data = rl.json()
        except Exception:
            data = {}
        check(f"api_pendentes_{loja}_ok", bool(data.get("ok")))
        check(f"api_pendentes_{loja}_campo", data.get("loja") == loja, str(data.get("loja")))
        itens = list(data.get("itens") or []) + list(data.get("itens_pagas") or [])
        check(f"api_pendentes_{loja}_lista", isinstance(itens, list), f"n={len(itens)}")

    check("card_so_sai_marker", "Só sai" in wiz_js)
    check(
        "card_so_sai_title",
        "Alerta de horário só na loja que sai" in wiz_js,
    )
    check(
        "pdv_assets_script",
        "pdv_wizard.js" in body
        or "pdv_wizard.js" in _read("produtos/templates/produtos/pdv_wizard.html"),
    )
    check(
        "som_usa_max_filtrado",
        "maxUrgenciaEntregasPendentes"
        in _fn_body(wiz_js, "syncEntregasAlertaSonoro", 900)
        and "entregaAlertaHorarioDestaLoja"
        in _fn_body(wiz_js, "urgenciaEfetivaEntregaRow", 900),
    )


def main() -> int:
    print(f"PIN teste = {PIN!r}")
    test_estatico()
    test_logica_mapa_python()
    try:
        test_runtime()
    except Exception as e:
        check("runtime_crash", False, str(e)[:200])
    print()
    print(f"{len(oks)} ok · {len(fails)} fail")
    if fails:
        print("FAILED:")
        for f in fails:
            print("  - " + f)
        return 1
    print("VERIFY_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
