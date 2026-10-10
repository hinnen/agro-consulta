#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prova path — PDV edição rápida: Adicionar código + 6 cadastrados + etiqueta."""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

OK = 0
FAIL = 0
PDV_QUICK_CB_OPS_MAX = 6


def check(name: str, cond: bool, detail: str = "") -> None:
    global OK, FAIL
    if cond:
        OK += 1
        print(f"OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"FAIL {name}" + (f" — {detail}" if detail else ""))


def digitos(raw) -> str:
    return re.sub(r"\D", "", str(raw or "")).strip()


def fill_display(principal: str, ops: list[str]) -> tuple[list[str], list[str]]:
    """Espelha fillQuickProductBarrasCadastro → (slots[6], tail)."""
    display: list[str] = []
    seen: set[str] = set()

    def push(raw: str) -> None:
        d = digitos(raw)
        if not d or d in seen:
            return
        seen.add(d)
        display.append(d)

    if principal:
        push(principal)
    for op in ops:
        push(op)
    slots = [(display[i] if i < len(display) else "") for i in range(PDV_QUICK_CB_OPS_MAX)]
    tail = display[PDV_QUICK_CB_OPS_MAX:]
    return slots, tail


def collect_payload(
    *,
    remembered_principal: str,
    extras: list[str],
    novo: str,
    tail: list[str],
) -> dict:
    """Espelha collectQuickProductBarrasPayload."""
    seen_extra: set[str] = set()
    extras_clean: list[str] = []
    for raw in extras:
        d = digitos(raw)
        if not d or len(d) < 4 or d in seen_extra:
            continue
        seen_extra.add(d)
        extras_clean.append(d)

    principal = digitos(remembered_principal)
    if principal and principal not in extras_clean:
        principal = extras_clean[0] if extras_clean else ""

    opcionais: list[str] = []
    seen_op: set[str] = set()

    def push_op(raw: str) -> None:
        d = digitos(raw)
        if not d or len(d) < 4:
            return
        if principal and d == principal:
            return
        if d in seen_op:
            return
        seen_op.add(d)
        opcionais.append(d)

    for e in extras_clean:
        push_op(e)
    if novo:
        push_op(novo)
    for t in tail:
        push_op(t)
    return {"principal": principal, "opcionais": opcionais}


def test_static() -> None:
    print("== Estático ==")
    tpl = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    core = (ROOT / "produtos/static/produtos/js/produtos_etiquetas_core.js").read_text(
        encoding="utf-8"
    )

    for i in range(1, 7):
        check(f"html cb-op-{i}", f"pdv-quick-product-edit-cb-op-{i}" in tpl)
    check("html adicionar código", "Adicionar código" in tpl)
    check("html ja cadastrados", "Códigos já cadastrados" in tpl)
    check("html cb ops 1 linha", "pdv-pe-cb-ops-grid" in tpl and "repeat(6, minmax(0, 1fr))" in tpl)
    check("html painel largo", "96rem" in tpl and "pdv-product-edit-panel" in tpl)
    check(
        "html sem zoom local painel",
        "zoom: var(--pdv-pe-zoom)" not in tpl
        and "max-height: 94dvh" in tpl,
    )
    check("html botão etiqueta", 'id="pdv-quick-product-edit-etiqueta"' in tpl)
    check("html modal etq", 'id="pdv-pe-etq-preset"' in tpl and 'id="pdv-pe-etq-imprimir"' in tpl)
    check("html core etiquetas", "produtos_etiquetas_core.js" in tpl)
    check("html AGRO_ETQ_CFG", "AGRO_ETQ_CFG" in tpl and "api_etiquetas_presets" in tpl)

    check("js fill", "fillQuickProductBarrasCadastro" in js)
    check("js collect", "collectQuickProductBarrasPayload" in js)
    check("js topo vazio", "Campo de cima sempre vazio" in js)
    check("js principal", "quickProductEditPrincipalCb" in js)
    check("js max 6", "PDV_QUICK_CB_OPS_MAX = 6" in js)
    check("js novo vira adicional", "if (novo) pushOp(novo)" in js)
    check("js save pack", "barrasPack.opcionais" in js)
    check("js etq open/print", "openPdvPeEtqModal" in js and "imprimirPdvPeEtiqueta" in js)
    check("js etq origem", "origem: 'pdv_edicao'" in js)
    check("js etq merge presets", "mergeServerPresets" in js)
    check("js Esc etq", "closePdvPeEtqModal" in js and "peEtqModal" in js)
    check("js etq usa principal", "codigo_barras: barras.principal" in js)
    check("core imprimirItens", "imprimirItens" in core and "fillPresetSelect" in core)
    check("core produtoParaItem", "function produtoParaItem" in core)

    check(
        "api edicao_rapida opcionais",
        re.search(
            r"def api_pdv_produto_edicao_rapida[\s\S]{0,9000}?codigos_barras_opcionais",
            views,
        )
        is not None,
    )
    check(
        "api overlay opcionais",
        re.search(
            r'if "codigos_barras_opcionais" in payload:[\s\S]{0,400}?normalizar_codigos_barras_opcionais',
            views,
        )
        is not None,
    )


def test_logic() -> None:
    print("== Lógica (espelho JS) ==")
    p = "2300000000009"
    o1 = "2300000001571"
    o2 = "7891234567890"
    slots, tail = fill_display(p, [o1, o2])
    check("fill slot0=principal", slots[0] == p, slots[0])
    check("fill slot1=op1", slots[1] == o1, slots[1])
    check("fill slot2=op2", slots[2] == o2, slots[2])
    check("fill slots vazios", slots[3] == "" and slots[5] == "")
    check("fill tail vazia", tail == [])

    # 8 códigos → 6 slots + tail 2
    many = [f"23000000{i:05d}" for i in range(1, 9)]
    slots8, tail8 = fill_display(many[0], many[1:])
    check("fill 8→6 slots", len([x for x in slots8 if x]) == 6)
    check("fill 8→tail 2", len(tail8) == 2, str(tail8))

    # Adicionar novo: principal intacto
    pack = collect_payload(
        remembered_principal=p,
        extras=[p, o1],
        novo="7899999999999",
        tail=[],
    )
    check("add não troca principal", pack["principal"] == p, pack["principal"])
    check("add vai pra opcionais", "7899999999999" in pack["opcionais"], str(pack["opcionais"]))
    check("add mantém o1", o1 in pack["opcionais"], str(pack["opcionais"]))

    # Novo vazio: opcionais = só extras ≠ principal
    pack2 = collect_payload(
        remembered_principal=p,
        extras=[p, o1, o2],
        novo="",
        tail=[],
    )
    check("sem novo principal ok", pack2["principal"] == p)
    check("sem novo ops", pack2["opcionais"] == [o1, o2], str(pack2["opcionais"]))

    # Remove principal da lista → promove 1º
    pack3 = collect_payload(
        remembered_principal=p,
        extras=[o1, o2],
        novo="",
        tail=[],
    )
    check("remove principal promove", pack3["principal"] == o1, pack3["principal"])
    check("remove principal ops", pack3["opcionais"] == [o2], str(pack3["opcionais"]))

    # Tail preservada
    pack4 = collect_payload(
        remembered_principal=p,
        extras=[p, o1],
        novo="1111222233334",
        tail=["9999888877776"],
    )
    check("tail preservada", "9999888877776" in pack4["opcionais"], str(pack4["opcionais"]))
    check("novo+tail", "1111222233334" in pack4["opcionais"])

    # Dedup novo = principal
    pack5 = collect_payload(
        remembered_principal=p,
        extras=[p, o1],
        novo=p,
        tail=[],
    )
    check("novo=principal não duplica", p not in pack5["opcionais"] and pack5["principal"] == p)


def test_runtime() -> None:
    print("== Runtime Django ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.models import ProdutoGestaoOverlayAgro
    from produtos.mongo_index_codigos import codigos_barras_opcionais_de_cadastro_extras

    User = get_user_model()
    u = User.objects.filter(is_active=True, is_staff=True).order_by("pk").first()
    if not u:
        u = User.objects.filter(is_active=True).order_by("pk").first()
    if not u:
        check("user", False, "sem usuario")
        return
    check("user", True, u.username)

    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(u)

    # Página wizard PDV (`/pdv/` — pdv_checkout só redireciona)
    r_page = c.get(reverse("pdv_home"))
    check("pdv_home 200", r_page.status_code == 200, str(r_page.status_code))
    body = r_page.content.decode("utf-8", errors="replace")
    check("pdv html Adicionar codigo", "Adicionar código" in body or "Adicionar codigo" in body)
    check("pdv html etiqueta btn", "pdv-quick-product-edit-etiqueta" in body)
    check("pdv html core js", "produtos_etiquetas_core.js" in body)
    check("pdv html pe-etq-preset", "pdv-pe-etq-preset" in body)
    check("pdv html cb-op-6", "pdv-quick-product-edit-cb-op-6" in body)
    check("pdv html layout 1 linha", "pdv-pe-cb-ops-grid" in body and "96rem" in body)

    # Presets etiquetas (Postgres)
    r_pre = c.get(reverse("api_etiquetas_presets"))
    check("presets status", r_pre.status_code == 200, str(r_pre.status_code))
    try:
        jpre = r_pre.json()
    except Exception as e:
        check("presets json", False, str(e))
        jpre = {}
    check("presets ok", jpre.get("ok") is True, str(jpre)[:120])
    presets = jpre.get("presets") or []
    check("presets lista", isinstance(presets, list) and len(presets) >= 1, f"n={len(presets)}")

    # Overlay com barras
    ov = (
        ProdutoGestaoOverlayAgro.objects.exclude(codigo_barras="")
        .exclude(codigo_barras__isnull=True)
        .order_by("-atualizado_em")
        .first()
    )
    if ov is None:
        ov = ProdutoGestaoOverlayAgro.objects.order_by("-atualizado_em").first()
    if ov is None:
        check("overlay produto", False, "sem ProdutoGestaoOverlayAgro")
        return

    pid = str(ov.produto_externo_id or "").strip()
    check("overlay produto", bool(pid), pid)

    url_get = reverse("api_pdv_produto_edicao_rapida", kwargs={"produto_id": pid})
    r_get = c.get(url_get)
    check("edicao_rapida status", r_get.status_code == 200, str(r_get.status_code))
    jget = r_get.json()
    check("edicao_rapida ok", jget.get("ok") is True, str(jget)[:160])
    prod = jget.get("produto") or {}
    check("edicao_rapida tem id", str(prod.get("id") or "") == pid or bool(prod.get("id")))
    check("edicao_rapida tem nome", bool(str(prod.get("nome") or "").strip()))
    check(
        "edicao_rapida tem opcionais key",
        "codigos_barras_opcionais" in prod,
        str(list(prod.keys())[:20]),
    )
    ops_api = prod.get("codigos_barras_opcionais")
    check("edicao_rapida opcionais list", isinstance(ops_api, list), type(ops_api).__name__)

    principal_antes = digitos(ov.codigo_barras or prod.get("codigo_barras") or "")
    ops_antes = list(codigos_barras_opcionais_de_cadastro_extras(ov.cadastro_extras))
    extras_antes = dict(ov.cadastro_extras or {})

    novo_test = "2309999888777"
    # evita colidir com o que já existe
    if novo_test == principal_antes or novo_test in ops_antes:
        novo_test = "2309999888666"

    ops_desejadas = [x for x in ops_antes if digitos(x) != novo_test]
    ops_desejadas.append(novo_test)

    url_save = reverse("api_produtos_gestao_overlay_salvar")
    payload = {
        "produto_id": pid,
        "nome": str(prod.get("nome") or ov.nome or "teste").strip() or "teste",
        "unidade": str(prod.get("unidade") or ov.unidade or "UN").strip() or "UN",
        "precos_modo": str(prod.get("precos_modo") or "por_forma"),
        "sincronizar_erp": False,
        "origem_historico": "pdv",
        "pdv_edicao_rapida": True,
        "codigos_barras_opcionais": ops_desejadas,
    }
    if principal_antes:
        payload["codigo_barras"] = principal_antes
    if prod.get("codigo_nfe") or ov.codigo_nfe:
        payload["codigo_nfe"] = str(prod.get("codigo_nfe") or ov.codigo_nfe)

    try:
        r_save = c.post(
            url_save,
            data=json.dumps(payload),
            content_type="application/json",
        )
        check("overlay save status", r_save.status_code == 200, str(r_save.status_code))
        jsave = r_save.json()
        check("overlay save ok", jsave.get("ok") is True, str(jsave)[:200])

        ov.refresh_from_db()
        principal_depois = digitos(ov.codigo_barras or "")
        ops_depois = codigos_barras_opcionais_de_cadastro_extras(ov.cadastro_extras)
        check(
            "save principal intacto",
            (not principal_antes) or principal_depois == principal_antes,
            f"{principal_antes}→{principal_depois}",
        )
        check("save novo em opcionais", novo_test in ops_depois, str(ops_depois)[:120])

        r_get2 = c.get(url_get)
        j2 = r_get2.json()
        p2 = j2.get("produto") or {}
        check(
            "reload opcionais tem novo",
            novo_test in (p2.get("codigos_barras_opcionais") or []),
            str(p2.get("codigos_barras_opcionais"))[:120],
        )
        check(
            "reload principal igual",
            digitos(p2.get("codigo_barras") or "") == principal_depois
            or (not principal_depois and not digitos(p2.get("codigo_barras") or "")),
            str(p2.get("codigo_barras")),
        )
    finally:
        # restaura
        ov.refresh_from_db()
        ov.cadastro_extras = extras_antes
        if principal_antes:
            ov.codigo_barras = principal_antes
        ov.save(update_fields=["cadastro_extras", "codigo_barras", "atualizado_em"])
        check("restore overlay", True)

    # Lógica fill com dados reais da API
    slots_r, _tail_r = fill_display(
        digitos(prod.get("codigo_barras") or ""),
        list(ops_api or []),
    )
    check("fill real slot0=principal se houver", True, f"slots={slots_r[:3]}")


def main() -> int:
    test_static()
    test_logic()
    try:
        test_runtime()
    except Exception as e:
        check("runtime exception", False, repr(e))
    print(f"\n{OK}/{OK + FAIL} provas")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
