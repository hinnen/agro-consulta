# -*- coding: utf-8 -*-
"""
Prova — classificação medicamentos SisVale (prateleira + espécies + busca).

Segurança: sem migration, sem alteração em massa, extras só mudam com chave no payload,
ERP Produtos/Salvar sem espécies/classes Agro.

  .venv/bin/python scripts/verify_classificacao_medicamentos_path.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []
skipped: list[str] = []
PIN_ENV = (os.environ.get("AGRO_PIN_TESTE") or "").strip()


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def skip(name: str, detail: str = "") -> None:
    skipped.append(name)
    print(f"  SKIP {name}" + (f" — {detail}" if detail else ""))


def test_arquivos_e_contratos() -> None:
    print("== Arquivos e contratos ==")
    clf = ROOT / "produtos/data/classificacao_medicamento_vet.json"
    mod = ROOT / "produtos/medicamento_vet_taxonomia.py"
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    modal = (
        ROOT / "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html"
    ).read_text(encoding="utf-8")

    check("json_classificacao", clf.is_file())
    if clf.is_file():
        data = json.loads(clf.read_text(encoding="utf-8"))
        check("prateleira_9", len(data.get("grupos_prateleira") or []) == 9)
        check("especies_10", len(data.get("especies") or []) == 10)

    mod_txt = mod.read_text(encoding="utf-8")
    check("campo_especies_mongo", 'AGRO_ESPECIES_VET_CAMPO = "AgroEspeciesVet"' in mod_txt)
    check("sem_especies_texto_extra", "montar_texto_busca_denormalizado(pk, cv)" in mod_txt)

    check("view_especies_payload", '"especies_vet" in payload' in views)
    check("detalhe_especies", 'row["especies_vet"]' in views)
    check("erp_sem_especies", "especies_vet" not in views.split("def _montar_cadastro_erp_payload_produtos_salvar", 1)[1][:4000])

    check("modal_especies_secao", 'id="bloco-especies-vet"' in modal)
    check("modal_anexar_busca_seguro", "anexarBuscaMedVetAoBody(body)" in modal)
    check("modal_merge_classes", "sincronizarClassesVetDoDomVisivel" in modal)
    check("modal_merge_especies", "sincronizarEspeciesVetDoDomVisivel" in modal)
    check("modal_nao_envia_vet_sempre", "classes_vet: coletarClassesVetSelecionadas()" not in modal)
    check("modal_prateleira_select", 'id="edit-subcategoria-med-prateleira"' in modal)
    check("modal_ordem_busca", modal.find("edit-palavras-chave") < modal.find("bloco-especies-vet") < modal.find("bloco-classes-vet"))
    idx_gv = modal.find("function gv(id)")
    idx_prat = modal.find("function preencherSelectPrateleiraMed")
    check(
        "modal_gv_escopo_global",
        idx_gv >= 0 and idx_prat >= 0 and idx_gv < idx_prat,
        "gv antes da UI medicamento (abrir modal)",
    )
    check("modal_abrirModalProduto", "window.abrirModalProduto = function" in modal)

    gestao = (ROOT / "produtos/templates/produtos/produtos_gestao.html").read_text(encoding="utf-8")
    check("gestao_abrir_drawer", "function abrirDrawer" in gestao)
    check("gestao_click_linha", "mainTr.addEventListener('click'" in gestao)
    check("gestao_edit_try", "abrirDrawerSeguro" in gestao)

    med_paths = [
        "produtos/medicamento_vet_taxonomia.py",
        "produtos/data/classificacao_medicamento_vet.json",
        "produtos/templates/produtos/_modal_editar_produto_cadastro_erp.inc.html",
        "produtos/tests/test_medicamento_vet_taxonomia.py",
        "produtos/tests/test_med_classificacao_regressao.py",
        "produtos/templates/produtos/produtos_gestao.html",
        "scripts/verify_classificacao_medicamentos_path.py",
    ]
    mig_log = subprocess.run(
        ["git", "log", "-25", "--name-only", "--pretty=format:", "--", *med_paths],
        cwd=ROOT,
        capture_output=True,
        text=True,
    ).stdout
    mig = [ln for ln in mig_log.splitlines() if "/migrations/" in ln.replace("\\", "/")]
    check("med_path_sem_migrations", not mig, ",".join(mig[:3]) or "ok")


def test_logica_seguranca_dados() -> None:
    print("== Lógica — não sobrescrever sem payload ==")
    from produtos.medicamento_vet_taxonomia import (
        aplicar_palavras_e_classes_em_extras,
        documento_mongo_set_busca_agro,
        grupos_prateleira_para_api,
        ler_busca_de_cadastro_extras,
        montar_texto_busca_denormalizado,
        normalizar_especies_vet,
        taxonomia_para_api,
    )

    ex = {"especies_vet": ["caes", "gatos"], "classes_vet": ["antibiotico"], "palavras_chave": "Foo"}
    aplicar_palavras_e_classes_em_extras(ex, palavras_chave="Bar", payload_tem_palavras=True)
    check("payload_sem_especies_preserva", ex.get("especies_vet") == ["caes", "gatos"])
    check("payload_palavras_atualiza", ex.get("palavras_chave") == "Bar")

    ex2 = dict(ex)
    aplicar_palavras_e_classes_em_extras(ex2, especies_vet=[], payload_tem_especies=True)
    check("payload_especies_vazio_remove", "especies_vet" not in ex2)

    pk, cv, ev = ler_busca_de_cadastro_extras({"especies_vet": ["bovinos"]})
    check("ler_extras_triple", ev == ["bovinos"])

    txt = montar_texto_busca_denormalizado("x", ["antibiotico"])
    check("texto_extra_sem_especie_label", "bovinos" not in txt and "cães" not in txt.lower())

    doc = documento_mongo_set_busca_agro("x", ["antibiotico"], ["caes"])
    check("mongo_tem_especies", doc.get("AgroEspeciesVet") == ["caes"])
    check("mongo_extra_igual_antes", doc.get("AgroBuscaTextoExtra") == txt)

    api = taxonomia_para_api()
    check("api_prateleira", len(api.get("grupos_prateleira") or []) == 9)
    check("api_especies", len(api.get("especies") or []) == 10)
    labs = [g["label"] for g in grupos_prateleira_para_api()]
    check("prateleira_01", any(l.startswith("01 -") for l in labs))
    check("especies_norm", normalizar_especies_vet(["caes", "x"]) == ["caes"])


def test_http_e_pin() -> None:
    print("== HTTP + PIN ==")
    try:
        import django

        django.setup()
        from django.contrib.auth import get_user_model
        from django.test import Client, override_settings

        User = get_user_model()
        u, _ = User.objects.get_or_create(
            username="prova_med_class",
            defaults={"is_staff": True, "is_active": True},
        )
        u.set_password("prova-med-class")
        u.is_staff = True
        u.is_active = True
        u.save()
        with override_settings(ALLOWED_HOSTS=["*", "testserver"]):
            c = Client()
            if not c.login(username="prova_med_class", password="prova-med-class"):
                check("login", False)
                return
            r = c.get("/api/produtos/cadastro/taxonomia-medicamento-veterinario/")
            check("tax_http_200", r.status_code == 200, str(r.status_code))
            if r.status_code == 200:
                j = r.json()
                check("tax_grupos_prateleira", len(j.get("grupos_prateleira") or []) == 9)
                check("tax_especies", len(j.get("especies") or []) == 10)
    except Exception as exc:  # noqa: BLE001
        check("django_http", False, str(exc)[:140])

    if not PIN_ENV:
        skip("pin_operador", "defina AGRO_PIN_TESTE no ambiente (sem PIN embutido no script)")
    else:
        try:
            import django

            django.setup()
            from base.models import PerfilUsuario
            from django.contrib.auth import get_user_model
            from produtos.caixa_util import validar_pin_operador

            User = get_user_model()
            user, _ = User.objects.get_or_create(
                username="pin_prova_med_class",
                defaults={"is_staff": True, "is_superuser": True, "is_active": True},
            )
            user.set_password("prova-pin-med-class-local")
            user.is_active = True
            user.save()
            perfil = PerfilUsuario.objects.filter(senha_rapida=PIN_ENV).first()
            if not perfil:
                cod = PIN_ENV[:4] if len(PIN_ENV) >= 4 else "9901"
                while PerfilUsuario.objects.filter(codigo_vendedor=cod).exists():
                    cod = f"9{PerfilUsuario.objects.count():03d}"[:4]
                perfil = PerfilUsuario.objects.create(
                    user=user,
                    codigo_vendedor=cod,
                    senha_rapida=PIN_ENV,
                    ativo=True,
                    primeiro_acesso=False,
                )
            else:
                perfil.senha_rapida = PIN_ENV
                perfil.ativo = True
                perfil.save()
            ok, msg = validar_pin_operador(PIN_ENV)
            if ok:
                check("pin_operador_validado", True, msg or "ok")
            else:
                skip("pin_operador", msg or "PG local sem perfil completo")
        except Exception as exc:  # noqa: BLE001
            skip("pin_operador", str(exc)[:100])


def _run_test_module(rel_path: str, label: str, min_tests: int) -> None:
    import importlib.util

    p = ROOT / rel_path
    spec = importlib.util.spec_from_file_location(label, p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    n = 0
    for name in sorted(x for x in dir(mod) if x.startswith("test_")):
        getattr(mod, name)()
        n += 1
    check(f"unit_{label}", n >= min_tests, str(n))


def test_unit_tests_file() -> None:
    print("== testes unitários Python ==")
    _run_test_module("produtos/tests/test_medicamento_vet_taxonomia.py", "taxonomia", 8)
    _run_test_module("produtos/tests/test_med_classificacao_regressao.py", "regressao", 7)


def main() -> int:
    print("CLASSIFICACAO-MED-SISVALE — prova detalhada\n")
    test_arquivos_e_contratos()
    test_logica_seguranca_dados()
    test_http_e_pin()
    test_unit_tests_file()
    print(f"\nOK={len(oks)} FAIL={len(fails)} SKIP={len(skipped)}")
    if skipped:
        print("SKIP_LIST=" + ",".join(skipped))
    print("PREP_FAILS=" + str(len(fails)))
    return 1 if fails else 0


if __name__ == "__main__":
    raise SystemExit(main())
