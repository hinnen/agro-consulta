# -*- coding: utf-8 -*-
"""
Prova — ordem das colunas e WhatsApp na lista do fiado (`FIADO-ORDEM-ZAP`).

  python scripts/verify_fiado_ordem_zap_path.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def test_arquivos() -> None:
    print("== Tela e script ==")
    html = (ROOT / "produtos/templates/produtos/fiado_gestao.html").read_text(encoding="utf-8")
    js = (ROOT / "produtos/static/produtos/js/fiado_gestao.js").read_text(encoding="utf-8")
    cols = ("cliente", "titulos", "venc", "valor", "pago", "saldo", "limite", "situacao")
    for col in cols:
        check(f"th_{col}", f'data-sort="{col}"' in html)
    acoes = ""
    for line in html.splitlines():
        if ">Ações<" in line or ">Acoes<" in line:
            acoes = line
            break
    check("acoes_sem_ordem", "fiado-sort" not in acoes, acoes.strip()[:80])
    check("css_sort", ".fiado-sort" in html and ".fiado-btn-wa" in html)
    check("js_inicia_saldo_desc", "let sortCol = 'saldo'" in js and "let sortDir = 'desc'" in js)
    check("js_troca_dir", "sortDir === 'desc' ? 'asc' : 'desc'" in js)
    check("js_colunas", "function valorOrdem" in js and "function ordenarClientes" in js)
    check("js_vazio_no_fim", "if (!av && bv) return 1" in js and "if (av && !bv) return -1" in js)
    check("js_nao_mexe_lista", ".slice().sort(" in js)
    check("js_botao_wa", "fiado-btn-wa" in js and "data-wa=" in js)
    check("js_sem_numero", "is-off" in js and "não tem WhatsApp no cadastro" in js)
    check("js_abre_fora", "agroAbrirUrlExterna" in js)
    check("js_nao_abre_ficha", "closest('.fiado-btn-wa')" in js and "stopPropagation" in js)
    check("js_baixa_segue", "fiado-btn-baixa" in js)


def test_ordem_js() -> None:
    print("== Ordem no JavaScript da tela ==")
    js_path = ROOT / "produtos/static/produtos/js/fiado_gestao.js"
    node = r"""
const fs = require('fs');
const src = fs.readFileSync(process.argv[1], 'utf8');
const a = src.indexOf('function valorOrdem');
const b = src.indexOf('function pintarMarcasSort');
if (a < 0 || b < 0) { console.log(JSON.stringify({erro:'extract'})); process.exit(2); }
let sortCol = 'saldo';
let sortDir = 'desc';
eval(src.slice(a, b));
const base = [
  {cliente_nome:'Cesar', saldo_aberto:10, titulos_abertos:1, valor_bruto:10, valor_pago:0, limite:5, situacao_resumo:'aberto', vencimento_mais_antigo:'2026-03-01', whatsapp_url:'https://api.whatsapp.com/send?phone=5511999990001'},
  {cliente_nome:'Ana', saldo_aberto:50, titulos_abertos:3, valor_bruto:80, valor_pago:20, limite:100, situacao_resumo:'vencido', vencimento_mais_antigo:'2024-01-01', whatsapp_url:''},
  {cliente_nome:'Bruno', saldo_aberto:50, titulos_abertos:2, valor_bruto:40, valor_pago:5, limite:0, situacao_resumo:'parcial', vencimento_mais_antigo:'', whatsapp_url:'https://api.whatsapp.com/send?phone=5511988880002'},
  {cliente_nome:'', saldo_aberto:1, titulos_abertos:0, valor_bruto:1, valor_pago:1, limite:null, limite_fiado_local:8, situacao_resumo:'zerado', vencimento_mais_antigo:'2025-06-01', whatsapp_url:''}
];
const orig = base.map(x => x.cliente_nome).join('|');
function nomes(col, dir) {
  sortCol = col; sortDir = dir;
  return ordenarClientes(base).map(x => x.cliente_nome).join('|');
}
const out = {
  saldoDesc: nomes('saldo','desc'),
  saldoAsc: nomes('saldo','asc'),
  cliDesc: nomes('cliente','desc'),
  cliAsc: nomes('cliente','asc'),
  titDesc: nomes('titulos','desc'),
  valDesc: nomes('valor','desc'),
  pagoDesc: nomes('pago','desc'),
  limDesc: nomes('limite','desc'),
  sitDesc: nomes('situacao','desc'),
  vencDesc: nomes('venc','desc'),
  vencAsc: nomes('venc','asc'),
  vazio: ordenarClientes(null).length,
  copia: orig === base.map(x => x.cliente_nome).join('|')
};
console.log(JSON.stringify(out));
"""
    try:
        r = subprocess.run(
            ["node", "--check", str(js_path)],
            capture_output=True,
            text=True,
            timeout=20,
        )
        check("node_sintaxe", r.returncode == 0, (r.stderr or "")[:120])
        r2 = subprocess.run(
            ["node", "-e", node, str(js_path)],
            capture_output=True,
            text=True,
            timeout=20,
        )
        check("node_rodou", r2.returncode == 0, (r2.stderr or r2.stdout or "")[:160])
        data = json.loads(r2.stdout.strip() or "{}")
    except FileNotFoundError:
        check("node", False, "node off")
        return
    except Exception as exc:
        check("node_json", False, str(exc)[:120])
        return

    # empate de saldo 50: Ana antes de Bruno (nome)
    check("saldo_maior", data.get("saldoDesc") == "Ana|Bruno|Cesar|", data.get("saldoDesc"))
    check("saldo_menor", data.get("saldoAsc") == "|Cesar|Ana|Bruno", data.get("saldoAsc"))
    check("nome_za", data.get("cliDesc") == "Cesar|Bruno|Ana|", data.get("cliDesc"))
    check("nome_az", data.get("cliAsc") == "|Ana|Bruno|Cesar", data.get("cliAsc"))
    check("titulos_maior", (data.get("titDesc") or "").split("|")[0] == "Ana", data.get("titDesc"))
    check("valor_maior", (data.get("valDesc") or "").split("|")[0] == "Ana", data.get("valDesc"))
    check("pago_maior", (data.get("pagoDesc") or "").split("|")[0] == "Ana", data.get("pagoDesc"))
    check("limite_maior", (data.get("limDesc") or "").split("|")[0] == "Ana", data.get("limDesc"))
    check("situacao_vencido_antes", (data.get("sitDesc") or "").split("|")[0] == "Ana", data.get("sitDesc"))
    # data maior = mais nova primeiro; sem data fica por último
    check("venc_recente", (data.get("vencDesc") or "").split("|")[0] == "Cesar", data.get("vencDesc"))
    check("venc_antigo", (data.get("vencAsc") or "").split("|")[0] == "Ana", data.get("vencAsc"))
    check("venc_sem_data_no_fim_desc", (data.get("vencDesc") or "").endswith("Bruno"), data.get("vencDesc"))
    check("venc_sem_data_no_fim_asc", (data.get("vencAsc") or "").endswith("Bruno"), data.get("vencAsc"))
    check("lista_vazia", data.get("vazio") == 0)
    check("nao_reordena_a_fonte", data.get("copia") is True)


def test_dados() -> None:
    print("== Cadastro e lista ==")
    import django

    django.setup()
    from django.contrib.auth import get_user_model
    from django.test import Client
    from django.urls import reverse

    from produtos.cliente_whatsapp_util import extrair_whatsapp_digits
    from produtos.fiado_gestao_util import listar_clientes_fiado, url_whatsapp_chat
    from produtos.models import ClienteAgro
    from produtos.pin_gerencial_util import validar_pin_gerencial

    pin_ok, rotulo, pin_err = validar_pin_gerencial("9973")
    check("pin_9973", pin_ok, rotulo or pin_err or "")
    pin_ruim, _, _ = validar_pin_gerencial("0000")
    check("pin_errado_recusa", not pin_ruim)

    check("url_vazio", url_whatsapp_chat("") == "")
    check("url_curto", url_whatsapp_chat("123") == "")
    check(
        "url_11",
        url_whatsapp_chat("(11) 99999-8888") == "https://api.whatsapp.com/send?phone=5511999998888",
    )
    check(
        "url_ja_55",
        url_whatsapp_chat("5511999998888") == "https://api.whatsapp.com/send?phone=5511999998888",
    )
    check("url_10", url_whatsapp_chat("1133334444") == "https://api.whatsapp.com/send?phone=551133334444")
    check("url_sem_texto", "text=" not in url_whatsapp_chat("11999998888"))

    rows = listar_clientes_fiado(apenas_com_saldo=True)
    check("lista_tem_gente", len(rows) > 0, str(len(rows)))
    saldos = [float(r.get("saldo_aberto") or 0) for r in rows]
    check("lista_ja_saldo_maior", saldos == sorted(saldos, reverse=True))
    check("toda_linha_tem_url", all("whatsapp_url" in r for r in rows))

    pks = [r.get("cliente_agro_pk") for r in rows if r.get("cliente_agro_pk")]
    clis = {c.pk: c for c in ClienteAgro.objects.filter(pk__in=pks).only("pk", "whatsapp", "nome")}
    bate = 0
    sem = 0
    ruim = []
    for r in rows:
        pk = r.get("cliente_agro_pk")
        url = r.get("whatsapp_url") or ""
        if not pk or pk not in clis:
            if url:
                ruim.append(f"sem ficha com url {url[:40]}")
            else:
                sem += 1
            continue
        esperado = url_whatsapp_chat(clis[pk].whatsapp)
        if url == esperado:
            bate += 1
            if esperado:
                dig = extrair_whatsapp_digits(clis[pk].whatsapp)
                if dig.startswith("55") and len(dig) >= 12:
                    alvo = dig
                else:
                    alvo = "55" + dig
                if f"phone={alvo}" not in url:
                    ruim.append(clis[pk].nome)
        else:
            ruim.append(clis[pk].nome or str(pk))
    check("url_bate_cadastro", not ruim, f"{bate} ok · sem número {sem} · ruim {ruim[:3]}")
    com_zap = sum(1 for r in rows if r.get("whatsapp_url"))
    check("tem_alguem_com_zap", com_zap > 0, str(com_zap))
    check(
        "zap_so_api",
        all(str(r.get("whatsapp_url") or "").startswith("https://api.whatsapp.com/send?phone=55") for r in rows if r.get("whatsapp_url")),
    )

    User = get_user_model()
    u = User.objects.filter(username="Renan").first() or User.objects.filter(is_superuser=True).first() or User.objects.first()
    check("usuario_local", u is not None, getattr(u, "username", ""))
    if not u:
        return
    c = Client(HTTP_HOST="127.0.0.1")
    c.force_login(u)
    page = c.get(reverse("fiado_gestao"))
    check("pagina_200", page.status_code == 200, str(page.status_code))
    body = page.content.decode("utf-8", "replace")
    check("pagina_colunas", 'data-sort="saldo"' in body and 'data-sort="cliente"' in body)
    check("pagina_js", "fiado_gestao.js" in body)
    api = c.get(reverse("api_fiado_clientes") + "?apenas_saldo=1")
    check("api_200", api.status_code == 200, str(api.status_code))
    payload = api.json()
    check("api_ok", payload.get("ok") is True)
    api_rows = payload.get("clientes") or []
    check("api_url", all("whatsapp_url" in r for r in api_rows), str(len(api_rows)))
    anon = Client(HTTP_HOST="127.0.0.1")
    trava = anon.get(reverse("api_fiado_clientes"))
    check("api_sem_login_nao_abre", trava.status_code in (301, 302, 401, 403), str(trava.status_code))


def main() -> int:
    test_arquivos()
    test_ordem_js()
    test_dados()
    total = len(oks) + len(fails)
    print(f"\n{len(oks)}/{total}")
    if fails:
        print("Falhou:", ", ".join(fails))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
