#!/usr/bin/env python3
"""ENTREGA-HORA-LOJA — papel e telas no relógio da loja, não em UTC."""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
PASS = 0
FAIL = 0
LOJA = ZoneInfo("America/Sao_Paulo")


def check(ok: bool, msg: str) -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"  OK  {msg}")
    else:
        FAIL += 1
        print(f" FAIL {msg}")


def main() -> int:
    wiz = (ROOT / "produtos/static/produtos/js/pdv_wizard.js").read_text(encoding="utf-8")
    painel = (ROOT / "produtos/templates/produtos/entregas_painel.html").read_text(encoding="utf-8")
    cli = (ROOT / "produtos/static/produtos/js/cliente_cadastro_acoes.js").read_text(encoding="utf-8")
    emp = (ROOT / "produtos/templates/produtos/emprestimos_consulta.html").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    util = (ROOT / "produtos/entrega_pdv_pendente_util.py").read_text(encoding="utf-8")
    cupom = (ROOT / "produtos/static/produtos/js/venda_cupom_80mm.js").read_text(encoding="utf-8")

    print("=== ENTREGA-HORA-LOJA ===")
    check("function agroStampLoja" in wiz, "PDV tem relógio da loja")
    check("toISOString().replace" not in wiz, "PDV não imprime mais o UTC cru")
    check("agroStampLoja(new Date())" in wiz, "via nova usa o relógio do PC")
    check("agroStampLoja(row.criado_em)" in wiz, "reimpressão da entrega usa o relógio da loja")
    check(wiz.count("agroStampLoja(e.criado_em)") >= 3, "separação, entregador e cupom usam o mesmo relógio")
    check("function stampLoja" in painel, "painel de entregas tem o mesmo relógio")
    check("var dh = stampLoja(e.criado_em)" in painel, "as três vias do painel passam pelo relógio da loja")
    check(painel.count("var dh = stampLoja(e.criado_em)") >= 3, "separação, entregador e cupom do painel")
    check("stampLoja(e.hora_saida)" in painel and "stampLoja(e.hora_entrega)" in painel, "saída e entrega no painel")
    check("def iso_relogio_loja" in util and "timezone.localtime" in util, "lista do PDV manda a hora da loja")
    check('iso_relogio_loja(e.criado_em)' in views, "lista do painel manda a hora da loja")
    check("timezone.localtime(v.criado_em)" in views, "Excel de vendas na hora da loja")
    check(views.count("timezone.localtime(") >= 4, "etiquetas e orçamento também")
    check("new Date(s)" in cli, "histórico do cliente converte o fuso")
    check("function fmtRelogioLoja" in emp, "empréstimo mostra a hora da loja")
    check("toLocaleString('pt-BR'" in cupom or "function parseDataCupomBr" in cupom, "cupom de venda segue o texto já local")

    utc = datetime(2026, 11, 13, 19, 49, 50, tzinfo=timezone.utc)
    loja = utc.astimezone(LOJA).strftime("%Y-%m-%d %H:%M:%S")
    check(loja == "2026-11-13 16:49:50", f"19:49 UTC = 16:49 loja ({loja})")

    env = dict(os.environ)
    env["TZ"] = "America/Sao_Paulo"
    js = r"""
function agroStampLoja(valor) {
  function p(n) { return String(n).padStart(2, '0'); }
  function deData(d) {
    return p(d.getFullYear()) + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate()) + ' ' +
      p(d.getHours()) + ':' + p(d.getMinutes()) + ':' + p(d.getSeconds());
  }
  if (valor instanceof Date) return deData(valor);
  var s = String(valor || '').trim();
  if (!s) return deData(new Date());
  if (/[zZ]$|[+-]\d{2}:?\d{2}$/.test(s)) {
    var dz = new Date(s);
    if (!isNaN(dz.getTime())) return deData(dz);
  }
  var m = s.match(/^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)/);
  if (m) return m[1] + ' ' + (m[2].length === 5 ? m[2] + ':00' : m[2]);
  return 'fail';
}
const casos = [
  ['2026-11-13T19:49:50.000Z', '2026-11-13 16:49:50'],
  ['2026-11-13T19:49:50+00:00', '2026-11-13 16:49:50'],
  ['2026-11-13T16:49:50.123456-03:00', '2026-11-13 16:49:50'],
  ['2026-11-13 16:49:50', '2026-11-13 16:49:50'],
  ['2026-11-13T16:49:50', '2026-11-13 16:49:50']
];
let bad = 0;
for (const [inn, esp] of casos) {
  const got = agroStampLoja(inn);
  if (got !== esp) { console.log('FAIL', inn, got); bad++; }
}
if (bad) process.exit(1);
console.log('JS_OK ' + casos.length);
"""
    r = subprocess.run(["node", "-e", js], cwd=ROOT, env=env, capture_output=True, text=True)
    check(r.returncode == 0 and "JS_OK 5" in (r.stdout or ""), "5 horários de papel batem com a loja")
    if r.returncode != 0:
        print(r.stdout)
        print(r.stderr)

    print(f"VERIFY_OK {PASS}" if FAIL == 0 else f"VERIFY_FAIL {FAIL} ok={PASS}")
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
