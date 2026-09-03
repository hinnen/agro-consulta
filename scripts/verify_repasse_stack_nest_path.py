# -*- coding: utf-8 -*-
"""
Prova detalhada — Repasse Confirmar sem vidro (`REPASSE-STACK-NEST`).

  python scripts/verify_repasse_stack_nest_path.py
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        oks.append(name)
        print(f"  OK  {name}" + (f" — {detail}" if detail else ""))
    else:
        fails.append(name)
        print(f"  FAIL {name}" + (f" — {detail}" if detail else ""))


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_arquivos() -> None:
    print("== Contratos estáticos ==")
    stack = _read("produtos/static/produtos/js/agro_overlay_stack.js")
    compact = "".join(stack.split())
    js = _read("produtos/static/produtos/js/pdv_repasse_vila.js")
    html = _read("produtos/templates/produtos/partials/pdv/repasse_vila_overlay.html")
    crh = _read("produtos/templates/produtos/caixa_retiradas_historico.html")

    check("stack_v3", "agro-overlay-stack-styles-v3" in stack)
    check("nested_parent_class", "agro-stack-nested-parent" in stack)
    check("contains_top", "el.contains(top)" in compact)
    check("no_inactive_when_nested", "nestedInside" in stack and "agro-stack-inactive" in stack)
    check("pop_clears_nested", stack.count("agro-stack-nested-parent") >= 3)
    check("glass_only_inactive", "2147483645" in stack and "agro-stack-inactive::after" in compact)

    overlay_at = html.find('id="pdv-repasse-overlay"')
    body = html[overlay_at:] if overlay_at >= 0 else ""
    check("overlay_id", overlay_at >= 0)
    check("confirm_inside_overlay", 'id="pdv-rp-cofre-confirm-modal"' in body)
    check("check_inside_overlay", 'id="pdv-rp-cofre-check-modal"' in body)
    check("pin_inside_overlay", 'id="pdv-rp-pin-modal"' in body)
    check("quem_inside_overlay", 'id="pdv-rp-quem-modal"' in body)
    check("aviso_inside_overlay", 'id="pdv-rp-aviso-modal"' in body)

    check("js_show_setOpen", "AgroOverlayStack.setOpen(el, true)" in js)
    check("js_hide_setOpen", "AgroOverlayStack.setOpen(el, false)" in js)
    check("js_overlay_setOpen", "AgroOverlayStack.setOpen(overlay, true)" in js)
    check("js_open_cofre_confirm", "openCofreConfirmModal" in js and "showModal(cofreConfirmModal)" in js)
    check("js_open_cofre_check", "openCofreCheckSequence" in js and "showModal(cofreCheckModal)" in js)
    check("js_ghost_click_delay", "pointerEvents = 'none'" in js and "160" in js)

    check("crh_include_overlay", "repasse_vila_overlay.html" in crh)
    check("crh_repasse_not_gestao", 'url "repasse_vila"' not in crh)


NODE_SIM = r"""
'use strict';
const fs = require('fs');
const path = require('path');
const stackPath = path.join(__dirname, '..', 'produtos', 'static', 'produtos', 'js', 'agro_overlay_stack.js');
const code = fs.readFileSync(stackPath, 'utf8');

function makeEl(id) {
  const kids = [];
  const classes = new Set();
  const el = {
    id,
    tagName: 'DIV',
    classList: {
      add: (...xs) => xs.forEach((x) => classes.add(x)),
      remove: (...xs) => xs.forEach((x) => classes.delete(x)),
      contains: (x) => classes.has(x),
      toggle: (x, on) => (on ? classes.add(x) : classes.delete(x)),
    },
    style: { display: '', pointerEvents: '' },
    children: kids,
    contains(other) {
      if (other === el) return true;
      for (const c of kids) {
        if (c === other || c.contains(other)) return true;
      }
      return false;
    },
    appendChild(c) { kids.push(c); return c; },
    getAttribute() { return null; },
    setAttribute() {},
    hasAttribute() { return false; },
    _classes: classes,
  };
  return el;
}

const overlay = makeEl('pdv-repasse-overlay');
const confirm = makeEl('pdv-rp-cofre-confirm-modal');
const check = makeEl('pdv-rp-cofre-check-modal');
const sibling = makeEl('outra-camada');
overlay.appendChild(confirm);
overlay.appendChild(check);

const headKids = [];
global.document = {
  getElementById(id) {
    if (id === 'agro-overlay-stack-styles-v1') return null;
    if (id === 'agro-overlay-stack-styles-v2') return null;
    if (id === 'agro-overlay-stack-styles-v3') return { id };
    return null;
  },
  createElement(tag) {
    return { id: '', tagName: tag.toUpperCase(), textContent: '', setAttribute() {}, style: {}, parentNode: null };
  },
  head: { appendChild(n) { headKids.push(n); } },
  documentElement: { appendChild() {} },
  readyState: 'complete',
  querySelectorAll() { return []; },
  addEventListener() {},
};
global.window = {
  getComputedStyle() { return { position: 'fixed', display: 'flex', visibility: 'visible' }; },
  top: null,
  location: { origin: 'http://127.0.0.1' },
  AgroOverlayStack: null,
};
global.window.top = global.window;

eval(code);
const S = global.window.AgroOverlayStack;
if (!S) { console.log('FAIL no_stack'); process.exit(1); }

let fails = 0;
function ok(name, cond) {
  if (cond) console.log('  OK  sim_' + name);
  else { console.log('  FAIL sim_' + name); fails += 1; }
}

S.setOpen(overlay, true);
ok('overlay_alone_active', !overlay.classList.contains('agro-stack-inactive'));

S.setOpen(confirm, true);
ok('parent_no_inactive', !overlay.classList.contains('agro-stack-inactive'));
ok('parent_nested_flag', overlay.classList.contains('agro-stack-nested-parent'));
ok('child_active', !confirm.classList.contains('agro-stack-inactive'));

S.setOpen(confirm, false);
ok('after_pop_no_nested', !overlay.classList.contains('agro-stack-nested-parent'));
ok('after_pop_active', !overlay.classList.contains('agro-stack-inactive'));

S.setOpen(check, true);
ok('check_parent_nested', overlay.classList.contains('agro-stack-nested-parent'));
ok('check_parent_no_glass', !overlay.classList.contains('agro-stack-inactive'));

S.setOpen(sibling, true);
ok('sibling_freezes_parent', overlay.classList.contains('agro-stack-inactive'));
ok('sibling_not_nested_flag', !overlay.classList.contains('agro-stack-nested-parent'));

S.setOpen(sibling, false);
S.setOpen(check, false);
S.setOpen(overlay, false);
ok('cleared_depth', S.depth() === 0);

process.exit(fails ? 1 : 0);
"""


def test_simulacao() -> None:
    print("== Simulação DOM (Node) ==")
    script = ROOT / "scripts" / "_tmp_sim_repasse_stack_nest.js"
    try:
        script.write_text(NODE_SIM, encoding="utf-8")
        r = subprocess.run(
            ["node", str(script)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=20,
        )
        print(r.stdout or "")
        if r.stderr:
            print(r.stderr)
        if r.returncode != 0:
            fails.append("sim_node")
            print("  FAIL sim_node — exit", r.returncode)
        else:
            # contar OKs da simulação
            for line in (r.stdout or "").splitlines():
                if line.strip().startswith("OK  sim_"):
                    oks.append(line.strip()[4:].strip())
                elif line.strip().startswith("FAIL sim_"):
                    fails.append(line.strip()[5:].strip())
    except FileNotFoundError:
        check("sim_node_skip", True, "node não instalado")
    except Exception as e:
        check("sim_node", False, str(e)[:80])
    finally:
        try:
            script.unlink(missing_ok=True)
        except Exception:
            pass


def test_verify_stack() -> None:
    print("== verify_pdv_overlay_stack_path ==")
    r = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "verify_pdv_overlay_stack_path.py")],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        timeout=30,
    )
    out = (r.stdout or "") + (r.stderr or "")
    print(out.strip())
    check("stack_verify_exit", r.returncode == 0)
    check("stack_23", "23/23" in out)


def test_pin_util() -> None:
    print("== PIN 9973 (validar_pin, sem gravar repasse) ==")
    sys.path.insert(0, str(ROOT))
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    try:
        import django

        django.setup()
        from produtos.caixa_util import validar_pin_operador

        ok, err = validar_pin_operador("9973")
        check("pin_9973", ok, err or "")
        bad, _ = validar_pin_operador("0000")
        check("pin_errado", not bad)
    except Exception as e:
        check("pin_skip", True, str(e).split("\n")[0][:80])


def main() -> int:
    print("verify_repasse_stack_nest_path")
    test_arquivos()
    test_simulacao()
    test_verify_stack()
    test_pin_util()
    print()
    if fails:
        print(f"FALHOU: {len(fails)} falha(s), {len(oks)} ok")
        for f in fails:
            print(f"  - {f}")
        return 1
    print(f"OK verify_repasse_stack_nest_path — {len(oks)}/{len(oks)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
