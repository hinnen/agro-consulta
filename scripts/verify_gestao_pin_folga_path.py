"""Gestão: descanso pede PIN só após 30 min parado. PDV e o resto ficam em 3 min."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FAILS = []
OKS = 0


def check(cond, msg):
    global OKS
    if cond:
        OKS += 1
        print("OK", msg)
    else:
        FAILS.append(msg)
        print("FAIL", msg)


def main():
    sspin = (ROOT / "produtos/templates/produtos/_screensaver_pin.html").read_text(encoding="utf-8")
    gestao = (ROOT / "produtos/templates/produtos/produtos_gestao.html").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    consulta = (ROOT / "produtos/templates/produtos/consulta_produtos.html").read_text(encoding="utf-8")
    checkout = (ROOT / "produtos/templates/produtos/pdv_checkout.html").read_text(encoding="utf-8")

    check('{% else %}data-idle-min="3"{% endif %}' in sspin, "padrão continua 3 min")
    check("sspin_idle_min" in sspin, "sspin aceita tempo só quando a tela passa")
    check('sspin_idle_min=30' in gestao, "gestão usa 30 min")
    check("sspin_idle_min" not in wiz, "wizard PDV sem folga")
    check("sspin_idle_min" not in consulta, "consulta PDV sem folga")
    check("sspin_idle_min" not in checkout, "checkout PDV sem folga")

    extras = []
    for p in (ROOT / "produtos/templates").rglob("*.html"):
        if p.name in ("produtos_gestao.html", "_screensaver_pin.html"):
            continue
        txt = p.read_text(encoding="utf-8", errors="replace")
        if "sspin_idle_min" in txt:
            extras.append(str(p.relative_to(ROOT)))
    rh = ROOT / "rh/templates"
    if rh.exists():
        for p in rh.rglob("*.html"):
            txt = p.read_text(encoding="utf-8", errors="replace")
            if "sspin_idle_min" in txt:
                extras.append(str(p.relative_to(ROOT)))
    check(not extras, "nenhuma outra tela afrouxa o PIN" + (f" ({extras})" if extras else ""))

    print(f"OKS={OKS} FAILS={len(FAILS)}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
