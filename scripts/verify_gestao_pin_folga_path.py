"""Gestão: descanso 5 min e o PIN de uma ação vale até parar. PDV continua em 3 min / a cada ação."""
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
    cad = (ROOT / "produtos/templates/produtos/produtos_cadastro_erp.html").read_text(encoding="utf-8")
    wiz = (ROOT / "produtos/templates/produtos/pdv_wizard.html").read_text(encoding="utf-8")
    consulta = (ROOT / "produtos/templates/produtos/consulta_produtos.html").read_text(encoding="utf-8")
    views = (ROOT / "produtos/views.py").read_text(encoding="utf-8")
    pick = (ROOT / "produtos/static/produtos/js/agro_picklist.js").read_text(encoding="utf-8")

    check('{% else %}data-idle-min="3"{% endif %}' in sspin, "padrão continua 3 min")
    check("gmSspinGestaoLiberado" in sspin, "gestão lembra o PIN da sessão")
    check("gestaoOkLocal()" in sspin, "ação na gestão não reabre o PIN")
    check("sspin_idle_min=5" in gestao and "sspin_gestao=1" in gestao, "gestão lista descanso 5 min")
    check("sspin_idle_min=5" in cad and "sspin_gestao=1" in cad, "cadastro gestão descanso 5 min")
    check("sspin_gestao" not in wiz, "wizard PDV sem folga")
    check("sspin_idle_min" not in wiz and "sspin_idle_min" not in consulta, "PDV sem tempo próprio")
    check("gestao_pin_liberado" in views, "servidor aceita o PIN já dado na gestão")
    check("usar_sessao" in pick, "lista + não pede PIN de novo se já identificado")

    extras = []
    for p in (ROOT / "produtos/templates").rglob("*.html"):
        if p.name in ("produtos_gestao.html", "produtos_cadastro_erp.html", "_screensaver_pin.html"):
            continue
        txt = p.read_text(encoding="utf-8", errors="replace")
        if "sspin_gestao" in txt or "sspin_idle_min" in txt:
            extras.append(str(p.relative_to(ROOT)))
    check(not extras, "nenhuma outra tela afrouxa o PIN" + (f" ({extras})" if extras else ""))

    print(f"OKS={OKS} FAILS={len(FAILS)}")
    return 1 if FAILS else 0


if __name__ == "__main__":
    raise SystemExit(main())
