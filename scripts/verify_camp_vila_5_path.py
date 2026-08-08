"""Verify CAMP-VILA-5 path: PDV bootstrap → JS → persistência venda → round 5¢."""
from __future__ import annotations

import os
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
import django

django.setup()

ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(("OK  " + msg).encode("ascii", "replace").decode("ascii"))
    else:
        fail += 1
        print(("FAIL " + msg).encode("ascii", "replace").decode("ascii"))


util = (ROOT / "produtos" / "campanha_pdv_util.py").read_text(encoding="utf-8")
views = (ROOT / "produtos" / "views.py").read_text(encoding="utf-8")
pdv_views = (ROOT / "pdv" / "views.py").read_text(encoding="utf-8")
settings = (ROOT / "config" / "settings.py").read_text(encoding="utf-8")
wiz = (ROOT / "produtos" / "templates" / "produtos" / "pdv_wizard.html").read_text(encoding="utf-8")
js_camp = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_campanha.js").read_text(encoding="utf-8")
js_promo = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_promocoes.js").read_text(encoding="utf-8")
js_state = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_state.js").read_text(encoding="utf-8")
js_wiz = (ROOT / "produtos" / "static" / "produtos" / "js" / "pdv_wizard.js").read_text(encoding="utf-8")

check("CAMPANHA_DATA_DEFAULT = date(2026, 8, 8)" in util, "data inauguração 08/08/2026")
check('CAMPANHA_DEPOSITO = "vila"' in util, "só depósito Vila")
check("arredondar_preco_5_centavos" in util, "arredonda 5 centavos no servidor")
check('"modo": "menor"' in util, "modo menor vs promo")
check("AGRO_CAMPANHA_INAUGURACAO_TEST" in settings, "settings lê TEST do .env")
check("AGRO_CAMPANHA_INAUGURACAO_OFF" in settings, "settings lê kill switch")
check("bootstrap_campanha" in pdv_views, "PDV home injeta campanhaPdv")
check('"campanhaPdv": bootstrap_campanha' in pdv_views, "bootstrap campanhaPdv no wizard")
check("pdv-campanha-faixa" in wiz, "faixa laranja no HTML")
check("pdv_campanha.js" in wiz, "script campanha no wizard")
check("pdv_campanha.js" in wiz and "pdv_promocoes.js" in wiz, "campanha carrega antes de promoções")
check(wiz.find("pdv_campanha.js") < wiz.find("pdv_promocoes.js"), "ordem script: campanha antes de promocao")
check("AgroPdvCampanha.setBootstrap" in js_wiz, "wizard liga bootstrap da campanha")
check("preco_base" in js_wiz, "payload manda preco_base pro servidor")
check("precoEnvioItem" in js_wiz, "envio usa preço sem campanha aplicada 2×")
check("aplicarNosItens" in js_promo, "promo chama campanha após recalc")
check("arredondar5Centavos" in js_camp, "JS arredonda 5¢")
check("preco_pos_promo" in js_camp and "campanha_id" in js_camp, "JS guarda base pós-promo")
check("if (item.preco_pos_promo != null && item.campanha_id)" in js_camp, "JS idempotente (não desconta 2×)")
check("aplicar_desconto_campanha_nos_itens" in views, "persistência venda aplica campanha")
check(views.count("aplicar_desconto_campanha_nos_itens") >= 2, "venda Agro + pedido ERP aplicam")
check("deposito_de_ponto_caixa" in views, "caixa manda no depósito da campanha")

from produtos.campanha_pdv_util import (
    aplicar_desconto_campanha_nos_itens,
    arredondar_preco_5_centavos,
    campanha_ativa_para_deposito,
)

check(campanha_ativa_para_deposito("vila", agora=date(2026, 8, 8)) is not None, "Vila no dia = ativa")
check(campanha_ativa_para_deposito("centro", agora=date(2026, 8, 8)) is None, "Centro no dia = inativa")
check(
    arredondar_preco_5_centavos(Decimal("2.375")) == Decimal("2.40"),
    "2,375 → 2,40",
)
itens_87, _ = aplicar_desconto_campanha_nos_itens(
    [{"id": "1", "preco": Decimal("87")}], "vila", agora=date(2026, 8, 8)
)
check(itens_87[0]["preco"] == 82.65, "milho 47kg 87 → 82,65")
itens_250, _ = aplicar_desconto_campanha_nos_itens(
    [{"id": "1", "preco": Decimal("2.50")}], "vila", agora=date(2026, 8, 8)
)
check(itens_250[0]["preco"] == 2.40, "milho 1kg 2,50 → 2,40")
itens_promo, _ = aplicar_desconto_campanha_nos_itens(
    [{"id": "1", "preco": 8, "preco_base": 100}], "vila", agora=date(2026, 8, 8)
)
check(itens_promo[0]["preco"] == 8.0, "promo forte R$ 8 vence o 5%")
itens_fraca, _ = aplicar_desconto_campanha_nos_itens(
    [{"id": "1", "preco": 98, "preco_base": 100}], "vila", agora=date(2026, 8, 8)
)
check(itens_fraca[0]["preco"] == 95.0, "promo fraca 98 → 95 (campanha)")

# Dupla aplicação no servidor: payload já com 5% e sem preco_base não pode cair de novo.
itens_dup, _ = aplicar_desconto_campanha_nos_itens(
    [{"id": "1", "preco": 82.65, "preco_base": 87}], "vila", agora=date(2026, 8, 8)
)
check(itens_dup[0]["preco"] == 82.65, "servidor com preco_base 87 não aplica 2× (fica 82,65)")

print(("\n%d/%d checks" % (ok, ok + fail)).encode("ascii", "replace").decode("ascii"))
raise SystemExit(1 if fail else 0)
