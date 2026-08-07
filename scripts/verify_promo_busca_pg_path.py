"""Verify promo product search uses Postgres path (no Mongo hard-fail)."""
from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ok = 0
fail = 0


def check(cond: bool, msg: str) -> None:
    global ok, fail
    if cond:
        ok += 1
        print(f"OK  {msg}")
    else:
        fail += 1
        print(f"FAIL {msg}")


util = (ROOT / "produtos" / "promocoes_util.py").read_text(encoding="utf-8")
mongo = (ROOT / "produtos" / "busca_produtos_mongo.py").read_text(encoding="utf-8")

check("agro_catalogo_usa_postgres" in util, "promocoes_util importa flag agro_pg")
check("usa_pg" in util, "promocoes_util tem ramo Postgres")
check("obter_conexao_mongo()" in util, "promocoes_util ainda tem legado Mongo")
# Não pode exigir Mongo antes do ramo PG
tree = ast.parse(util)
fn = None
for node in tree.body:
    if isinstance(node, ast.FunctionDef) and node.name == "buscar_produtos_para_promocao":
        fn = node
        break
check(fn is not None, "função buscar_produtos_para_promocao existe")
if fn is not None:
    src = ast.get_source_segment(util, fn) or ""
    # Early return on Mongo None must be inside else / not-usa_pg
    check("if usa_pg:" in src, "ramo if usa_pg presente")
    check(
        "if db is None or client is None:" in src and "else:" in src,
        "early-return Mongo só no legado",
    )

check("prods_mongo_style_busca_pdv" in mongo, "motor_pdv usa catalogo_agro no agro_pg")
check("agro_catalogo_usa_postgres" in mongo, "busca_produtos_mongo checa agro_pg")
check("motor_busca_consulta_documentos" in mongo, "legado Mongo preservado")

print(f"\n{ok}/{ok + fail} checks")
raise SystemExit(1 if fail else 0)
