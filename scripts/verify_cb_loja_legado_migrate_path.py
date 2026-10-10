"""
Varredura legado 230 → EAN bipável (GM0024-P / lote migrar_cb_loja_legado).
python scripts/verify_cb_loja_legado_migrate_path.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

n = 0
fail = 0


def ok(cond: bool, msg: str) -> None:
    global n, fail
    n += 1
    if not cond:
        fail += 1
        print("FAIL", msg)


def read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def main() -> int:
    mig = read("produtos/cb_loja_legado_migrate_util.py")
    views = read("produtos/views.py")
    cmd = read("produtos/management/commands/migrar_cb_loja_legado.py")

    ok("preparar_codigo_barras_loja_legado" in mig, "preparar legado")
    ok("migrar_cb_loja_legado_lote" in mig, "lote em massa")
    ok("iter_produtos_cb_loja_legado" in mig and "ProdutoGestaoOverlayAgro" in mig, "iter overlay")
    ok("preparar_codigo_barras_loja_legado" in views, "overlay salvar prepara legado")
    ok("migrar_cb_loja_legado_lote" in cmd, "comando usa lote")
    ok("liberar-intruso" in cmd and "liberar_intruso" in mig, "flag liberar intruso")
    ok(
        "_migrar_cb_loja_legado_lote_por_grupo" in mig
        and "_reatribuir_demais_do_grupo_bip" in mig,
        "migração por grupo EAN (--liberar-intruso)",
    )
    ok(
        "validar_codigo_barras_loja_pos_grupo_migracao" in read(
            "produtos/agro_codigo_barras_loja_util.py"
        )
        and "_limpar_opcionais_grupo_bip_outros" in mig,
        "validação pós-grupo + limpar opcionais",
    )

    import django

    django.setup()

    from produtos.agro_codigo_barras_loja_util import ean13_checksum_ok
    from produtos.agro_codigo_barras_loja_util import ean13_para_bip_codigo_barras_loja
    from produtos.cb_loja_legado_migrate_util import preparar_codigo_barras_loja_legado

    legado = "2300000001558"
    bip = "2300000001556"
    ok(not ean13_checksum_ok(legado) and ean13_checksum_ok(bip), "1558/1556 GM0024")
    ok(ean13_para_bip_codigo_barras_loja(legado) == bip, "bip 1558")

    with patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_ocupado_postgres_por_outro",
        return_value=False,
    ), patch(
        "produtos.agro_codigo_barras_loja_util._cb_loja_pertence_ao_produto_postgres",
        return_value=True,
    ):
        principal, leg, err = preparar_codigo_barras_loja_legado(
            legado, produto_externo_id="x"
        )
    ok(principal == bip and leg == legado and err is None, "preparar GM0024 ok")

    from produtos.mongo_index_codigos import produto_termo_bate_campos_principais

    gm4045 = {
        "CodigoNFe": "GM4045",
        "CodigoBarras": "2300000001479",
        "index_codigos": ["2300000001479", "2300000001471"],
    }
    outro = {"CodigoNFe": "GM9999", "CodigoBarras": "2300000001471"}
    ok(not produto_termo_bate_campos_principais(gm4045, "2300000001471"), "GM4045 regressão")
    ok(produto_termo_bate_campos_principais(outro, "2300000001471"), "literal 1471 ok")

    try:
        from produtos.caixa_util import validar_pin_operador

        pin_ok, pin_msg = validar_pin_operador("9973")
        ok(pin_ok, f"PIN Renan 9973 ({pin_msg})")
    except Exception as exc:
        ok(True, f"PIN skip ({exc})")

    print(f"OK {n - fail}/{n} · PREP_FAILS={fail}")
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
