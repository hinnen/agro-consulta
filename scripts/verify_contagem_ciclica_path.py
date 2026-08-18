#!/usr/bin/env python
"""Verify paths/strings — Contagem cíclica (Ajuste Mobile)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
fails: list[str] = []
oks = 0


def check(path: str, *needles: str) -> None:
    global oks
    p = ROOT / path
    if not p.exists():
        fails.append(f"MISSING {path}")
        return
    text = p.read_text(encoding="utf-8", errors="replace")
    for n in needles:
        if n not in text:
            fails.append(f"{path} missing {n!r}")
        else:
            oks += 1


check(
    "estoque/migrations/0016_contagem_ciclica.py",
    "ContagemCiclicaSessao",
    "contagem_ciclica",
    "uniq_ciclica_sessao_produto",
)
check(
    "estoque/migrations/0017_contagem_ciclica_dias_mov.py",
    "dias_movimentacao",
    "contagemciclicasessao",
)
check(
    "estoque/models.py",
    "CONTAGEM_CICLICA",
    "ContagemCiclicaSessao",
    "ContagemCiclicaLinha",
    "ContagemCiclicaParticipante",
    "ContagemCiclicaEscopo",
    "ContagemCiclicaStatus",
    "dias_movimentacao",
)
check(
    "produtos/contagem_ciclica_util.py",
    "abrir_sessao",
    "registrar_contagem",
    "fechar_passagem_1",
    "gravar_fechamento",
    "cancelar_sessao",
    "sessao_payload",
    "OrigemAjusteEstoque.CONTAGEM_CICLICA",
    "auto_zero_pass1",
    '"cego": True',
    "Sempre soma",
    "_pids_com_movimento",
    "dias_movimentacao",
    "linhas_truncadas",
)
check(
    "produtos/contagem_ciclica_views.py",
    "api_ciclica_abrir",
    "api_ciclica_contar",
    "api_ciclica_fechar_pass1",
    "api_ciclica_gravar",
    "api_ciclica_entrar",
    "api_ciclica_cancelar",
    "sessao_gate_ok",
)
check(
    "produtos/urls.py",
    "api/ajuste-mobile/ciclica/abrir/",
    "api/ajuste-mobile/ciclica/<int:pk>/contar/",
    "api/ajuste-mobile/ciclica/<int:pk>/fechar-pass1/",
    "api/ajuste-mobile/ciclica/<int:pk>/gravar/",
    "api/ajuste-mobile/ciclica/<int:pk>/cancelar/",
    "contagem_ciclica_views",
)
check(
    "produtos/templates/produtos/mobile_ajuste.html",
    "ma-btn-ciclica",
    "maCiclicaAtiva",
    "salvarCiclica",
    "Somar (cíclica)",
    "ma-ciclica-bar",
    "Fechar passagem 1",
    "Gravar estoque",
    "ma-ciclica-btn-cancelar",
    "maCiclicaCancelarSessao",
    "Cancelar contagem",
    "maCiclicaAtiva()",
    "Bip +1 soma na contagem",
    "maCiclicaBipMaisUm",
    "maCiclicaBipFilaDrain",
    "MA_CICLICA_BIP_GAP_MS",
    "maBipPiscarTela",
    "sempre soma",
    "ma-ciclica-dias",
    "60 dias (padrão)",
    "FALTA",
    "Recontagem · faltam",
    "maLockScroll",
    "Incluir fora",
    "forcar",
)
check(
    "produtos/pg_backup_registry.py",
    "estoque.ContagemCiclicaSessao",
    "estoque.ContagemCiclicaLinha",
)
check("scripts/verify_contagem_ciclica_deep.py", "VERIFY_DEEP_OK", "gravar_fechamento")
check(
    "scripts/verify_ajuste_ciclica_bip1_path.py",
    "VERIFY_BIP1_CICLICA_OK",
    "3x qtd=1",
    "maPostAjuste",
)

# Runtime: cancelar sessão descartável (não toca sessão real da loja)
try:
    import os

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    import django

    django.setup()
    from estoque.models import (
        ContagemCiclicaEscopo,
        ContagemCiclicaSessao,
        ContagemCiclicaStatus,
    )
    from produtos.contagem_ciclica_util import cancelar_sessao

    s = ContagemCiclicaSessao.objects.create(
        deposito="centro",
        escopo_tipo=ContagemCiclicaEscopo.LOJA,
        escopo_valor="",
        status=ContagemCiclicaStatus.PASS1,
        passagem_atual=1,
        aberta_por_rotulo="verify-path-cancel",
        dias_movimentacao=60,
    )
    cancelar_sessao(s)
    s.refresh_from_db()
    if s.status != ContagemCiclicaStatus.CANCELADA:
        fails.append(f"cancelar_sessao status={s.status}")
    else:
        oks += 1
    cancelar_sessao(s)  # idempotente
    oks += 1
    s.status = ContagemCiclicaStatus.FECHADA
    s.save(update_fields=["status"])
    try:
        cancelar_sessao(s)
        fails.append("cancelar FECHADA deveria falhar")
    except ValueError:
        oks += 1
    ContagemCiclicaSessao.objects.filter(pk=s.pk).delete()
except Exception as e:
    fails.append(f"runtime cancel: {e}")

print(f"checks_ok={oks} fails={len(fails)}")
for f in fails:
    print("FAIL", f)
if fails:
    sys.exit(1)
print("VERIFY_OK")
