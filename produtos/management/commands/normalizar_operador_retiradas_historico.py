"""
Normaliza usuario_lancou / criado_por em títulos «Saída caixa» — e-mail → parte local.

Causa: fallback antigo gravava admin@agro.com; devoluções já usavam username admin.

Uso:
  python manage.py normalizar_operador_retiradas_historico --dry-run
  python manage.py normalizar_operador_retiradas_historico
"""

from __future__ import annotations

from django.core.management.base import BaseCommand
from django.db.models import Q

from produtos.caixa_util import normalizar_rotulo_operador_exibicao
from produtos.models import TituloFinanceiroAgro


def _norm_campo(val: str) -> tuple[str, bool]:
    raw = (val or "").strip()
    if not raw or "@" not in raw:
        return raw, False
    novo = normalizar_rotulo_operador_exibicao(raw)
    if not novo or novo == raw:
        return raw, False
    return novo[:150], True


class Command(BaseCommand):
    help = "Corrige operador e-mail em histórico de saídas caixa (TituloFinanceiroAgro)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Só lista alterações, sem gravar.",
        )

    def handle(self, *args, **options):
        dry = bool(options.get("dry_run"))
        qs = TituloFinanceiroAgro.objects.filter(
            despesa=True,
            descricao__icontains="Saída caixa",
        ).filter(
            Q(usuario_lancou__contains="@")
            | Q(criado_por__contains="@")
        )
        total = qs.count()
        alterados = 0
        self.stdout.write(f"Encontrados {total} título(s) com @ no operador.")
        for t in qs.iterator(chunk_size=200):
            mudou = False
            ul, ch_ul = _norm_campo(t.usuario_lancou)
            cp, ch_cp = _norm_campo(t.criado_por)
            if ch_ul:
                mudou = True
                t.usuario_lancou = ul
            if ch_cp:
                mudou = True
                t.criado_por = cp
            if not mudou:
                continue
            alterados += 1
            self.stdout.write(
                f"  pk={t.pk} {t.data_competencia} "
                f"usuario_lancou={t.usuario_lancou!r} criado_por={t.criado_por!r}"
            )
            if not dry:
                t.save(update_fields=["usuario_lancou", "criado_por"])
        suf = " (dry-run)" if dry else ""
        self.stdout.write(self.style.SUCCESS(f"Alterados: {alterados}{suf}"))
