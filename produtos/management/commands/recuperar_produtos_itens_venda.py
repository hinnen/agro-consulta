"""Recupera ``Produto`` ausentes a partir de itens de venda Agro (sem Mongo).

Uso (Render Shell / one-off):
  python manage.py recuperar_produtos_itens_venda --nome-contem kitekat
  python manage.py recuperar_produtos_itens_venda --venda 3751
  python manage.py recuperar_produtos_itens_venda --faltantes-recentes 7 --dry-run
"""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.utils import timezone


def _dec(v) -> Decimal:
    try:
        return Decimal(str(v or 0))
    except Exception:
        return Decimal("0")


def recuperar_produtos_de_itens(
    *,
    nome_contem: str = "",
    venda_id: int | None = None,
    dias: int = 0,
    dry_run: bool = False,
) -> dict:
    from django.core.cache import cache
    from produtos.models import ItemVendaAgro, Produto

    qs = ItemVendaAgro.objects.all().order_by("-id")
    if venda_id:
        qs = qs.filter(venda_id=int(venda_id))
    if nome_contem.strip():
        qs = qs.filter(descricao__icontains=nome_contem.strip())
    if dias and dias > 0:
        desde = timezone.now() - timedelta(days=int(dias))
        qs = qs.filter(venda__criado_em__gte=desde)

    vistos: set[str] = set()
    candidatos: list[ItemVendaAgro] = []
    for it in qs.iterator(chunk_size=500):
        pid = str(it.produto_id_externo or "").strip()
        key = pid or f"nome:{(it.descricao or '').strip().lower()}"
        if not key or key in vistos:
            continue
        vistos.add(key)
        candidatos.append(it)

    criados = 0
    ja_existem = 0
    reativados = 0
    erros = 0
    detalhes: list[dict] = []

    for it in candidatos:
        pid = str(it.produto_id_externo or "").strip()
        nome = (it.descricao or "").strip() or "Produto recuperado"
        codigo = (it.codigo or "").strip() or (pid[:50] if pid else f"REC-{it.id}")
        try:
            p = None
            if pid:
                p = Produto.objects.filter(produto_externo_id=pid).first()
            if p is None and codigo:
                p = Produto.objects.filter(
                    Q(codigo_interno__iexact=codigo) | Q(codigo_nfe__iexact=codigo)
                ).first()
            if p is None:
                p = Produto.objects.filter(nome__iexact=nome).first()

            if p is not None:
                mudou = False
                if p.cadastro_inativo or not p.ativo:
                    if not dry_run:
                        p.cadastro_inativo = False
                        p.ativo = True
                        p.save(update_fields=["cadastro_inativo", "ativo"])
                    reativados += 1
                    mudou = True
                ja_existem += 1
                detalhes.append(
                    {
                        "acao": "reativado" if mudou else "ja_existe",
                        "nome": nome,
                        "pid": pid or p.produto_externo_id,
                        "codigo": codigo,
                    }
                )
                continue

            if dry_run:
                criados += 1
                detalhes.append(
                    {"acao": "criaria", "nome": nome, "pid": pid, "codigo": codigo}
                )
                continue

            pid_final = pid or f"agro-rec-{it.id}"
            # Evita colisão unique
            if Produto.objects.filter(produto_externo_id=pid_final).exists():
                pid_final = f"agro-rec-{it.id}-{it.venda_id}"

            marca = "KITEKAT" if "kitekat" in nome.lower() else ""

            Produto.objects.create(
                produto_externo_id=pid_final[:64],
                erp_produto_id=pid_final[:64],
                codigo_interno=codigo[:50],
                codigo_nfe=codigo[:64],
                nome=nome[:300],
                marca=marca,
                unidade="UN",
                preco_venda=_dec(it.valor_unitario),
                custo=Decimal("0"),
                cadastro_inativo=False,
                ativo=True,
                cadastro_somente_agro=True,
            )
            criados += 1
            detalhes.append(
                {"acao": "criado", "nome": nome, "pid": pid_final, "codigo": codigo}
            )
        except Exception as e:
            erros += 1
            detalhes.append({"acao": "erro", "nome": nome, "erro": str(e)[:200]})

    if not dry_run and (criados or reativados):
        try:
            # limpa slim do dia
            hoje = timezone.localdate().isoformat()
            cache.delete(f"pdv_catalogo_slim_v1:{hoje}")
        except Exception:
            pass

    return {
        "ok": True,
        "dry_run": dry_run,
        "candidatos": len(candidatos),
        "criados": criados,
        "ja_existem": ja_existem,
        "reativados": reativados,
        "erros": erros,
        "detalhes": detalhes[:80],
    }


class Command(BaseCommand):
    help = "Recupera produtos ausentes a partir de itens de venda (Postgres), sem Mongo."

    def add_arguments(self, parser):
        parser.add_argument("--nome-contem", default="", help="Filtro na descrição do item")
        parser.add_argument("--venda", type=int, default=0, help="PK da VendaAgro")
        parser.add_argument(
            "--faltantes-recentes",
            type=int,
            default=0,
            dest="dias",
            help="Olhar itens dos últimos N dias",
        )
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **opts):
        out = recuperar_produtos_de_itens(
            nome_contem=str(opts.get("nome_contem") or ""),
            venda_id=int(opts.get("venda") or 0) or None,
            dias=int(opts.get("dias") or 0),
            dry_run=bool(opts.get("dry_run")),
        )
        self.stdout.write(self.style.SUCCESS(str(out)))
