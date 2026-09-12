"""Audita produtos vendidos recentemente que sumiram / sumiram do PDV (Postgres).

Compara ``ItemVendaAgro`` dos últimos N dias com ``Produto``:
- ausente: não existe no cadastro PG
- inativo: existe mas cadastro_inativo / ativo=False (PDV não lista)
- ok: existe e ativo

Uso:
  python manage.py auditar_produtos_vendidos_ausentes --dias 14
  python manage.py auditar_produtos_vendidos_ausentes --dias 7 --json
"""
from __future__ import annotations

from datetime import timedelta

from django.core.management.base import BaseCommand
from django.db.models import Count, Max, Q
from django.utils import timezone


def auditar_vendidos_vs_catalogo(*, dias: int = 14, limite: int = 500) -> dict:
    from produtos.models import ItemVendaAgro, Produto

    dias = max(1, int(dias or 14))
    desde = timezone.now() - timedelta(days=dias)

    # Agrupa por id externo (preferência) senão por descrição normalizada
    rows = (
        ItemVendaAgro.objects.filter(venda__criado_em__gte=desde)
        .values("produto_id_externo", "codigo", "descricao")
        .annotate(
            n_vendas=Count("venda_id", distinct=True),
            n_linhas=Count("id"),
            ultima=Max("venda__criado_em"),
        )
        .order_by("-n_linhas")
    )

    ausentes: list[dict] = []
    inativos: list[dict] = []
    ok_n = 0
    vistos: set[str] = set()

    for r in rows.iterator(chunk_size=400):
        pid = str(r.get("produto_id_externo") or "").strip()
        codigo = str(r.get("codigo") or "").strip()
        nome = str(r.get("descricao") or "").strip()
        key = pid or f"c:{codigo.lower()}" or f"n:{nome.lower()}"
        if not key or key in vistos:
            continue
        vistos.add(key)

        p = None
        if pid:
            p = Produto.objects.filter(produto_externo_id=pid).first()
        if p is None and codigo:
            p = Produto.objects.filter(
                Q(codigo_interno__iexact=codigo) | Q(codigo_nfe__iexact=codigo)
            ).first()
        if p is None and nome:
            p = Produto.objects.filter(nome__iexact=nome).first()

        base = {
            "produto_id_externo": pid,
            "codigo": codigo,
            "descricao": nome,
            "n_linhas": int(r.get("n_linhas") or 0),
            "n_vendas": int(r.get("n_vendas") or 0),
            "ultima": (r.get("ultima").isoformat() if r.get("ultima") else ""),
        }

        if p is None:
            ausentes.append({**base, "status": "ausente"})
            if len(ausentes) + len(inativos) >= limite:
                break
            continue

        if p.cadastro_inativo or not p.ativo:
            inativos.append(
                {
                    **base,
                    "status": "inativo",
                    "produto_pk": p.pk,
                    "nome_cadastro": p.nome,
                    "cadastro_inativo": bool(p.cadastro_inativo),
                    "ativo": bool(p.ativo),
                }
            )
            if len(ausentes) + len(inativos) >= limite:
                break
            continue

        ok_n += 1

    # Totais do catálogo (contexto “perdemos tudo?”)
    total_produto = Produto.objects.count()
    total_ativos = Produto.objects.filter(cadastro_inativo=False, ativo=True).count()
    total_inativos = total_produto - total_ativos

    return {
        "ok": True,
        "dias": dias,
        "desde": desde.isoformat(),
        "skus_unicos_vendidos": len(vistos),
        "ok_no_catalogo_ativo": ok_n,
        "ausentes": ausentes,
        "inativos": inativos,
        "n_ausentes": len(ausentes),
        "n_inativos": len(inativos),
        "catalogo_total": total_produto,
        "catalogo_ativos": total_ativos,
        "catalogo_inativos": total_inativos,
        "limite": limite,
    }


class Command(BaseCommand):
    help = "Lista produtos vendidos nos últimos N dias que sumiram ou estão inativos no cadastro PG."

    def add_arguments(self, parser):
        parser.add_argument("--dias", type=int, default=14)
        parser.add_argument("--limite", type=int, default=500)
        parser.add_argument("--json", action="store_true")

    def handle(self, *args, **opts):
        import json

        out = auditar_vendidos_vs_catalogo(
            dias=int(opts.get("dias") or 14),
            limite=int(opts.get("limite") or 500),
        )
        if opts.get("json"):
            self.stdout.write(json.dumps(out, ensure_ascii=False, default=str))
            return

        self.stdout.write(
            f"Últimos {out['dias']}d · SKUs vendidos={out['skus_unicos_vendidos']} · "
            f"OK ativo={out['ok_no_catalogo_ativo']} · "
            f"AUSENTES={out['n_ausentes']} · INATIVOS={out['n_inativos']}"
        )
        self.stdout.write(
            f"Catálogo PG: total={out['catalogo_total']} ativos={out['catalogo_ativos']} "
            f"inativos={out['catalogo_inativos']}"
        )
        if out["ausentes"]:
            self.stdout.write(self.style.WARNING("--- AUSENTES (não estão no Produto) ---"))
            for a in out["ausentes"][:80]:
                self.stdout.write(
                    f"  {a['codigo'] or '—'} | {a['descricao'][:60]} | "
                    f"linhas={a['n_linhas']} id={a['produto_id_externo'][:12] or '—'}"
                )
        if out["inativos"]:
            self.stdout.write(self.style.WARNING("--- INATIVOS (existem mas PDV não lista) ---"))
            for a in out["inativos"][:80]:
                self.stdout.write(
                    f"  {a['codigo'] or '—'} | {a['descricao'][:60]} | pk={a.get('produto_pk')}"
                )
        if not out["ausentes"] and not out["inativos"]:
            self.stdout.write(
                self.style.SUCCESS(
                    "Nenhum vendido recente ausente/inativo. "
                    "Se a loja não acha, o problema é BUSCA (timeout/filtro), não cadastro."
                )
            )
