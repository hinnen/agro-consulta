"""Recupera / repara ``Produto`` a partir de itens de venda Agro (sem Mongo).

Uso:
  python manage.py recuperar_produtos_itens_venda --faltantes-recentes 90
  python manage.py recuperar_produtos_itens_venda --nome-contem kitekat --reparar
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


def _nome_parece_quebrado(nome_pg: str, nome_venda: str) -> bool:
    """True se o cadastro PG não carrega as palavras da venda (busca não acha)."""
    nv = (nome_venda or "").strip().lower()
    np = (nome_pg or "").strip().lower()
    if not nv:
        return False
    if not np or np in ("—", "-", "produto recuperado"):
        return True
    # ObjectId / hex longo no nome
    if len(np) >= 24 and all(c in "0123456789abcdef" for c in np.replace(" ", "")):
        return True
    tokens = [t for t in nv.split() if len(t) >= 3]
    if not tokens:
        return False
    faltando = [t for t in tokens if t not in np]
    # Se falta a maioria das palavras da venda → nome PG quebrado/errado
    return len(faltando) >= max(1, (len(tokens) + 1) // 2)


def _codigo_parece_ruim(codigo_pg: str, codigo_venda: str, pid: str) -> bool:
    cg = (codigo_pg or "").strip()
    cv = (codigo_venda or "").strip()
    if not cv:
        return False
    if not cg:
        return True
    if pid and cg == pid:
        return True
    if len(cg) >= 24 and all(c in "0123456789abcdef" for c in cg.lower()):
        return True
    return False


def recuperar_produtos_de_itens(
    *,
    nome_contem: str = "",
    venda_id: int | None = None,
    dias: int = 0,
    dry_run: bool = False,
    reparar: bool = True,
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
    reparados = 0
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
                fields: list[str] = []
                acao = "ja_existe"
                if p.cadastro_inativo or not p.ativo:
                    if not dry_run:
                        p.cadastro_inativo = False
                        p.ativo = True
                        fields.extend(["cadastro_inativo", "ativo"])
                    reativados += 1
                    acao = "reativado"

                if reparar:
                    if _nome_parece_quebrado(p.nome or "", nome):
                        if not dry_run:
                            p.nome = nome[:300]
                            fields.append("nome")
                            if "kitekat" in nome.lower() and not (p.marca or "").strip():
                                p.marca = "KITEKAT"
                                fields.append("marca")
                        reparados += 1
                        acao = "reparado_nome"
                    if _codigo_parece_ruim(p.codigo_interno or "", codigo, pid):
                        if not dry_run:
                            p.codigo_interno = codigo[:50]
                            p.codigo_nfe = codigo[:64]
                            fields.extend(["codigo_interno", "codigo_nfe"])
                        if acao == "ja_existe":
                            acao = "reparado_codigo"
                            reparados += 1
                        else:
                            reparados += 0  # already counted
                    # Preço 0 na ficha e venda tem preço → completa (não sobrescreve preço loja >0)
                    if (not p.preco_venda or p.preco_venda <= 0) and _dec(it.valor_unitario) > 0:
                        if not dry_run:
                            p.preco_venda = _dec(it.valor_unitario)
                            fields.append("preco_venda")

                if fields and not dry_run:
                    # unique fields list
                    fields = list(dict.fromkeys(fields))
                    p.save(update_fields=fields)

                ja_existem += 1
                detalhes.append(
                    {
                        "acao": acao,
                        "nome_venda": nome,
                        "nome_pg": (p.nome or "")[:80],
                        "pid": pid or p.produto_externo_id,
                        "codigo_venda": codigo,
                        "codigo_pg": p.codigo_interno,
                        "ativo": bool(p.ativo) and not bool(p.cadastro_inativo),
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

    if not dry_run and (criados or reativados or reparados):
        try:
            hoje = timezone.localdate().isoformat()
            cache.delete(f"pdv_catalogo_slim_v1:{hoje}")
            cache.delete(f"pdv_catalogo_slim_v2:{hoje}")
        except Exception:
            pass

    return {
        "ok": True,
        "dry_run": dry_run,
        "reparar": reparar,
        "candidatos": len(candidatos),
        "criados": criados,
        "ja_existem": ja_existem,
        "reativados": reativados,
        "reparados": reparados,
        "erros": erros,
        "detalhes": detalhes[:100],
    }


class Command(BaseCommand):
    help = "Recupera/repara produtos a partir de itens de venda (Postgres), sem Mongo."

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
        parser.add_argument(
            "--reparar",
            action="store_true",
            default=True,
            help="Corrige nome/código quebrado a partir da venda (padrão: sim)",
        )
        parser.add_argument("--sem-reparar", action="store_true", help="Não altera existentes")

    def handle(self, *args, **opts):
        reparar = not bool(opts.get("sem_reparar"))
        out = recuperar_produtos_de_itens(
            nome_contem=str(opts.get("nome_contem") or ""),
            venda_id=int(opts.get("venda") or 0) or None,
            dias=int(opts.get("dias") or 0),
            dry_run=bool(opts.get("dry_run")),
            reparar=reparar,
        )
        self.stdout.write(self.style.SUCCESS(str(out)))
