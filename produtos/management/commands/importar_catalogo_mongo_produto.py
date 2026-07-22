"""Importa catálogo Mongo (DtoProduto) → PostgreSQL (Produto).

Modo seguro (loja / emergência):
  python manage.py importar_catalogo_mongo_produto --somente-faltantes

Só **cria** produto que não existe no PG (mesmo ``produto_externo_id``).
Não altera preço/custo/nome dos que já estão no Postgres.
Saldo (ledger/ajustes) nunca é tocado.
"""
from __future__ import annotations

from decimal import Decimal

from django.core.cache import cache
from django.core.management.base import BaseCommand
from django.utils import timezone

from produtos.models import Produto
from produtos.views import obter_conexao_mongo, _extrair_codigo_barras


def _txt(v, mx=300):
    return str(v or "").strip()[:mx]


def _dec(v):
    try:
        return Decimal(str(v).replace(",", ".")).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _erp_id_decimal(doc: dict) -> str:
    raw = _txt(doc.get("Id") or doc.get("_id"), 64)
    digits = "".join(ch for ch in raw if ch.isdigit())
    if digits and len(digits) <= 18:
        return digits
    cod = _txt(doc.get("CodigoNFe") or doc.get("Codigo"), 64)
    cd = "".join(ch for ch in cod if ch.isdigit())
    return cd if cd else raw


def _invalidar_slim_pdv() -> None:
    try:
        hoje = timezone.localdate().isoformat()
        cache.delete(f"pdv_catalogo_slim_v1:{hoje}")
    except Exception:
        pass


def executar_importar_catalogo_mongo_produto(
    *,
    limit: int = 0,
    skip: int = 0,
    dry_run: bool = False,
    somente_faltantes: bool = False,
) -> dict:
    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return {"ok": False, "erro": "Mongo indisponível."}

    limit = max(0, int(limit or 0))
    skip = max(0, int(skip or 0))
    somente_faltantes = bool(somente_faltantes)

    col = client.col_p
    try:
        total_mongo = int(db[col].estimated_document_count())
    except Exception:
        try:
            total_mongo = int(db[col].count_documents({}))
        except Exception:
            total_mongo = -1

    # IDs já no PG — evita update_or_create em massa (protege preço loja).
    ids_pg: set[str] = set()
    if somente_faltantes:
        ids_pg = set(
            Produto.objects.exclude(produto_externo_id__isnull=True)
            .exclude(produto_externo_id="")
            .values_list("produto_externo_id", flat=True)
        )

    cur = db[col].find({}).skip(skip)
    if limit:
        cur = cur.limit(limit)

    criados = atualizados = erros = ignorados_sem_id = ignorados_fantasma = 0
    ja_existem = 0
    amostras_criados: list[str] = []

    for doc in cur:
        try:
            raw_id = doc.get("Id")
            if raw_id is None or str(raw_id).strip() == "":
                raw_id = doc.get("_id")
            pid = _txt(raw_id, 64)
            if not pid:
                ignorados_sem_id += 1
                continue
            from produtos.catalogo_nome_util import (
                deve_ignorar_import_mongo_fantasma,
                nome_parece_objectid_corrupto,
            )

            if deve_ignorar_import_mongo_fantasma(doc, pid):
                ignorados_fantasma += 1
                continue

            if somente_faltantes and pid in ids_pg:
                ja_existem += 1
                continue

            codigo = _txt(doc.get("CodigoNFe") or doc.get("Codigo") or pid, 50) or pid[:50]
            cb = _txt(_extrair_codigo_barras(doc), 50) or None
            nome = _txt(doc.get("Nome") or "—", 300) or "—"

            if nome_parece_objectid_corrupto(nome, pid):
                nome = "—"
            defaults = {
                "codigo_interno": codigo,
                "codigo_barras": cb,
                "codigo_nfe": _txt(doc.get("CodigoNFe") or doc.get("Codigo"), 64),
                "erp_produto_id": _erp_id_decimal(doc)[:64],
                "nome": nome,
                "marca": _txt(doc.get("Marca"), 120),
                "categoria": _txt(
                    doc.get("NomeCategoria") or doc.get("Categoria") or doc.get("Grupo"), 200
                ),
                "subcategoria": _txt(
                    doc.get("SubGrupo") or doc.get("Subcategoria") or doc.get("NomeSubcategoria"),
                    200,
                ),
                "fornecedor_texto": _txt(
                    doc.get("NomeFornecedor") or doc.get("Fornecedor") or doc.get("Fabricante"),
                    300,
                ),
                "unidade": _txt(doc.get("Unidade") or doc.get("SiglaUnidade") or "UN", 20) or "UN",
                "descricao": _txt(
                    doc.get("Descricao") or doc.get("Observacao") or doc.get("Complemento"), 16000
                ),
                "ncm": _txt(doc.get("NCM") or doc.get("CodigoNCM"), 16),
                "custo": _dec(doc.get("PrecoCusto") or doc.get("ValorCusto")),
                "preco_venda": _dec(doc.get("ValorVenda") or doc.get("PrecoVenda")),
                "cadastro_inativo": bool(doc.get("CadastroInativo")),
                "cadastro_somente_agro": bool(
                    doc.get("CadastroSomenteAgro") or doc.get("cadastroSomenteAgro")
                ),
                "ativo": not bool(doc.get("CadastroInativo")),
            }
            if dry_run:
                criados += 1
                if len(amostras_criados) < 15:
                    amostras_criados.append(f"{pid}|{nome[:60]}|{defaults['preco_venda']}")
                continue

            from produtos.catalogo_agro import defaults_import_com_overlay

            defaults = defaults_import_com_overlay(pid, defaults)

            if somente_faltantes:
                # create-only: se corrida criou no meio, não sobrescreve
                obj, created = Produto.objects.get_or_create(
                    produto_externo_id=pid,
                    defaults=defaults,
                )
                if created:
                    criados += 1
                    ids_pg.add(pid)
                    if len(amostras_criados) < 15:
                        amostras_criados.append(f"{pid}|{nome[:60]}|{obj.preco_venda}")
                else:
                    ja_existem += 1
            else:
                _obj, created = Produto.objects.update_or_create(
                    produto_externo_id=pid,
                    defaults=defaults,
                )
                if created:
                    criados += 1
                    if len(amostras_criados) < 15:
                        amostras_criados.append(f"{pid}|{nome[:60]}|{_obj.preco_venda}")
                else:
                    atualizados += 1
        except Exception:
            erros += 1

    if not dry_run and criados:
        _invalidar_slim_pdv()

    try:
        total_pg = int(Produto.objects.count())
    except Exception:
        total_pg = -1

    return {
        "ok": True,
        "criados": criados,
        "atualizados": atualizados,
        "ja_existem": ja_existem,
        "erros": erros,
        "ignorados_sem_id": ignorados_sem_id,
        "ignorados_fantasma": ignorados_fantasma,
        "total_mongo": total_mongo,
        "total_pg": total_pg,
        "dry_run": dry_run,
        "somente_faltantes": somente_faltantes,
        "limit": limit,
        "skip": skip,
        "amostras_criados": amostras_criados,
    }


class Command(BaseCommand):
    help = (
        "Importa DtoProduto (Mongo) → Produto (PG). "
        "Use --somente-faltantes na loja: não altera preço dos existentes."
    )

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Máximo de documentos (0 = todos)")
        parser.add_argument("--skip", type=int, default=0)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument(
            "--somente-faltantes",
            action="store_true",
            help="Só cria IDs ausentes no PG; não atualiza preço/custo/nome dos existentes.",
        )

    def handle(self, *args, **options):
        out = executar_importar_catalogo_mongo_produto(
            limit=int(options.get("limit") or 0),
            skip=int(options.get("skip") or 0),
            dry_run=bool(options.get("dry_run")),
            somente_faltantes=bool(options.get("somente_faltantes")),
        )
        if not out.get("ok"):
            self.stderr.write(out.get("erro") or "Falha.")
            return
        self.stdout.write(self.style.SUCCESS(str(out)))
