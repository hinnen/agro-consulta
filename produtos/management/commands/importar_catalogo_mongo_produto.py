"""Importa catálogo Mongo (DtoProduto) → PostgreSQL (Produto). Rodar no staging antes de AGRO_FONTE_CATALOGO=agro_pg."""
from __future__ import annotations

from decimal import Decimal

from django.core.management.base import BaseCommand

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


def executar_importar_catalogo_mongo_produto(
    *,
    limit: int = 0,
    skip: int = 0,
    dry_run: bool = False,
) -> dict:
    client, db = obter_conexao_mongo()
    if db is None or client is None:
        return {"ok": False, "erro": "Mongo indisponível."}

    limit = max(0, int(limit or 0))
    skip = max(0, int(skip or 0))

    col = client.col_p
    try:
        total_mongo = int(db[col].estimated_document_count())
    except Exception:
        try:
            total_mongo = int(db[col].count_documents({}))
        except Exception:
            total_mongo = -1

    cur = db[col].find({}).skip(skip)
    if limit:
        cur = cur.limit(limit)

    criados = atualizados = erros = ignorados_sem_id = 0

    for doc in cur:
        try:
            raw_id = doc.get("Id")
            if raw_id is None or str(raw_id).strip() == "":
                raw_id = doc.get("_id")
            pid = _txt(raw_id, 64)
            if not pid:
                ignorados_sem_id += 1
                continue
            codigo = _txt(doc.get("CodigoNFe") or doc.get("Codigo") or pid, 50) or pid[:50]
            cb = _txt(_extrair_codigo_barras(doc), 50) or None
            nome = _txt(doc.get("Nome") or "—", 300) or "—"
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
                continue
            _obj, created = Produto.objects.update_or_create(
                produto_externo_id=pid,
                defaults=defaults,
            )
            if created:
                criados += 1
            else:
                atualizados += 1
        except Exception:
            erros += 1

    try:
        total_pg = int(Produto.objects.count())
    except Exception:
        total_pg = -1

    return {
        "ok": True,
        "criados": criados,
        "atualizados": atualizados,
        "erros": erros,
        "ignorados_sem_id": ignorados_sem_id,
        "total_mongo": total_mongo,
        "total_pg": total_pg,
        "dry_run": dry_run,
        "limit": limit,
        "skip": skip,
    }


class Command(BaseCommand):
    help = "Importa DtoProduto (Mongo espelho ERP) para a tabela Produto (PostgreSQL)."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Máximo de documentos (0 = todos)")
        parser.add_argument("--skip", type=int, default=0)
        parser.add_argument("--dry-run", action="store_true")

    def handle(self, *args, **options):
        out = executar_importar_catalogo_mongo_produto(
            limit=int(options.get("limit") or 0),
            skip=int(options.get("skip") or 0),
            dry_run=bool(options.get("dry_run")),
        )
        if not out.get("ok"):
            self.stderr.write(out.get("erro") or "Falha.")
            return
        self.stdout.write(
            self.style.SUCCESS(
                "Importação concluída — criados={criados} atualizados={atualizados} erros={erros}{dry}".format(
                    criados=out["criados"],
                    atualizados=out["atualizados"],
                    erros=out["erros"],
                    dry=" (dry-run)" if out.get("dry_run") else "",
                )
            )
        )
