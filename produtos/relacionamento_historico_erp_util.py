"""Import único Mongo ERP → Postgres para histórico F8 (FL-042). Somente leitura no PDV."""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from produtos.models import (
    ClienteAgro,
    Produto,
    ProdutoGestaoOverlayAgro,
    RelacionamentoHistoricoImportLoteAgro,
    RelacionamentoItemHistoricoErpAgro,
    RelacionamentoVendaHistoricoErpAgro,
)
from produtos.mongo_vendas_util import (
    _nome_cliente_dto_venda,
    _nome_cliente_excluir_top_ranking,
    _valor_cabecalho_venda,
    _valor_linha_item,
)
from produtos.mongo_vendas_util import _filtro_venda_ativa_mongo

logger = logging.getLogger(__name__)

_GM_RE = re.compile(r"^GM?\d+$", re.I)


def _cfg_date(name: str, default: str) -> date:
    raw = getattr(settings, name, None)
    if raw is None:
        try:
            from decouple import config as dec_config

            raw = dec_config(name, default=default)
        except Exception:
            raw = default
    if isinstance(raw, date):
        return raw
    s = str(raw or default).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return date.fromisoformat(default)


def rel_historico_erp_habilitado() -> bool:
    raw = getattr(settings, "AGRO_REL_HISTORICO_ERP", None)
    if raw is None:
        try:
            from decouple import config as dec_config

            return dec_config("AGRO_REL_HISTORICO_ERP", default=True, cast=bool)
        except Exception:
            return True
    return bool(raw)


def rel_erp_historico_ate() -> date:
    return _cfg_date("AGRO_REL_ERP_ATE", "2026-05-26")


def rel_pdv_sisvale_desde() -> date:
    return _cfg_date("AGRO_REL_PDV_DESDE", "2026-05-27")


def _dec(val) -> Decimal:
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.01"))
    except Exception:
        return Decimal("0")


def _dec_qtd(val) -> Decimal:
    try:
        return Decimal(str(val or 0)).quantize(Decimal("0.001"))
    except Exception:
        return Decimal("0")


def _norm_cpf(cpf: str) -> str:
    return re.sub(r"\D", "", str(cpf or ""))


def _norm_codigo_gm(raw: str) -> str:
    s = str(raw or "").strip().upper()
    if not s:
        return ""
    if s.isdigit() and len(s) <= 6:
        return f"GM{s}"
    if s.startswith("GM"):
        return s
    if _GM_RE.match(s):
        return s if s.startswith("GM") else f"GM{s}"
    return s


def _id_keys(raw) -> list[str]:
    from produtos.views import _dto_venda_join_str_keys

    return _dto_venda_join_str_keys(raw)


_REL_HIST_VENDA_PROJ = {
    "Id": 1,
    "_id": 1,
    "Data": 1,
    "DataFaturamento": 1,
    "NumeroVenda": 1,
    "Numero": 1,
    "CodigoVenda": 1,
    "Codigo": 1,
    "ValorTotal": 1,
    "ValorLiquido": 1,
    "ValorFinal": 1,
    "Total": 1,
    "Valor": 1,
    "ClienteID": 1,
    "ClienteId": 1,
    "PessoaID": 1,
    "IdCliente": 1,
    "ClienteFornecedorID": 1,
    "ClienteNome": 1,
    "NomeCliente": 1,
    "cliente": 1,
    "Cliente": 1,
    "RazaoSocial": 1,
    "NomeFantasia": 1,
    "PessoaNome": 1,
    "FormaPagamento": 1,
    "NomeFormaPagamento": 1,
    "Pagamento": 1,
    "ID": 1,
    "PedidoID": 1,
    "pedido_id": 1,
    # Linhas embutidas no espelho API (quando não há DtoVendaProduto separado)
    "Produtos": 1,
    "Itens": 1,
    "Items": 1,
    "Linhas": 1,
    "PedidoItens": 1,
    "VendaProdutos": 1,
}

_EMBEDDED_ITEM_KEYS: tuple[str, ...] = (
    "Produtos",
    "Itens",
    "Items",
    "Linhas",
    "PedidoItens",
    "VendaProdutos",
)

_VENDA_FK_MONGO: tuple[str, ...] = (
    "VendaID",
    "VendaId",
    "vendaID",
    "IdVenda",
    "PedidoID",
    "PedidoId",
    "Venda",
)

_EXTRA_HEADER_JOIN_KEYS: tuple[str, ...] = ("ID", "PedidoID", "pedido_id")


def _fetch_vendas_cabecalhos_intervalo(
    db,
    desde: datetime,
    ate: datetime,
    *,
    mongo_max_time_ms: int | None = 120_000,
) -> list[dict]:
    """DtoVenda no intervalo — projeção completa (cliente + total) para import F8."""
    from produtos.views import _mongo_expr_dto_venda_data_intervalo

    filtro = _filtro_venda_ativa_mongo()
    proj = _REL_HIST_VENDA_PROJ
    vendas: list[dict] = []
    try:
        cur = db["DtoVenda"].find(
            {"$and": [{"Data": {"$gte": desde, "$lte": ate}}, filtro]},
            proj,
        )
        if mongo_max_time_ms is not None:
            cur = cur.max_time_ms(int(mongo_max_time_ms))
        vendas = list(cur)
    except Exception as exc:
        logger.warning("rel_hist_erp DtoVenda find: %s", exc)
        vendas = []
    if vendas:
        return vendas
    try:
        pipeline = [
            {
                "$match": {
                    "$and": [
                        filtro,
                        {"$expr": _mongo_expr_dto_venda_data_intervalo(desde, ate)},
                    ]
                }
            },
            {"$project": proj},
        ]
        agg_kw: dict[str, Any] = {"allowDiskUse": True}
        if mongo_max_time_ms is not None:
            agg_kw["maxTimeMS"] = int(mongo_max_time_ms)
        vendas = list(db["DtoVenda"].aggregate(pipeline, **agg_kw))
    except Exception as exc:
        logger.warning("rel_hist_erp DtoVenda aggregate: %s", exc)
    return vendas


def _eh_consumidor_nao_identificado(nome: str) -> bool:
    return _nome_cliente_excluir_top_ranking(nome)


def _cliente_id_mongo(doc: dict) -> str:
    for k in (
        "ClienteID",
        "PessoaID",
        "ClienteId",
        "IdCliente",
        "ClienteFornecedorID",
    ):
        v = doc.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s and s not in ("None", "null", "0"):
            return s
    # «Cliente» numérico (ID) — não confundir com nome string
    v = doc.get("Cliente")
    if v is not None and not isinstance(v, str):
        s = str(v).strip()
        if s and s not in ("None", "null", "0"):
            return s
    return ""


def _forma_venda_mongo(doc: dict) -> str:
    for k in (
        "FormaPagamento",
        "NomeFormaPagamento",
        "Pagamento",
        "Forma",
        "DescricaoFormaPagamento",
    ):
        v = doc.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s[:120]
    return ""


def _item_descricao(item: dict) -> str:
    for k in (
        "Descricao",
        "descricao",
        "NomeProduto",
        "Produto",
        "Nome",
        "DescricaoProduto",
        "nome",
        "name",
    ):
        v = item.get(k)
        if v and not isinstance(v, dict):
            return str(v).strip()[:300]
    return ""


def _produto_id_item(raw: dict) -> str:
    for k in (
        "ProdutoID",
        "produtoID",
        "ProdutoId",
        "IdProduto",
        "ProductID",
        "productId",
        "Codigo",
        "codigo",
        "Id",
        "ID",
    ):
        v = raw.get(k)
        if v is not None and not isinstance(v, dict):
            s = str(v).strip()
            if s and s not in ("None", "0"):
                return s
    prod = raw.get("Produto")
    if isinstance(prod, dict):
        return _produto_id_item(prod)
    return ""


def _normalizar_item_embedded(raw: dict) -> dict:
    """Linha embutida no cabeçalho DtoVenda → formato compatível com DtoVendaProduto."""
    pid = _produto_id_item(raw)
    qtd = raw.get("Quantidade") or raw.get("quantidade") or raw.get("Qtd") or raw.get("qtd") or 0
    out = dict(raw)
    if pid:
        out["ProdutoID"] = pid
    out["Quantidade"] = qtd
    if not _item_descricao(out):
        prod = raw.get("Produto")
        if isinstance(prod, dict):
            dsc = _item_descricao(prod)
            if dsc:
                out["Descricao"] = dsc
    return out


def _itens_embedded_cabecalho(doc: dict) -> list[dict]:
    out: list[dict] = []
    for k in _EMBEDDED_ITEM_KEYS:
        val = doc.get(k)
        if not isinstance(val, list):
            continue
        for it in val:
            if isinstance(it, dict):
                norm = _normalizar_item_embedded(it)
                if _produto_id_item(norm) or _item_descricao(norm):
                    out.append(norm)
    return out


def _build_query_itens_venda(venda_ids_obj: list, venda_ids_scalar: list) -> dict | None:
    ors: list[dict] = []
    for fld in _VENDA_FK_MONGO:
        if venda_ids_obj:
            ors.append({fld: {"$in": venda_ids_obj}})
        if venda_ids_scalar:
            ors.append({fld: {"$in": venda_ids_scalar}})
    return {"$or": ors} if ors else None


def _venda_ids_para_query_import(vendas_headers: list) -> tuple[list, list]:
    from produtos.views import _dto_venda_join_str_keys, _dto_venda_venda_produto_in_lists
    from bson import ObjectId

    oids, scalars = _dto_venda_venda_produto_in_lists(vendas_headers)
    seen_o: set[str] = {str(x) for x in oids}
    seen_s: set[str] = set()
    for s in scalars:
        seen_s.add(f"s:{s}" if isinstance(s, str) else f"o:{type(s).__name__}:{s!r}")

    def add_scalar(x) -> None:
        k = f"s:{x}" if isinstance(x, str) else f"o:{type(x).__name__}:{x!r}"
        if k not in seen_s:
            seen_s.add(k)
            scalars.append(x)

    # ObjectId no cabeçalho → VendaID string na linha (Mongo não iguala tipo)
    for oid in list(oids):
        add_scalar(str(oid))

    for doc in vendas_headers:
        for hk in _EXTRA_HEADER_JOIN_KEYS + ("Codigo", "CodigoVenda", "NumeroVenda", "Numero"):
            raw = doc.get(hk)
            if raw is None:
                continue
            for jk in _dto_venda_join_str_keys(raw):
                add_scalar(jk)
            if isinstance(raw, int):
                add_scalar(raw)
            elif isinstance(raw, str) and raw.isdigit():
                try:
                    add_scalar(int(raw))
                except (TypeError, ValueError):
                    pass
            if isinstance(raw, ObjectId):
                so = str(raw)
                if so not in seen_o:
                    seen_o.add(so)
                    oids.append(raw)
                add_scalar(so)
    return oids, scalars


def _build_mongo_pessoa_index(db, client_m) -> dict[str, dict[str, str]]:
    """DtoPessoa: chaves de ID → nome + CPF (ponte ClienteID da venda → ClienteAgro)."""
    from produtos.views import (
        _colecoes_pessoa_disponiveis,
        _documento_pessoa,
        _nome_exibicao_pessoa,
        _projecao_pessoa,
    )

    index: dict[str, dict[str, str]] = {}
    if db is None:
        return index
    proj = _projecao_pessoa()
    for coll in _colecoes_pessoa_disponiveis(db, client_m):
        try:
            cursor = db[coll].find({}, proj)
        except Exception as exc:
            logger.warning("rel_hist_erp pessoa %s: %s", coll, exc)
            continue
        for doc in cursor:
            nome = _nome_exibicao_pessoa(doc)
            cpf = _norm_cpf(_documento_pessoa(doc))
            if not nome and not cpf:
                continue
            entry = {"nome": nome, "cpf": cpf}
            for raw in (doc.get("Id"), doc.get("_id"), doc.get("PessoaID")):
                for k in _id_keys(raw):
                    index[k] = entry
        if index:
            break
    return index


def _build_mapas_cliente() -> tuple[dict[str, ClienteAgro], dict[str, ClienteAgro], dict[str, ClienteAgro], dict[str, int]]:
    por_ext: dict[str, ClienteAgro] = {}
    por_cpf: dict[str, ClienteAgro] = {}
    por_nome: dict[str, ClienteAgro] = {}
    meta = {"ativos": 0, "com_externo_id": 0}
    for cli in ClienteAgro.objects.filter(ativo=True).only("pk", "externo_id", "cpf", "nome"):
        meta["ativos"] += 1
        ext = str(cli.externo_id or "").strip()
        if ext:
            meta["com_externo_id"] += 1
            for k in _id_keys(ext):
                if k not in por_ext:
                    por_ext[k] = cli
        cpf = _norm_cpf(cli.cpf)
        if len(cpf) >= 11 and cpf not in por_cpf:
            por_cpf[cpf] = cli
        nome = (cli.nome or "").strip().upper()
        if nome and nome not in por_nome:
            por_nome[nome] = cli
    return por_ext, por_cpf, por_nome, meta


def _match_cliente(
    cliente_id_erp: str,
    nome_snapshot: str,
    por_ext: dict[str, ClienteAgro],
    por_cpf: dict[str, ClienteAgro],
    por_nome: dict[str, ClienteAgro],
    pessoa_idx: dict[str, dict[str, str]] | None = None,
) -> ClienteAgro | None:
    cid = str(cliente_id_erp or "").strip()
    for k in _id_keys(cid) if cid else []:
        if k in por_ext:
            return por_ext[k]

    nome = (nome_snapshot or "").strip().upper()
    if nome and nome in por_nome:
        return por_nome[nome]

    cpf = _norm_cpf(cid)
    if len(cpf) >= 11 and cpf in por_cpf:
        return por_cpf[cpf]

    if pessoa_idx and cid:
        pessoa = None
        for k in _id_keys(cid):
            pessoa = pessoa_idx.get(k)
            if pessoa:
                break
        if pessoa:
            for k in _id_keys(cid):
                if k in por_ext:
                    return por_ext[k]
            pnome = (pessoa.get("nome") or "").strip().upper()
            if pnome and pnome in por_nome:
                return por_nome[pnome]
            pcpf = pessoa.get("cpf") or ""
            if len(pcpf) >= 11 and pcpf in por_cpf:
                return por_cpf[pcpf]
    return None


def _codigo_gm_de_produto_mongo(doc: dict | None) -> str:
    if not doc:
        return ""
    candidatos: list[str] = []
    for k in (
        "CodigoNFe",
        "Codigo",
        "CodigoInterno",
        "CodigoBarras",
        "Referencia",
        "Sku",
        "SKU",
    ):
        v = doc.get(k)
        if v:
            candidatos.append(str(v).strip())
    ix = doc.get("index_codigos") or doc.get("IndexCodigos")
    if isinstance(ix, list):
        candidatos.extend(str(x).strip() for x in ix if x)
    for c in candidatos:
        n = _norm_codigo_gm(c)
        if n.startswith("GM") and any(ch.isdigit() for ch in n):
            return n[:64]
    for c in candidatos:
        if c:
            return c[:64]
    return ""


def _venda_id_erp(doc: dict) -> str:
    keys = _venda_join_keys_header(doc)
    return keys[0][:64] if keys else ""


def _venda_join_keys_header(doc: dict) -> list[str]:
    """Todas as chaves do cabeçalho para casar com ``DtoVendaProduto.VendaID`` (H2)."""
    from produtos.views import _HEADER_KEYS_VENDA_JOIN, _dto_venda_join_str_keys

    out: list[str] = []
    seen: set[str] = set()
    for hk in _HEADER_KEYS_VENDA_JOIN + _EXTRA_HEADER_JOIN_KEYS:
        for k in _dto_venda_join_str_keys(doc.get(hk)):
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


def _aware_data_venda(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if timezone.is_naive(dt):
        return timezone.make_aware(dt, timezone.get_current_timezone())
    return dt


def _itens_raw_venda(itens_por_venda: dict[str, list[dict]], join_keys: list[str]) -> list[dict]:
    """Linhas Mongo da venda — tenta todas as chaves do cabeçalho (Id vs NumeroVenda)."""
    if not join_keys:
        return []
    out: list[dict] = []
    seen: set[str] = set()
    for jk in join_keys:
        for it in itens_por_venda.get(jk, []):
            uid = str(it.get("_id") or id(it))
            if uid in seen:
                continue
            seen.add(uid)
            out.append(it)
    return out


def _fetch_produtos_mongo(db, produto_ids: set[str]) -> dict[str, dict]:
    if not produto_ids or db is None:
        return {}
    from produtos.views import _mongo_ids_para_query_in

    ids = _mongo_ids_para_query_in(list(produto_ids))
    if not ids:
        return {}
    proj = {
        "Id": 1,
        "_id": 1,
        "CodigoNFe": 1,
        "Codigo": 1,
        "CodigoInterno": 1,
        "CodigoBarras": 1,
        "Referencia": 1,
        "index_codigos": 1,
        "IndexCodigos": 1,
        "Nome": 1,
    }
    out: dict[str, dict] = {}
    try:
        for doc in db["DtoProduto"].find({"$or": [{"Id": {"$in": ids}}, {"_id": {"$in": ids}}]}, proj):
            for k in ("Id", "_id"):
                key = str(doc.get(k) or "")
                if key:
                    out[key] = doc
    except Exception as exc:
        logger.warning("rel_hist_erp DtoProduto: %s", exc)
    return out


def _overlay_codigo_por_produto_id(produto_ids: set[str]) -> dict[str, str]:
    if not produto_ids:
        return {}
    q = Q()
    for pid in produto_ids:
        if pid:
            q |= Q(produto_externo_id=str(pid))
    out: dict[str, str] = {}
    for row in ProdutoGestaoOverlayAgro.objects.filter(q).values("produto_externo_id", "codigo_nfe"):
        pid = str(row.get("produto_externo_id") or "")
        cod = _norm_codigo_gm(row.get("codigo_nfe") or "")
        if pid and cod:
            out[pid] = cod
    return out


def normalizar_codigo_gm_rel(raw: str) -> str:
    return _norm_codigo_gm(raw)


def codigos_gm_ativos_no_catalogo(codigos: list[str]) -> set[str]:
    """Códigos GM vendáveis no PDV: overlay + Produto PG + Mongo DtoProduto ativo."""
    norms = {_norm_codigo_gm(c) for c in codigos if str(c or "").strip()}
    norms = {n for n in norms if n}
    if not norms:
        return set()

    found: set[str] = set()
    q = Q()
    for c in norms:
        q |= Q(codigo_nfe__iexact=c)
    for raw in ProdutoGestaoOverlayAgro.objects.filter(q).values_list("codigo_nfe", flat=True):
        n = _norm_codigo_gm(raw)
        if n:
            found.add(n)

    missing = norms - found
    if missing:
        try:
            q_pg = Q()
            for c in missing:
                q_pg |= Q(codigo_nfe__iexact=c)
            for raw in Produto.objects.filter(q_pg, cadastro_inativo=False).values_list(
                "codigo_nfe", flat=True
            ):
                n = _norm_codigo_gm(raw)
                if n in missing:
                    found.add(n)
        except Exception as exc:
            logger.warning("codigos_gm_ativos produto pg: %s", exc)
        missing = norms - found

    if missing:
        try:
            from produtos.views import obter_conexao_mongo

            _client, db = obter_conexao_mongo()
            if db is not None:
                ors: list[dict] = []
                for c in missing:
                    ors.append({"CodigoNFe": c})
                    ors.append({"Codigo": c})
                    if c.upper().startswith("GM"):
                        tail = c[2:].lstrip("0") or c[2:]
                        if tail.isdigit():
                            ors.append({"Codigo": int(tail)})
                            ors.append({"Codigo": tail})
                            ors.append({"CodigoNFe": tail})
                if ors:
                    proj = {"CodigoNFe": 1, "Codigo": 1, "CodigoInterno": 1, "index_codigos": 1}
                    for doc in db["DtoProduto"].find(
                        {"$and": [{"CadastroInativo": {"$ne": True}}, {"$or": ors}]},
                        proj,
                        max_time_ms=8000,
                    ).limit(max(50, len(missing) * 4)):
                        gm = _codigo_gm_de_produto_mongo(doc)
                        n = _norm_codigo_gm(gm)
                        if n in missing:
                            found.add(n)
        except Exception as exc:
            logger.warning("codigos_gm_ativos mongo: %s", exc)

    return found


def venda_ids_erp_ja_importados() -> set[str]:
    return set(
        RelacionamentoVendaHistoricoErpAgro.objects.values_list("venda_id_erp", flat=True).distinct()
    )


def _add_months(d: date, months: int) -> date:
    m = d.month - 1 + months
    y = d.year + m // 12
    m = m % 12 + 1
    day = min(d.day, [31, 29 if y % 4 == 0 and (y % 100 != 0 or y % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return date(y, m, day)


def importar_historico_erp_mongo(
    *,
    ate: date | None = None,
    pdv_desde: date | None = None,
    desde: date | None = None,
    lote_id: str = "",
    dry_run: bool = False,
    chunk_meses: int = 1,
) -> dict[str, Any]:
    from produtos.views import (
        _dto_venda_resolve_data_cabecalho,
        _dto_venda_venda_produto_in_lists,
        obter_conexao_mongo,
    )

    ate = ate or rel_erp_historico_ate()
    pdv_desde = pdv_desde or rel_pdv_sisvale_desde()
    desde = desde or date(2015, 1, 1)
    lote_id = (lote_id or "").strip() or f"erp-hist-{timezone.localdate().isoformat()}"

    stats: dict[str, Any] = {
        "lote_id": lote_id,
        "dry_run": dry_run,
        "erp_ate": ate.isoformat(),
        "pdv_desde": pdv_desde.isoformat(),
        "desde": desde.isoformat(),
        "cabecalhos_lidos": 0,
        "vendas_no_corte": 0,
        "vendas_consumidor": 0,
        "vendas_sem_cliente": 0,
        "vendas_duplicadas": 0,
        "vendas_fora_corte": 0,
        "vendas_importadas": 0,
        "itens_importados": 0,
        "itens_mongo_linhas": 0,
        "itens_embedded": 0,
        "vendas_sem_itens": 0,
        "itens_sem_codigo_catalogo": 0,
        "dto_venda_produto_total": 0,
        "clientes_com_venda": 0,
        "clientes_agro_ativos": 0,
        "clientes_agro_com_externo_id": 0,
        "pessoas_mongo_index": 0,
    }

    if not dry_run and RelacionamentoHistoricoImportLoteAgro.objects.filter(lote_id=lote_id).exists():
        return {"ok": False, "erro": f"Lote «{lote_id}» já existe. Use outro --lote ou reverta antes."}

    client, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível."}

    try:
        stats["dto_venda_produto_total"] = int(db["DtoVendaProduto"].estimated_document_count())
    except Exception:
        stats["dto_venda_produto_total"] = -1

    por_ext, por_cpf, por_nome, cli_meta = _build_mapas_cliente()
    stats["clientes_agro_ativos"] = cli_meta.get("ativos", 0)
    stats["clientes_agro_com_externo_id"] = cli_meta.get("com_externo_id", 0)
    pessoa_idx = _build_mongo_pessoa_index(db, client)
    stats["pessoas_mongo_index"] = len(pessoa_idx)
    ja_importados = venda_ids_erp_ja_importados()

    dt_ate_fim = datetime.combine(ate, datetime.max.time())
    cursor = datetime.combine(desde, datetime.min.time())
    vendas_buffer: list[dict] = []
    clientes_vistos: set[int] = set()

    while cursor.date() <= ate:
        fim_d = _add_months(cursor.date(), max(1, int(chunk_meses)))
        fim_chunk = datetime.combine(fim_d, datetime.min.time()) - timedelta(seconds=1)
        if fim_chunk > dt_ate_fim:
            fim_chunk = dt_ate_fim
        headers = _fetch_vendas_cabecalhos_intervalo(db, cursor, fim_chunk, mongo_max_time_ms=120_000)
        stats["cabecalhos_lidos"] += len(headers)
        for doc in headers:
            dt = _dto_venda_resolve_data_cabecalho(doc)
            if dt is None:
                continue
            d = dt.date()
            if d > ate:
                stats["vendas_fora_corte"] += 1
                continue
            if d >= pdv_desde:
                stats["vendas_fora_corte"] += 1
                continue
            stats["vendas_no_corte"] += 1
            vid = _venda_id_erp(doc)
            if not vid:
                continue
            if vid in ja_importados:
                stats["vendas_duplicadas"] += 1
                continue
            nome_snap = _nome_cliente_dto_venda(doc)
            if _eh_consumidor_nao_identificado(nome_snap):
                stats["vendas_consumidor"] += 1
                continue
            cid = _cliente_id_mongo(doc)
            cli = _match_cliente(
                cid, nome_snap, por_ext, por_cpf, por_nome, pessoa_idx=pessoa_idx
            )
            if cli is None:
                stats["vendas_sem_cliente"] += 1
                continue
            total = _valor_cabecalho_venda(doc)
            vendas_buffer.append(
                {
                    "doc": doc,
                    "venda_id_erp": vid,
                    "join_keys": _venda_join_keys_header(doc),
                    "cliente": cli,
                    "cliente_id_erp": cid,
                    "cliente_nome_snapshot": nome_snap or cli.nome,
                    "data_venda": _aware_data_venda(dt),
                    "total": total,
                    "forma": _forma_venda_mongo(doc),
                }
            )
            clientes_vistos.add(cli.pk)
            ja_importados.add(vid)
        cursor = fim_chunk + timedelta(seconds=1)

    stats["clientes_com_venda"] = len(clientes_vistos)

    if dry_run:
        produto_ids: set[str] = set()
        # amostra itens para estimar órfãos (primeiras 200 vendas)
        amostra = vendas_buffer[:200]
        if amostra:
            venda_ids_obj, venda_ids_scalar = _venda_ids_para_query_import([x["doc"] for x in amostra])
            query_itens = _build_query_itens_venda(venda_ids_obj, venda_ids_scalar)
            if query_itens:
                try:
                    for item in db["DtoVendaProduto"].find(query_itens).limit(5000):
                        pid = str(item.get("ProdutoID") or "").strip()
                        if pid:
                            produto_ids.add(pid)
                except Exception as exc:
                    logger.warning("rel_hist_erp dry-run itens: %s", exc)
        overlay_codigos = _overlay_codigo_por_produto_id(produto_ids)
        prod_map = _fetch_produtos_mongo(db, produto_ids)
        sem_cat = 0
        for pid in produto_ids:
            cod = overlay_codigos.get(pid) or _codigo_gm_de_produto_mongo(prod_map.get(pid))
            if not cod or cod not in codigos_gm_ativos_no_catalogo([cod]):
                sem_cat += 1
        stats["itens_sem_codigo_catalogo"] = sem_cat
        stats["vendas_importadas"] = len(vendas_buffer)
        return {"ok": True, "stats": stats}

    lote = RelacionamentoHistoricoImportLoteAgro.objects.create(
        lote_id=lote_id,
        erp_ate=ate,
        pdv_desde=pdv_desde,
        dry_run=False,
        stats_json=stats,
    )

    batch_size = 150
    for i in range(0, len(vendas_buffer), batch_size):
        chunk = vendas_buffer[i : i + batch_size]
        venda_ids_obj, venda_ids_scalar = _venda_ids_para_query_import([x["doc"] for x in chunk])
        query_itens = _build_query_itens_venda(venda_ids_obj, venda_ids_scalar)
        itens_por_venda: dict[str, list[dict]] = defaultdict(list)
        produto_ids: set[str] = set()
        if query_itens:
            try:
                from produtos.views import _dto_venda_join_str_keys

                for item in db["DtoVendaProduto"].find(query_itens):
                    stats["itens_mongo_linhas"] += 1
                    vid_raw = (
                        item.get("VendaID")
                        or item.get("VendaId")
                        or item.get("IdVenda")
                        or item.get("PedidoID")
                        or item.get("Venda")
                    )
                    keys = _dto_venda_join_str_keys(vid_raw)
                    if not keys:
                        continue
                    pid = str(item.get("ProdutoID") or "").strip()
                    if pid:
                        produto_ids.add(pid)
                    for k in keys:
                        itens_por_venda[k].append(item)
            except Exception as exc:
                logger.warning("rel_hist_erp itens batch: %s", exc)

        for row in chunk:
            for it in _itens_embedded_cabecalho(row["doc"]):
                pid = _produto_id_item(it)
                if pid:
                    produto_ids.add(pid)

        overlay_codigos = _overlay_codigo_por_produto_id(produto_ids)
        prod_map = _fetch_produtos_mongo(db, produto_ids)
        codigos_check: list[str] = []
        for pid in produto_ids:
            codigos_check.append(overlay_codigos.get(pid) or _codigo_gm_de_produto_mongo(prod_map.get(pid)))
        ativos = codigos_gm_ativos_no_catalogo(codigos_check)

        with transaction.atomic():
            for row in chunk:
                vid = row["venda_id_erp"]
                itens_raw = _itens_raw_venda(itens_por_venda, row.get("join_keys") or [vid])
                if not itens_raw:
                    emb = _itens_embedded_cabecalho(row["doc"])
                    if emb:
                        itens_raw = emb
                        stats["itens_embedded"] += len(emb)
                total = row["total"]
                if total is None or total == 0:
                    soma = Decimal("0")
                    for it in itens_raw:
                        soma += _valor_linha_item(it)
                    total = soma if soma else Decimal("0")
                else:
                    total = _dec(total)

                venda_obj = RelacionamentoVendaHistoricoErpAgro.objects.create(
                    lote=lote,
                    cliente_agro=row["cliente"],
                    venda_id_erp=vid,
                    cliente_id_erp=row["cliente_id_erp"],
                    cliente_nome_snapshot=row["cliente_nome_snapshot"],
                    data_venda=row["data_venda"],
                    total=total,
                    forma_pagamento=row["forma"],
                )
                stats["vendas_importadas"] += 1
                bulk_itens: list[RelacionamentoItemHistoricoErpAgro] = []
                for it in itens_raw[:40]:
                    pid = str(it.get("ProdutoID") or "").strip()
                    cod = overlay_codigos.get(pid) or _codigo_gm_de_produto_mongo(prod_map.get(pid))
                    cod = _norm_codigo_gm(cod) if cod else ""
                    if cod and cod not in ativos:
                        stats["itens_sem_codigo_catalogo"] += 1
                    qtd = _dec_qtd(it.get("Quantidade") or it.get("quantidade") or 0)
                    vtot = _dec(_valor_linha_item(it))
                    vunit = _dec(vtot / qtd) if qtd else Decimal("0")
                    bulk_itens.append(
                        RelacionamentoItemHistoricoErpAgro(
                            venda=venda_obj,
                            produto_id_erp=pid[:64],
                            codigo_gm=cod[:64],
                            descricao=_item_descricao(it)[:300],
                            quantidade=qtd,
                            valor_unitario=vunit,
                            valor_total=vtot,
                        )
                    )
                if bulk_itens:
                    RelacionamentoItemHistoricoErpAgro.objects.bulk_create(bulk_itens)
                    stats["itens_importados"] += len(bulk_itens)
                else:
                    stats["vendas_sem_itens"] += 1

    lote.stats_json = stats
    lote.save(update_fields=["stats_json"])
    return {"ok": True, "stats": stats, "lote_id": lote_id}


def probe_itens_venda_mongo(*, limite: int = 3, ate: date | None = None) -> dict[str, Any]:
    """Diagnóstico: DtoVendaProduto vs cabeçalho (amostra no corte ERP, não só vendas novas)."""
    from produtos.views import obter_conexao_mongo

    ate = ate or rel_erp_historico_ate()
    client, db = obter_conexao_mongo()
    if db is None:
        return {"ok": False, "erro": "Mongo indisponível."}
    out: dict[str, Any] = {"ok": True, "amostras": [], "erp_ate": ate.isoformat()}
    try:
        out["dto_venda_total"] = int(db["DtoVenda"].estimated_document_count())
        out["dto_venda_produto_total"] = int(db["DtoVendaProduto"].estimated_document_count())
    except Exception as exc:
        out["contagem_erro"] = str(exc)
    cols = sorted(n for n in db.list_collection_names() if "venda" in n.lower() or "pedido" in n.lower())
    out["colecoes_venda_pedido"] = cols[:30]

    try:
        one_item = db["DtoVendaProduto"].find_one({}, {"VendaID": 1, "VendaId": 1, "ProdutoID": 1})
        if one_item:
            vid = one_item.get("VendaID") or one_item.get("VendaId")
            out["item_mongo_amostra"] = {
                "VendaID": repr(vid),
                "VendaID_tipo": type(vid).__name__,
                "keys": sorted(str(k) for k in one_item.keys())[:16],
            }
    except Exception as exc:
        out["item_mongo_amostra"] = {"erro": str(exc)}

    dt_ate = datetime.combine(ate, datetime.max.time())
    try:
        cur = (
            db["DtoVenda"]
            .find({"Data": {"$lte": dt_ate}}, _REL_HIST_VENDA_PROJ)
            .sort("Data", -1)
            .limit(max(1, int(limite)))
        )
        docs = list(cur)
    except Exception as exc:
        return {"ok": False, "erro": f"DtoVenda sample: {exc}"}
    for doc in docs:
        join_keys = _venda_join_keys_header(doc)
        oids, scalars = _venda_ids_para_query_import([doc])
        q = _build_query_itens_venda(oids, scalars)
        n_mongo = 0
        sample_item_keys: list[str] = []
        if q:
            try:
                one = db["DtoVendaProduto"].find_one(q)
                if one:
                    sample_item_keys = sorted(str(k) for k in one.keys())[:24]
                n_mongo = db["DtoVendaProduto"].count_documents(q)
            except Exception as exc:
                n_mongo = -1
                sample_item_keys = [f"erro: {exc}"]
        emb = _itens_embedded_cabecalho(doc)
        dt = doc.get("Data")
        out["amostras"].append(
            {
                "venda_id": _venda_id_erp(doc),
                "data": dt.isoformat()[:19] if isinstance(dt, datetime) else str(dt)[:19],
                "join_keys": join_keys[:8],
                "query_scalars_n": len(scalars),
                "query_oids_n": len(oids),
                "cabecalho_keys": sorted(str(k) for k in doc.keys())[:40],
                "itens_mongo": n_mongo,
                "itens_embedded": len(emb),
                "embedded_keys": [k for k in _EMBEDDED_ITEM_KEYS if isinstance(doc.get(k), list) and doc.get(k)],
                "item_mongo_keys": sample_item_keys,
            }
        )
    return out


def reverter_historico_erp(*, lote_id: str = "", tudo: bool = False) -> dict[str, Any]:
    lote_id = (lote_id or "").strip()
    if tudo:
        n_v = RelacionamentoVendaHistoricoErpAgro.objects.count()
        n_l = RelacionamentoHistoricoImportLoteAgro.objects.count()
        RelacionamentoHistoricoImportLoteAgro.objects.all().delete()
        return {"ok": True, "vendas_removidas": n_v, "lotes_removidos": n_l}
    if not lote_id:
        return {"ok": False, "erro": "Informe --lote ou --tudo."}
    lote = RelacionamentoHistoricoImportLoteAgro.objects.filter(lote_id=lote_id).first()
    if not lote:
        return {"ok": False, "erro": f"Lote «{lote_id}» não encontrado."}
    n_v = lote.vendas.count()
    lote.delete()
    return {"ok": True, "vendas_removidas": n_v, "lote_id": lote_id}


def resumo_historico_erp_cliente(cli: ClienteAgro) -> dict[str, Any]:
    if not rel_historico_erp_habilitado():
        return {"ativo": False, "n_vendas": 0, "venda_ids": []}
    qs = RelacionamentoVendaHistoricoErpAgro.objects.filter(cliente_agro=cli).order_by("-data_venda")
    ids = list(qs.values_list("pk", flat=True)[:500])
    return {
        "ativo": True,
        "erp_ate": rel_erp_historico_ate().isoformat(),
        "pdv_desde": rel_pdv_sisvale_desde().isoformat(),
        "n_vendas": qs.count(),
        "venda_ids": ids,
    }
