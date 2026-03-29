"""Sincroniza clientes de Mongo (DtoPessoa) e API ERP para ClienteAgro — somente leitura nas fontes."""

import logging
import re
from typing import Any

from django.conf import settings

from .models import ClienteAgro

logger = logging.getLogger(__name__)


def _doc_para_campo_cpf(doc: str) -> str:
    raw = str(doc or "").strip()
    if raw in ("", "—"):
        return ""
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return raw[:14]
    return digits[:14]


def _telefone_para_whatsapp(tel: str) -> str:
    return re.sub(r"\D", "", str(tel or "").strip())[:20]


def sincronizar_clientes_fontes_para_agro(
    *,
    max_mongo: int = 50_000,
    max_erp: int = 50_000,
) -> dict[str, Any]:
    """
    Importa/atualiza ClienteAgro a partir de Mongo + Pessoas/GetAll.
    Não envia nada ao ERP. Clientes com editado_local=True não são sobrescritos.
    """
    # Import cycle: views define Mongo/ERP helpers
    from produtos import views as produtos_views

    criados = 0
    atualizados = 0
    ignorados_editados = 0
    pulados = 0

    fontes: list[tuple[str, list[dict[str, Any]]]] = []

    res_mongo: list[dict[str, Any]] = []
    client_m, db = produtos_views.obter_conexao_mongo()
    if db is not None:
        try:
            proj = produtos_views._projecao_pessoa()
            for coll in produtos_views._colecoes_pessoa_disponiveis(db, client_m):
                try:
                    clis = list(db[coll].find({}, proj).limit(max_mongo))
                except Exception as exc:
                    logger.warning("sync clientes: ignorando coleção %s: %s", coll, exc)
                    continue
                res_mongo = produtos_views._montar_linhas_cliente(clis)
                if res_mongo:
                    break
        except Exception:
            logger.exception("sync clientes mongo")
    fontes.append(("mongo", res_mongo))

    res_erp: list[dict[str, Any]] = []
    try:
        res_erp = produtos_views._clientes_lista_via_erp_api(max_total=max_erp)
    except Exception:
        logger.exception("sync clientes erp")
    fontes.append(("erp_api", res_erp))

    seen_ids: set[str] = set()

    for origem, rows in fontes:
        for row in rows:
            eid = str((row or {}).get("id") or "").strip()
            nome = str((row or {}).get("nome") or "").strip()
            if not eid or len(nome) < 2:
                pulados += 1
                continue
            if eid in seen_ids:
                continue
            seen_ids.add(eid)

            doc = (row or {}).get("documento") or ""
            tel = (row or {}).get("telefone") or ""
            endereco_val = str((row or {}).get("endereco") or "").strip()[:500]
            cep_val = str((row or {}).get("cep") or "").strip()[:12]
            uf_val = str((row or {}).get("uf") or "").strip()[:2].upper()
            cidade_val = str((row or {}).get("cidade") or "").strip()[:120]
            bairro_val = str((row or {}).get("bairro") or "").strip()[:120]
            logr_val = str((row or {}).get("logradouro") or "").strip()[:300]
            num_val = str((row or {}).get("numero") or "").strip()[:30]
            comp_val = str((row or {}).get("complemento") or "").strip()[:200]
            cpf_val = _doc_para_campo_cpf(str(doc))
            wa = _telefone_para_whatsapp(str(tel))

            try:
                cli = ClienteAgro.objects.filter(externo_id=eid).first()
            except Exception:
                logger.exception("sync clientes query externo_id=%s", eid[:40])
                continue

            if cli:
                if cli.editado_local:
                    ignorados_editados += 1
                    continue
                cli.nome = nome[:200]
                cli.cpf = cpf_val
                cli.whatsapp = wa
                cli.cep = cep_val
                cli.uf = uf_val
                cli.cidade = cidade_val
                cli.bairro = bairro_val
                cli.logradouro = logr_val
                cli.numero = num_val
                cli.complemento = comp_val
                cli.endereco = endereco_val
                cli.origem_import = origem
                cli.ativo = True
                cli.save(
                    update_fields=[
                        "nome",
                        "cpf",
                        "whatsapp",
                        "cep",
                        "uf",
                        "cidade",
                        "bairro",
                        "logradouro",
                        "numero",
                        "complemento",
                        "endereco",
                        "origem_import",
                        "ativo",
                        "atualizado_em",
                    ]
                )
                atualizados += 1
            else:
                ClienteAgro.objects.create(
                    externo_id=eid,
                    nome=nome[:200],
                    cpf=cpf_val,
                    whatsapp=wa,
                    cep=cep_val,
                    uf=uf_val,
                    cidade=cidade_val,
                    bairro=bairro_val,
                    logradouro=logr_val,
                    numero=num_val,
                    complemento=comp_val,
                    endereco=endereco_val,
                    ativo=True,
                    origem_import=origem,
                    editado_local=False,
                )
                criados += 1

    out = {
        "ok": True,
        "criados": criados,
        "atualizados": atualizados,
        "ignorados_editados_local": ignorados_editados,
        "pulados_sem_id_ou_nome": pulados,
        "linhas_mongo": len(res_mongo),
        "linhas_erp": len(res_erp),
        "unicos_importados": len(seen_ids),
    }
    if settings.DEBUG:
        logger.info("sincronizar_clientes_fontes_para_agro: %s", out)
    return out
