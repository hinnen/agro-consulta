"""13º como título próprio nas contas a pagar. Não entra no líquido da folha mensal."""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db import transaction

from produtos.mongo_financeiro_util import COL_DTO_LANCAMENTO, _banco_placeholder_para_select
from produtos.views import obter_conexao_mongo

from rh.models import DecimoTerceiroFuncionario, Funcionario
from rh.services.salario_financeiro_mongo import (
    _aplicar_totais_no_documento_mongo,
    _rh_financeiro_usa_postgres,
    _usuario_label,
)
from rh.services.vale_manual_financeiro import _split_id_nome
from rh.services.fechamento import primeiro_dia_mes


def _plano_decimo() -> str:
    nome = (getattr(settings, "AGRO_RH_PLANO_DECIMO_TERCEIRO", None) or "").strip()
    return nome or "Décimo terceiro"


def lancar_decimo_terceiro(
    funcionario: Funcionario,
    *,
    ano: int,
    parcela: str,
    valor: Decimal,
    data_vencimento: date,
    usuario,
    forma_value: str = "",
    banco_value: str = "",
    observacao: str = "",
) -> dict[str, Any]:
    valor = Decimal(valor).quantize(Decimal("0.01"))
    if valor <= 0:
        return {"ok": False, "erro": "Informe um valor maior que zero."}
    if parcela not in DecimoTerceiroFuncionario.Parcela.values:
        return {"ok": False, "erro": "Escolha a parcela do 13º."}

    ca = funcionario.cliente_agro
    pessoa_nome = (ca.nome or funcionario.nome_cache or "").strip()[:300]
    if not pessoa_nome:
        return {"ok": False, "erro": "Pessoa base sem nome."}
    empresa_nome = (funcionario.empresa.nome_fantasia or "").strip()
    if not empresa_nome:
        return {"ok": False, "erro": "Empresa inválida."}

    fid, fn = _split_id_nome(forma_value)
    bid, bn = _split_id_nome(banco_value)
    bn = (bn or "").strip()
    if not bn:
        ph = _banco_placeholder_para_select()
        bid = str(ph.get("id") or "").strip() or None
        bn = str(ph.get("nome") or "").strip() or "ADICIONAR BANCO"
    fn = (fn or "").strip()

    with transaction.atomic():
        reg, _criado = DecimoTerceiroFuncionario.objects.select_for_update().get_or_create(
            funcionario=funcionario,
            ano=ano,
            parcela=parcela,
            defaults={
                "empresa": funcionario.empresa,
                "valor": valor,
                "data_vencimento": data_vencimento,
                "observacao": (observacao or "")[:500],
                "criado_por": usuario if getattr(usuario, "is_authenticated", False) else None,
            },
        )
        reg.empresa = funcionario.empresa
        reg.valor = valor
        reg.data_vencimento = data_vencimento
        reg.observacao = (observacao or "")[:500]

        _, db = obter_conexao_mongo()
        use_pg = _rh_financeiro_usa_postgres(db)
        mid = (reg.mongo_lancamento_id or "").strip()
        pago = Decimal("0")
        if mid and use_pg:
            from produtos.models import TituloFinanceiroAgro

            titulo = TituloFinanceiroAgro.objects.filter(mongo_id=mid, despesa=True).first()
            if titulo is None:
                mid = ""
                reg.mongo_lancamento_id = ""
            else:
                pago = Decimal(titulo.valor_pago or 0)
        elif mid and db is not None:
            from bson import ObjectId

            doc = db[COL_DTO_LANCAMENTO].find_one({"_id": ObjectId(mid)}, {"ValorPago": 1})
            if not doc:
                mid = ""
                reg.mongo_lancamento_id = ""
            else:
                pago = Decimal(str(doc.get("ValorPago") or 0))
        if pago > valor:
            transaction.set_rollback(True)
            return {
                "ok": False,
                "erro": "O valor novo é menor do que o que já foi pago nesse título.",
            }

        if mid:
            if use_pg:
                from produtos.lancamentos_financeiro_pg_write_util import (
                    alinhar_titulo_pg_apos_sync_folha_rh,
                )

                r = alinhar_titulo_pg_apos_sync_folha_rh(
                    mid,
                    valor_bruto=valor,
                    valor_pago=pago,
                    data_vencimento=data_vencimento,
                )
                if not r.get("ok"):
                    return r
            elif db is None:
                return {"ok": False, "erro": "Financeiro indisponível."}
            else:
                r = _aplicar_totais_no_documento_mongo(
                    db,
                    mid,
                    saida=float(valor),
                    valor_pago=float(pago),
                    data_vencimento=data_vencimento,
                )
                if not r.get("ok"):
                    return r
            reg.save()
            return {"ok": True, "id": mid, "criado": False, "registro_id": reg.pk}

        pessoa_id = ((ca.externo_id or "").strip() or f"local:{ca.pk}")[:120]
        rotulo = reg.get_parcela_display()
        desc = f"13º {rotulo} — {pessoa_nome} — {ano}"[:500]
        linhas = [
            {
                "plano_conta": _plano_decimo()[:200],
                "plano_conta_id": None,
                "valor": float(valor),
                "descricao": desc,
                "observacao": (observacao or "Décimo terceiro lançado no RH")[:500],
            }
        ]
        from produtos.lancamentos_financeiro_pg_write_util import inserir_lancamentos_manual_lote_dispatch

        resultado = inserir_lancamentos_manual_lote_dispatch(
            db,
            despesa=True,
            empresa_nome=empresa_nome[:200],
            empresa_id=None,
            pessoa_nome=pessoa_nome,
            pessoa_id=pessoa_id,
            data_competencia=primeiro_dia_mes(data_vencimento),
            data_vencimento=data_vencimento,
            banco_nome=bn[:200],
            banco_id=bid,
            forma_nome=(fn[:200] if fn else ""),
            forma_id=fid,
            grupo_nome=None,
            grupo_id=None,
            usuario_label=_usuario_label(usuario),
            linhas=linhas,
        )
        ids = resultado.get("ids") or []
        if not ids:
            transaction.set_rollback(True)
            msg = "Falha ao gravar no financeiro."
            erros = resultado.get("erros") or []
            if erros:
                msg = str(erros[0].get("erro") or msg)[:400]
            return {"ok": False, "erro": msg}
        new_id = str(ids[0]).strip()
        reg.mongo_lancamento_id = new_id[:32]
        reg.save()
        return {"ok": True, "id": new_id, "criado": True, "registro_id": reg.pk}
