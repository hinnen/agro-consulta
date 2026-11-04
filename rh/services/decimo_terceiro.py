"""13º como título próprio nas contas a pagar. Não entra no líquido da folha mensal."""
from __future__ import annotations

import calendar
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


def _dias_trabalhados_no_mes(admissao: date, ano: int, mes: int) -> int:
    ultimo = calendar.monthrange(ano, mes)[1]
    inicio = date(ano, mes, 1)
    fim = date(ano, mes, ultimo)
    if admissao > fim:
        return 0
    if admissao > inicio:
        inicio = admissao
    return (fim - inicio).days + 1


def sugerir_decimo(funcionario: Funcionario, ano: int) -> dict[str, Any]:
    """Sugestão pela Lei 4.090: 1/12 do salário vigente em dezembro por mês com 15 dias ou mais.

    Salário fixo não usa média. Hora extra e comissão não estão no cadastro.
    INSS e IR não entram aqui: vão como desconto na 2ª parcela, quando o escritório passar.
    """
    from rh.services.fechamento import money_two_decimals, salario_vigente_em

    admissao = funcionario.data_admissao
    salario = salario_vigente_em(funcionario, date(ano, 12, 1))
    if salario <= 0:
        salario = salario_vigente_em(funcionario, date(ano, 12, 31))
    meses = []
    if admissao:
        for mes in range(1, 13):
            dias = _dias_trabalhados_no_mes(admissao, ano, mes)
            if dias >= 15:
                meses.append(mes)
    avos = len(meses)
    bruto = money_two_decimals((salario / Decimal("12")) * Decimal(avos)) if salario > 0 else Decimal("0")
    primeira = money_two_decimals(bruto / Decimal("2")) if bruto else Decimal("0")
    segunda = money_two_decimals(bruto - primeira) if bruto else Decimal("0")
    return {
        "admissao": admissao,
        "salario": salario,
        "avos": avos,
        "bruto": bruto,
        "primeira": primeira,
        "segunda": segunda,
        "vence_primeira": date(ano, 11, 30),
        "vence_segunda": date(ano, 12, 20),
        "sem_admissao": admissao is None,
        "sem_salario": salario <= 0,
    }


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
    valor_titulo: Decimal | None = None,
) -> dict[str, Any]:
    valor = Decimal(valor).quantize(Decimal("0.01"))
    a_pagar = valor if valor_titulo is None else Decimal(valor_titulo).quantize(Decimal("0.01"))
    if valor <= 0 or a_pagar <= 0:
        return {"ok": False, "erro": "Informe um valor a pagar maior que zero."}
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
            from bson.errors import InvalidId

            try:
                oid = ObjectId(mid)
            except (InvalidId, TypeError):
                oid = None
            doc = db[COL_DTO_LANCAMENTO].find_one({"_id": oid}, {"ValorPago": 1}) if oid else None
            if not doc:
                mid = ""
                reg.mongo_lancamento_id = ""
            else:
                pago = Decimal(str(doc.get("ValorPago") or 0))
        if pago > a_pagar:
            transaction.set_rollback(True)
            return {
                "ok": False,
                "erro": "O valor a pagar ficou menor do que o que já foi pago nesse título.",
            }

        if mid:
            if use_pg:
                from produtos.lancamentos_financeiro_pg_write_util import (
                    alinhar_titulo_pg_apos_sync_folha_rh,
                )

                r = alinhar_titulo_pg_apos_sync_folha_rh(
                    mid,
                    valor_bruto=a_pagar,
                    valor_pago=pago,
                    data_vencimento=data_vencimento,
                )
                if not r.get("ok"):
                    transaction.set_rollback(True)
                    return r
            elif db is None:
                transaction.set_rollback(True)
                return {"ok": False, "erro": "Financeiro indisponível."}
            else:
                r = _aplicar_totais_no_documento_mongo(
                    db,
                    mid,
                    saida=float(a_pagar),
                    valor_pago=float(pago),
                    data_vencimento=data_vencimento,
                )
                if not r.get("ok"):
                    transaction.set_rollback(True)
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
                "valor": float(a_pagar),
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
