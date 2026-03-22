import json
from typing import Any

from django.conf import settings

from integracoes.venda_erp_api import VendaERPAPIClient


def _compactar_dict(dados: dict[str, Any]) -> dict[str, Any]:
    return {
        chave: valor
        for chave, valor in dados.items()
        if valor not in (None, "")
    }


def enviar_para_pdv(codigo: str, quantidade: int = 1):
    codigo = str(codigo or "").strip()
    if not codigo:
        return False, "Código vazio."

    codigo_campo = getattr(settings, "VENDA_ERP_PDV_CODIGO_CAMPO", "codigoBarras")
    quantidade_campo = getattr(settings, "VENDA_ERP_PDV_QUANTIDADE_CAMPO", "quantidade")

    valor_item = {
        codigo_campo: codigo,
        quantidade_campo: quantidade,
    }

    payload = _compactar_dict(
        {
            "tipoOperacao": getattr(settings, "VENDA_ERP_PDV_TIPO_OPERACAO", None),
            "caixa": getattr(settings, "VENDA_ERP_PDV_CAIXA", ""),
            "caixaID": getattr(settings, "VENDA_ERP_PDV_CAIXA_ID", ""),
            "empresa": getattr(settings, "VENDA_ERP_PDV_EMPRESA", ""),
            "empresaID": getattr(settings, "VENDA_ERP_PDV_EMPRESA_ID", ""),
            "usuario": getattr(settings, "VENDA_ERP_PDV_USUARIO", ""),
            "usuarioID": getattr(settings, "VENDA_ERP_PDV_USUARIO_ID", ""),
            "valores": [valor_item],
        }
    )

    client = VendaERPAPIClient()
    sucesso, status_code, resposta = client.salvar_operacao_pdv(payload)

    if sucesso:
        return True, f"Operação PDV registrada com sucesso. HTTP {status_code}."

    resposta_legivel = resposta
    try:
        if isinstance(resposta, (dict, list)):
            resposta_legivel = json.dumps(resposta, ensure_ascii=False)
    except Exception:
        pass

    return False, (
        f"Falha ao registrar operação PDV. HTTP {status_code}. "
        f"Payload enviado: {payload}. Resposta: {resposta_legivel}"
    )