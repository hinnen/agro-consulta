"""Painel NFC-e por venda — consulta / reemissão."""
from __future__ import annotations

from typing import Any

from produtos.models import NfceDocumentoAgro, VendaAgro
from produtos.nfce_config_util import nfce_config_resumo, nfce_configurada


def venda_nfce_pendente(venda: VendaAgro) -> bool:
    nfce = getattr(venda, "nfce", None)
    if nfce and nfce.status == NfceDocumentoAgro.Status.AUTORIZADA:
        return False
    if getattr(venda, "nfce_solicitada", False):
        return True
    if nfce and nfce.status in (
        NfceDocumentoAgro.Status.REJEITADA,
        NfceDocumentoAgro.Status.ERRO,
    ):
        return True
    return False


def _erro_nfce_venda(venda: VendaAgro, nfce: NfceDocumentoAgro | None) -> str:
    if nfce:
        msg = (nfce.mensagem_sefaz or "").strip()
        if msg:
            return msg
        if nfce.status == NfceDocumentoAgro.Status.REJEITADA:
            return "NFC-e rejeitada pela SEFAZ."
        if nfce.status == NfceDocumentoAgro.Status.ERRO:
            return "Erro técnico na emissão da NFC-e."
    if getattr(venda, "nfce_solicitada", False):
        return "Cupom fiscal não foi emitido nesta venda."
    return ""


def painel_nfce_venda(venda: VendaAgro, *, _cfg: dict[str, Any] | None = None) -> dict[str, Any]:
    cfg = _cfg if _cfg is not None else nfce_config_resumo()
    ativo = bool(cfg.get("ativo"))
    nfce = getattr(venda, "nfce", None)
    solic = bool(getattr(venda, "nfce_solicitada", False))
    out: dict[str, Any] = {
        "ativo": ativo,
        "solicitada": solic,
        "pendente": False,
        "autorizada": False,
        "status": "",
        "status_label": "",
        "erro": "",
        "numero": None,
        "serie": None,
        "chave": "",
        "protocolo": "",
        "dest_cpf": "",
        "consumidor_sem_identificacao": False,
        "pode_reemitir": False,
        "pode_cancelar": False,
        "pode_imprimir_fiscal": False,
        "documento_id": nfce.pk if nfce else None,
    }
    if not ativo:
        if solic or (nfce and nfce.status != NfceDocumentoAgro.Status.AUTORIZADA):
            out["pendente"] = True
            out["erro"] = "NFC-e desligada ou incompleta no servidor (.env)."
            out["pode_reemitir"] = False
        return out
    if nfce:
        out["dest_cpf"] = nfce.dest_cpf or ""
        out["consumidor_sem_identificacao"] = bool(nfce.consumidor_sem_identificacao)
        out["numero"] = nfce.numero or None
        out["serie"] = nfce.serie or None
        out["chave"] = nfce.chave or ""
        out["protocolo"] = nfce.protocolo or ""
        out["status"] = nfce.status
        out["status_label"] = nfce.get_status_display()
    elif venda.cliente_documento:
        out["dest_cpf"] = venda.cliente_documento[:11]

    if nfce and nfce.status == NfceDocumentoAgro.Status.AUTORIZADA:
        out["autorizada"] = True
        out["status_label"] = "Autorizada"
        out["pode_imprimir_fiscal"] = True
        out["pode_cancelar"] = nfce_configurada()
        return out

    if nfce and nfce.status == NfceDocumentoAgro.Status.CANCELADA:
        out["status_label"] = "Cancelada"
        out["erro"] = nfce.mensagem_sefaz or "NFC-e cancelada na SEFAZ."
        return out

    if venda_nfce_pendente(venda):
        out["pendente"] = True
        out["erro"] = _erro_nfce_venda(venda, nfce)
        out["pode_reemitir"] = not venda.devolvida_em and nfce_configurada()
        if not out["status_label"]:
            out["status_label"] = "Pendente"
        return out

    if solic:
        out["status_label"] = "Não solicitada"
    return out


def registrar_nfce_erro_venda(
    venda: VendaAgro,
    mensagem: str,
    *,
    cpf_dest: str = "",
    sem_identificacao: bool = False,
    tp_amb: int = 2,
) -> NfceDocumentoAgro:
    NfceDocumentoAgro.objects.filter(venda=venda).exclude(
        status=NfceDocumentoAgro.Status.AUTORIZADA
    ).delete()
    return NfceDocumentoAgro.objects.create(
        venda=venda,
        status=NfceDocumentoAgro.Status.ERRO,
        mensagem_sefaz=(mensagem or "Erro na NFC-e.")[:2000],
        dest_cpf=cpf_dest or "",
        consumidor_sem_identificacao=sem_identificacao,
        tp_amb=tp_amb,
    )
