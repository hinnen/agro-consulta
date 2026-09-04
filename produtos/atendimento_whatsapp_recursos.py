"""
Recursos extras WhatsApp × SisVale — todos DESLIGADOS por padrão.

Renan liga um a um em Bot → Recursos. Código atrás de ``recurso_on``.
"""
from __future__ import annotations

from typing import Any

# id da flag · rótulo na tela · dica curta
RECURSOS_CATALOGO: list[tuple[str, str, str]] = [
    ("feat_pdv_abre_zap", "PDV abre o Zap", "Ícone do PDV abre o chat (em vez de «Em breve»)."),
    ("feat_pdv_aviso_msg", "Aviso no PDV", "Som + badge quando chega mensagem nova."),
    ("feat_respostas_prontas", "Respostas prontas", "Botões de texto rápido no chat."),
    ("feat_xfer_nota", "Nota ao transferir", "Observação interna ao passar Centro↔Vila."),
    ("feat_fiado_pix", "Fiado + Pix", "Depois do saldo, bot lembra Pix / pagar na loja."),
    ("feat_orcamento_zap", "Orçamento no Zap", "Enviar orçamento do PDV pelo chat da loja."),
    ("feat_lembrete_fiado", "Lembrete fiado", "Avisar cliente marcado (atraso) — sem disparo em massa."),
    ("feat_comprovante_venda", "Comprovante de venda", "Texto «sua compra» no Zap após venda."),
    ("feat_entrega_status", "Status de entrega", "Avisar cliente: saiu / a caminho / chegou."),
    ("feat_pedir_loja_aviso", "Pedir loja → Zap", "Avisa a outra loja quando pedido muda de status."),
    ("feat_lista_espera", "Lista de espera", "Avisar quando produto sem estoque chegar."),
    ("feat_fornecedor_zap", "Folha p/ fornecedor", "Atalho de Compras pelo Zap da loja."),
    ("feat_menu_curto", "Menu curto (1·2·3)", "1 Fiado · 2 Horário · 3 Atendente (além Centro/Vila)."),
    ("feat_audio_texto", "Áudio → texto", "Transcrever áudio do cliente (quando ligado)."),
    ("feat_relatorio_dia", "Relatório do dia", "Resumo: chats, tempo, quem atendeu (PIN)."),
    ("feat_vip_tag", "VIP / alerta no chat", "Marcar cliente (fiado alto, sempre Vila…)."),
    ("feat_ponte_backup", "Ponte backup", "2º PC se o 1º cair (só prepara; sem auto ainda)."),
    ("feat_horario_bot", "Horário reforçado", "Usa textos de horário do Bot com mais clareza."),
]

RECURSO_IDS = [r[0] for r in RECURSOS_CATALOGO]

# Defaults: TUDO off
RECURSOS_DEFAULT: dict[str, Any] = {rid: False for rid in RECURSO_IDS}
RECURSOS_DEFAULT.update(
    {
        "respostas_prontas": (
            "Temos sim|Orçamento já já|Pode retirar na loja|"
            "Entrega na terça|Já verifico e te retorno"
        ),
        "msg_fiado_pix_extra": (
            "\n\nPara pagar: passe na loja ou peça a chave Pix por aqui."
        ),
        "msg_menu_curto_extra": (
            "\n\nAtalhos:\n"
            "*F* — fiado\n"
            "*H* — horário / endereço\n"
            "*A* — falar com atendente"
        ),
        "msg_horario_loja": (
            "Horário *{empresa}*: seg–sáb {ini}–{fim}.\n"
            "Endereço e dúvidas: responda por aqui."
        ),
        "msg_comprovante_venda": (
            "Olá, {nome}! Compra *#{venda}* · total *{total}*.\n"
            "Obrigado pela preferência — *{empresa}*."
        ),
        "msg_entrega_saiu": "Olá, {nome}! Seu pedido *saiu para entrega*.",
        "msg_entrega_caminho": "Olá, {nome}! Entrega *a caminho*.",
        "msg_entrega_chegou": "Olá, {nome}! Entrega *concluída*. Qualquer coisa, estamos aqui.",
        "msg_lembrete_fiado": (
            "Olá, {nome}. Lembrete do fiado em aberto: *{total}*.\n"
            "Passe na loja quando puder. *{empresa}*"
        ),
        "msg_lista_espera": (
            "Olá, {nome}! O item *{produto}* que você pediu *chegou* na loja."
        ),
    }
)


def recurso_on(cfg: dict | None, key: str) -> bool:
    from produtos.atendimento_whatsapp_bot_config import cfg_flag

    return cfg_flag(cfg, key, default=False)


def flags_recursos(cfg: dict | None) -> dict[str, bool]:
    return {rid: recurso_on(cfg, rid) for rid in RECURSO_IDS}


def catalogo_para_api(cfg: dict | None = None) -> list[dict]:
    flags = flags_recursos(cfg)
    out = []
    for rid, titulo, dica in RECURSOS_CATALOGO:
        out.append({"id": rid, "titulo": titulo, "dica": dica, "ligado": bool(flags.get(rid))})
    return out


def conversa_extras(conv) -> dict:
    raw = getattr(conv, "extras", None)
    return dict(raw) if isinstance(raw, dict) else {}


def salvar_conversa_extras(conv, patch: dict) -> dict:
    cur = conversa_extras(conv)
    cur.update(patch or {})
    conv.extras = cur
    conv.save(update_fields=["extras"])
    return cur


def _empresa(cfg: dict) -> str:
    return str((cfg or {}).get("nome_empresa") or "loja")


def enviar_texto_cliente(conversa_id: int, texto: str, *, autor: str = "Sistema") -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_util import enviar_loja

    m, err = enviar_loja(conversa_id=int(conversa_id), texto=texto, autor=autor)
    if err or m is None:
        return False, err or "Não enviou."
    return True, ""


def acao_comprovante_venda(
    *,
    conversa_id: int,
    venda: str,
    total: str,
    nome: str = "",
    autor: str = "",
) -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_comprovante_venda"):
        return False, "Recurso desligado (Bot → Recursos)."
    tpl = str(cfg.get("msg_comprovante_venda") or RECURSOS_DEFAULT["msg_comprovante_venda"])
    txt = (
        tpl.replace("{nome}", (nome or "cliente").strip() or "cliente")
        .replace("{venda}", str(venda or "").strip() or "—")
        .replace("{total}", str(total or "").strip() or "—")
        .replace("{empresa}", _empresa(cfg))
    )
    return enviar_texto_cliente(conversa_id, txt, autor=autor or "Loja")


def acao_entrega_status(
    *,
    conversa_id: int,
    status: str,
    nome: str = "",
    autor: str = "",
) -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_entrega_status"):
        return False, "Recurso desligado (Bot → Recursos)."
    st = (status or "").strip().lower()
    key = {
        "saiu": "msg_entrega_saiu",
        "caminho": "msg_entrega_caminho",
        "chegou": "msg_entrega_chegou",
    }.get(st)
    if not key:
        return False, "Status: saiu, caminho ou chegou."
    tpl = str(cfg.get(key) or RECURSOS_DEFAULT[key])
    txt = tpl.replace("{nome}", (nome or "cliente").strip() or "cliente").replace(
        "{empresa}", _empresa(cfg)
    )
    return enviar_texto_cliente(conversa_id, txt, autor=autor or "Loja")


def acao_lembrete_fiado(*, conversa_id: int, autor: str = "") -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot
    from produtos.atendimento_whatsapp_util import montar_texto_fiado
    from produtos.models import WhatsAppConversaAgro

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_lembrete_fiado"):
        return False, "Recurso desligado (Bot → Recursos)."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return False, "Conversa não encontrada."
    tel = (conv.telefone or "").strip()
    corpo = montar_texto_fiado(tel, cfg)
    # Prefixo lembrete se template distinto
    return enviar_texto_cliente(conversa_id, corpo, autor=autor or "Loja")


def acao_lista_espera_avisar(
    *,
    conversa_id: int,
    produto: str,
    autor: str = "",
) -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot
    from produtos.models import WhatsAppConversaAgro

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_lista_espera"):
        return False, "Recurso desligado (Bot → Recursos)."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return False, "Conversa não encontrada."
    nome = (conv.nome or "cliente").strip() or "cliente"
    prod = str(produto or "").strip() or "produto"
    tpl = str(cfg.get("msg_lista_espera") or RECURSOS_DEFAULT["msg_lista_espera"])
    txt = tpl.replace("{nome}", nome).replace("{produto}", prod).replace("{empresa}", _empresa(cfg))
    ok, err = enviar_texto_cliente(conversa_id, txt, autor=autor or "Loja")
    if ok:
        ex = conversa_extras(conv)
        espera = list(ex.get("espera") or [])
        espera = [e for e in espera if str(e.get("produto") or "") != prod]
        salvar_conversa_extras(conv, {"espera": espera})
    return ok, err


def acao_marcar_espera(*, conversa_id: int, produto: str) -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot
    from produtos.models import WhatsAppConversaAgro

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_lista_espera"):
        return False, "Recurso desligado (Bot → Recursos)."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return False, "Conversa não encontrada."
    prod = str(produto or "").strip()[:120]
    if not prod:
        return False, "Informe o produto."
    ex = conversa_extras(conv)
    espera = list(ex.get("espera") or [])
    if not any(str(e.get("produto") or "") == prod for e in espera):
        espera.append({"produto": prod})
    salvar_conversa_extras(conv, {"espera": espera[:30]})
    return True, ""


def acao_set_vip(*, conversa_id: int, vip: bool, tags: list | None = None) -> tuple[bool, str]:
    from produtos.atendimento_whatsapp_bot_config import carregar_bot
    from produtos.models import WhatsAppConversaAgro

    cfg = carregar_bot()
    if not recurso_on(cfg, "feat_vip_tag"):
        return False, "Recurso desligado (Bot → Recursos)."
    try:
        conv = WhatsAppConversaAgro.objects.get(pk=int(conversa_id))
    except (WhatsAppConversaAgro.DoesNotExist, TypeError, ValueError):
        return False, "Conversa não encontrada."
    patch: dict = {"vip": bool(vip)}
    if tags is not None:
        patch["tags"] = [str(t).strip()[:40] for t in (tags or []) if str(t).strip()][:8]
    salvar_conversa_extras(conv, patch)
    return True, ""


def relatorio_dia() -> dict:
    """Resumo simples do dia — só dados; UI/API checa a flag."""
    from datetime import timedelta

    from django.utils import timezone

    from produtos.models import WhatsAppConversaAgro, WhatsAppMensagemAgro

    ini = timezone.localtime().replace(hour=0, minute=0, second=0, microsecond=0)
    msgs = WhatsAppMensagemAgro.objects.filter(criado_em__gte=ini)
    por_autor: dict[str, int] = {}
    for m in msgs.filter(direcao="out").values_list("autor_nome", flat=True):
        k = (m or "—").strip() or "—"
        por_autor[k] = por_autor.get(k, 0) + 1
    return {
        "desde": ini.isoformat(),
        "msgs": msgs.count(),
        "msgs_cliente": msgs.filter(direcao="in").count(),
        "msgs_loja": msgs.filter(direcao="out").count(),
        "msgs_bot": msgs.filter(direcao="bot").count(),
        "conversas_ativas": WhatsAppConversaAgro.objects.filter(ultima_em__gte=ini).count(),
        "por_autor": por_autor,
    }
