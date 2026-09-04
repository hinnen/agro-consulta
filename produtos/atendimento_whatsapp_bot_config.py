"""Configuração do bot WhatsApp — Postgres (multi-PC). Chave default = esta loja; outras depois."""
from __future__ import annotations

import copy
from datetime import datetime, time

from django.utils import timezone

CHAVE_DEFAULT = "default"

# Textos atuais da GM Agro — viram o padrão se o campo ficar vazio.
BOT_DEFAULT: dict = {
    "bot_ligado": True,
    "nome_empresa": "GM Agro",
    "atraso_resposta_seg": 2,
    "atraso_entre_msgs_seg": 2,
    "horario_ativo": True,
    "horario_ini": "08:00",
    "horario_fim": "18:00",
    "horario_dias": [1, 2, 3, 4, 5, 6],
    "msg_fora_horario": (
        "Olá! Agora estamos *fora do horário* (seg–sáb 8h–18h).\n"
        "Deixe sua mensagem que a loja responde no próximo expediente."
    ),
    "ainda_atende_fora": False,
    "aviso_fora_ligado": True,
    "aviso_fora_minutos": 60,
    "aviso_fora_uma_vez": False,
    "aviso_fora_so_texto": False,
    "separar_lojas": True,
    "enviar_boas_vindas": True,
    "msg_boas_vindas": "Olá! Bem-vindo à *{empresa}*.",
    "nome_fontes": "cadastro,agenda,perfil,telefone",
    "ordem": "fiado_depois_loja",
    "msg_menu": (
        "Olá! Você quer falar com qual loja?\n\n"
        "1 — Centro (Jacupiranga)\n"
        "2 — Vila Elias\n\n"
        "Responda *1* ou *2*.\n\n"
        "Para ver o fiado em aberto, escreva *fiado*."
    ),
    "msg_pedir_de_novo": (
        "Responda *1* para o Centro ou *2* para a Vila Elias. Para o fiado, escreva *fiado*."
    ),
    "repetir_menu": True,
    "loja1_id": "centro",
    "loja1_rotulo": "Centro (Jacupiranga)",
    "loja1_palavras": "1, centro, loja centro, jacupiranga, c",
    "msg_ok_loja1": "Certo! Você está falando com a loja do *Centro*. Em breve alguém atende por aqui.",
    "loja2_id": "vila",
    "loja2_rotulo": "Vila Elias",
    "loja2_palavras": "2, vila, vila elias, loja vila, v",
    "msg_ok_loja2": "Certo! Você está falando com a loja da *Vila Elias*. Em breve alguém atende por aqui.",
    "fiado_ligado": True,
    "fiado_palavras": (
        "fiado, meu fiado, saldo, saldo fiado, consulta fiado, consultar fiado, "
        "quanto eu devo, quanto devo, o que eu devo, divida, minha divida"
    ),
    "fiado_max_parcelas": 8,
    "msg_fiado_aberto": (
        "*Fiado em aberto*\n\n"
        "Olá, {nome}.\n"
        "Total: *{total}*\n\n"
        "{linhas}\n\n"
        "Para pagar, passe na loja."
    ),
    "msg_fiado_vazio": "Olá, {nome}. Você *não* tem fiado em aberto neste cadastro.",
    "msg_fiado_sem_cadastro": (
        "Não achamos cadastro com este WhatsApp.\n"
        "Confira se o número na loja é o mesmo deste celular."
    ),
    "msg_fiado_varios": (
        "Encontramos mais de um cadastro com este WhatsApp. "
        "Fale com a loja (responda *1* ou *2*) para conferir o fiado."
    ),
    "fiado_manda_menu": True,
    "ausencia_ligada": True,
    "msg_ausencia": "No momento a loja está ocupada. Já já alguém responde por aqui.",
}

# Recursos extras (pacote WA-REC-OFF) — importados e mesclados; todos False.
from produtos.atendimento_whatsapp_recursos import RECURSO_IDS, RECURSOS_DEFAULT  # noqa: E402

BOT_DEFAULT.update(copy.deepcopy(RECURSOS_DEFAULT))

BOOL_KEYS = (
    "bot_ligado",
    "horario_ativo",
    "ainda_atende_fora",
    "aviso_fora_ligado",
    "aviso_fora_uma_vez",
    "aviso_fora_so_texto",
    "separar_lojas",
    "enviar_boas_vindas",
    "repetir_menu",
    "fiado_ligado",
    "fiado_manda_menu",
    "ausencia_ligada",
) + tuple(RECURSO_IDS)


def _as_bool(v, default: bool = False) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return default
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    if s in ("0", "false", "off", "nao", "não", "n", "no", ""):
        return False
    if s in ("1", "true", "on", "sim", "s", "yes"):
        return True
    return default


def cfg_flag(cfg: dict | None, key: str, default: bool | None = None) -> bool:
    if default is None:
        default = bool(BOT_DEFAULT.get(key, False))
    if not cfg or not isinstance(cfg, dict):
        return default
    if key not in cfg:
        return default
    return _as_bool(cfg.get(key), default)


def _merge(base: dict, extra: dict | None) -> dict:
    out = copy.deepcopy(base)
    if extra and isinstance(extra, dict):
        for k, v in extra.items():
            if k in out:
                out[k] = v
    for k in BOOL_KEYS:
        out[k] = _as_bool(out.get(k), bool(BOT_DEFAULT.get(k, False)))
    return out


def carregar_bot(*, chave: str = CHAVE_DEFAULT) -> dict:
    from produtos.models import WhatsAppBotConfigAgro

    obj, _ = WhatsAppBotConfigAgro.objects.get_or_create(chave=(chave or CHAVE_DEFAULT)[:32])
    return _merge(BOT_DEFAULT, obj.dados if isinstance(obj.dados, dict) else {})


def salvar_bot(dados: dict, *, chave: str = CHAVE_DEFAULT, usuario: str = "") -> dict:
    from produtos.models import WhatsAppBotConfigAgro

    limpo = _merge(BOT_DEFAULT, dados if isinstance(dados, dict) else {})
    try:
        limpo["atraso_resposta_seg"] = max(0, min(120, int(limpo.get("atraso_resposta_seg") or 0)))
    except (TypeError, ValueError):
        limpo["atraso_resposta_seg"] = 1
    try:
        limpo["atraso_entre_msgs_seg"] = max(0, min(60, int(limpo.get("atraso_entre_msgs_seg") or 0)))
    except (TypeError, ValueError):
        limpo["atraso_entre_msgs_seg"] = 1
    try:
        limpo["aviso_fora_minutos"] = max(0, min(1440, int(limpo.get("aviso_fora_minutos") or 0)))
    except (TypeError, ValueError):
        limpo["aviso_fora_minutos"] = 60
    try:
        limpo["fiado_max_parcelas"] = max(1, min(20, int(limpo.get("fiado_max_parcelas") or 8)))
    except (TypeError, ValueError):
        limpo["fiado_max_parcelas"] = 8
    dias = limpo.get("horario_dias") or []
    if not isinstance(dias, list):
        dias = []
    limpo["horario_dias"] = sorted({int(d) for d in dias if str(d).isdigit() and 0 <= int(d) <= 6})
    fontes = []
    for p in str(limpo.get("nome_fontes") or "").replace(";", ",").split(","):
        k = p.strip().lower()
        if k in ("cadastro", "agenda", "perfil", "telefone") and k not in fontes:
            fontes.append(k)
    for k in ("cadastro", "agenda", "perfil", "telefone"):
        if k not in fontes:
            fontes.append(k)
    limpo["nome_fontes"] = ",".join(fontes)
    obj, _ = WhatsAppBotConfigAgro.objects.get_or_create(chave=(chave or CHAVE_DEFAULT)[:32])
    obj.dados = limpo
    obj.atualizado_por = (usuario or "")[:120]
    obj.save()
    return limpo


def resetar_bot(*, chave: str = CHAVE_DEFAULT, usuario: str = "") -> dict:
    return salvar_bot(copy.deepcopy(BOT_DEFAULT), chave=chave, usuario=usuario)


def _sem_acento(s: str) -> str:
    import unicodedata

    n = unicodedata.normalize("NFD", s or "")
    return "".join(c for c in n if unicodedata.category(c) != "Mn")


def _palavras(raw: str) -> list[str]:
    out = []
    for p in str(raw or "").split(","):
        t = _sem_acento(p.strip().lower())
        if t:
            out.append(t)
    return out


def _casa_palavra(texto: str, palavras: list[str]) -> bool:
    t = (texto or "").strip()
    if not t:
        return False
    for p in palavras:
        if t == p:
            return True
        if len(p) >= 3 and p in t:
            return True
    return False


def fora_do_horario(cfg: dict, agora: datetime | None = None) -> bool:
    if not cfg_flag(cfg, "horario_ativo"):
        return False
    agora = agora or timezone.localtime()
    wd = int(agora.weekday())  # 0=seg … 6=dom — JS/ISO: 0=dom no nosso form
    # Form usa 0=dom, 1=seg … 6=sáb (igual JS getDay)
    js_day = (wd + 1) % 7
    dias = cfg.get("horario_dias") or []
    if js_day not in {int(d) for d in dias}:
        return True

    def _hm(s: str) -> time:
        parts = str(s or "08:00").strip().split(":")
        try:
            return time(int(parts[0]), int(parts[1] if len(parts) > 1 else 0))
        except (TypeError, ValueError):
            return time(8, 0)

    ini = _hm(cfg.get("horario_ini") or "08:00")
    fim = _hm(cfg.get("horario_fim") or "18:00")
    hh = agora.time().replace(second=0, microsecond=0)
    if ini <= fim:
        return not (ini <= hh <= fim)
    return not (hh >= ini or hh <= fim)


def delays_bot(cfg: dict, n_msgs: int) -> list[int]:
    a = int(cfg.get("atraso_resposta_seg") or 0)
    e = int(cfg.get("atraso_entre_msgs_seg") or 0)
    out = []
    acc = a
    for i in range(max(0, n_msgs)):
        out.append(acc)
        acc += e
    return out
