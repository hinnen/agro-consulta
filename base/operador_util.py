"""Operadores de PIN (PerfilUsuario) — cadastro / vínculo RH / desativar."""
from __future__ import annotations

import re
import unicodedata

from django.contrib.auth.models import User
from django.db import transaction
from django.db.models import Q

PIN_BOOTSTRAP = "1234"


def _slug_ascii(texto: str) -> str:
    raw = (texto or "").strip().lower()
    nfkd = unicodedata.normalize("NFKD", raw)
    ascii_only = "".join(c for c in nfkd if not unicodedata.combining(c))
    slug = re.sub(r"[^a-z0-9]+", "", ascii_only)
    return slug[:24] or "operador"


def _partir_nome(nome: str) -> tuple[str, str]:
    partes = (nome or "").strip().split()
    if not partes:
        return "Operador", ""
    if len(partes) == 1:
        return partes[0][:150], ""
    return partes[0][:150], " ".join(partes[1:])[:150]


def proximo_codigo_vendedor() -> str:
    from base.models import PerfilUsuario

    usados = set(
        PerfilUsuario.objects.values_list("codigo_vendedor", flat=True)
    )
    for n in range(1, 10000):
        code = f"{n:04d}"
        if code not in usados:
            return code
    raise RuntimeError("Limite de códigos de operador esgotado.")


def username_unico(base: str) -> str:
    base = _slug_ascii(base)[:20] or "operador"
    cand = base
    n = 1
    while User.objects.filter(username=cand).exists():
        n += 1
        suf = str(n)
        cand = f"{base[: max(1, 20 - len(suf))]}{suf}"
    return cand


def nome_exibicao_perfil(perfil) -> str:
    if getattr(perfil, "funcionario_id", None) and perfil.funcionario_id:
        try:
            fn = perfil.funcionario
            rot = (getattr(fn, "nome_exibicao", None) or getattr(fn, "nome_cache", "") or "").strip()
            if rot:
                return rot
        except Exception:
            pass
    u = perfil.user
    return (u.get_full_name() or u.first_name or u.username or perfil.codigo_vendedor or "").strip()


def serializar_perfil(perfil) -> dict:
    pin_raw = (getattr(perfil, "senha_rapida", None) or "").strip()
    personalizado = bool(pin_raw) and pin_raw != PIN_BOOTSTRAP
    fun = None
    if getattr(perfil, "funcionario_id", None):
        try:
            f = perfil.funcionario
            fun = {
                "id": f.pk,
                "nome": (getattr(f, "nome_exibicao", None) or f.nome_cache or "").strip(),
                "loja": (f.loja.nome if f.loja_id else "") or "",
                "empresa": (f.empresa.nome_fantasia if f.empresa_id else "") or "",
            }
        except Exception:
            fun = None
    nome = nome_exibicao_perfil(perfil)
    return {
        "id": perfil.pk,
        "nome": nome,
        "nome_curto": nome,
        "codigo_vendedor": perfil.codigo_vendedor,
        "pin_personalizado": personalizado,
        "primeiro_acesso": bool(getattr(perfil, "primeiro_acesso", True)) or not personalizado,
        "ativo": bool(getattr(perfil, "ativo", True)),
        "funcionario": fun,
        "funcionario_id": fun["id"] if fun else None,
    }


def listar_operadores(*, incluir_inativos: bool = False) -> list[dict]:
    from base.models import PerfilUsuario

    qs = PerfilUsuario.objects.select_related(
        "user", "funcionario", "funcionario__loja", "funcionario__empresa"
    )
    if not incluir_inativos:
        qs = qs.filter(ativo=True)
    lista = [serializar_perfil(p) for p in qs]
    lista.sort(key=lambda x: ((not x["ativo"]), (x["nome"] or "").lower()))
    return lista


def buscar_funcionarios_rh(q: str = "", *, limite: int = 40) -> list[dict]:
    from base.models import PerfilUsuario
    from rh.models import Funcionario

    qs = (
        Funcionario.objects.filter(ativo=True)
        .select_related("empresa", "loja")
        .order_by("nome_cache")
    )
    termo = (q or "").strip()
    if termo:
        qs = qs.filter(
            Q(nome_cache__icontains=termo)
            | Q(apelido_interno__icontains=termo)
            | Q(cliente_agro__nome__icontains=termo)
        )
    vinculados = {
        row["funcionario_id"]: row["id"]
        for row in PerfilUsuario.objects.filter(funcionario_id__isnull=False, ativo=True).values(
            "id", "funcionario_id"
        )
    }
    out = []
    for f in qs[: max(1, min(limite, 100))]:
        pid = vinculados.get(f.pk)
        out.append(
            {
                "id": f.pk,
                "nome": (f.nome_exibicao or f.nome_cache or "").strip(),
                "loja": (f.loja.nome if f.loja_id else "") or "",
                "empresa": (f.empresa.nome_fantasia if f.empresa_id else "") or "",
                "ja_tem_pin": pid is not None,
                "perfil_id": pid,
            }
        )
    return out


@transaction.atomic
def criar_operador(*, nome: str = "", funcionario_id: int | None = None) -> tuple[bool, dict | None, str]:
    from base.models import PerfilUsuario
    from rh.models import Funcionario

    funcionario = None
    if funcionario_id:
        funcionario = Funcionario.objects.filter(pk=funcionario_id, ativo=True).first()
        if not funcionario:
            return False, None, "Funcionário não encontrado no RH."
        ja = PerfilUsuario.objects.filter(funcionario=funcionario, ativo=True).first()
        if ja:
            return False, None, "Este funcionário já tem operador de PIN ativo."
        nome = (funcionario.nome_exibicao or funcionario.nome_cache or "").strip()

    nome = (nome or "").strip()
    if len(nome) < 2:
        return False, None, "Informe o nome do operador (mín. 2 letras)."

    first, last = _partir_nome(nome)
    user = User.objects.create(
        username=username_unico(nome),
        first_name=first,
        last_name=last,
        is_staff=False,
        is_active=True,
    )
    perfil = PerfilUsuario.objects.create(
        user=user,
        codigo_vendedor=proximo_codigo_vendedor(),
        senha_rapida=PIN_BOOTSTRAP,
        primeiro_acesso=True,
        ativo=True,
        funcionario=funcionario,
    )
    data = serializar_perfil(
        PerfilUsuario.objects.select_related(
            "user", "funcionario", "funcionario__loja", "funcionario__empresa"
        ).get(pk=perfil.pk)
    )
    return True, data, ""


@transaction.atomic
def vincular_funcionario(perfil_id: int, funcionario_id: int) -> tuple[bool, dict | None, str]:
    from base.models import PerfilUsuario
    from rh.models import Funcionario

    perfil = (
        PerfilUsuario.objects.select_related("user", "funcionario")
        .filter(pk=perfil_id)
        .first()
    )
    if not perfil:
        return False, None, "Operador não encontrado."
    funcionario = Funcionario.objects.filter(pk=funcionario_id, ativo=True).first()
    if not funcionario:
        return False, None, "Funcionário não encontrado no RH."
    outro = (
        PerfilUsuario.objects.filter(funcionario=funcionario, ativo=True)
        .exclude(pk=perfil.pk)
        .first()
    )
    if outro:
        return False, None, "Esse funcionário já está vinculado a outro operador."

    perfil.funcionario = funcionario
    nome = (funcionario.nome_exibicao or funcionario.nome_cache or "").strip()
    if nome:
        first, last = _partir_nome(nome)
        u = perfil.user
        u.first_name = first
        u.last_name = last
        u.save(update_fields=["first_name", "last_name"])
    perfil.save(update_fields=["funcionario"])
    data = serializar_perfil(
        PerfilUsuario.objects.select_related(
            "user", "funcionario", "funcionario__loja", "funcionario__empresa"
        ).get(pk=perfil.pk)
    )
    return True, data, ""


def desativar_operador(perfil_id: int) -> tuple[bool, str]:
    from base.models import PerfilUsuario

    perfil = PerfilUsuario.objects.filter(pk=perfil_id).first()
    if not perfil:
        return False, "Operador não encontrado."
    if not perfil.ativo:
        return True, ""
    perfil.ativo = False
    perfil.save(update_fields=["ativo"])
    return True, ""


def reativar_operador(perfil_id: int) -> tuple[bool, str]:
    from base.models import PerfilUsuario

    perfil = PerfilUsuario.objects.filter(pk=perfil_id).first()
    if not perfil:
        return False, "Operador não encontrado."
    if perfil.funcionario_id:
        conflito = (
            PerfilUsuario.objects.filter(funcionario_id=perfil.funcionario_id, ativo=True)
            .exclude(pk=perfil.pk)
            .exists()
        )
        if conflito:
            return False, "Já existe outro PIN ativo para este funcionário RH."
    perfil.ativo = True
    # Volta ao bootstrap: 1ª vez obriga trocar PIN
    perfil.senha_rapida = PIN_BOOTSTRAP
    perfil.primeiro_acesso = True
    perfil.save(update_fields=["ativo", "senha_rapida", "primeiro_acesso"])
    return True, ""


def resetar_pin_bootstrap(perfil_id: int) -> tuple[bool, str]:
    from base.models import PerfilUsuario

    perfil = PerfilUsuario.objects.filter(pk=perfil_id, ativo=True).first()
    if not perfil:
        return False, "Operador não encontrado ou inativo."
    perfil.senha_rapida = PIN_BOOTSTRAP
    perfil.primeiro_acesso = True
    perfil.save(update_fields=["senha_rapida", "primeiro_acesso"])
    return True, ""
