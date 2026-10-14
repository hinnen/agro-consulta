import re
import unicodedata

from base.models import Empresa

from produtos.models import ClienteAgro

from rh.models import Funcionario


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def resolver_empresa_por_nome_fantasia(nome: str) -> Empresa | None:
    n = (nome or "").strip()
    if not n:
        return None
    e = Empresa.objects.filter(nome_fantasia__iexact=n).first()
    if e:
        return e
    return Empresa.objects.filter(nome_fantasia__icontains=n).first()


def resolver_cliente_agro_por_mongo_cliente_id(cid: str) -> ClienteAgro | None:
    """Resolve ClienteAgro a partir de ClienteID gravado no DtoLancamento (ERP ou local:pk)."""
    raw = (cid or "").strip()
    if not raw:
        return None
    ca = ClienteAgro.objects.filter(externo_id=raw, ativo=True).order_by("pk").first()
    if ca:
        return ca
    low = raw.lower()
    if low.startswith("local:"):
        try:
            pk = int(raw.split(":", 1)[1])
            return ClienteAgro.objects.filter(pk=pk, ativo=True).first()
        except (ValueError, IndexError):
            return None
    if raw.isdigit():
        return ClienteAgro.objects.filter(pk=int(raw), ativo=True).first()
    return None


def resolver_perfil_rh_por_empresa_e_cliente(
    empresa: Empresa,
    cliente_agro: ClienteAgro,
) -> Funcionario | None:
    return (
        Funcionario.objects.filter(empresa=empresa, cliente_agro=cliente_agro, ativo=True)
        .select_related("cliente_agro")
        .first()
    )


def resolver_perfil_rh_por_cliente_agro_qualquer_empresa(
    cliente_agro: ClienteAgro,
) -> tuple[Funcionario | None, str | None]:
    """
    Perfil RH ativo para a pessoa, em qualquer empresa (vale no caixa da outra loja).
    Retorna (perfil, erro) — erro só se houver ambiguidade.
    """
    qs = (
        Funcionario.objects.filter(cliente_agro=cliente_agro, ativo=True)
        .select_related("cliente_agro", "empresa")
        .order_by("empresa_id", "pk")
    )
    rows = list(qs[:5])
    if not rows:
        return None, None
    if len(rows) > 1:
        nomes = ", ".join(
            sorted({(r.empresa.nome_fantasia or "").strip() or f"#{r.empresa_id}" for r in rows})
        )
        return (
            None,
            f"Mais de um perfil RH ativo para esta pessoa ({nomes}). Ajuste no cadastro RH.",
        )
    return rows[0], None


def resolver_perfil_rh_para_vale_operacao_caixa(
    empresa_nome_caixa: str | None,
    *,
    mongo_cliente_id: str | None = None,
    cliente_agro_id: int | None = None,
    texto_quem: str | None = None,
) -> tuple[Funcionario | None, str, str | None]:
    """
    Vale / salário no caixa: prioriza perfil RH da pessoa (ClienteAgro), não a empresa
    do turno (Centro × Vila). Filial pode descontar da gaveta Vila com funcionário
    cadastrado na empresa Centro.

    Retorna (perfil, modo, erro). erro preenchido = falha definitiva; perfil None e
    erro None = delegar fluxo antigo.
    """
    nome_caixa = (empresa_nome_caixa or "").strip()
    empresa_turno = resolver_empresa_por_nome_fantasia(nome_caixa) if nome_caixa else None
    if empresa_turno:
        f, modo = resolver_perfil_rh_para_vale(
            empresa_turno,
            mongo_cliente_id=mongo_cliente_id,
            texto_quem=texto_quem,
        )
        if f:
            return f, modo, None

    ca: ClienteAgro | None = None
    if cliente_agro_id:
        ca = ClienteAgro.objects.filter(pk=int(cliente_agro_id), ativo=True).first()
    cid = (mongo_cliente_id or "").strip()
    if ca is None and cid:
        ca = resolver_cliente_agro_por_mongo_cliente_id(cid)
    if ca:
        f, err = resolver_perfil_rh_por_cliente_agro_qualquer_empresa(ca)
        if err:
            return None, "none", err
        if f:
            return f, "id_loja", None

    raw = (texto_quem or "").strip()
    if raw:
        vistos: dict[int, Funcionario] = {}
        for emp in Empresa.objects.filter(ativo=True).only("pk", "nome_fantasia"):
            hit = resolver_perfil_rh_por_nome_legado(emp, raw)
            if hit:
                vistos[hit.pk] = hit
        if len(vistos) == 1:
            return next(iter(vistos.values())), "nome_loja", None
        if len(vistos) > 1:
            return (
                None,
                "none",
                "Nome ambíguo entre empresas — escolha a pessoa na lista «Quem está levando».",
            )

    return None, "none", None


def resolver_perfil_rh_para_vale(
    empresa: Empresa,
    *,
    mongo_cliente_id: str | None = None,
    texto_quem: str | None = None,
) -> tuple[Funcionario | None, str]:
    """
    Ordem: (1) id estruturado Mongo/ClienteAgro → perfil RH
           (2) fallback nome / nome_cache / apelido_interno
    Retorna (perfil ou None, motivo: 'id'|'nome'|'none').
    """
    cid = (mongo_cliente_id or "").strip()
    if cid:
        ca = resolver_cliente_agro_por_mongo_cliente_id(cid)
        if ca:
            f = resolver_perfil_rh_por_empresa_e_cliente(empresa, ca)
            if f:
                return f, "id"
    raw = (texto_quem or "").strip()
    if raw:
        f = resolver_perfil_rh_por_nome_legado(empresa, raw)
        if f:
            return f, "nome"
    return None, "none"


def _chaves_nome_quem_caixa(nome_quem: str) -> list[str]:
    """Gera variantes do texto exibido no caixa (ex.: remove sufixo numérico tipo loja/código)."""
    n = _norm(nome_quem)
    if not n:
        return []
    out = [n]
    stripped = re.sub(r"\s+\d+$", "", n).strip()
    if stripped and stripped not in out:
        out.append(stripped)
    return out


def resolver_perfil_rh_por_nome_legado(empresa: Empresa, nome_quem: str) -> Funcionario | None:
    """Fallback: texto da saída de caixa (nome exibido) → perfil RH."""
    keys = _chaves_nome_quem_caixa(nome_quem)
    if not keys:
        return None
    qs = (
        Funcionario.objects.filter(empresa=empresa)
        .select_related("cliente_agro")
        .filter(ativo=True)
    )
    for key in keys:
        for f in qs:
            if _norm(f.nome_cache) == key:
                return f
            if f.apelido_interno and _norm(f.apelido_interno) == key:
                return f
            if f.cliente_agro_id and _norm(f.cliente_agro.nome) == key:
                return f
    for key in keys:
        for f in qs:
            partes = re.split(r"[\s\-]+", _norm(f.nome_cache))
            if partes and partes[0] == key:
                return f
            if f.cliente_agro_id:
                partes_c = re.split(r"[\s\-]+", _norm(f.cliente_agro.nome))
                if partes_c and partes_c[0] == key:
                    return f
    return None
