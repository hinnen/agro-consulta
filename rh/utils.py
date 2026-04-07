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
