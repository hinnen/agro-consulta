"""Quem pode acessar a gestão restrita do RH (alinhado ao menu PDV)."""

_RH_USUARIOS_ACESSO_RESTRITO = frozenset({"admin", "geraldinho", "geraldo", "renan"})


def usuario_rh_acesso_restrito(user) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    allowed = _RH_USUARIOS_ACESSO_RESTRITO
    parts = []
    if hasattr(user, "get_username"):
        parts.append(user.get_username() or "")
    parts.append(getattr(user, "first_name", None) or "")
    parts.append(getattr(user, "last_name", None) or "")
    if hasattr(user, "get_full_name"):
        full = (user.get_full_name() or "").strip()
        if full:
            parts.append(full)
            parts.extend(full.split())
    for raw in parts:
        key = (raw or "").strip().lower()
        if key and key in allowed:
            return True
    return False
