"""Id canônico de produto no espelho Mongo (DtoProduto) — evita campo ``Id`` corrompido."""
from __future__ import annotations

import re

from bson import ObjectId

_RE_LIXO_ERP_ID = re.compile(r"(?i)produto\s+alterado")


def mongo_campo_id_erp_parece_valido(val) -> bool:
    """``Id`` numérico/curto do ERP; rejeita mensagens («PRODUTO ALTERADO COM SUCESSO…»)."""
    s = str(val or "").strip().strip('"\'')
    if not s or len(s) > 64:
        return False
    if _RE_LIXO_ERP_ID.search(s):
        return False
    if any(ch.isspace() for ch in s):
        return False
    if re.fullmatch(r"[0-9a-f]{24}", s.lower()):
        return True
    if s.isdigit() and 1 <= len(s) <= 12:
        return True
    if re.fullmatch(r"AGRO[0-9A-Fa-f]{8,48}", s):
        return True
    if s.lower().startswith("local:") and s[6:].isdigit():
        return True
    return False


def id_externo_de_doc_mongo(p: dict) -> str:
    """
    Id usado em cadastro/planilha/overlay a partir do documento Mongo.
    Mantém ``Id`` ERP quando válido; se ``Id`` estiver corrompido (texto de resposta ERP), usa ``_id``.
    """
    if not isinstance(p, dict):
        return ""
    erp_id = p.get("Id")
    if mongo_campo_id_erp_parece_valido(erp_id):
        return str(erp_id).strip()
    oid_raw = p.get("_id")
    if oid_raw is not None:
        try:
            if isinstance(oid_raw, ObjectId):
                return str(oid_raw)
            s = str(oid_raw).strip()
            if re.fullmatch(r"[0-9a-f]{24}", s.lower()):
                return s
            return str(ObjectId(s))
        except Exception:
            s = str(oid_raw).strip()
            if s:
                return s[:64]
    return str(erp_id or "").strip()[:64]
