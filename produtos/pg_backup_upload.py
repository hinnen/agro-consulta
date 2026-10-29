"""Upload de artefatos de backup Postgres (FL-048) — webhook ou S3-compatível."""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests
from django.conf import settings


def _upload_configured() -> bool:
    mode = (getattr(settings, "AGRO_PG_BACKUP_UPLOAD_MODE", "") or "").strip().lower()
    if mode == "webhook":
        return bool((getattr(settings, "AGRO_PG_BACKUP_WEBHOOK_URL", "") or "").strip())
    if mode == "s3":
        return bool(
            (getattr(settings, "AGRO_PG_BACKUP_S3_BUCKET", "") or "").strip()
            and (getattr(settings, "AGRO_PG_BACKUP_S3_ACCESS_KEY", "") or "").strip()
            and (getattr(settings, "AGRO_PG_BACKUP_S3_SECRET_KEY", "") or "").strip()
        )
    return False


def upload_backup_blob(data: bytes, filename: str) -> dict[str, Any]:
    """Envia ZIP para destino configurado. Levanta RuntimeError se falhar."""
    mode = (getattr(settings, "AGRO_PG_BACKUP_UPLOAD_MODE", "") or "").strip().lower()
    if not mode or mode == "none":
        return {"ok": False, "skipped": True, "motivo": "upload desligado (AGRO_PG_BACKUP_UPLOAD_MODE)"}
    if mode == "webhook":
        return _upload_webhook(data, filename)
    if mode == "s3":
        return _upload_s3(data, filename)
    raise RuntimeError(f"AGRO_PG_BACKUP_UPLOAD_MODE inválido: {mode!r}")


def _upload_webhook(data: bytes, filename: str) -> dict[str, Any]:
    url = (getattr(settings, "AGRO_PG_BACKUP_WEBHOOK_URL", "") or "").strip()
    if not url:
        raise RuntimeError("AGRO_PG_BACKUP_WEBHOOK_URL vazio.")
    headers: dict[str, str] = {}
    token = (getattr(settings, "AGRO_PG_BACKUP_WEBHOOK_TOKEN", "") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    files = {"file": (filename, data, "application/zip")}
    meta = {
        "filename": filename,
        "bytes": len(data),
        "origem": "sistvale-pg-backup-nightly",
    }
    resp = requests.post(url, files=files, data=meta, headers=headers, timeout=600)
    if resp.status_code >= 400:
        raise RuntimeError(f"Webhook HTTP {resp.status_code}: {resp.text[:300]}")
    return {"ok": True, "mode": "webhook", "bytes": len(data), "filename": filename}


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _aws4_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    return _hmac_sha256(k_service, "aws4_request")


def _upload_s3(data: bytes, filename: str) -> dict[str, Any]:
    bucket = (getattr(settings, "AGRO_PG_BACKUP_S3_BUCKET", "") or "").strip()
    access_key = (getattr(settings, "AGRO_PG_BACKUP_S3_ACCESS_KEY", "") or "").strip()
    secret_key = (getattr(settings, "AGRO_PG_BACKUP_S3_SECRET_KEY", "") or "").strip()
    if not bucket or not access_key or not secret_key:
        raise RuntimeError("S3: bucket/access/secret obrigatórios.")

    endpoint = (getattr(settings, "AGRO_PG_BACKUP_S3_ENDPOINT", "") or "").strip().rstrip("/")
    region = (getattr(settings, "AGRO_PG_BACKUP_S3_REGION", "") or "us-east-1").strip()
    prefix = (getattr(settings, "AGRO_PG_BACKUP_S3_PREFIX", "") or "sistvale/pg-backup").strip().strip("/")
    object_key = f"{prefix}/{filename}" if prefix else filename

    if endpoint:
        host = endpoint.replace("https://", "").replace("http://", "")
        url = f"https://{host}/{bucket}/{quote(object_key, safe='/')}"
    else:
        host = f"{bucket}.s3.{region}.amazonaws.com"
        url = f"https://{host}/{quote(object_key, safe='/')}"

    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(data).hexdigest()
    canonical_uri = "/" + quote(object_key, safe="/")
    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [
            "PUT",
            canonical_uri,
            "",
            canonical_headers,
            signed_headers,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
        ]
    )
    signing_key = _aws4_signing_key(secret_key, date_stamp, region, "s3")
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    headers = {
        "Host": host,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
        "Authorization": authorization,
        "Content-Type": "application/zip",
    }
    resp = requests.put(url, data=data, headers=headers, timeout=600)
    if resp.status_code >= 400:
        raise RuntimeError(f"S3 HTTP {resp.status_code}: {resp.text[:300]}")
    return {
        "ok": True,
        "mode": "s3",
        "bytes": len(data),
        "filename": filename,
        "key": object_key,
    }


def upload_status_resumo() -> dict[str, Any]:
    mode = (getattr(settings, "AGRO_PG_BACKUP_UPLOAD_MODE", "") or "").strip().lower() or "none"
    return {
        "mode": mode,
        "configured": _upload_configured(),
    }
