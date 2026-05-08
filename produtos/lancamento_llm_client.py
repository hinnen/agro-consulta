"""Cliente mínimo LLM → JSON para rotinas financeiras (Gemini / Groq / OpenAI).

Sem credencial configurada não chama rede; outros módulos importam só o que precisam."""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import requests
from decouple import config

logger = logging.getLogger(__name__)


def resolver_credencial_llm() -> tuple[str, str] | None:
    """(provedor, api_key) ou ``None``."""
    provider = config("AGRO_LANCAMENTO_LLM_PROVIDER", default="").strip().lower()
    key_agg = config("AGRO_LANCAMENTO_LLM_API_KEY", default="").strip()
    gem = config("GEMINI_API_KEY", default="").strip() or config("GOOGLE_API_KEY", default="").strip()
    groq = config("GROQ_API_KEY", default="").strip()
    oai = config("OPENAI_API_KEY", default="").strip()

    if key_agg:
        if provider in ("gemini", "groq", "openai"):
            return provider, key_agg
        return "gemini", key_agg
    if not provider:
        if gem:
            return "gemini", gem
        if groq:
            return "groq", groq
        if oai:
            return "openai", oai
    if provider == "gemini" and gem:
        return "gemini", gem
    if provider == "groq" and groq:
        return "groq", groq
    if provider == "openai" and oai:
        return "openai", oai
    return None


def parse_json_llm(text: str) -> dict[str, Any] | None:
    text = (text or "").strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]*\}", text)
        if not m:
            return None
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else None
        except json.JSONDecodeError:
            return None


def _gemini(prompt: str, api_key: str) -> dict[str, Any] | None:
    model = (
        config("AGRO_LANCAMENTO_LLM_GEMINI_MODEL", default="gemini-2.0-flash").strip()
        or "gemini-2.0-flash"
    )
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{model}:generateContent?key={api_key}"
    )
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"temperature": 0.05, "responseMimeType": "application/json"},
    }
    try:
        r = requests.post(url, json=body, timeout=22)
        r.raise_for_status()
        data = r.json()
    except requests.RequestException as exc:
        logger.warning("GEMINI LANCAM JSON: %s", exc)
        return None
    cands = data.get("candidates") or []
    if not cands:
        return None
    parts = ((cands[0].get("content") or {}).get("parts")) or []
    if not parts:
        return None
    return parse_json_llm(str(parts[0].get("text") or ""))


def _openai_compat(
    prompt: str,
    api_key: str,
    *,
    endpoint: str,
    model: str,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    if extra_headers:
        headers.update(extra_headers)
    body = {
        "model": model,
        "temperature": 0.05,
        "messages": [{"role": "user", "content": prompt}],
        "response_format": {"type": "json_object"},
    }
    try:
        r = requests.post(endpoint, headers=headers, json=body, timeout=24)
        r.raise_for_status()
        raw = ((((r.json().get("choices") or [None])[0] or {}).get("message")) or {}).get("content") or ""
        return parse_json_llm(str(raw))
    except requests.RequestException as exc:
        logger.warning("OPENAI-compat LLM JSON: %s", exc)
        return None


def _groq(prompt: str, api_key: str) -> dict[str, Any] | None:
    model = (
        config("AGRO_LANCAMENTO_LLM_GROQ_MODEL", default="llama-3.1-8b-instant").strip()
        or "llama-3.1-8b-instant"
    )
    return _openai_compat(
        prompt,
        api_key,
        endpoint="https://api.groq.com/openai/v1/chat/completions",
        model=model,
    )


def _openai(prompt: str, api_key: str) -> dict[str, Any] | None:
    model = config("AGRO_LANCAMENTO_LLM_OPENAI_MODEL", default="gpt-4o-mini").strip() or "gpt-4o-mini"
    endpoint = (
        config("OPENAI_API_BASE_URL", default="https://api.openai.com/v1/chat/completions")
        .strip()
        or "https://api.openai.com/v1/chat/completions"
    )
    return _openai_compat(prompt, api_key, endpoint=endpoint, model=model)


def gerar_json_llm(prompt: str) -> tuple[dict[str, Any] | None, str]:
    """Executa o prompt e devolve (dict_parseado, nome_provedor)."""
    cred = resolver_credencial_llm()
    if not cred:
        return None, ""
    provider, api_key = cred
    parsed: dict[str, Any] | None = None
    if provider == "gemini":
        parsed = _gemini(prompt, api_key)
    elif provider == "groq":
        parsed = _groq(prompt, api_key)
    elif provider == "openai":
        parsed = _openai(prompt, api_key)
    return parsed, provider or ""
