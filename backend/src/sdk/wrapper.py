"""
LLM SDK Wrapper
Multi-provider support: Anthropic, OpenAI, Google Gemini, DeepSeek, Grok (xAI)
Captures: latency, tokens, timestamps, errors, previews, session metadata
"""

import asyncio
import time
import uuid
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional, Tuple

import httpx


PREVIEW_LENGTH = 200  # chars to capture for input/output previews

PROVIDER_CONFIGS = {
    "anthropic": {
        "url": "https://api.anthropic.com/v1/messages",
        "default_model": "claude-haiku-4-5-20251001",
        "env_key": "ANTHROPIC_API_KEY",
    },
    "openai": {
        "url": "https://api.openai.com/v1/chat/completions",
        "default_model": "gpt-4.1",
        "env_key": "OPENAI_API_KEY",
    },
    "gemini": {
        "url": "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
        "default_model": "gemini-2.0-flash",
        "env_key": "GEMINI_API_KEY",
    },
    "deepseek": {
        "url": "https://api.deepseek.com/v1/chat/completions",
        "default_model": "deepseek-chat",
        "env_key": "DEEPSEEK_API_KEY",
    },
    "xai": {
        "url": "https://api.x.ai/v1/chat/completions",
        "default_model": "grok-3",
        "env_key": "XAI_API_KEY",
    },
}


class LLMWrapper:
    """
    Thin wrapper around LLM provider APIs.
    Automatically captures inference metadata and sends to ingestion pipeline.
    """

    def __init__(self, pipeline, redactor, db=None):
        self.pipeline = pipeline
        self.redactor = redactor
        self.db = db

    async def chat(
        self,
        message: str,
        history: List[Dict],
        session_id: str,
        provider: str = "anthropic",
        model: Optional[str] = None,
    ) -> Tuple[str, Dict]:
        import os
        cfg = PROVIDER_CONFIGS.get(provider, PROVIDER_CONFIGS["anthropic"])
        model = model or cfg["default_model"]
        api_key = os.environ.get(cfg["env_key"], "")

        # Redact PII before sending upstream
        clean_message = self.redactor.redact(message)

        request_id = str(uuid.uuid4())
        ts_start = datetime.utcnow().isoformat()
        t0 = time.monotonic()

        try:
            if provider == "anthropic":
                response_text, usage = await self._call_anthropic(
                    clean_message, history, model, api_key
                )
            elif provider in ("openai", "deepseek", "xai"):
                response_text, usage = await self._call_openai_compat(
                    clean_message, history, model, api_key, cfg["url"]
                )
            elif provider == "gemini":
                response_text, usage = await self._call_gemini(
                    clean_message, history, model, api_key
                )
            else:
                raise ValueError(f"Unsupported provider: {provider}")

            latency_ms = (time.monotonic() - t0) * 1000
            ts_end = datetime.utcnow().isoformat()
            status = "success"
            error = None

        except Exception as e:
            latency_ms = (time.monotonic() - t0) * 1000
            ts_end = datetime.utcnow().isoformat()
            response_text = f"Error: {str(e)}"
            usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
            status = "error"
            error = str(e)

        log = {
            "session_id": session_id,
            "request_id": request_id,
            "provider": provider,
            "model": model,
            "latency_ms": round(latency_ms, 2),
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
            "total_tokens": usage.get("total_tokens", 0),
            "timestamp_start": ts_start,
            "timestamp_end": ts_end,
            "status": status,
            "error": error,
            "input_preview": clean_message[:PREVIEW_LENGTH],
            "output_preview": response_text[:PREVIEW_LENGTH],
            "metadata": {"history_length": len(history)},
        }

        # Fire-and-forget ingestion (non-blocking)
        asyncio.create_task(self.pipeline.process(log))

        # Persist messages
        if self.pipeline.db:
            await self.pipeline.db.ensure_conversation(session_id, provider, model)
            await self.pipeline.db.save_message(
                session_id, "user", message,
                token_count=usage.get("prompt_tokens", 0),
                pii_redacted=clean_message != message
            )
            if status == "success":
                await self.pipeline.db.save_message(
                    session_id, "assistant", response_text,
                    token_count=usage.get("completion_tokens", 0)
                )

        return response_text, log

    async def stream_chat(
        self,
        message: str,
        history: List[Dict],
        session_id: str,
        provider: str = "anthropic",
        model: Optional[str] = None,
    ) -> AsyncGenerator[str, None]:
        """Streaming variant - yields text chunks."""
        import os
        cfg = PROVIDER_CONFIGS.get(provider, PROVIDER_CONFIGS["anthropic"])
        model = model or cfg["default_model"]
        api_key = os.environ.get(cfg["env_key"], "")
        clean_message = self.redactor.redact(message)

        request_id = str(uuid.uuid4())
        ts_start = datetime.utcnow().isoformat()
        t0 = time.monotonic()
        full_response = ""

        try:
            if provider == "anthropic":
                async for chunk in self._stream_anthropic(clean_message, history, model, api_key):
                    full_response += chunk
                    yield chunk
            else:
                async for chunk in self._stream_openai_compat(
                    clean_message, history, model, api_key, cfg["url"]
                ):
                    full_response += chunk
                    yield chunk

            status = "success"
            error = None
        except Exception as e:
            status = "error"
            error = str(e)
            yield f"\n[Error: {e}]"

        latency_ms = (time.monotonic() - t0) * 1000
        ts_end = datetime.utcnow().isoformat()

        log = {
            "session_id": session_id,
            "request_id": request_id,
            "provider": provider,
            "model": model,
            "latency_ms": round(latency_ms, 2),
            "prompt_tokens": 0,   # streaming APIs often don't return token counts mid-stream
            "completion_tokens": 0,
            "total_tokens": 0,
            "timestamp_start": ts_start,
            "timestamp_end": ts_end,
            "status": status,
            "error": error,
            "input_preview": clean_message[:PREVIEW_LENGTH],
            "output_preview": full_response[:PREVIEW_LENGTH],
            "metadata": {"stream": True, "history_length": len(history)},
        }
        asyncio.create_task(self.pipeline.process(log))

    # ── Provider Implementations ───────────────────────────────────────────────

    async def _call_anthropic(self, message, history, model, api_key):
        messages = self._history_to_anthropic(history, message)
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={"model": model, "max_tokens": 1024, "messages": messages},
            )
            r.raise_for_status()
            data = r.json()
            text = data["content"][0]["text"]
            usage = data.get("usage", {})
            return text, {
                "prompt_tokens": usage.get("input_tokens", 0),
                "completion_tokens": usage.get("output_tokens", 0),
                "total_tokens": usage.get("input_tokens", 0) + usage.get("output_tokens", 0),
            }

    async def _stream_anthropic(self, message, history, model, api_key):
        import json as _json
        messages = self._history_to_anthropic(history, message)
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST",
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={"model": model, "max_tokens": 1024, "messages": messages, "stream": True},
            ) as r:
                r.raise_for_status()
                async for line in r.aiter_lines():
                    if line.startswith("data:"):
                        try:
                            data = _json.loads(line[5:].strip())
                            if data.get("type") == "content_block_delta":
                                yield data["delta"].get("text", "")
                        except Exception:
                            pass

    async def _call_openai_compat(self, message, history, model, api_key, url):
        messages = self._history_to_openai(history, message)
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(
                url,
                headers={"Authorization": f"Bearer {api_key}", "content-type": "application/json"},
                json={"model": model, "messages": messages},
            )
            r.raise_for_status()
            data = r.json()
            text = data["choices"][0]["message"]["content"]
            usage = data.get("usage", {})
            return text, {
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
                "total_tokens": usage.get("total_tokens", 0),
            }

    async def _stream_openai_compat(self, message, history, model, api_key, url):
        import json as _json
        messages = self._history_to_openai(history, message)
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST", url,
                headers={"Authorization": f"Bearer {api_key}", "content-type": "application/json"},
                json={"model": model, "messages": messages, "stream": True},
            ) as r:
                r.raise_for_status()
                async for line in r.aiter_lines():
                    if line.startswith("data:") and "[DONE]" not in line:
                        try:
                            data = _json.loads(line[5:].strip())
                            delta = data["choices"][0]["delta"].get("content", "")
                            if delta:
                                yield delta
                        except Exception:
                            pass

    async def _call_gemini(self, message, history, model, api_key):
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        contents = self._history_to_gemini(history, message)
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.post(url, json={"contents": contents})
            r.raise_for_status()
            data = r.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            usage = data.get("usageMetadata", {})
            return text, {
                "prompt_tokens": usage.get("promptTokenCount", 0),
                "completion_tokens": usage.get("candidatesTokenCount", 0),
                "total_tokens": usage.get("totalTokenCount", 0),
            }

    # ── Message Format Converters ─────────────────────────────────────────────

    def _history_to_anthropic(self, history, new_message):
        msgs = []
        for m in history[-10:]:  # last 10 turns for context window
            msgs.append({"role": m["role"], "content": m["content"]})
        msgs.append({"role": "user", "content": new_message})
        return msgs

    def _history_to_openai(self, history, new_message):
        msgs = [{"role": "system", "content": "You are a helpful assistant."}]
        for m in history[-10:]:
            msgs.append({"role": m["role"], "content": m["content"]})
        msgs.append({"role": "user", "content": new_message})
        return msgs

    def _history_to_gemini(self, history, new_message):
        contents = []
        for m in history[-10:]:
            role = "user" if m["role"] == "user" else "model"
            contents.append({"role": role, "parts": [{"text": m["content"]}]})
        contents.append({"role": "user", "parts": [{"text": new_message}]})
        return contents
