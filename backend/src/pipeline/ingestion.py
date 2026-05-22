"""
Ingestion Pipeline
Validates, enriches, and persists inference log payloads.
Designed to be swappable with an event-based system (Kafka/SQS) with minimal changes.
"""

import asyncio
import json
from datetime import datetime
from typing import Dict, Optional


REQUIRED_FIELDS = {"request_id", "provider", "model", "latency_ms",
                   "timestamp_start", "timestamp_end", "status"}


class ValidationError(Exception):
    pass


class IngestionPipeline:
    """
    Processes log payloads through a pipeline of stages:
      1. Validate  - enforce schema
      2. Enrich    - add derived fields
      3. Persist   - write to database

    In a production event-based architecture, stage 3 would publish to a
    Kafka topic and a separate consumer would handle persistence — enabling
    back-pressure, replay, and fan-out to multiple sinks.
    """

    def __init__(self, db):
        self.db = db
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self._worker_task: Optional[asyncio.Task] = None

    def start_worker(self):
        """Start background consumer (for async queue pattern)."""
        self._worker_task = asyncio.create_task(self._consume())

    async def _consume(self):
        while True:
            payload = await self._queue.get()
            try:
                await self._persist(payload)
            except Exception as e:
                # In prod: dead-letter queue / retry with exponential backoff
                print(f"[pipeline] persist error: {e}")
            finally:
                self._queue.task_done()

    async def process(self, raw: Dict) -> Dict:
        """Main entry point - validate, enrich, persist, return record."""
        validated = self._validate(raw)
        enriched = self._enrich(validated)
        record_id = await self._persist(enriched)
        return {"id": record_id, **enriched}

    def _validate(self, payload: Dict) -> Dict:
        missing = REQUIRED_FIELDS - set(payload.keys())
        if missing:
            raise ValidationError(f"Missing required fields: {missing}")

        # Type coercions / sanitisation
        payload["latency_ms"] = float(payload["latency_ms"])
        payload["prompt_tokens"] = int(payload.get("prompt_tokens", 0))
        payload["completion_tokens"] = int(payload.get("completion_tokens", 0))
        payload["total_tokens"] = int(
            payload.get("total_tokens") or
            payload["prompt_tokens"] + payload["completion_tokens"]
        )
        payload["status"] = payload["status"].lower()
        if payload["status"] not in {"success", "error", "timeout", "cancelled"}:
            payload["status"] = "unknown"

        return payload

    def _enrich(self, payload: Dict) -> Dict:
        """Add derived metadata fields."""
        # Tokens-per-second throughput
        latency_s = payload["latency_ms"] / 1000
        completion_tokens = payload.get("completion_tokens", 0)
        payload["tokens_per_second"] = (
            round(completion_tokens / latency_s, 2) if latency_s > 0 and completion_tokens > 0
            else 0
        )

        # Latency bucket for histogram grouping (no GROUP BY needed)
        ms = payload["latency_ms"]
        if ms < 500:
            payload["latency_bucket"] = "<500ms"
        elif ms < 1000:
            payload["latency_bucket"] = "500ms-1s"
        elif ms < 3000:
            payload["latency_bucket"] = "1s-3s"
        else:
            payload["latency_bucket"] = ">3s"

        # Estimated cost (rough approximations per 1M tokens)
        COST_TABLE = {
            "anthropic": {"input": 3.0, "output": 15.0},
            "openai": {"input": 2.0, "output": 8.0},
            "gemini": {"input": 0.35, "output": 1.05},
            "deepseek": {"input": 0.27, "output": 1.10},
            "xai": {"input": 2.0, "output": 10.0},
        }
        costs = COST_TABLE.get(payload.get("provider", ""), {"input": 0, "output": 0})
        payload["estimated_cost_usd"] = round(
            (payload.get("prompt_tokens", 0) * costs["input"] +
             payload.get("completion_tokens", 0) * costs["output"]) / 1_000_000,
            6
        )

        payload["ingested_at"] = datetime.utcnow().isoformat()
        return payload

    async def _persist(self, payload: Dict) -> str:
        return await self.db.save_log(payload)
