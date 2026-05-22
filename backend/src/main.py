"""
LLM Inference Logger - Main FastAPI Application
Ingestion pipeline, REST API, and WebSocket support
"""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timedelta
from typing import AsyncGenerator, Optional

import aiosqlite
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .db.database import Database
from .pipeline.ingestion import IngestionPipeline
from .pii.redactor import PIIRedactor
from .sdk.wrapper import LLMWrapper

app = FastAPI(title="LLM Inference Logger", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
pipeline = IngestionPipeline(db)
redactor = PIIRedactor()
llm = LLMWrapper(pipeline=pipeline, redactor=redactor)


@app.on_event("startup")
async def startup():
    await db.initialize()


# ─── Pydantic Models ──────────────────────────────────────────────────────────

class ChatMessage(BaseModel):
    content: str
    session_id: Optional[str] = None
    provider: str = "anthropic"
    model: Optional[str] = None
    stream: bool = False


class LogPayload(BaseModel):
    session_id: str
    request_id: str
    provider: str
    model: str
    latency_ms: float
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    timestamp_start: str
    timestamp_end: str
    status: str
    error: Optional[str] = None
    input_preview: str
    output_preview: str
    metadata: Optional[dict] = None


# ─── Chat Endpoints ───────────────────────────────────────────────────────────

@app.post("/api/chat")
async def chat(msg: ChatMessage, background_tasks: BackgroundTasks):
    session_id = msg.session_id or str(uuid.uuid4())
    history = await db.get_conversation_history(session_id)

    if msg.stream:
        async def event_stream():
            full_response = ""
            async for chunk in llm.stream_chat(
                message=msg.content,
                history=history,
                session_id=session_id,
                provider=msg.provider,
                model=msg.model,
            ):
                full_response += chunk
                yield f"data: {json.dumps({'chunk': chunk, 'session_id': session_id})}\n\n"
            yield f"data: {json.dumps({'done': True, 'session_id': session_id})}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    response, log = await llm.chat(
        message=msg.content,
        history=history,
        session_id=session_id,
        provider=msg.provider,
        model=msg.model,
    )

    return {
        "session_id": session_id,
        "response": response,
        "log": log,
    }


@app.get("/api/conversations")
async def list_conversations():
    return await db.list_conversations()


@app.get("/api/conversations/{session_id}")
async def get_conversation(session_id: str):
    messages = await db.get_conversation_history(session_id)
    meta = await db.get_conversation_meta(session_id)
    if not messages and not meta:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return {"session_id": session_id, "messages": messages, "meta": meta}


@app.delete("/api/conversations/{session_id}")
async def cancel_conversation(session_id: str):
    await db.cancel_conversation(session_id)
    return {"status": "cancelled", "session_id": session_id}


# ─── Ingestion Endpoint ───────────────────────────────────────────────────────

@app.post("/api/ingest/log")
async def ingest_log(payload: LogPayload):
    processed = await pipeline.process(payload.dict())
    return {"status": "ok", "record_id": processed["id"]}


# ─── Metrics / Dashboard ──────────────────────────────────────────────────────

@app.get("/api/metrics/summary")
async def metrics_summary():
    return await db.get_metrics_summary()


@app.get("/api/metrics/latency")
async def metrics_latency(hours: int = 24):
    return await db.get_latency_over_time(hours)


@app.get("/api/metrics/throughput")
async def metrics_throughput(hours: int = 24):
    return await db.get_throughput_over_time(hours)


@app.get("/api/metrics/errors")
async def metrics_errors(hours: int = 24):
    return await db.get_error_rate_over_time(hours)


@app.get("/api/metrics/providers")
async def metrics_providers():
    return await db.get_provider_breakdown()


@app.get("/api/logs")
async def get_logs(limit: int = 50, offset: int = 0):
    return await db.get_logs(limit=limit, offset=offset)


@app.get("/health")
async def health():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}
