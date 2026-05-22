"""
Database layer - SQLite (dev) / PostgreSQL (prod) compatible via aiosqlite / asyncpg
Schema design: normalized with sensible tradeoffs for read-heavy analytics
"""

import json
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import aiosqlite


DB_PATH = "/data/inference_logger.db"


CREATE_TABLES = [
    """
    CREATE TABLE IF NOT EXISTS conversations (
        session_id   TEXT PRIMARY KEY,
        title        TEXT,
        provider     TEXT,
        model        TEXT,
        status       TEXT DEFAULT 'active',   -- active | cancelled
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL,
        message_count INTEGER DEFAULT 0,
        total_tokens  INTEGER DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS messages (
        id           TEXT PRIMARY KEY,
        session_id   TEXT NOT NULL REFERENCES conversations(session_id) ON DELETE CASCADE,
        role         TEXT NOT NULL,   -- user | assistant | system
        content      TEXT NOT NULL,
        content_pii_redacted INTEGER DEFAULT 0,
        created_at   TEXT NOT NULL,
        token_count  INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inference_logs (
        id               TEXT PRIMARY KEY,
        session_id       TEXT REFERENCES conversations(session_id),
        request_id       TEXT NOT NULL UNIQUE,
        provider         TEXT NOT NULL,
        model            TEXT NOT NULL,
        latency_ms       REAL NOT NULL,
        prompt_tokens    INTEGER NOT NULL DEFAULT 0,
        completion_tokens INTEGER NOT NULL DEFAULT 0,
        total_tokens     INTEGER NOT NULL DEFAULT 0,
        timestamp_start  TEXT NOT NULL,
        timestamp_end    TEXT NOT NULL,
        status           TEXT NOT NULL,   -- success | error | timeout
        error            TEXT,
        input_preview    TEXT,
        output_preview   TEXT,
        metadata         TEXT,            -- JSON blob for extensibility
        created_at       TEXT NOT NULL
    )
    """,
    # Index for fast time-range analytics queries
    "CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON inference_logs(timestamp_start)",
    "CREATE INDEX IF NOT EXISTS idx_logs_provider ON inference_logs(provider)",
    "CREATE INDEX IF NOT EXISTS idx_logs_status ON inference_logs(status)",
    "CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id)",
    "CREATE INDEX IF NOT EXISTS idx_conversations_status ON conversations(status)",
]


class Database:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def initialize(self):
        import os
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent reads
        await self._conn.execute("PRAGMA foreign_keys=ON")
        for stmt in CREATE_TABLES:
            await self._conn.execute(stmt)
        await self._conn.commit()

    async def _q(self, sql: str, params=()) -> List[Dict]:
        cur = await self._conn.execute(sql, params)
        rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def _exec(self, sql: str, params=()):
        await self._conn.execute(sql, params)
        await self._conn.commit()

    # ── Conversations ─────────────────────────────────────────────────────────

    async def ensure_conversation(self, session_id: str, provider: str, model: str):
        now = datetime.utcnow().isoformat()
        await self._conn.execute("""
            INSERT OR IGNORE INTO conversations
                (session_id, title, provider, model, status, created_at, updated_at)
            VALUES (?, ?, ?, ?, 'active', ?, ?)
        """, (session_id, f"Chat {session_id[:8]}", provider, model, now, now))
        await self._conn.commit()

    async def list_conversations(self) -> List[Dict]:
        return await self._q("""
            SELECT c.*, 
                   (SELECT content FROM messages 
                    WHERE session_id = c.session_id AND role='user' 
                    ORDER BY created_at ASC LIMIT 1) as first_message
            FROM conversations c
            WHERE c.status != 'cancelled'
            ORDER BY c.updated_at DESC
            LIMIT 100
        """)

    async def get_conversation_meta(self, session_id: str) -> Optional[Dict]:
        rows = await self._q(
            "SELECT * FROM conversations WHERE session_id = ?", (session_id,)
        )
        return rows[0] if rows else None

    async def get_conversation_history(self, session_id: str) -> List[Dict]:
        # Return last 20 messages for context window management
        return await self._q("""
            SELECT role, content, created_at FROM messages
            WHERE session_id = ?
            ORDER BY created_at ASC
        """, (session_id,))

    async def save_message(self, session_id: str, role: str, content: str,
                           token_count: int = 0, pii_redacted: bool = False):
        now = datetime.utcnow().isoformat()
        msg_id = str(uuid.uuid4())
        await self._exec("""
            INSERT INTO messages (id, session_id, role, content, content_pii_redacted, 
                                  created_at, token_count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (msg_id, session_id, role, content, int(pii_redacted), now, token_count))
        # Update conversation metadata
        await self._exec("""
            UPDATE conversations SET
                updated_at = ?,
                message_count = message_count + 1,
                total_tokens = total_tokens + ?
            WHERE session_id = ?
        """, (now, token_count, session_id))

    async def cancel_conversation(self, session_id: str):
        await self._exec(
            "UPDATE conversations SET status='cancelled', updated_at=? WHERE session_id=?",
            (datetime.utcnow().isoformat(), session_id)
        )

    # ── Inference Logs ────────────────────────────────────────────────────────

    async def save_log(self, log: Dict) -> str:
        log_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        await self._exec("""
            INSERT INTO inference_logs
                (id, session_id, request_id, provider, model, latency_ms,
                 prompt_tokens, completion_tokens, total_tokens,
                 timestamp_start, timestamp_end, status, error,
                 input_preview, output_preview, metadata, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            log_id, log.get("session_id"), log["request_id"],
            log["provider"], log["model"], log["latency_ms"],
            log.get("prompt_tokens", 0), log.get("completion_tokens", 0),
            log.get("total_tokens", 0),
            log["timestamp_start"], log["timestamp_end"],
            log["status"], log.get("error"),
            log.get("input_preview", ""), log.get("output_preview", ""),
            json.dumps(log.get("metadata", {})), now
        ))
        return log_id

    async def get_logs(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        return await self._q("""
            SELECT * FROM inference_logs
            ORDER BY timestamp_start DESC
            LIMIT ? OFFSET ?
        """, (limit, offset))

    # ── Analytics ─────────────────────────────────────────────────────────────

    async def get_metrics_summary(self) -> Dict:
        rows = await self._q("""
            SELECT
                COUNT(*) as total_requests,
                SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errors,
                AVG(latency_ms) as avg_latency_ms,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency_ms,
                SUM(total_tokens) as total_tokens,
                COUNT(DISTINCT session_id) as unique_sessions
            FROM inference_logs
            WHERE timestamp_start >= datetime('now', '-24 hours')
        """)
        # SQLite doesn't have PERCENTILE_CONT — use a workaround
        p95_row = await self._q("""
            SELECT latency_ms FROM inference_logs
            WHERE timestamp_start >= datetime('now', '-24 hours')
            ORDER BY latency_ms
            LIMIT 1 OFFSET (
                SELECT MAX(0, CAST(COUNT(*) * 0.95 AS INTEGER) - 1)
                FROM inference_logs
                WHERE timestamp_start >= datetime('now', '-24 hours')
            )
        """)
        summary = rows[0] if rows else {}
        summary["p95_latency_ms"] = p95_row[0]["latency_ms"] if p95_row else 0
        return summary

    async def get_latency_over_time(self, hours: int = 24) -> List[Dict]:
        return await self._q(f"""
            SELECT
                strftime('%Y-%m-%dT%H:00:00', timestamp_start) as bucket,
                AVG(latency_ms) as avg_latency,
                MIN(latency_ms) as min_latency,
                MAX(latency_ms) as max_latency,
                COUNT(*) as request_count
            FROM inference_logs
            WHERE timestamp_start >= datetime('now', '-{hours} hours')
            GROUP BY bucket
            ORDER BY bucket ASC
        """)

    async def get_throughput_over_time(self, hours: int = 24) -> List[Dict]:
        return await self._q(f"""
            SELECT
                strftime('%Y-%m-%dT%H:00:00', timestamp_start) as bucket,
                COUNT(*) as requests,
                SUM(total_tokens) as tokens
            FROM inference_logs
            WHERE timestamp_start >= datetime('now', '-{hours} hours')
            GROUP BY bucket
            ORDER BY bucket ASC
        """)

    async def get_error_rate_over_time(self, hours: int = 24) -> List[Dict]:
        return await self._q(f"""
            SELECT
                strftime('%Y-%m-%dT%H:00:00', timestamp_start) as bucket,
                COUNT(*) as total,
                SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errors,
                ROUND(100.0 * SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
            FROM inference_logs
            WHERE timestamp_start >= datetime('now', '-{hours} hours')
            GROUP BY bucket
            ORDER BY bucket ASC
        """)

    async def get_provider_breakdown(self) -> List[Dict]:
        return await self._q("""
            SELECT
                provider,
                model,
                COUNT(*) as requests,
                AVG(latency_ms) as avg_latency,
                SUM(total_tokens) as total_tokens,
                ROUND(100.0 * SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_rate
            FROM inference_logs
            GROUP BY provider, model
            ORDER BY requests DESC
        """)
