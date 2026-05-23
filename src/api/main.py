"""Webhook receiver — recebe sinal do Bitrix24 e enfileira."""
from __future__ import annotations
import hashlib
import json
import os

import redis
import structlog
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

log = structlog.get_logger()
app = FastAPI(title="RPA Webhook Engine")
r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
STREAM = "jobs:rpa"


class WebhookPayload(BaseModel):
    deal_id: str = Field(..., min_length=1)
    action: str
    cpf: str | None = None
    extra: dict | None = None


def idempotency_key(p: WebhookPayload) -> str:
    raw = f"{p.deal_id}|{p.action}|{json.dumps(p.model_dump(), sort_keys=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@app.post("/webhook/deal_update")
def receive(payload: WebhookPayload):
    key = idempotency_key(payload)
    if r.set(f"processed:{key}", "1", nx=True, ex=3600) is None:
        log.info("duplicate_webhook_ignored", key=key, deal_id=payload.deal_id)
        return {"status": "duplicate", "key": key}

    msg_id = r.xadd(STREAM, {
        "deal_id": payload.deal_id,
        "action": payload.action,
        "cpf": payload.cpf or "",
        "extra": json.dumps(payload.extra or {}, ensure_ascii=False),
        "idem_key": key,
    })
    log.info("job_enqueued", msg_id=msg_id, deal_id=payload.deal_id, action=payload.action)
    return {"status": "queued", "message_id": msg_id, "idem_key": key}


@app.get("/health")
def health():
    try:
        r.ping(); return {"redis": "ok"}
    except Exception as e:
        raise HTTPException(503, f"redis down: {e}")
