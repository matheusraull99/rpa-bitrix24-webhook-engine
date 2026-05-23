"""Worker: lê stream Redis e executa bots."""
from __future__ import annotations
import json
import os
import signal
import time

import redis
import structlog
import typer
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from ..bots.registry import resolve
from ..bots import bitrix_client

log = structlog.get_logger()
STREAM = "jobs:rpa"
GROUP = "rpa-workers"
DLQ = "jobs:dlq"
MAX_RETRIES = 3
VISIBILITY_MS = 5 * 60 * 1000

m_total = Counter("rpa_jobs_total", "jobs", ["bot", "status"])
m_dur = Histogram("rpa_duration_seconds", "duracao", ["bot"])
m_lag = Gauge("rpa_queue_lag", "lag")


def ensure_group(r):
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e): raise


def handle_msg(msg_id: str, fields: dict, r) -> None:
    job = {**fields, "extra": json.loads(fields.get("extra") or "{}")}
    bot = job["action"]
    deal_id = job["deal_id"]

    # lock por deal pra evitar concorrência
    lock_key = f"lock:deal:{deal_id}"
    if not r.set(lock_key, msg_id, nx=True, ex=300):
        log.warning("deal_locked", deal_id=deal_id); return  # outra worker pegou

    try:
        fn = resolve(bot)
        with m_dur.labels(bot=bot).time():
            result = fn(job)
        if os.getenv("BITRIX_WEBHOOK"):
            bitrix_client.comment_to_timeline(deal_id, f"RPA OK: {json.dumps(result, ensure_ascii=False)}")
        m_total.labels(bot=bot, status="ok").inc()
        r.xack(STREAM, GROUP, msg_id)
        log.info("job_ok", msg_id=msg_id, deal_id=deal_id)
    except Exception as exc:
        m_total.labels(bot=bot, status="fail").inc()
        retries = int(r.hincrby(f"retries:{msg_id}", "n", 1))
        if retries >= MAX_RETRIES:
            r.xadd(DLQ, {**fields, "error": str(exc), "retries": retries})
            r.xack(STREAM, GROUP, msg_id)
            log.error("job_dlq", msg_id=msg_id, deal_id=deal_id, error=str(exc))
        else:
            log.warning("job_retry", msg_id=msg_id, attempt=retries, error=str(exc))
    finally:
        r.delete(lock_key)


def main(metrics_port: int = 9101):
    start_http_server(metrics_port)
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379"), decode_responses=True)
    ensure_group(r)
    consumer = f"worker-{os.getpid()}"

    stop = False
    def handler(*_): nonlocal stop; stop = True
    signal.signal(signal.SIGTERM, handler); signal.signal(signal.SIGINT, handler)

    log.info("worker_start", consumer=consumer)
    while not stop:
        m_lag.set(r.xlen(STREAM))
        resp = r.xreadgroup(GROUP, consumer, {STREAM: ">"}, count=1, block=5000)
        if not resp:
            # poll pending (msgs travadas em outro worker que morreu)
            stale = r.xautoclaim(STREAM, GROUP, consumer, min_idle_time=VISIBILITY_MS, count=10)
            for msg_id, fields in stale[1]:
                handle_msg(msg_id, fields, r)
            continue
        for _, msgs in resp:
            for msg_id, fields in msgs:
                handle_msg(msg_id, fields, r)


if __name__ == "__main__":
    typer.run(main)
