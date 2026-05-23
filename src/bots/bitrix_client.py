"""Cliente Bitrix24 REST com retry."""
from __future__ import annotations
import os
import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

log = structlog.get_logger()
WEBHOOK_BASE = os.getenv("BITRIX_WEBHOOK", "")  # ex: https://sua-empresa.bitrix24.com.br/rest/1/abc/


@retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=20))
def _call(method: str, params: dict) -> dict:
    if not WEBHOOK_BASE:
        raise RuntimeError("BITRIX_WEBHOOK não configurado")
    r = httpx.post(f"{WEBHOOK_BASE}{method}.json", json=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Bitrix error: {data}")
    return data.get("result", {})


def comment_to_timeline(deal_id: str, text: str) -> None:
    _call("crm.timeline.comment.add", {
        "fields": {"ENTITY_ID": deal_id, "ENTITY_TYPE": "deal", "COMMENT": text}
    })
    log.info("timeline_comment_sent", deal_id=deal_id)


def update_field(deal_id: str, field: str, value) -> None:
    _call("crm.deal.update", {"id": deal_id, "fields": {field: value}})
    log.info("deal_updated", deal_id=deal_id, field=field)


def move_stage(deal_id: str, stage_id: str) -> None:
    update_field(deal_id, "STAGE_ID", stage_id)


def upload_file(deal_id: str, file_path: str, field: str = "UF_CRM_ARQUIVO") -> None:
    """Anexa arquivo a um UF do tipo file."""
    import base64
    with open(file_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()
    update_field(deal_id, field, [file_path.split("/")[-1], b64])
