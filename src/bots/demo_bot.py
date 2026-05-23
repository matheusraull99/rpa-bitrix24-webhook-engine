"""Bot demo: simula consulta web e devolve resultado."""
from playwright.sync_api import sync_playwright
import structlog

log = structlog.get_logger()


def executar(job: dict) -> dict:
    """job = {deal_id, action, cpf, extra}"""
    log.info("bot_start", deal_id=job["deal_id"], action=job["action"])
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("https://example.com")
        titulo = page.title()
        browser.close()
    return {"ok": True, "titulo_site": titulo, "cpf_consultado": job.get("cpf")}
