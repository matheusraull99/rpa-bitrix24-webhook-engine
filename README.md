# RPA Bitrix24 Webhook Engine

Plataforma de orquestração para RPAs disparados por **webhooks do Bitrix24**. Padrão de arquitetura usado em produção para automações que precisam:

- Receber sinal do CRM (mudança de stage, nova negociação, etc.)
- Enfileirar com **idempotência** (Bitrix re-envia webhook em falha)
- Executar **workers Playwright** em paralelo controlado
- **Atualizar o card** com resultado, anexos e timeline comment
- Sobreviver a restart sem perder execuções pendentes

## Stack

- FastAPI (webhook receiver)
- Redis Streams (fila durável + consumer groups)
- Playwright (workers)
- httpx (Bitrix REST)
- structlog (logs JSON com `deal_id` + `run_id` correlatos)
- Docker Compose

## Arquitetura

```
       Bitrix24 ─ webhook ─► FastAPI (/webhook/deal_update)
                                  │
                                  │ XADD Redis stream
                                  ▼
                       ┌───── jobs:rpa ─────┐
                       │                    │
                       ▼                    ▼
              Worker 1 (Playwright)   Worker N
                       │                    │
                       └───── PATCH card via Bitrix REST
```

## Como rodar

```bash
docker compose up -d redis
pip install -r requirements.txt
playwright install chromium

# Terminal 1: API
uvicorn src.api.main:app --reload --port 8000

# Terminal 2: worker
python -m src.workers.runner --bot consulta_inss

# Disparar webhook fake (simulando Bitrix)
curl -X POST http://localhost:8000/webhook/deal_update \
  -H "Content-Type: application/json" \
  -d '{"deal_id":"123","cpf":"12345678901","action":"consulta_inss"}'
```

## Idempotência

Bitrix re-envia o mesmo webhook em retry. Sem proteção, o robô roda 2-3x e atualiza o card duplicado.

Solução:

1. **Hash idempotente** = `sha256(deal_id + action + payload)`
2. Antes de enfileirar, `SADD processed:{hash}` com TTL de 1h
3. Se já existe → 200 OK, mas não enfileira

## Update do card no Bitrix

`src/bots/bitrix_client.py` encapsula:

- `comment_to_timeline(deal_id, text)` — escreve no histórico do card
- `update_field(deal_id, field, value)` — atualiza UF_CRM_*
- `upload_file(deal_id, path)` — anexa PDF/screenshot
- `move_stage(deal_id, stage_id)` — move pra próxima fase

## Garantias

- **At-least-once**: ack só depois de `update_field` confirmado
- **Visibility timeout**: 5 min — se worker travar, msg volta pra fila
- **Dead letter queue**: após 3 retries, vai pra `jobs:dlq` com motivo
- **Concorrência por deal**: 1 worker por `deal_id` simultâneo (lock Redis)

## Configuração de bots (plug-in)

`src/bots/registry.py`:

```python
BOTS = {
    "consulta_inss": "src.bots.inss:executar",
    "baixa_comprovante": "src.bots.comprovante:executar",
    "atualiza_cnpj_receita": "src.bots.receita:executar",
}
```

Adicionar um bot novo = 1 função + 1 linha no registry.

## Métricas Prometheus

- `rpa_jobs_total{bot, status}`
- `rpa_duration_seconds{bot}` (histogram)
- `rpa_queue_lag` (gauge)
- `bitrix_api_errors_total{endpoint, code}`
