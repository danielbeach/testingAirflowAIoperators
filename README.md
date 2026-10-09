# Testing the Apache Airflow AI Operators

These are example DAGs for the new **Common AI provider**
([`apache-airflow-providers-common-ai`](https://airflow.apache.org/docs/apache-airflow-providers-common-ai/),
[launch blog post](https://airflow.apache.org/blog/common-ai-provider/)). The project runs locally with the
**Astro CLI** and uses **uv** for the local Python environment, tests and linting.

There is one DAG for each of the six LLM tasks and operators. Each DAG covers a realistic data engineering
reason to use that operator.

| # | DAG | Decorator / Operator | Use case |
|---|-----|----------------------|----------|
| 1 | `llm_review_enrichment` | `@task.llm` / `LLMOperator` | Turn free-text reviews into typed columns (sentiment, topics, churn risk), fanned out with `.expand()`, then write a digest |
| 2 | `agent_business_review` | `@task.agent` / `AgentOperator` | A multi-step SQL analyst agent explores the `shop` DB with `SQLToolset` and returns a structured business review |
| 3 | `llm_branch_failure_triage` | `@task.llm_branch` / `LLMBranchOperator` | Read a pipeline failure message and choose `rerun`, `page_oncall`, `open_dq_ticket` or `ignore` |
| 4 | `llm_sql_self_serve_analytics` | `@task.llm_sql` / `LLMSQLQueryOperator` | Turn a stakeholder question into SQL that sqlglot validates, then run it with `SQLExecuteQueryOperator` |
| 5 | `llm_file_analysis_vendor_gate` | `@task.llm_file_analysis` / `LLMFileAnalysisOperator` | Check a vendor CSV drop for data quality and send it to load or quarantine |
| 6 | `llm_schema_compare_pre_etl` | `@task.llm_schema_compare` / `LLMSchemaCompareOperator` | Detect drift between `shop.customers` and `warehouse.customers` before the nightly ETL |

---

## What is the LLM Operator?

`LLMOperator` (and its TaskFlow twin, `@task.llm`) is the base building block of the provider. It is an
Airflow task that makes **one LLM call**: it sends a prompt (plus an optional system prompt) to whichever model
an Airflow Connection points at, and it pushes the model's response to XCom. The response is either plain
text or a **validated Pydantic object**.

It runs on [Pydantic AI](https://ai.pydantic.dev/), so one `pydanticai` connection type works with OpenAI,
Anthropic, Groq, Mistral, Ollama, vLLM and others. Azure, Bedrock and Vertex have their own connection types.
To switch providers, you edit the connection. You don't change the DAG.

### Two ways to write it

```python
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.sdk import task

# 1. Classic operator: prompt is a (Jinja-templated) argument
LLMOperator(
    task_id="summarize",
    llm_conn_id="pydanticai_default",
    system_prompt="You are a financial analyst. Be concise.",
    prompt="Summarize: {{ ti.xcom_pull(task_ids='fetch_report') }}",
)

# 2. TaskFlow decorator: the function body *builds and returns the prompt*
@task.llm(llm_conn_id="pydanticai_default", system_prompt="Extract entities.", output_type=Entities)
def extract(text: str) -> str:
    return f"Extract entities from: {text}"
```

The decorator is the same operator under the hood. Your function's return value becomes `prompt`, so you can
build prompts with ordinary Python from upstream XComs. It also behaves like any other task: it supports
`.expand()`, retries, pools and SLAs.

### Key parameters

| Parameter | What it does |
|-----------|--------------|
| `llm_conn_id` | Airflow connection holding the API key and default model (`extra: {"model": "anthropic:claude-sonnet-5-5"}`) |
| `model_id` | Override the connection's model per task |
| `system_prompt` | Instructions for the model |
| `prompt` | The user message. In the decorator, the function's return value |
| `output_type` | A Pydantic model (or other type) for **structured output**. The model is forced to fill the schema and the result is validated |
| `serialize_output` | Push `model_dump()` (a plain dict) to XCom instead of the Pydantic object |
| `usage_limits` | Guardrails: `request_limit`, token limits, `cost_limit` (can be templated) |
| `fallback_conn_ids` | Fail over to other LLM connections if the primary is down |
| `agent_params` | Passed through to the Pydantic AI `Agent` (`model_settings`, `retries`, ...) |
| `require_approval`, `allow_modifications`, `approval_timeout` | **Human-in-the-loop**: pause the task and approve or edit the output in the Airflow UI |

### Why it matters

The other five operators **subclass or extend** it: `llm_branch`, `llm_sql`, `llm_file_analysis` and
`llm_schema_compare` are `LLMOperator`s with a specialised prompt and output schema. `agent` adds a tool loop.
Because of that, connections, structured output, usage limits, fallbacks, HITL approval and logging of tokens
and cost work the same way across all six. Every LLM call in your pipeline is a normal Airflow task: it is
retryable, observable, templatable and governed by connections. You don't need a separate Python script that
calls `openai.chat.completions.create` from inside a `PythonOperator`.

---

## The six tasks/operators at a glance

| Decorator | Operator | One-liner |
|-----------|----------|-----------|
| `@task.llm` | `LLMOperator` | One prompt, one response (text or Pydantic model) |
| `@task.agent` | `AgentOperator` | Multi-step agent with **toolsets** (`SQLToolset`, `HookToolset`, `MCPToolset`, `DataFusionToolset`), durable execution and HITL review |
| `@task.llm_branch` | `LLMBranchOperator` | LLM picks downstream task IDs, sent to the model as a constrained enum |
| `@task.llm_sql` | `LLMSQLQueryOperator` | Natural language to SQL. Introspects the schema and validates with sqlglot. **Returns SQL but doesn't run it** |
| `@task.llm_file_analysis` | `LLMFileAnalysisOperator` | Reads CSV, Parquet, Avro, JSON or logs (and images or PDFs with `multi_modal=True`) from any `ObjectStoragePath` |
| `@task.llm_schema_compare` | `LLMSchemaCompareOperator` | Detects schema drift across systems. Returns `SchemaCompareResult(compatible, mismatches[], summary)` |

---

## Project layout

```
.
├── dags/
│   ├── 01_llm_review_enrichment.py
│   ├── 02_agent_business_review.py
│   ├── 03_llm_branch_failure_triage.py
│   ├── 04_llm_sql_self_serve_analytics.py
│   ├── 05_llm_file_analysis_vendor_gate.py
│   └── 06_llm_schema_compare_pre_etl.py
├── include/
│   ├── data/vendor_transactions_2025-10-08.csv   # deliberately messy vendor file (DAG 5)
│   └── sql/init/                                  # seeds demo-postgres: shop + drifted warehouse DBs
├── tests/dags/test_dag_integrity.py               # DagBag parse test (no LLM calls)
├── docker-compose.override.yml                    # adds demo-postgres to the Astro stack
├── Dockerfile                                     # Astro Runtime 3.3 (Airflow 3.3)
├── requirements.txt                               # providers installed into the Astro image
├── pyproject.toml / uv.lock                       # local env for IDE, tests, ruff
└── .env.example                                   # connections (LLM key + demo DBs)
```

## Setup

### Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) and Docker
- An API key for Anthropic or OpenAI (or any other Pydantic AI provider)

### 1. Local Python env (IDE, tests, lint)

```bash
uv sync
uv run pytest            # parses all DAGs
uv run ruff check .
```

### 2. Configure connections

```bash
cp .env.example .env
# edit .env -> put your API key in AIRFLOW_CONN_PYDANTICAI_DEFAULT
```

```bash
AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai", "password": "sk-ant-...", "extra": {"model": "anthropic:claude-sonnet-5-5"}}'
```

The model string is `<provider>:<model>`, for example `openai:gpt-5` or `anthropic:claude-sonnet-5-5`.
The demo database connections are already filled in:

| Conn ID | Points at | Used by |
|---------|-----------|---------|
| `shop_llm_reader` | `shop` DB as the **read-only** `llm_reader` role | agent (DAG 2), LLM SQL (DAG 4) |
| `shop_db` | `shop` DB (source) | schema compare (DAG 6) |
| `warehouse_db` | `warehouse` DB (target, intentionally drifted) | schema compare (DAG 6) |

### 3. Start Airflow

```bash
astro dev start
```

- Airflow UI: <http://localhost:8081> (set in `.astro/config.yaml` so it doesn't clash with another project on 8080)
- Demo Postgres: `localhost:5433` (user `demo` / pass `demo`, databases `shop` and `warehouse`)

The `demo-postgres` init scripts only run when the volume is first created. To reseed, run
`astro dev kill && astro dev start`.

### 4. Run a DAG

Unpause it and trigger it from the UI. DAGs 3, 4 and 5 accept **Trigger with config** params (an error
message, a question, a file path), so you can try your own inputs. From the CLI:

```bash
astro dev run dags test llm_schema_compare_pre_etl
```

### Smoke-test without an API key

Pydantic AI ships a fake `test` model that returns schema-valid dummy data. Use it to test DAG wiring for free:

```bash
docker exec -e AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type":"pydanticai","extra":{"model":"test"}}' \
  $(docker ps -qf name=scheduler) airflow dags test llm_file_analysis_vendor_gate
```

DAGs 1, 3, 5 and 6 pass end to end this way. DAGs 2 and 4 fail with the test model because it generates
junk SQL, and the SQL guardrails (`SQLSafetyError`) reject it. That's the expected result. They need a real
model.

## Notes and gotchas

- **Apple Silicon (M4) + Astro Runtime 3.3-9:** the image's `cryptography 50.x` wheel crashes every Airflow
  container with exit code **132 (SIGILL)**. `requirements.txt` pins `cryptography==46.0.7` as a workaround.
- **Security:** `allowed_tables` on `SQLToolset` is an application-level guardrail, *not* a security
  boundary. The real boundary is the least-privilege `llm_reader` Postgres role, which only has `SELECT` on
  three tables. Point anything an LLM touches at a role like that.
- `@task.llm_sql` **only generates** SQL. Running it is a separate, auditable `SQLExecuteQueryOperator` task.
  Add `require_approval=True, allow_modifications=True` to have a person review the SQL first.
- `LLMSchemaCompareOperator` replaces the default system prompt if you pass `system_prompt`. The default
  contains the type-equivalence rules, so pass your own only if you mean to replace them.
- The provider is version `0.x` and moves quickly (`0.1.0` at launch, `0.10.0` when this repo was written).
  Expect breaking changes.
