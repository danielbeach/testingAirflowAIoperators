"""
06 - @task.llm_schema_compare / LLMSchemaCompareOperator: cross-system drift check.

Why: Source and target schemas drift. ``BIGINT`` vs ``INTEGER``, ``TIMESTAMPTZ`` vs
``TIMESTAMP``, a new column upstream that the warehouse never got. A plain
``information_schema`` diff flags ``varchar(255)`` vs ``text`` as a mismatch and
misses the semantic risk of ``timestamptz`` -> ``timestamp``. The LLM does the
cross-system type reasoning and returns a typed ``SchemaCompareResult``:
``compatible``, a list of ``mismatches`` (with severity + suggested migration SQL),
and a ``summary``.

Run this *before* the nightly ETL and short-circuit on drift.

Seeded drift (include/sql/init/02_warehouse.sql): id BIGINT->INTEGER,
email 255->100, loyalty_tier missing, signup_ts TZ dropped, lifetime_value precision.
"""

from __future__ import annotations

from pendulum import datetime

from airflow.sdk import dag, task

LLM_CONN_ID = "pydanticai_default"


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "llm_schema_compare"],
    doc_md=__doc__,
)
def llm_schema_compare_pre_etl():
    @task.llm_schema_compare(
        llm_conn_id=LLM_CONN_ID,
        db_conn_ids=["shop_db", "warehouse_db"],  # source, target
        table_names=["customers"],
        context_strategy="full",  # include PKs / FKs / indexes
    )
    def check_customers_drift(ds=None) -> str:
        return (
            f"Nightly load {ds}: shop.customers is copied into warehouse.customers. "
            "Flag anything that would fail the load or silently lose data. Report only."
        )

    @task.branch
    def decide(result: dict) -> str:
        return "run_etl" if result["compatible"] else "open_migration_ticket"

    @task
    def run_etl():
        print("INSERT INTO warehouse.customers SELECT ... FROM shop.customers")

    @task
    def open_migration_ticket(result: dict):
        print(result["summary"])
        for m in sorted(result["mismatches"], key=lambda m: m["severity"]):
            print(f"[{m['severity'].upper()}] {m['column']}: {m['source_type']} -> {m['target_type']}")
            print(f"    {m['description']}")
            print(f"    fix: {m['migration_query']}")

    result = check_customers_drift()
    decide(result) >> [run_etl(), open_migration_ticket(result)]


llm_schema_compare_pre_etl()
