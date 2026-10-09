"""
04 - @task.llm_sql / LLMSQLQueryOperator: natural language -> validated SQL.

Why: Stakeholders ask the same shape of question all week ("revenue by country
last quarter?"). ``@task.llm_sql`` introspects the real table schemas from the
DB connection, asks the LLM for a query, strips code fences, and validates it
with sqlglot (SELECT/UNION/... only - no DROP, no UPDATE). It *returns* SQL; it
does not execute it. A normal ``SQLExecuteQueryOperator`` runs it as a separate,
auditable step, against a read-only role.

Add ``require_approval=True, allow_modifications=True`` to have a human review /
edit the SQL in the Airflow UI before it runs (edits are re-validated).

Try it: Trigger DAG w/ config and change ``question``.
"""

from __future__ import annotations

from pendulum import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk import Param, dag, task

LLM_CONN_ID = "pydanticai_default"
DB_CONN_ID = "shop_llm_reader"


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "llm_sql"],
    doc_md=__doc__,
    params={
        "question": Param(
            "For each customer country, show shipped revenue and number of distinct "
            "customers, highest revenue first.",
            type="string",
        )
    },
)
def llm_sql_self_serve_analytics():
    @task.llm_sql(
        llm_conn_id=LLM_CONN_ID,
        db_conn_id=DB_CONN_ID,
        table_names=["customers", "orders", "order_items"],
        dialect="postgres",
        system_prompt="Prefer CTEs over nested subqueries. Revenue = orders.total where status='shipped'.",
    )
    def generate_sql(question: str) -> str:
        return question

    sql = generate_sql("{{ params.question }}")

    run_sql = SQLExecuteQueryOperator(
        task_id="run_sql",
        conn_id=DB_CONN_ID,
        sql=sql,
        show_return_value_in_logs=True,
    )

    @task
    def report(generated_sql: str, rows: list) -> None:
        print("SQL:\n" + generated_sql)
        print("Result:")
        for row in rows:
            print(row)

    report(sql, run_sql.output)


llm_sql_self_serve_analytics()
