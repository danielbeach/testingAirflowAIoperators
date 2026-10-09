"""
02 - @task.agent / AgentOperator: multi-step agent with tools.

Why: Some questions can't be answered with one prompt. "Why did revenue move this
month?" needs the model to look at the schema, run a query, notice something,
run another query, and then write it up. ``@task.agent`` gives the model a
toolset (here: ``SQLToolset`` against a read-only Postgres role) and lets it loop
until it has an answer, returning a typed report you can ship downstream.

Guardrails on display:
  * least-privilege DB role (``llm_reader``) - the real security boundary
  * ``allowed_tables`` / ``max_rows`` - application-level scoping
  * ``usage_limits`` - cap requests / tokens per run
"""

from __future__ import annotations

from pendulum import datetime
from pydantic import BaseModel, Field

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.toolsets.sql import SQLToolset
from airflow.sdk import dag, task

LLM_CONN_ID = "pydanticai_default"
DB_CONN_ID = "shop_llm_reader"


class BusinessReview(BaseModel):
    headline: str = Field(description="One-line takeaway")
    revenue_by_month: dict[str, float] = Field(description="YYYY-MM -> shipped revenue")
    top_customers: list[str]
    top_categories: list[str]
    anomalies: list[str] = Field(description="Cancellations, returns, outliers worth a look")
    queries_run: list[str] = Field(description="The SQL you ran to reach these conclusions")


shop_tools = SQLToolset(
    db_conn_id=DB_CONN_ID,
    allowed_tables=["customers", "orders", "order_items"],
    max_rows=50,
)


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "agent"],
    doc_md=__doc__,
)
def agent_business_review():
    @task.agent(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a senior analytics engineer. Use the SQL tools to inspect the schema "
            "before querying. Only count orders with status='shipped' as revenue. "
            "Cite every query you ran."
        ),
        toolsets=[shop_tools],
        output_type=BusinessReview,
        serialize_output=True,
        usage_limits={"request_limit": 15, "total_tokens_limit": 60_000},
    )
    def weekly_business_review(question: str) -> str:
        return question

    @task
    def publish(review: dict) -> None:
        # In real life: post to Slack / write to a reporting table.
        print(f"# {review['headline']}")
        for k, v in review.items():
            print(f"{k}: {v}")

    publish(
        weekly_business_review(
            "Produce a business review of the shop: monthly shipped revenue, top 3 customers "
            "by spend, top product categories, and any anomalies (cancellations, returns, "
            "customers with no shipped orders)."
        )
    )

    # Same thing, classic-operator style, as a free-text answer.
    AgentOperator(
        task_id="ad_hoc_question",
        llm_conn_id=LLM_CONN_ID,
        system_prompt="You are a SQL analyst. Explore the schema with the tools, then answer.",
        prompt="Which product category has the highest return or cancellation rate, and why might that be?",
        toolsets=[shop_tools],
    )


agent_business_review()
