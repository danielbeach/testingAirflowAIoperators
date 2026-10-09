"""
01 - @task.llm / LLMOperator: enrich unstructured text with structured fields.

Why: You have a pile of free-text product reviews (or support tickets, call notes,
log lines...). Analytics wants sentiment, topics, and a churn-risk flag as real
columns. One LLM call per record with a Pydantic ``output_type`` turns prose into
typed rows, and dynamic task mapping fans it out so each review is its own
retryable, observable task instance.

Shows both flavours:
  * ``@task.llm``  -> the decorated function returns the *prompt* string.
  * ``LLMOperator`` -> classic operator, ``prompt`` is a templated argument.
"""

from __future__ import annotations

from typing import Literal

from pendulum import datetime
from pydantic import BaseModel, Field

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.sdk import dag, task

LLM_CONN_ID = "pydanticai_default"


class ReviewInsights(BaseModel):
    """Structured fields extracted from one review."""

    sentiment: Literal["positive", "neutral", "negative"]
    topics: list[str] = Field(description="Short lowercase topics, e.g. 'shipping', 'battery life'")
    churn_risk: bool = Field(description="True if the customer signals they may stop buying")
    summary: str = Field(description="One-sentence summary of the review")


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "llm"],
    doc_md=__doc__,
)
def llm_review_enrichment():
    @task
    def fetch_new_reviews() -> list[str]:
        # In real life: SELECT body FROM reviews WHERE ingested_at > {{ data_interval_start }}
        return [
            "Keyboard feels amazing, but it took three weeks to arrive. Not ordering here again.",
            "Monitor is fine. Colors are a little washed out out of the box.",
            "Standing desk motor died after 2 months and support never replied. Switching brands.",
            "Fast shipping and the headphones sound incredible. Bought a second pair for my partner.",
        ]

    @task.llm(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are a customer-insights analyst. Extract structured fields from a single "
            "product review. Be conservative: only set churn_risk when the customer says so."
        ),
        output_type=ReviewInsights,
        serialize_output=True,  # push a plain dict to XCom
    )
    def enrich_review(review: str) -> str:
        # The return value is the prompt sent to the model.
        return f"Review:\n{review}"

    @task
    def to_rows(reviews: list[str], insights: list[dict]) -> list[dict]:
        rows = [{"review": r, **i} for r, i in zip(reviews, insights, strict=True)]
        for row in rows:
            print(row)
        # In real life: insert into analytics.review_insights
        return rows

    reviews = fetch_new_reviews()
    insights = enrich_review.expand(review=reviews)
    rows = to_rows(reviews, insights)

    # Classic operator form: summarise the enriched batch for a Slack/email digest.
    digest = LLMOperator(
        task_id="weekly_digest",
        llm_conn_id=LLM_CONN_ID,
        system_prompt="You write crisp, 3-bullet executive digests. No preamble.",
        prompt=(
            "Summarise these enriched product reviews for the product team, calling out "
            "anything with churn_risk=true:\n{{ ti.xcom_pull(task_ids='to_rows') }}"
        ),
        usage_limits={"request_limit": 3, "output_tokens_limit": 600},
    )

    rows >> digest


llm_review_enrichment()
