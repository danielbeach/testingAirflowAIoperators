"""
05 - @task.llm_file_analysis / LLMFileAnalysisOperator: let the LLM read a file.

Why: A vendor drops a CSV every morning. Schema checks pass, but the data is
subtly wrong - duplicate txn ids, negative amounts, mixed date formats, 'usd' vs
'USD', a $9,999,999 outlier. Writing a rule for every new way a vendor can mess
up is endless. Here the operator reads the file (CSV / Parquet / Avro / JSON /
logs, or images + PDFs with ``multi_modal=True``) via ObjectStoragePath, samples
it into the prompt, and returns a structured QA verdict that gates the load.

``file_path`` works with ``file://``, ``s3://``, ``gs://``, ``abfs://`` ... and
can be a single file or a prefix (bounded by ``max_files`` / size limits).
"""

from __future__ import annotations

from typing import Literal

from pendulum import datetime
from pydantic import BaseModel, Field

from airflow.sdk import Param, dag, task

LLM_CONN_ID = "pydanticai_default"


class Issue(BaseModel):
    severity: Literal["critical", "warning", "info"]
    column: str
    description: str
    example_row_ids: list[str] = Field(default_factory=list)


class FileQAReport(BaseModel):
    safe_to_load: bool = Field(description="False if any critical issue exists")
    row_count: int
    issues: list[Issue]
    summary: str


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "llm_file_analysis"],
    doc_md=__doc__,
    params={
        "file_path": Param(
            "file:///usr/local/airflow/include/data/vendor_transactions_2025-10-08.csv",
            type="string",
        )
    },
)
def llm_file_analysis_vendor_gate():
    @task.llm_file_analysis(
        llm_conn_id=LLM_CONN_ID,
        file_path="{{ params.file_path }}",
        sample_rows=50,  # small file: let the model see all of it
        system_prompt=(
            "You are a data-quality reviewer for card transaction feeds. "
            "Critical = would corrupt financial reporting (duplicate txn_id, negative or "
            "absurd amounts, missing card_last4). Warning = needs normalising (casing, "
            "date formats, country codes, missing mcc, timestamps outside the file date)."
        ),
        output_type=FileQAReport,
        serialize_output=True,
    )
    def inspect_vendor_file() -> str:
        return "Review this vendor transaction file before we load it into the warehouse."

    @task.branch
    def gate(report: dict) -> str:
        return "load_to_warehouse" if report["safe_to_load"] else "quarantine_file"

    @task
    def load_to_warehouse():
        print("COPY INTO raw.vendor_transactions ...")

    @task
    def quarantine_file(report: dict, **context):
        print(f"Moving {context['params']['file_path']} to quarantine/")
        for issue in report["issues"]:
            print(f"[{issue['severity']}] {issue['column']}: {issue['description']}")

    report = inspect_vendor_file()
    gate(report) >> [load_to_warehouse(), quarantine_file(report)]


llm_file_analysis_vendor_gate()
