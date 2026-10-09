"""
03 - @task.llm_branch / LLMBranchOperator: let the LLM pick the next task.

Why: Routing on messy text is where ``if "timeout" in msg`` rules rot. Here a
pipeline-failure message comes in (from an on_failure_callback, an alert webhook,
a DQ check...). The LLM reads it and chooses which downstream task runs:
rerun, page on-call, open a data-quality ticket, or ignore.

The downstream task IDs are discovered from the DAG and handed to the model as a
constrained enum, so it *cannot* return a branch that doesn't exist. ``branches``
adds a description to each option so the model isn't guessing from the task id.

Try it: Trigger DAG w/ config and paste a different ``error_message``.
"""

from __future__ import annotations

from pendulum import datetime

from airflow.sdk import Param, dag, task

LLM_CONN_ID = "pydanticai_default"


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["common-ai", "llm_branch"],
    doc_md=__doc__,
    params={
        "failed_task": Param("load_orders", type="string"),
        "error_message": Param(
            "psycopg2.OperationalError: could not connect to server: Connection timed out. "
            "Is the server running on host db.internal (10.0.4.12) and accepting TCP/IP "
            "connections on port 5432?",
            type="string",
        ),
    },
)
def llm_branch_failure_triage():
    @task.llm_branch(
        llm_conn_id=LLM_CONN_ID,
        system_prompt=(
            "You are an on-call data engineer triaging a failed Airflow task. "
            "Pick the remediation that addresses the root cause."
        ),
        branches={
            "rerun": "Transient: timeouts, dropped connections, rate limits, spot instance loss.",
            "page_oncall": "Needs a human now: outage, credentials revoked, disk full, security issue.",
            "open_dq_ticket": (
                "The pipeline worked but the data is wrong: schema drift, null spikes, "
                "duplicate keys, failed expectations."
            ),
            "ignore": "Known flaky check or duplicate alert with no impact.",
        },
    )
    def triage(failed_task: str, error_message: str) -> str:
        return f"Task `{failed_task}` failed with:\n{error_message}"

    @task
    def rerun(**context):
        print(f"Clearing {context['params']['failed_task']} for a retry")

    @task
    def page_oncall(**context):
        print(f"PagerDuty: {context['params']['error_message'][:200]}")

    @task
    def open_dq_ticket(**context):
        print(f"Jira DQ ticket opened for {context['params']['failed_task']}")

    @task
    def ignore():
        print("Acknowledged, no action.")

    triage("{{ params.failed_task }}", "{{ params.error_message }}") >> [
        rerun(),
        page_oncall(),
        open_dq_ticket(),
        ignore(),
    ]


llm_branch_failure_triage()
