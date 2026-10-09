"""Parse every DAG in dags/ and check it loads cleanly (no LLM calls are made)."""

from __future__ import annotations

from pathlib import Path

import pytest

from airflow.dag_processing.dagbag import DagBag

DAGS_DIR = Path(__file__).parents[2] / "dags"

EXPECTED = {
    "llm_review_enrichment": "llm",
    "agent_business_review": "agent",
    "llm_branch_failure_triage": "llm_branch",
    "llm_sql_self_serve_analytics": "llm_sql",
    "llm_file_analysis_vendor_gate": "llm_file_analysis",
    "llm_schema_compare_pre_etl": "llm_schema_compare",
}


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


@pytest.mark.parametrize(("dag_id", "tag"), EXPECTED.items())
def test_dag_present_and_tagged(dagbag: DagBag, dag_id: str, tag: str) -> None:
    dag = dagbag.dags.get(dag_id)
    assert dag is not None, f"{dag_id} missing"
    assert {"common-ai", tag} <= set(dag.tags)
