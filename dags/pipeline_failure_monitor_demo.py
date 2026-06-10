"""
Airflow DAG for the pipeline failure predictor demo.

Place this file in Airflow's dags folder, or point AIRFLOW__CORE__DAGS_FOLDER
to the repository's dags directory. The DAG uses the same simulation engine as
the Streamlit dashboard so the orchestration demo and visual demo stay aligned.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(
    os.environ.get("PIPELINE_PROJECT_ROOT", Path(__file__).resolve().parents[1])
)
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from airflow.sdk import Param, dag, get_current_context, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context


@dag(
    dag_id="pipeline_failure_monitor_demo",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["final-year-project", "failure-prediction", "plugin-demo"],
    params={
        "demo_type": Param(
            "etl",
            type="string",
            enum=["attached", "etl", "synthetic"],
        ),
        "pipeline_command": Param(
            "python examples/real_pipeline_job.py --scenario mixed --delay 0.2",
            type="string",
        ),
        "scenario": Param(
            "mixed",
            type="string",
            enum=["normal", "warning", "recovering", "risky", "mixed"],
        ),
        "loops": Param(1, type="integer", minimum=1, maximum=5),
        "min_lines": Param(5, type="integer", minimum=3, maximum=20),
        "stop_policy": Param(
            "advise",
            type="string",
            enum=["advise", "auto_stop"],
        ),
        "fail_airflow_task_on_high": Param(False, type="boolean"),
    },
)
def pipeline_failure_monitor_demo():
    @task
    def prepare_run() -> dict:
        context = get_current_context()
        params = context["params"]
        return {
            "demo_type": params["demo_type"],
            "pipeline_command": params["pipeline_command"],
            "scenario": params["scenario"],
            "loops": int(params["loops"]),
            "min_lines": int(params["min_lines"]),
            "stop_policy": params["stop_policy"],
            "fail_airflow_task_on_high": bool(params["fail_airflow_task_on_high"]),
            "project_root": str(PROJECT_ROOT),
        }

    @task
    def run_pipeline_with_predictor(config: dict) -> dict:
        project_root = Path(config["project_root"])
        if config["demo_type"] == "attached":
            from live_pipeline_runner import run_attached_pipeline

            result = run_attached_pipeline(
                config["pipeline_command"],
                stop_policy=config["stop_policy"],
                min_lines=config["min_lines"],
                working_dir=project_root,
                raw_log_path=project_root / "simulated" / "airflow_attached_pipeline.raw.log",
                normalized_log_path=project_root / "simulated" / "airflow_attached_pipeline.normalized.log",
            )
            return result["summary"]

        if config["demo_type"] == "etl":
            from etl_pipeline_demo import run_monitored_etl

            result = run_monitored_etl(
                scenario=config["scenario"],
                stop_policy=config["stop_policy"],
                min_lines=config["min_lines"],
                log_path=project_root / "simulated" / "airflow_etl_pipeline.log",
                output_path=project_root / "outputs" / "airflow_curated_orders.csv",
            )
            return result["summary"]

        from simulation_engine import SimulationSession

        log_path = project_root / "simulated" / "airflow_pipeline_run.log"
        session = SimulationSession(
            scenario=config["scenario"],
            loops=config["loops"],
            stop_policy=config["stop_policy"],
            min_lines=config["min_lines"],
            log_path=log_path,
        )
        session.start()
        session.run_to_end(events_per_tick=5)
        return session.summary()

    @task
    def evaluate_intervention(summary: dict, config: dict) -> dict:
        high_count = summary["risk_counts"].get("HIGH", 0)
        decision = "continue"
        if high_count and summary["stop_policy"] == "auto_stop":
            decision = "pipeline_stopped"
        elif high_count:
            decision = "operator_review_advised"

        result = {
            "decision": decision,
            "high_risk_count": high_count,
            "report_path": summary.get("report_path"),
        }
        print(result)

        if high_count and config.get("fail_airflow_task_on_high"):
            from airflow.exceptions import AirflowException

            raise AirflowException(
                f"HIGH risk detected in {high_count} prediction(s). "
                f"Report: {summary.get('report_path')}"
            )
        return result

    @task
    def publish_report(summary: dict, decision: dict) -> dict:
        final = {
            "summary": summary,
            "decision": decision,
        }
        print(final)
        return final

    config = prepare_run()
    summary = run_pipeline_with_predictor(config)
    decision = evaluate_intervention(summary, config)
    publish_report(summary, decision)


pipeline_failure_monitor_demo()
