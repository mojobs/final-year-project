from __future__ import annotations

import html
import json
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

from config import ETL_MODEL_SAVE_PATH, OUTPUTS_DIR
from simulation_engine import (
    PIPELINE_STAGES,
    SimulationSession,
    available_scenarios,
)
from etl_pipeline_demo import ETL_SCENARIOS, run_monitored_etl
from live_pipeline_runner import DEFAULT_COMMAND, LiveAttachSession


st.set_page_config(
    page_title="Pipeline Failure Predictor",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)


RISK_COLORS = {
    "LOW": "#287d4f",
    "MEDIUM": "#a86800",
    "HIGH": "#b3261e",
}

ETL_STAGES = [
    {"id": "Extract", "label": "Extract"},
    {"id": "Validate", "label": "Validate"},
    {"id": "Transform", "label": "Transform"},
    {"id": "Load", "label": "Load"},
    {"id": "Report", "label": "Report"},
]


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ink: #17202a;
            --muted: #5d6978;
            --line: #d7dde5;
            --surface: #ffffff;
            --soft: #f5f7fa;
            --green: #287d4f;
            --amber: #a86800;
            --red: #b3261e;
            --blue: #2368a2;
        }
        .app-title {
            font-size: 1.55rem;
            font-weight: 720;
            color: var(--ink);
            margin: 0 0 .15rem 0;
        }
        .app-subtitle {
            color: var(--muted);
            font-size: .95rem;
            margin: 0 0 1.1rem 0;
        }
        .stage-grid {
            display: grid;
            grid-template-columns: repeat(9, minmax(92px, 1fr));
            gap: .55rem;
            margin: .35rem 0 1rem 0;
        }
        .stage {
            min-height: 84px;
            border: 1px solid var(--line);
            border-left-width: 5px;
            border-radius: 8px;
            background: var(--surface);
            padding: .6rem .65rem;
        }
        .stage-label {
            font-size: .88rem;
            font-weight: 700;
            color: var(--ink);
        }
        .stage-state {
            font-size: .78rem;
            color: var(--muted);
            margin-top: .38rem;
            text-transform: uppercase;
        }
        .stage-count {
            font-size: .72rem;
            color: var(--muted);
            margin-top: .25rem;
        }
        .stage.pending { border-left-color: #aeb7c2; background: #f8fafc; }
        .stage.running { border-left-color: var(--blue); background: #eef6fc; }
        .stage.done { border-left-color: var(--green); background: #f1f8f4; }
        .stage.risk { border-left-color: var(--red); background: #fff4f3; }
        .stage.stopped { border-left-color: var(--red); background: #fff4f3; }
        .risk-pill {
            display: inline-flex;
            align-items: center;
            min-width: 72px;
            justify-content: center;
            color: white;
            border-radius: 999px;
            padding: .22rem .58rem;
            font-size: .76rem;
            font-weight: 760;
            letter-spacing: 0;
        }
        .action-banner {
            border: 1px solid var(--line);
            border-left: 5px solid var(--blue);
            border-radius: 8px;
            padding: .75rem .9rem;
            background: var(--soft);
            color: var(--ink);
            margin: .3rem 0 .9rem 0;
        }
        .action-banner.high {
            border-left-color: var(--red);
            background: #fff4f3;
        }
        .action-banner.medium {
            border-left-color: var(--amber);
            background: #fff9ec;
        }
        .log-line {
            font-family: Consolas, monospace;
            font-size: .78rem;
            padding: .28rem .35rem;
            border-bottom: 1px solid #ebeff4;
            white-space: pre-wrap;
        }
        .log-warn { color: var(--amber); }
        .log-error, .log-fatal { color: var(--red); font-weight: 650; }
        @media (max-width: 1100px) {
            .stage-grid { grid-template-columns: repeat(3, minmax(110px, 1fr)); }
        }
        @media (max-width: 720px) {
            .stage-grid { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def live_fragment(func):
    fragment = getattr(st, "fragment", None)
    if fragment:
        return fragment(run_every=0.8)(func)
    return func


def rerun_app() -> None:
    rerun = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if rerun:
        rerun()


def risk_pill(risk: str) -> str:
    color = RISK_COLORS.get(risk, "#5d6978")
    return (
        f'<span class="risk-pill" style="background:{color}">'
        f"{html.escape(risk)}</span>"
    )


def get_simulation() -> SimulationSession | None:
    return st.session_state.get("simulation")


def set_simulation(simulation: SimulationSession | None) -> None:
    st.session_state.simulation = simulation


def get_etl_result() -> dict | None:
    return st.session_state.get("etl_result")


def set_etl_result(result: dict | None) -> None:
    st.session_state.etl_result = result


def get_attached_result() -> dict | None:
    session = get_attached_session()
    if session:
        return session.snapshot()
    return st.session_state.get("attached_result")


def set_attached_result(result: dict | None) -> None:
    st.session_state.attached_result = result


def get_attached_session() -> LiveAttachSession | None:
    return st.session_state.get("attached_session")


def set_attached_session(session: LiveAttachSession | None) -> None:
    st.session_state.attached_session = session


def start_selected_demo() -> None:
    old_session = get_attached_session()
    if old_session and old_session.is_running():
        old_session.stop()

    set_etl_result(None)
    set_attached_result(None)
    set_attached_session(None)
    st.session_state.attached_inspection_open = False
    set_simulation(None)

    if st.session_state.demo_type == "Attach live command":
        command = st.session_state.get("attached_command") or DEFAULT_COMMAND
        model_path = ETL_MODEL_SAVE_PATH if Path(ETL_MODEL_SAVE_PATH).exists() else None
        session = LiveAttachSession(
            command,
            stop_policy=st.session_state.stop_policy,
            min_lines=st.session_state.min_lines,
            model_path=model_path,
        )
        session.start()
        set_attached_session(session)
        return

    if st.session_state.demo_type == "Monitored ETL pipeline":
        result = run_monitored_etl(
            scenario=st.session_state.scenario,
            stop_policy=st.session_state.stop_policy,
            min_lines=st.session_state.min_lines,
            seed=st.session_state.seed,
        )
        set_etl_result(result)
        return

    simulation = SimulationSession(
        scenario=st.session_state.scenario,
        loops=st.session_state.loops,
        stop_policy=st.session_state.stop_policy,
        min_lines=st.session_state.min_lines,
        seed=st.session_state.seed,
    )
    simulation.start()
    set_simulation(simulation)


def render_header() -> None:
    st.markdown('<div class="app-title">Pipeline Failure Predictor</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="app-subtitle">Visual demo for the predictor plugin, ETL integration, reports, and orchestration handoff.</div>',
        unsafe_allow_html=True,
    )


def render_sidebar() -> None:
    st.sidebar.header("Run Control")
    st.sidebar.selectbox(
        "Demo type",
        ["Attach live command", "Monitored ETL pipeline", "Visual simulator"],
        key="demo_type",
    )
    scenario_options = ETL_SCENARIOS if st.session_state.demo_type in {"Monitored ETL pipeline", "Attach live command"} else available_scenarios()
    st.sidebar.selectbox("Scenario", scenario_options, index=scenario_options.index("mixed"), key="scenario")
    if st.session_state.demo_type == "Attach live command":
        default_command = (
            f"{Path(sys.executable).name} examples/real_pipeline_job.py "
            f"--scenario {st.session_state.scenario} --delay 0.25 "
            "--records 1200 --batches 12"
        )
        if (
            "attached_command" not in st.session_state
            or st.session_state.get("_attached_command_scenario") != st.session_state.scenario
        ):
            st.session_state.attached_command = default_command
            st.session_state._attached_command_scenario = st.session_state.scenario
        st.sidebar.text_area(
            "Pipeline command",
            key="attached_command",
            height=92,
        )
    if st.session_state.demo_type == "Visual simulator":
        st.sidebar.slider("Loops", min_value=1, max_value=5, value=1, key="loops")
    st.sidebar.slider("Predict after lines", min_value=3, max_value=20, value=5, key="min_lines")
    if st.session_state.demo_type == "Visual simulator":
        st.sidebar.slider("Lines per tick", min_value=1, max_value=6, value=2, key="events_per_tick")
    if st.session_state.demo_type != "Attach live command":
        st.sidebar.number_input("Seed", min_value=1, max_value=9999, value=42, key="seed")
    st.sidebar.radio(
        "HIGH risk policy",
        ["review", "advise", "auto_stop"],
        format_func=lambda value: (
            "Review gate"
            if value == "review"
            else "Advise operator"
            if value == "advise"
            else "Auto-stop pipeline"
        ),
        key="stop_policy",
    )
    if st.session_state.demo_type == "Visual simulator":
        st.sidebar.toggle("Live playback", value=True, key="live_playback")

    start_col, reset_col = st.sidebar.columns(2)
    attached_session = get_attached_session()
    attached_running = bool(attached_session and attached_session.is_running())
    if start_col.button("Start", type="primary", use_container_width=True, disabled=attached_running):
        start_selected_demo()
        rerun_app()
    if reset_col.button("Reset", use_container_width=True):
        if attached_session and attached_session.is_running():
            attached_session.stop()
        set_simulation(None)
        set_etl_result(None)
        set_attached_result(None)
        set_attached_session(None)
        st.session_state.attached_inspection_open = False
        rerun_app()

    if st.session_state.demo_type == "Attach live command" and attached_running:
        if st.sidebar.button("Stop pipeline", key="sidebar_stop_pipeline", use_container_width=True):
            attached_session.stop()
            rerun_app()

    simulation = get_simulation()
    if simulation and st.session_state.demo_type == "Visual simulator":
        pause_col, step_col = st.sidebar.columns(2)
        if simulation.status == "running":
            if pause_col.button("Pause", use_container_width=True):
                simulation.pause()
        elif simulation.status == "paused":
            if pause_col.button("Resume", use_container_width=True):
                simulation.resume()

        if step_col.button("Step", use_container_width=True):
            if simulation.status == "paused":
                simulation.step(1)

        if st.sidebar.button("Generate report", use_container_width=True):
            simulation.report_path = simulation.save_report()


def render_system_map() -> None:
    data = pd.DataFrame(
        [
            {"Layer": "Dashboard", "Role": "Visual simulation", "Status": "Active"},
            {"Layer": "LivePipelineRunner", "Role": "Attach plugin to a running command", "Status": "Ready"},
            {"Layer": "PredictorPlugin", "Role": "Reusable stop/advice API", "Status": "Connected"},
            {"Layer": "ETL pipeline", "Role": "Real CSV extract-transform-load demo", "Status": "Available"},
            {"Layer": "Airflow DAG", "Role": "Workflow orchestration", "Status": "Ready"},
        ]
    )
    st.dataframe(data, use_container_width=True, hide_index=True)


def render_model_comparison() -> None:
    output_dir = Path(OUTPUTS_DIR)
    comparison_options = {
        "Demo / ETL benchmark": output_dir,
        "Alibaba PAI production trace": output_dir / "comparison_alibaba",
        "Google Borg production trace": output_dir / "comparison_google2019",
    }
    available_options = {
        label: path
        for label, path in comparison_options.items()
        if (path / "model_comparison.csv").exists()
    }

    if not available_options:
        st.info("No model comparison has been generated yet.")
        st.code("python main.py compare --source etl --synthetic-loops 20", language="powershell")
        st.caption("Use HDFS or Google once the labeled dataset files are available.")
        return

    selected_label = st.selectbox(
        "Comparison dataset",
        list(available_options),
        key="comparison_dataset",
    )
    selected_dir = available_options[selected_label]
    csv_path = selected_dir / "model_comparison.csv"
    chart_path = selected_dir / "model_comparison.png"
    json_path = selected_dir / "model_comparison.json"

    df = pd.read_csv(csv_path)
    st.dataframe(df, use_container_width=True, hide_index=True)
    if chart_path.exists():
        st.image(str(chart_path), caption="Model comparison chart")
    if json_path.exists():
        st.caption(f"Comparison JSON: {json_path}")


def render_kpis(simulation: SimulationSession | None) -> None:
    if not simulation:
        cols = st.columns(5)
        cols[0].metric("Status", "Idle")
        cols[1].metric("Current Risk", "LOW")
        cols[2].metric("Lines", "0 / 0")
        cols[3].metric("Predictions", 0)
        cols[4].metric("Failure Probability", "0%")
        return

    summary = simulation.summary()
    cols = st.columns(5)
    cols[0].metric("Status", summary["status"].upper())
    cols[1].markdown(risk_pill(summary["current_risk"]), unsafe_allow_html=True)
    cols[1].caption("Current Risk")
    cols[2].metric("Lines", f'{summary["lines_processed"]} / {summary["total_lines"]}')
    cols[3].metric("Predictions", summary["total_predictions"])
    cols[4].metric("Failure Probability", f'{summary["latest_probability"]:.0%}')
    st.progress(simulation.progress)


def render_action_banner(simulation: SimulationSession | None) -> None:
    if not simulation:
        st.markdown(
            '<div class="action-banner">No active pipeline session.</div>',
            unsafe_allow_html=True,
        )
        return

    risk = simulation.current_risk
    css_class = "high" if risk == "HIGH" else "medium" if risk == "MEDIUM" else ""
    if simulation.status == "stopped":
        message = f"Pipeline stopped: {simulation.stop_reason or 'intervention policy triggered'}."
    elif risk == "HIGH" and simulation.stop_policy == "advise":
        message = "HIGH risk detected. Operator review is advised before continuing."
    elif risk == "HIGH":
        message = "HIGH risk detected. Auto-stop policy is armed."
    elif risk == "MEDIUM":
        message = "MEDIUM risk detected. Continue with closer monitoring."
    elif simulation.status == "completed":
        message = "Pipeline completed. Final report is ready."
    else:
        message = "Pipeline is running within acceptable risk bounds."

    st.markdown(
        f'<div class="action-banner {css_class}">{html.escape(message)}</div>',
        unsafe_allow_html=True,
    )


def render_stage_board(simulation: SimulationSession | None) -> None:
    statuses = {}
    counts = {}
    if simulation:
        statuses = simulation.stage_status
        counts = simulation.stage_counts

    cards = []
    for stage in PIPELINE_STAGES:
        stage_id = stage["id"]
        status = statuses.get(stage_id, "pending")
        count = counts.get(stage_id, 0)
        cards.append(
            (
                f'<div class="stage {html.escape(status)}">'
                f'<div class="stage-label">{html.escape(stage["label"])}</div>'
                f'<div class="stage-state">{html.escape(status)}</div>'
                f'<div class="stage-count">{count} events</div>'
                "</div>"
            )
        )
    st.markdown(f'<div class="stage-grid">{"".join(cards)}</div>', unsafe_allow_html=True)


def render_etl_stage_board(result: dict | None) -> None:
    summary = (result or {}).get("summary", {}) if result else {}
    statuses = summary.get("stage_status", {}) or {}
    counts = summary.get("stage_counts", {}) or {}
    cards = []
    for stage in ETL_STAGES:
        stage_id = stage["id"]
        status = statuses.get(stage_id, "pending")
        count = counts.get(stage_id, 0)
        cards.append(
            (
                f'<div class="stage {html.escape(str(status))}">'
                f'<div class="stage-label">{html.escape(stage["label"])}</div>'
                f'<div class="stage-state">{html.escape(str(status))}</div>'
                f'<div class="stage-count">{count} events</div>'
                "</div>"
            )
        )
    st.markdown(f'<div class="stage-grid">{"".join(cards)}</div>', unsafe_allow_html=True)


def render_attached_stage_board(result: dict | None) -> None:
    summary = (result or {}).get("summary", {}) if result else {}
    statuses = summary.get("stage_status", {}) or {}
    counts = summary.get("stage_counts", {}) or {}
    stages = [
        {"id": stage, "label": stage}
        for stage in statuses.keys()
        if stage != "External"
    ]
    if "External" in statuses:
        stages.append({"id": "External", "label": "External"})
    if not stages:
        stages = ETL_STAGES + [{"id": "External", "label": "External"}]

    cards = []
    for stage in stages:
        stage_id = stage["id"]
        status = statuses.get(stage_id, "pending")
        count = counts.get(stage_id, 0)
        cards.append(
            (
                f'<div class="stage {html.escape(str(status))}">'
                f'<div class="stage-label">{html.escape(stage["label"])}</div>'
                f'<div class="stage-state">{html.escape(str(status))}</div>'
                f'<div class="stage-count">{count} events</div>'
                "</div>"
            )
        )
    st.markdown(f'<div class="stage-grid">{"".join(cards)}</div>', unsafe_allow_html=True)


def render_etl_dashboard() -> None:
    result = get_etl_result()
    summary = (result or {}).get("summary", {}) if result else {}
    alerts = (result or {}).get("alerts", []) if result else []
    logs = (result or {}).get("logs", []) if result else []

    if not result:
        render_kpis(None)
        render_action_banner(None)
        render_etl_stage_board(None)
        st.info("Choose Monitored ETL pipeline in the sidebar and click Start.")
        render_system_map()
        return

    cols = st.columns(5)
    cols[0].metric("Status", str(summary.get("status", "unknown")).upper())
    cols[1].markdown(risk_pill(str(summary.get("current_risk", "LOW"))), unsafe_allow_html=True)
    cols[1].caption("Current Risk")
    cols[2].metric("Rows Loaded", summary.get("rows_loaded", 0))
    cols[3].metric("Predictions", summary.get("total_predictions", 0))
    cols[4].metric("Latest Prediction", f'{float(summary.get("latest_probability", 0.0) or 0.0):.0%}')
    cols[4].caption(f'Peak risk: {float(summary.get("peak_probability", 0.0) or 0.0):.0%}')

    css_class = "high" if summary.get("current_risk") == "HIGH" else "medium" if summary.get("current_risk") == "MEDIUM" else ""
    message = summary.get("recommendation") or "Pipeline run finished."
    st.markdown(
        f'<div class="action-banner {css_class}">{html.escape(str(message))}</div>',
        unsafe_allow_html=True,
    )
    render_etl_stage_board(result)

    tab_monitor, tab_logs, tab_output, tab_compare, tab_report, tab_system = st.tabs(
        ["Monitor", "Pipeline Logs", "Output", "Model Comparison", "Report", "System"]
    )
    with tab_monitor:
        if alerts:
            timeline_rows = [
                {
                    "Prediction": index,
                    "Failure Probability (%)": round(
                        float(alert.get("failure_probability", 0.0) or 0.0) * 100,
                    ),
                }
                for index, alert in enumerate(alerts, start=1)
            ]
            st.markdown("#### Risk Timeline")
            st.line_chart(
                pd.DataFrame(timeline_rows).set_index("Prediction"),
                height=220,
            )
            rows = [
                {
                    "Risk": alert.get("risk_level"),
                    "Probability": f'{float(alert.get("failure_probability", 0.0) or 0.0):.0%}',
                    "Process": alert.get("process_id"),
                    "Stage": alert.get("stage"),
                    "Lines": alert.get("lines_seen"),
                    "Action": alert.get("action"),
                }
                for alert in alerts
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            latest = alerts[-1]
            explanations = latest.get("explanation") or []
            if explanations:
                st.dataframe(pd.DataFrame(explanations), use_container_width=True, hide_index=True)
        else:
            st.info("No predictions emitted yet.")
    with tab_logs:
        if logs:
            for item in logs[-40:]:
                level = str(item.get("level", "")).lower()
                css = "log-fatal" if level == "fatal" else "log-error" if level == "error" else "log-warn" if level == "warn" else ""
                st.markdown(
                    f'<div class="log-line {css}">{html.escape(str(item.get("line", "")))}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No logs captured.")
    with tab_output:
        output_path = summary.get("output_path")
        if output_path and Path(output_path).exists():
            st.dataframe(pd.read_csv(output_path), use_container_width=True, hide_index=True)
            st.caption(f"Loaded output: {output_path}")
        else:
            st.warning("No curated output file was produced. The run may have been stopped before load.")
    with tab_compare:
        render_model_comparison()
    with tab_report:
        st.json(summary)
        report_paths = summary.get("report_paths", {}) or {}
        if report_paths:
            st.caption(f"JSON report: {report_paths.get('json')}")
            st.caption(f"HTML report: {report_paths.get('html')}")
        st.download_button(
            "Download ETL monitoring JSON",
            data=json.dumps({"summary": summary, "alerts": alerts, "logs": logs}, indent=2),
            file_name="etl_pipeline_monitoring_report.json",
            mime="application/json",
            use_container_width=True,
        )
    with tab_system:
        render_system_map()


def render_review_controls(
    session: LiveAttachSession | None,
    summary: dict,
    alerts: list[dict],
    logs: list[dict],
    resource_samples: list[dict],
) -> None:
    if not summary.get("review_required"):
        return

    st.warning("HIGH risk detected. The pipeline is held for operator review.")
    inspect_col, continue_col, stop_col = st.columns([1, 1, 1])

    if inspect_col.button("Check process", key="review_check_process", use_container_width=True):
        st.session_state.attached_inspection_open = True
        rerun_app()

    if continue_col.button("Continue pipeline", key="review_continue_pipeline", type="primary", use_container_width=True):
        if session:
            session.continue_pipeline()
        st.session_state.attached_inspection_open = False
        rerun_app()

    if stop_col.button("Stop pipeline", key="review_stop_pipeline", use_container_width=True):
        if session:
            session.stop()
        rerun_app()

    if st.session_state.get("attached_inspection_open", False):
        latest_high = next(
            (alert for alert in reversed(alerts) if alert.get("risk_level") == "HIGH"),
            alerts[-1] if alerts else {},
        )
        st.subheader("Process Review")
        review_cols = st.columns(4)
        review_cols[0].metric("Process", latest_high.get("process_id", "-"))
        review_cols[1].metric("Stage", latest_high.get("stage", "-"))
        review_cols[2].metric(
            "Failure Probability",
            f'{float(latest_high.get("failure_probability", 0.0) or 0.0):.0%}',
        )
        review_cols[3].metric("Action", latest_high.get("action", "-"))

        explanations = latest_high.get("explanation") or []
        if explanations:
            st.dataframe(pd.DataFrame(explanations), use_container_width=True, hide_index=True)

        recent_logs = [
            {
                "Level": item.get("level"),
                "Stage": item.get("stage"),
                "Log": item.get("raw_line") or item.get("line"),
            }
            for item in logs[-12:]
        ]
        if recent_logs:
            st.dataframe(pd.DataFrame(recent_logs), use_container_width=True, hide_index=True)

        if resource_samples:
            latest_metrics = resource_samples[-1]
            st.caption(
                "Runtime metrics at review: "
                f"CPU {latest_metrics.get('cpu_percent', 0.0)}%, "
                f"memory {latest_metrics.get('memory_rss_mb', 0.0)} MB, "
                f"disk used {latest_metrics.get('disk_used_percent', 0.0)}%."
            )


def is_terminal_pipeline_status(status: str | None) -> bool:
    return str(status or "").lower() in {
        "completed",
        "completed_with_high_risk",
        "failed",
        "failed_with_high_risk",
        "stopped",
        "error",
    }


def render_attached_run_state(result: dict, summary: dict) -> None:
    status = str(summary.get("status", "idle")).lower()
    running = bool(result.get("running"))
    finished_at = summary.get("finished_at")
    report_paths = summary.get("report_paths", {}) or {}

    if summary.get("review_required"):
        st.warning("Pipeline is waiting for your decision. The batch job has not finished yet.")
        return

    if running and status in {"running", "starting"}:
        st.info("Pipeline is still running. The dashboard will update as new logs arrive.")
        return

    if running and status == "resuming":
        st.info("Operator review accepted. The pipeline is resuming.")
        return

    if running and status == "stopping":
        st.warning("Stop requested. The pipeline process is shutting down.")
        return

    if running and status == "waiting_for_review":
        st.warning("Pipeline is paused for review and has not finished.")
        return

    if is_terminal_pipeline_status(status):
        if status == "completed":
            st.success(f"Pipeline has finished running successfully. Finished at: {finished_at or 'just now'}.")
        elif status == "completed_with_high_risk":
            st.warning(f"Pipeline finished, but HIGH risk was detected. Finished at: {finished_at or 'just now'}.")
        elif status == "stopped":
            st.error(f"Pipeline was stopped before normal completion. Finished at: {finished_at or 'just now'}.")
        else:
            st.error(f"Pipeline has finished with status `{status}`. Finished at: {finished_at or 'just now'}.")

        if report_paths:
            st.caption(f"Final report generated: {report_paths.get('html') or report_paths.get('json')}")


@live_fragment
def render_attached_dashboard() -> None:
    result = get_attached_result()
    session = get_attached_session()
    summary = (result or {}).get("summary", {}) if result else {}
    alerts = (result or {}).get("alerts", []) if result else []
    logs = (result or {}).get("logs", []) if result else []
    resource_samples = (result or {}).get("resource_samples", []) if result else []

    if not result:
        render_kpis(None)
        render_action_banner(None)
        render_attached_stage_board(None)
        st.info("Choose Attach live command in the sidebar and click Start.")
        render_system_map()
        return

    cols = st.columns(5)
    cols[0].metric("Status", str(summary.get("status", "unknown")).upper())
    cols[1].markdown(risk_pill(str(summary.get("current_risk", "LOW"))), unsafe_allow_html=True)
    cols[1].caption("Current Risk")
    cols[2].metric("Return Code", summary.get("return_code", "-"))
    cols[3].metric("Predictions", summary.get("total_predictions", 0))
    cols[4].metric("Latest Prediction", f'{float(summary.get("latest_probability", 0.0) or 0.0):.0%}')
    cols[4].caption(f'Peak risk: {float(summary.get("peak_probability", 0.0) or 0.0):.0%}')

    render_attached_run_state(result, summary)

    css_class = "high" if summary.get("current_risk") == "HIGH" else "medium" if summary.get("current_risk") == "MEDIUM" else ""
    message = summary.get("recommendation") or "Attached pipeline run finished."
    st.markdown(
        f'<div class="action-banner {css_class}">{html.escape(str(message))}</div>',
        unsafe_allow_html=True,
    )
    render_review_controls(session, summary, alerts, logs, resource_samples)
    render_attached_stage_board(result)

    tab_monitor, tab_logs, tab_resources, tab_compare, tab_report, tab_system = st.tabs(
        ["Monitor", "Captured Logs", "Resources", "Model Comparison", "Report", "System"]
    )
    with tab_monitor:
        if alerts:
            timeline_rows = [
                {
                    "Prediction": index,
                    "Failure Probability (%)": round(
                        float(alert.get("failure_probability", 0.0) or 0.0) * 100,
                    ),
                }
                for index, alert in enumerate(alerts, start=1)
            ]
            st.markdown("#### Risk Timeline")
            st.line_chart(
                pd.DataFrame(timeline_rows).set_index("Prediction"),
                height=220,
            )
            rows = [
                {
                    "Risk": alert.get("risk_level"),
                    "Probability": f'{float(alert.get("failure_probability", 0.0) or 0.0):.0%}',
                    "Process": alert.get("process_id"),
                    "Stage": alert.get("stage"),
                    "Lines": alert.get("lines_seen"),
                    "Action": alert.get("action"),
                }
                for alert in alerts
            ]
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("No predictions emitted yet.")
    with tab_logs:
        if logs:
            for item in logs[-50:]:
                level = str(item.get("level", "")).lower()
                css = "log-fatal" if level == "fatal" else "log-error" if level == "error" else "log-warn" if level == "warn" else ""
                line = item.get("raw_line") or item.get("line", "")
                st.markdown(
                    f'<div class="log-line {css}">{html.escape(str(line))}</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.info("No logs captured.")
    with tab_resources:
        resource_summary = summary.get("resource_summary", {}) or {}
        if resource_summary:
            cols = st.columns(4)
            cols[0].metric("Samples", resource_summary.get("samples", 0))
            cols[1].metric("Max CPU", f'{float(resource_summary.get("max_cpu_percent", 0.0) or 0.0):.1f}%')
            cols[2].metric("Max Memory", f'{float(resource_summary.get("max_memory_rss_mb", 0.0) or 0.0):.1f} MB')
            cols[3].metric("Disk Used", f'{float(resource_summary.get("max_disk_used_percent", 0.0) or 0.0):.1f}%')
        if resource_samples:
            st.line_chart(pd.DataFrame(resource_samples).set_index("timestamp")[["cpu_percent", "memory_rss_mb"]])
        else:
            st.info("No resource samples captured.")
    with tab_compare:
        render_model_comparison()
    with tab_report:
        st.json(summary)
        report_paths = summary.get("report_paths", {}) or {}
        if report_paths:
            st.caption(f"JSON report: {report_paths.get('json')}")
            st.caption(f"HTML report: {report_paths.get('html')}")
        st.download_button(
            "Download attached monitoring JSON",
            data=json.dumps({"summary": summary, "alerts": alerts, "logs": logs}, indent=2),
            file_name="attached_pipeline_monitoring_report.json",
            mime="application/json",
            use_container_width=True,
        )
    with tab_system:
        render_system_map()


def render_alerts(simulation: SimulationSession | None) -> None:
    if not simulation or not simulation.alerts:
        st.info("No predictions emitted yet.")
        return

    rows = []
    for alert in simulation.alerts[-30:]:
        rows.append(
            {
                "Risk": alert.get("risk_level"),
                "Probability": f'{float(alert.get("failure_probability", 0.0)):.0%}',
                "Process": alert.get("process_id"),
                "Stage": alert.get("stage"),
                "Lines": alert.get("lines_seen"),
                "Action": alert.get("action"),
            }
        )
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def render_log_stream(simulation: SimulationSession | None) -> None:
    if not simulation or not simulation.logs:
        st.info("No logs streamed yet.")
        return

    lines = []
    for item in simulation.logs[-18:]:
        level = str(item["level"]).lower()
        css = "log-fatal" if level == "fatal" else "log-error" if level == "error" else "log-warn" if level == "warn" else ""
        lines.append(
            f'<div class="log-line {css}">{html.escape(item["line"])}</div>'
        )
    st.markdown("".join(lines), unsafe_allow_html=True)


def render_metric_charts(simulation: SimulationSession | None) -> None:
    if not simulation:
        st.info("Start a session to populate metrics.")
        return

    counts = simulation.risk_counts()
    risk_df = pd.DataFrame(
        [{"Risk": key, "Predictions": value} for key, value in counts.items()]
    ).set_index("Risk")
    st.bar_chart(risk_df)

    if simulation.logs:
        level_rows = {}
        for item in simulation.logs:
            level_rows[item["level"]] = level_rows.get(item["level"], 0) + 1
        level_df = pd.DataFrame(
            [{"Level": key, "Count": value} for key, value in level_rows.items()]
        ).set_index("Level")
        st.bar_chart(level_df)


def render_explanations(simulation: SimulationSession | None) -> None:
    if not simulation or not simulation.alerts:
        st.info("No explanation data yet.")
        return

    latest = simulation.alerts[-1]
    st.markdown(
        f"Latest process: `{latest.get('process_id')}` "
        f"{risk_pill(latest.get('risk_level', 'LOW'))}",
        unsafe_allow_html=True,
    )
    explanations = latest.get("explanation") or []
    if explanations:
        st.dataframe(pd.DataFrame(explanations), use_container_width=True, hide_index=True)


def render_report(simulation: SimulationSession | None) -> None:
    if not simulation:
        st.info("No session report available.")
        return

    payload = simulation.report_payload()
    st.json(payload["summary"])
    report_json = json.dumps(payload, indent=2)
    st.download_button(
        "Download session report",
        data=report_json,
        file_name="pipeline_monitoring_report.json",
        mime="application/json",
        use_container_width=True,
    )
    if simulation.report_path:
        st.caption(f"Saved: {Path(simulation.report_path)}")


@live_fragment
def render_live_dashboard() -> None:
    simulation = get_simulation()
    if simulation and simulation.status == "running" and st.session_state.get("live_playback", True):
        simulation.step(st.session_state.get("events_per_tick", 2))

    render_kpis(simulation)
    render_action_banner(simulation)
    render_stage_board(simulation)

    tab_monitor, tab_logs, tab_metrics, tab_compare, tab_report, tab_system = st.tabs(
        ["Monitor", "Live Logs", "Metrics", "Model Comparison", "Report", "System"]
    )
    with tab_monitor:
        render_alerts(simulation)
        render_explanations(simulation)
    with tab_logs:
        render_log_stream(simulation)
    with tab_metrics:
        render_metric_charts(simulation)
    with tab_compare:
        render_model_comparison()
    with tab_report:
        render_report(simulation)
    with tab_system:
        render_system_map()


def main() -> None:
    inject_styles()
    render_sidebar()
    render_header()
    if st.session_state.get("demo_type") == "Attach live command":
        render_attached_dashboard()
    elif st.session_state.get("demo_type") == "Monitored ETL pipeline":
        render_etl_dashboard()
    else:
        render_live_dashboard()


if __name__ == "__main__":
    main()
