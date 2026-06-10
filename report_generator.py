"""
report_generator.py
-------------------
Create operator-facing JSON and HTML reports for monitored pipeline sessions.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from config import OUTPUTS_DIR


RISK_COLORS = {
    "LOW": "#287d4f",
    "MEDIUM": "#a86800",
    "HIGH": "#b3261e",
}


def generate_report_files(
    payload: dict[str, Any],
    *,
    prefix: str = "pipeline_monitoring_report",
    output_dir: str | Path = OUTPUTS_DIR,
) -> dict[str, str]:
    """Write JSON and HTML reports and return their paths."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    json_path = out / f"{prefix}_{stamp}.json"
    html_path = out / f"{prefix}_{stamp}.html"

    json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    html_path.write_text(render_html_report(payload), encoding="utf-8")
    return {
        "json": str(json_path),
        "html": str(html_path),
    }


def render_html_report(payload: dict[str, Any]) -> str:
    summary = payload.get("summary", {}) or {}
    alerts = payload.get("alerts", []) or []
    logs = payload.get("logs", []) or []
    high_risk = summary.get("high_risk_processes", []) or []
    risk_counts = summary.get("risk_counts", {}) or {}
    resource_summary = summary.get("resource_summary", {}) or {}
    generated_at = datetime.now(timezone.utc).isoformat()
    current_risk = str(summary.get("current_risk", "LOW"))
    risk_color = RISK_COLORS.get(current_risk, "#5d6978")

    alert_rows = "".join(_alert_row(alert) for alert in alerts[-50:])
    log_rows = "".join(_log_row(item) for item in logs[-80:])
    high_rows = "".join(_high_risk_row(item) for item in high_risk)
    resource_rows = "".join(
        _summary_row(label, value)
        for label, value in _resource_summary_rows(resource_summary)
    )
    count_cards = "".join(
        _count_card(risk, int(risk_counts.get(risk, 0) or 0))
        for risk in ("LOW", "MEDIUM", "HIGH")
    )

    if not alert_rows:
        alert_rows = "<tr><td colspan='6'>No predictions were emitted.</td></tr>"
    if not log_rows:
        log_rows = "<tr><td colspan='5'>No logs were captured.</td></tr>"
    if not high_rows:
        high_rows = "<tr><td colspan='5'>No HIGH-risk processes were found.</td></tr>"
    if not resource_rows:
        resource_rows = "<tr><td colspan='2'>No process resource samples were captured.</td></tr>"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Pipeline Monitoring Report</title>
  <style>
    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      color: #17202a;
      background: #f4f6f8;
    }}
    .wrap {{
      max-width: 1180px;
      margin: 0 auto;
      padding: 28px;
    }}
    h1 {{
      font-size: 28px;
      margin: 0 0 4px 0;
    }}
    h2 {{
      font-size: 18px;
      margin: 26px 0 10px 0;
    }}
    .muted {{
      color: #5d6978;
      font-size: 13px;
    }}
    .hero {{
      background: #fff;
      border: 1px solid #d7dde5;
      border-left: 8px solid {risk_color};
      border-radius: 8px;
      padding: 20px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(140px, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .card {{
      background: #fff;
      border: 1px solid #d7dde5;
      border-radius: 8px;
      padding: 14px;
    }}
    .label {{
      color: #5d6978;
      font-size: 12px;
      text-transform: uppercase;
    }}
    .value {{
      margin-top: 6px;
      font-size: 22px;
      font-weight: 700;
    }}
    .risk {{
      display: inline-block;
      min-width: 76px;
      text-align: center;
      color: #fff;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 12px;
      font-weight: 700;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: #fff;
      border: 1px solid #d7dde5;
      border-radius: 8px;
      overflow: hidden;
    }}
    th, td {{
      padding: 9px 10px;
      border-bottom: 1px solid #e8edf3;
      text-align: left;
      font-size: 13px;
      vertical-align: top;
    }}
    th {{
      background: #edf1f5;
      font-size: 12px;
      text-transform: uppercase;
      color: #4b5663;
    }}
    code {{
      font-family: Consolas, monospace;
      font-size: 12px;
      white-space: pre-wrap;
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>Pipeline Monitoring Report</h1>
      <div class="muted">Generated at {escape(generated_at)}</div>
      <div class="grid">
        {_metric_card("Status", summary.get("status", "unknown"))}
        {_metric_card("Current Risk", _risk_badge(current_risk))}
        {_metric_card("Predictions", summary.get("total_predictions", 0))}
        {_metric_card("Lines Processed", summary.get("lines_processed", 0))}
      </div>
      <div class="grid">
        {count_cards}
      </div>
    </section>

    <h2>Intervention Decision</h2>
    <table>
      <tr><th>Field</th><th>Value</th></tr>
      <tr><td>Stop policy</td><td>{escape(str(summary.get("stop_policy", "-")))}</td></tr>
      <tr><td>Stop reason</td><td>{escape(str(summary.get("stop_reason") or "None"))}</td></tr>
      <tr><td>Log file</td><td>{escape(str(summary.get("log_path", "-")))}</td></tr>
      <tr><td>Output artifact</td><td>{escape(str(summary.get("output_path", "-")))}</td></tr>
    </table>

    <h2>High-Risk Processes</h2>
    <table>
      <tr><th>Process</th><th>Probability</th><th>Stage</th><th>Action</th><th>Lines Seen</th></tr>
      {high_rows}
    </table>

    <h2>Runtime Resource Metrics</h2>
    <table>
      <tr><th>Metric</th><th>Value</th></tr>
      {resource_rows}
    </table>

    <h2>Recent Predictions</h2>
    <table>
      <tr><th>Risk</th><th>Probability</th><th>Process</th><th>Stage</th><th>Action</th><th>Reason</th></tr>
      {alert_rows}
    </table>

    <h2>Recent Logs</h2>
    <table>
      <tr><th>#</th><th>Level</th><th>Stage</th><th>Process</th><th>Log Line</th></tr>
      {log_rows}
    </table>
  </main>
</body>
</html>
"""


def _metric_card(label: str, value: Any) -> str:
    return (
        "<div class='card'>"
        f"<div class='label'>{escape(str(label))}</div>"
        f"<div class='value'>{value if isinstance(value, str) else escape(str(value))}</div>"
        "</div>"
    )


def _count_card(risk: str, value: int) -> str:
    return _metric_card(f"{risk} predictions", value)


def _risk_badge(risk: str) -> str:
    color = RISK_COLORS.get(risk, "#5d6978")
    return f"<span class='risk' style='background:{color}'>{escape(risk)}</span>"


def _alert_row(alert: dict[str, Any]) -> str:
    risk = str(alert.get("risk_level", "LOW"))
    probability = float(alert.get("failure_probability", 0.0) or 0.0)
    explanation = alert.get("explanation") or []
    reason = "-"
    if explanation:
        first = explanation[0]
        reason = f"{first.get('feature', '-')}: {first.get('direction', '-')}"
    return (
        "<tr>"
        f"<td>{_risk_badge(risk)}</td>"
        f"<td>{probability:.0%}</td>"
        f"<td>{escape(str(alert.get('process_id', '-')))}</td>"
        f"<td>{escape(str(alert.get('stage', '-')))}</td>"
        f"<td>{escape(str(alert.get('action', '-')))}</td>"
        f"<td>{escape(reason)}</td>"
        "</tr>"
    )


def _high_risk_row(item: dict[str, Any]) -> str:
    probability = float(item.get("failure_probability", 0.0) or 0.0)
    return (
        "<tr>"
        f"<td>{escape(str(item.get('process_id', '-')))}</td>"
        f"<td>{probability:.0%}</td>"
        f"<td>{escape(str(item.get('stage', '-')))}</td>"
        f"<td>{escape(str(item.get('action', '-')))}</td>"
        f"<td>{escape(str(item.get('lines_seen', '-')))}</td>"
        "</tr>"
    )


def _summary_row(label: str, value: Any) -> str:
    return (
        "<tr>"
        f"<td>{escape(str(label))}</td>"
        f"<td>{escape(str(value))}</td>"
        "</tr>"
    )


def _resource_summary_rows(resource_summary: dict[str, Any]) -> list[tuple[str, Any]]:
    if not resource_summary:
        return []
    return [
        ("Samples", resource_summary.get("samples", 0)),
        ("Metrics backend", resource_summary.get("metrics_backend", "-")),
        ("Max CPU %", resource_summary.get("max_cpu_percent", 0.0)),
        ("Average CPU %", resource_summary.get("avg_cpu_percent", 0.0)),
        ("Max memory RSS MB", resource_summary.get("max_memory_rss_mb", 0.0)),
        ("Max disk used %", resource_summary.get("max_disk_used_percent", 0.0)),
    ]


def _log_row(item: dict[str, Any]) -> str:
    line = item.get("raw_line") or item.get("line", "-")
    return (
        "<tr>"
        f"<td>{escape(str(item.get('index', '-')))}</td>"
        f"<td>{escape(str(item.get('level', '-')))}</td>"
        f"<td>{escape(str(item.get('stage', '-')))}</td>"
        f"<td>{escape(str(item.get('block', item.get('process_id', '-'))))}</td>"
        f"<td><code>{escape(str(line))}</code></td>"
        "</tr>"
    )
