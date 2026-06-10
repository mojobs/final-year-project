"""
Example: using PredictorPlugin from any Python pipeline.

Run from the project root:

    python examples/plugin_api_integration.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from demo_pipeline import format_hdfs_line, hdfs_timestamp
from predictor_plugin import PredictorPlugin


def make_log(index: int, level: str, component: str, message: str) -> str:
    date, clock = hdfs_timestamp(index)
    return format_hdfs_line(
        date=date,
        clock=clock,
        pid=411,
        level=level,
        component=component,
        message=message,
    )


def main() -> None:
    block = "blk_-777000001"
    plugin = PredictorPlugin(min_lines=5, stop_policy="auto_stop")
    pipeline_logs = [
        make_log(0, "INFO", "pipeline.Extractor", f"Extracted batch for block {block}"),
        make_log(1, "INFO", "pipeline.SchemaValidator", f"Schema validation passed for block {block}"),
        make_log(2, "WARN", "pipeline.Loader", f"Queue full while buffering block {block}; retrying once"),
        make_log(3, "ERROR", "pipeline.Loader", f"Timeout while loading block {block}; retrying"),
        make_log(4, "FATAL", "pipeline.Loader", f"Critical corruption detected for block {block}; terminate job"),
    ]

    for line in pipeline_logs:
        alerts = plugin.ingest_log(line, metadata={"stage": "Load", "block": block})
        for alert in alerts:
            print(
                f"{alert['risk_level']} risk "
                f"{alert['failure_probability']:.0%} "
                f"action={alert['action']}"
            )
        if plugin.should_stop():
            print("Pipeline stopped by plugin policy.")
            break

    summary = plugin.finalize(
        status="stopped" if plugin.should_stop() else "completed",
        extra_summary={"example": "plugin_api_integration"},
    )
    print("JSON report:", summary["report_path"])
    print("HTML report:", summary["html_report_path"])


if __name__ == "__main__":
    main()
