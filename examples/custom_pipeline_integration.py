"""
Example: integrating the failure prediction plugin into a custom pipeline.

Run from the project root:

    python examples/custom_pipeline_integration.py data/HDFS.log
"""

import argparse
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline_monitor import PipelineMonitor


def send_alert(alert: dict) -> None:
    """Replace this with Slack, email, Airflow XCom, logs, or a webhook."""
    print(
        "[ALERT] "
        f"process={alert['process_id']} "
        f"risk={alert['risk_level']} "
        f"failure_probability={alert['failure_probability']:.0%}"
    )


def monitor_existing_log(log_path: str, max_lines: int | None, min_lines: int) -> None:
    monitor = PipelineMonitor(
        source="hdfs",
        print_alerts=False,
        alert_callback=send_alert,
        min_lines=min_lines,
    )
    monitor.process_file(log_path, max_lines=max_lines)
    monitor._print_session_summary()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("log_path", help="Path to a pipeline or HDFS log file")
    parser.add_argument("--max-lines", type=int, default=5000)
    parser.add_argument("--min-lines", type=int, default=5)
    args = parser.parse_args()

    monitor_existing_log(args.log_path, args.max_lines, args.min_lines)


if __name__ == "__main__":
    main()
