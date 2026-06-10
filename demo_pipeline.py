"""
demo_pipeline.py
----------------
Small live data-pipeline simulator for demoing the failure monitor.

It writes HDFS-style log lines slowly into simulated/live_pipeline.log so
pipeline_monitor.py can watch the file in --live mode and produce predictions.

Typical demo:

Terminal 1:
    python main.py monitor --file simulated/live_pipeline.log --source hdfs --live --min-lines 5 --demo-risk-calibration

Terminal 2:
    python demo_pipeline.py --scenario mixed
"""

from __future__ import annotations

import argparse
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

from config import LIVE_STOP_MARKER


DEFAULT_LOG_PATH = Path("simulated") / "live_pipeline.log"


STABLE_MESSAGES = [
    ("INFO", "pipeline.Extractor", "Extracted customer batch for block {block}"),
    ("INFO", "pipeline.SchemaValidator", "Schema validation passed for block {block}"),
    ("INFO", "pipeline.Cleaner", "Filled optional missing values for block {block}"),
    ("INFO", "pipeline.Transformer", "Applied currency normalization for block {block}"),
    ("INFO", "pipeline.Aggregator", "Generated daily summary for block {block}"),
    ("INFO", "pipeline.Loader", "Loaded staged records for block {block}"),
    ("INFO", "pipeline.Auditor", "Checksum verified for block {block}"),
    ("INFO", "pipeline.Committer", "Commit successful for block {block}"),
    ("INFO", "pipeline.Reporter", "Published success metric for block {block}"),
    ("INFO", "pipeline.Orchestrator", "Pipeline stage completed for block {block}"),
]

WARNING_MESSAGES = [
    ("INFO", "pipeline.Extractor", "Extracted order batch for block {block}"),
    ("INFO", "pipeline.SchemaValidator", "Schema validation passed for block {block}"),
    ("WARN", "pipeline.Cleaner", "Slow transform observed for block {block}; latency threshold near limit"),
    ("INFO", "pipeline.Transformer", "Applied fallback mapping for block {block}"),
    ("WARN", "pipeline.Loader", "Temporary queue pressure for block {block}; continuing"),
    ("INFO", "pipeline.Auditor", "Checksum verified for block {block}"),
    ("INFO", "pipeline.Committer", "Commit successful for block {block}"),
    ("INFO", "pipeline.Reporter", "Published success metric for block {block}"),
    ("INFO", "pipeline.Orchestrator", "Downstream handoff completed for block {block}"),
    ("INFO", "pipeline.Orchestrator", "Pipeline stage completed for block {block}"),
]

RECOVERING_MESSAGES = [
    ("INFO", "pipeline.Extractor", "Extracted inventory batch for block {block}"),
    ("WARN", "pipeline.SchemaValidator", "Optional column missing for block {block}; fallback default applied"),
    ("WARN", "pipeline.Loader", "Queue full while buffering block {block}; retrying once"),
    ("INFO", "pipeline.Loader", "Recovered writer connection for block {block}"),
    ("INFO", "pipeline.Transformer", "Replayed transform safely for block {block}"),
    ("INFO", "pipeline.Auditor", "Checksum verified after retry for block {block}"),
    ("INFO", "pipeline.Committer", "Commit successful for block {block}"),
    ("INFO", "pipeline.Reporter", "Published recovered status for block {block}"),
    ("INFO", "pipeline.Orchestrator", "Recovery path completed for block {block}"),
    ("INFO", "pipeline.Orchestrator", "Pipeline stage completed for block {block}"),
]

ERROR_MESSAGES = [
    ("INFO", "pipeline.Extractor", "Extracted payment batch for block {block}"),
    ("INFO", "pipeline.SchemaValidator", "Schema validation passed for block {block}"),
    ("WARN", "pipeline.Loader", "Slow write detected for block {block}; high latency threshold exceeded"),
    ("ERROR", "pipeline.Loader", "Timeout while loading block {block}; retrying"),
    ("WARN", "pipeline.Orchestrator", "Retry scheduled for block {block}; fallback target selected"),
    ("ERROR", "pipeline.Loader", "Connection reset while processing block {block}"),
    ("INFO", "pipeline.Orchestrator", "Manual intervention requested for block {block}"),
    ("WARN", "pipeline.Loader", "Second retry delayed for block {block}; monitoring pressure"),
    ("ERROR", "pipeline.Loader", "Load attempt failed for block {block}; partial output discarded"),
    ("INFO", "pipeline.Orchestrator", "Escalation ticket opened for block {block}"),
]

FATAL_MESSAGES = [
    ("INFO", "dfs.DataNode$DataXceiver", "Receiving block {block} from upstream extractor"),
    ("WARN", "dfs.DataNode$DataXceiver", "Slow write detected for block {block}; high latency threshold exceeded"),
    ("WARN", "dfs.DFSClient", "Retrying packet send for block {block} after connection reset"),
    ("ERROR", "dfs.DataNode$DataXceiver", "IOException while processing block {block}"),
    ("WARN", "dfs.BlockManager", "Retry scheduled for block {block}; fallback replica selected"),
    ("ERROR", "dfs.DataNode$DataXceiver", "Timeout while loading block {block}; retrying"),
    ("FATAL", "dfs.FSNamesystem", "Unrecoverable failure for block {block}; abort pipeline stage"),
    ("ERROR", "dfs.NameNode", "Block report failed for block {block}; data loss risk detected"),
    ("FATAL", "dfs.FSNamesystem", "Critical corruption detected for block {block}; terminate job"),
    ("ERROR", "dfs.BlockManager", "Final recovery attempt failed for block {block}"),
]


SCENARIOS = {
    "normal": [STABLE_MESSAGES],
    "warning": [WARNING_MESSAGES],
    "risky": [ERROR_MESSAGES, FATAL_MESSAGES],
    "recovering": [RECOVERING_MESSAGES],
    "mixed": [
        STABLE_MESSAGES,
        WARNING_MESSAGES,
        RECOVERING_MESSAGES,
        ERROR_MESSAGES,
        FATAL_MESSAGES,
    ],
}

PROFILE_NAMES = {
    id(STABLE_MESSAGES): "stable",
    id(WARNING_MESSAGES): "warning",
    id(RECOVERING_MESSAGES): "recovering",
    id(ERROR_MESSAGES): "error",
    id(FATAL_MESSAGES): "fatal",
}


def hdfs_timestamp(offset_seconds: int) -> tuple[str, str]:
    """Return HDFS-style date/time strings: YYMMDD HHMMSS."""
    ts = datetime.now() + timedelta(seconds=offset_seconds)
    return ts.strftime("%y%m%d"), ts.strftime("%H%M%S")


def make_block_id(index: int, profile_name: str) -> str:
    """Create stable-looking HDFS block IDs for the monitor to group."""
    prefixes = {
        "stable": "1",
        "warning": "2",
        "recovering": "3",
        "error": "-8",
        "fatal": "-9",
    }
    prefix = prefixes.get(profile_name, "7")
    return f"blk_{prefix}{index:09d}"


def format_hdfs_line(
    *,
    date: str,
    clock: str,
    pid: int,
    level: str,
    component: str,
    message: str,
) -> str:
    return f"{date} {clock} {pid} {level} {component}: {message}"


def write_line(log_path: Path, line: str) -> None:
    with log_path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
        f.flush()


def run_pipeline(
    *,
    log_path: Path,
    scenario: str,
    delay: float,
    loops: int,
    start_delay: float,
    reset_log: bool,
    seed: int,
) -> None:
    random.seed(seed)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    if reset_log:
        log_path.write_text("", encoding="utf-8")
    else:
        log_path.touch(exist_ok=True)

    print(f"Demo pipeline writing to: {log_path}")
    print(f"Scenario: {scenario}")
    print("Start the monitor first; this simulator will begin shortly.")
    if start_delay > 0:
        time.sleep(start_delay)

    templates = SCENARIOS[scenario]
    line_no = 0

    for loop_idx in range(1, loops + 1):
        for block_idx, template in enumerate(templates, start=1):
            profile_name = PROFILE_NAMES.get(id(template), "custom")
            block = make_block_id(loop_idx * 10 + block_idx, profile_name=profile_name)
            pid = random.randint(100, 999)

            print(f"\n[PIPELINE] profile={profile_name} block={block}")
            for level, component, message_template in template:
                date, clock = hdfs_timestamp(line_no)
                message = message_template.format(block=block)
                line = format_hdfs_line(
                    date=date,
                    clock=clock,
                    pid=pid,
                    level=level,
                    component=component,
                    message=message,
                )
                write_line(log_path, line)
                print(f"[LOG] {line}")
                line_no += 1
                time.sleep(delay)

    write_line(log_path, LIVE_STOP_MARKER)
    print(f"\n[LOG] {LIVE_STOP_MARKER}")
    print("\nDemo pipeline finished.")
    print(f"Live log file: {log_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Simulate a live data pipeline that writes HDFS-style logs."
    )
    parser.add_argument(
        "--log-file",
        default=str(DEFAULT_LOG_PATH),
        help="Path of the live log file to write.",
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(SCENARIOS),
        default="mixed",
        help="Type of pipeline run to simulate.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.7,
        help="Seconds to wait between log lines.",
    )
    parser.add_argument(
        "--loops",
        type=int,
        default=3,
        help="How many batches of blocks to simulate.",
    )
    parser.add_argument(
        "--start-delay",
        type=float,
        default=3.0,
        help="Seconds to wait before writing logs so the monitor can attach.",
    )
    parser.add_argument(
        "--reset-log",
        action="store_true",
        help="Clear the log file before writing. Use before starting the monitor.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for repeatable demo output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_pipeline(
        log_path=Path(args.log_file),
        scenario=args.scenario,
        delay=args.delay,
        loops=max(1, args.loops),
        start_delay=max(0.0, args.start_delay),
        reset_log=args.reset_log,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
