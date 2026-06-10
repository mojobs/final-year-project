"""
Unmonitored CSV pipeline used by the live-attach demo.

This script deliberately does not import PredictorPlugin. It behaves like an
ordinary batch job that prints logs while it extracts, validates, transforms,
and loads a CSV file. live_pipeline_runner.py attaches to this process from the
outside and monitors the logs.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "sample_orders.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "attached_pipeline_orders.csv"

USD_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.25,
    "NGN": 0.00067,
}


def busy_work(size: int = 3500) -> None:
    total = 0
    for i in range(size):
        total += (i * i) % 97
    if total < 0:
        print(total)


def emit(level: str, stage: str, message: str, delay: float) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"{stamp} [{level}] {stage} - {message}", flush=True)
    if delay > 0:
        time.sleep(delay)


def load_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def write_curated(rows: list[dict], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = ["order_id", "customer_id", "order_date", "status", "amount_usd", "risk_partition"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def expand_rows(rows: list[dict], target_count: int) -> list[dict]:
    if not rows:
        return []
    expanded = []
    for idx in range(max(len(rows), target_count)):
        row = dict(rows[idx % len(rows)])
        row["order_id"] = f"{row.get('order_id', 'ORD')}-{idx + 1:06d}"
        expanded.append(row)
        if len(expanded) >= target_count:
            break
    return expanded


def split_batches(rows: list[dict], batch_count: int) -> list[list[dict]]:
    batch_count = max(1, min(batch_count, max(len(rows), 1)))
    size = max(1, round(len(rows) / batch_count))
    return [rows[i:i + size] for i in range(0, len(rows), size)]


def run_pipeline(
    *,
    scenario: str,
    input_path: Path,
    output_path: Path,
    delay: float,
    records: int,
    batches: int,
) -> int:
    emit("INFO", "Extract", f"Opening source connection for {input_path}", delay)
    source_rows = load_rows(input_path)
    rows = expand_rows(source_rows, records)
    partitions = split_batches(rows, batches)
    emit("INFO", "Extract", f"Discovered {len(partitions)} source partitions", delay)
    for idx, partition in enumerate(partitions, start=1):
        busy_work()
        emit(
            "INFO",
            "Extract",
            f"Read partition {idx}/{len(partitions)} with {len(partition)} records",
            delay,
        )
        if scenario in {"warning", "recovering", "mixed"} and idx == 3:
            emit("WARN", "Extract", "Source latency near threshold on partition 3", delay)
        if scenario == "risky" and idx == 4:
            emit("WARN", "Extract", "Source read retry needed after transient network delay", delay)

    emit("INFO", "Validate", "Checking required order columns", delay)
    required = {"order_id", "customer_id", "order_date", "amount", "currency", "status"}
    missing = required.difference(rows[0].keys() if rows else set())
    if missing:
        emit("ERROR", "Validate", f"Missing required columns: {sorted(missing)}", delay)
        return 2

    zero_amounts = [row for row in rows if float(row.get("amount") or 0) <= 0]
    if zero_amounts:
        emit("WARN", "Validate", f"{len(zero_amounts)} non-positive amounts queued for review", delay)

    if scenario in {"recovering", "mixed"}:
        emit("WARN", "Validate", "Optional channel column missing; fallback defaults applied", delay)
        emit("INFO", "Validate", "Validation recovered after fallback defaults", delay)
    for idx, partition in enumerate(partitions, start=1):
        busy_work(1800)
        emit(
            "INFO",
            "Validate",
            f"Validated partition {idx}/{len(partitions)} with {len(partition)} records",
            delay,
        )

    emit("INFO", "Transform", "Starting currency normalization", delay)
    curated = []
    for idx, partition in enumerate(partitions, start=1):
        for row in partition:
            amount = float(row.get("amount") or 0.0)
            currency = (row.get("currency") or "USD").upper()
            amount_usd = round(amount * USD_RATES.get(currency, 1.0), 2)
            curated.append(
                {
                    "order_id": row.get("order_id", ""),
                    "customer_id": row.get("customer_id", ""),
                    "order_date": row.get("order_date", ""),
                    "status": row.get("status", ""),
                    "amount_usd": amount_usd,
                    "risk_partition": "review" if amount_usd <= 0 else "clean",
                }
            )
        busy_work(2400)
        emit(
            "INFO",
            "Transform",
            f"Normalized partition {idx}/{len(partitions)} into USD amounts",
            delay,
        )

    if scenario in {"warning", "recovering", "mixed"}:
        emit("WARN", "Transform", "Slow transform observed; retry window is close", delay)
    if scenario in {"recovering", "mixed"}:
        emit("WARN", "Transform", "Retry scheduled after transient mapping error", delay)
        emit("INFO", "Transform", "Retry completed successfully", delay)

    emit("INFO", "Load", f"Preparing to write {len(curated)} curated rows", delay)
    emit("INFO", "Load", "Opening curated output writer", delay)
    if scenario in {"risky", "mixed"}:
        for idx, partition in enumerate(partitions, start=1):
            busy_work(2600)
            if idx <= 2:
                emit("INFO", "Load", f"Buffered load partition {idx}/{len(partitions)}", delay)
            elif idx == 3:
                emit("WARN", "Load", "Queue full while buffering records; retrying partition 3", delay)
            elif idx == 4:
                emit("WARN", "Load", "Retry scheduled because write latency crossed threshold", delay)
            elif idx == 5:
                emit("INFO", "Load", "Retry attempt accepted by curated writer", delay)
            elif idx == 6:
                emit("ERROR", "Load", "Timeout while loading records to curated output", delay)
            elif idx == 7:
                emit("WARN", "Load", "Backpressure still high after retry window", delay)
            elif idx == 8:
                emit("ERROR", "Load", "Connection reset during final write attempt", delay)
            elif idx == 9:
                emit("ERROR", "Load", "Disk write refused after repeated retry attempts", delay)
            else:
                emit("FATAL", "Load", "Critical corruption detected; terminating job", delay)
                time.sleep(max(delay, 0.1) * 6)
                return 3
        return 3

    write_curated(curated, output_path)
    for idx, partition in enumerate(partitions, start=1):
        busy_work(1600)
        emit("INFO", "Load", f"Wrote curated partition {idx}/{len(partitions)} with {len(partition)} records", delay)
    emit("INFO", "Load", f"Curated output written to {output_path}", delay)
    emit("INFO", "Report", "Computing row counts and data-quality summary", delay)
    emit("INFO", "Report", "Pipeline completion report generated", delay)
    return 0


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="Run an ordinary CSV ETL pipeline.")
    parser.add_argument("--scenario", choices=["normal", "warning", "recovering", "risky", "mixed"], default="mixed")
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--records", type=int, default=1200)
    parser.add_argument("--batches", type=int, default=12)
    args = parser.parse_args()

    exit_code = run_pipeline(
        scenario=args.scenario,
        input_path=Path(args.input),
        output_path=Path(args.output),
        delay=args.delay,
        records=args.records,
        batches=args.batches,
    )
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
