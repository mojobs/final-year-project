"""
etl_pipeline_demo.py
--------------------
Concrete ETL pipeline demo monitored by the predictor plugin.

The job reads a small orders CSV, validates it, transforms currency amounts,
loads a curated CSV, and generates a monitoring report. Every stage emits
HDFS-style log lines into the plugin so the same trained predictor can classify
the running process as LOW, MEDIUM or HIGH risk.
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import LIVE_STOP_MARKER, PROJECT_ROOT
from demo_pipeline import format_hdfs_line, hdfs_timestamp, write_line
from predictor_plugin import PredictorPlugin


ETL_SCENARIOS = ["normal", "warning", "recovering", "risky", "mixed"]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "sample_orders.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "outputs" / "curated_orders.csv"
DEFAULT_LOG = PROJECT_ROOT / "simulated" / "etl_pipeline.log"

USD_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.25,
    "NGN": 0.00067,
}


class PipelineStopped(Exception):
    pass


class MonitoredETLPipeline:
    def __init__(
        self,
        *,
        scenario: str = "mixed",
        stop_policy: str = "advise",
        min_lines: int = 5,
        input_path: str | Path = DEFAULT_INPUT,
        output_path: str | Path = DEFAULT_OUTPUT,
        log_path: str | Path = DEFAULT_LOG,
        seed: int = 42,
    ) -> None:
        if scenario not in ETL_SCENARIOS:
            raise ValueError(f"Unknown ETL scenario: {scenario}")

        self.scenario = scenario
        self.stop_policy = stop_policy
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.log_path = Path(log_path)
        self.seed = seed
        self.pid = random.Random(seed).randint(100, 999)
        self.block_id = self._block_for_scenario(scenario)
        self.line_no = 0
        self.stage_status = {
            "Extract": "pending",
            "Validate": "pending",
            "Transform": "pending",
            "Load": "pending",
            "Report": "pending",
        }
        self.stage_counts = {stage: 0 for stage in self.stage_status}
        self.rows: list[dict] = []
        self.curated_rows: list[dict] = []
        self.output_created = False
        self.plugin = PredictorPlugin(
            min_lines=min_lines,
            stop_policy=stop_policy,  # type: ignore[arg-type]
            demo_risk_calibration=True,
            log_path=self.log_path,
            report_prefix="etl_pipeline_monitoring_report",
        )

    def run(self) -> dict:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")

        status = "completed"
        try:
            self.extract()
            self.validate()
            self.transform()
            self.load()
            self.stage_status["Report"] = "running"
            self.emit(
                "INFO",
                "pipeline.Reporter",
                f"Generated pipeline completion summary for block {self.block_id}",
                "Report",
            )
            self.stage_status["Report"] = "done"
        except PipelineStopped:
            status = "stopped"
        finally:
            write_line(self.log_path, LIVE_STOP_MARKER)

        if status != "stopped" and self.plugin.has_high_risk():
            status = "completed_with_high_risk"

        summary = self.plugin.finalize(
            status=status,
            output_path=self.output_path if self.output_created else None,
            extra_summary={
                "scenario": self.scenario,
                "pipeline_type": "monitored_etl",
                "input_path": str(self.input_path),
                "rows_extracted": len(self.rows),
                "rows_loaded": len(self.curated_rows) if self.output_created else 0,
                "stage_status": dict(self.stage_status),
                "stage_counts": dict(self.stage_counts),
            },
        )
        return {
            "summary": summary,
            "alerts": list(self.plugin.alerts),
            "logs": list(self.plugin.logs),
        }

    def extract(self) -> None:
        self._stage_running("Extract")
        self.emit(
            "INFO",
            "pipeline.Extractor",
            f"Starting CSV extraction for block {self.block_id}",
            "Extract",
        )
        if not self.input_path.exists():
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        with self.input_path.open("r", encoding="utf-8", newline="") as f:
            self.rows = list(csv.DictReader(f))

        self.emit(
            "INFO",
            "pipeline.Extractor",
            f"Extracted {len(self.rows)} order rows for block {self.block_id}",
            "Extract",
        )
        if self.scenario in {"mixed", "warning"}:
            self.emit(
                "WARN",
                "pipeline.Extractor",
                f"Source latency near threshold while extracting block {self.block_id}",
                "Extract",
            )
        self._stage_done("Extract")

    def validate(self) -> None:
        self._stage_running("Validate")
        required = {"order_id", "customer_id", "order_date", "amount", "currency", "status"}
        missing = required.difference(self.rows[0].keys() if self.rows else set())
        if missing:
            self.emit(
                "ERROR",
                "pipeline.SchemaValidator",
                f"Missing required columns {sorted(missing)} for block {self.block_id}",
                "Validate",
            )
            if self.scenario in {"risky", "mixed"}:
                self._maybe_stop()
        else:
            self.emit(
                "INFO",
                "pipeline.SchemaValidator",
                f"Schema validation passed for block {self.block_id}",
                "Validate",
            )

        zero_amounts = [
            row for row in self.rows
            if float(row.get("amount") or 0) <= 0
        ]
        if zero_amounts:
            level = "WARN" if self.scenario != "risky" else "ERROR"
            self.emit(
                level,
                "pipeline.SchemaValidator",
                f"{len(zero_amounts)} non-positive amounts detected for block {self.block_id}; retrying validation",
                "Validate",
            )
        if self.scenario in {"recovering", "mixed"}:
            self.emit(
                "WARN",
                "pipeline.SchemaValidator",
                f"Optional channel column missing for block {self.block_id}; fallback default applied",
                "Validate",
            )
            self.emit(
                "INFO",
                "pipeline.SchemaValidator",
                f"Recovered validation defaults for block {self.block_id}",
                "Validate",
            )
        self._stage_done("Validate")

    def transform(self) -> None:
        self._stage_running("Transform")
        self.emit(
            "INFO",
            "pipeline.Transformer",
            f"Starting currency normalization for block {self.block_id}",
            "Transform",
        )
        self.curated_rows = []
        for row in self.rows:
            amount = float(row.get("amount") or 0.0)
            currency = (row.get("currency") or "USD").upper()
            amount_usd = round(amount * USD_RATES.get(currency, 1.0), 2)
            self.curated_rows.append(
                {
                    "order_id": row.get("order_id", ""),
                    "customer_id": row.get("customer_id", ""),
                    "order_date": row.get("order_date", ""),
                    "status": row.get("status", ""),
                    "amount_usd": amount_usd,
                    "risk_partition": "review" if amount_usd <= 0 else "clean",
                }
            )

        if self.scenario in {"warning", "recovering", "mixed"}:
            self.emit(
                "WARN",
                "pipeline.Transformer",
                f"Slow transform observed for block {self.block_id}; latency threshold near limit",
                "Transform",
            )
        if self.scenario in {"recovering", "mixed"}:
            self.emit(
                "WARN",
                "pipeline.Transformer",
                f"Retry scheduled for block {self.block_id}; fallback mapping selected",
                "Transform",
            )
            self.emit(
                "INFO",
                "pipeline.Transformer",
                f"Replayed transform safely for block {self.block_id}",
                "Transform",
            )
        self.emit(
            "INFO",
            "pipeline.Transformer",
            f"Transformed {len(self.curated_rows)} rows for block {self.block_id}",
            "Transform",
        )
        self._stage_done("Transform")

    def load(self) -> None:
        self._stage_running("Load")
        self.emit(
            "INFO",
            "pipeline.Loader",
            f"Preparing curated output write for block {self.block_id}",
            "Load",
        )
        if self.scenario in {"risky", "mixed"}:
            self.emit(
                "WARN",
                "pipeline.Loader",
                f"Queue full while buffering block {self.block_id}; retrying once",
                "Load",
            )
            self.emit(
                "ERROR",
                "pipeline.Loader",
                f"Timeout while loading block {self.block_id}; retrying",
                "Load",
            )
            self.emit(
                "ERROR",
                "pipeline.Loader",
                f"Connection reset while processing block {self.block_id}",
                "Load",
            )
            self.emit(
                "FATAL",
                "pipeline.Loader",
                f"Critical corruption detected for block {self.block_id}; terminate job",
                "Load",
            )
            self._maybe_stop()

        if self.plugin.should_stop():
            raise PipelineStopped(self.plugin.stop_reason or "HIGH risk auto-stop")

        with self.output_path.open("w", encoding="utf-8", newline="") as f:
            fieldnames = ["order_id", "customer_id", "order_date", "status", "amount_usd", "risk_partition"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.curated_rows)
        self.output_created = True

        self.emit(
            "INFO",
            "pipeline.Loader",
            f"Loaded {len(self.curated_rows)} curated rows for block {self.block_id}",
            "Load",
        )
        self.emit(
            "INFO",
            "pipeline.Committer",
            f"Commit successful for block {self.block_id}",
            "Load",
        )
        self._stage_done("Load")

    def emit(self, level: str, component: str, message: str, stage: str) -> list[dict]:
        date, clock = hdfs_timestamp(self.line_no)
        line = format_hdfs_line(
            date=date,
            clock=clock,
            pid=self.pid,
            level=level,
            component=component,
            message=message,
        )
        self.line_no += 1
        write_line(self.log_path, line)
        self.stage_counts[stage] += 1
        return self.plugin.ingest_log(
            line,
            metadata={
                "level": level,
                "component": component,
                "stage": stage,
                "block": self.block_id,
                "profile": self.scenario,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _maybe_stop(self) -> None:
        if self.plugin.should_stop():
            active = next((s for s, v in self.stage_status.items() if v == "running"), "Load")
            self.stage_status[active] = "stopped"
            raise PipelineStopped(self.plugin.stop_reason or "HIGH risk auto-stop")

    def _stage_running(self, stage: str) -> None:
        for name, status in list(self.stage_status.items()):
            if status == "running":
                self.stage_status[name] = "done"
        self.stage_status[stage] = "running"

    def _stage_done(self, stage: str) -> None:
        if self.stage_status.get(stage) == "running":
            self.stage_status[stage] = "done"

    @staticmethod
    def _block_for_scenario(scenario: str) -> str:
        prefixes = {
            "normal": "41",
            "warning": "42",
            "recovering": "43",
            "risky": "-84",
            "mixed": "-94",
        }
        return f"blk_{prefixes.get(scenario, '49')}0000001"


def run_monitored_etl(
    *,
    scenario: str = "mixed",
    stop_policy: str = "advise",
    min_lines: int = 5,
    input_path: str | Path = DEFAULT_INPUT,
    output_path: str | Path = DEFAULT_OUTPUT,
    log_path: str | Path = DEFAULT_LOG,
    seed: int = 42,
) -> dict:
    pipeline = MonitoredETLPipeline(
        scenario=scenario,
        stop_policy=stop_policy,
        min_lines=min_lines,
        input_path=input_path,
        output_path=output_path,
        log_path=log_path,
        seed=seed,
    )
    return pipeline.run()


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="Run the monitored ETL pipeline demo.")
    parser.add_argument("--scenario", choices=ETL_SCENARIOS, default="mixed")
    parser.add_argument("--stop-policy", choices=["advise", "auto_stop"], default="advise")
    parser.add_argument("--min-lines", type=int, default=5)
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--log-file", default=str(DEFAULT_LOG))
    args = parser.parse_args()

    result = run_monitored_etl(
        scenario=args.scenario,
        stop_policy=args.stop_policy,
        min_lines=args.min_lines,
        input_path=args.input,
        output_path=args.output,
        log_path=args.log_file,
    )
    summary = result["summary"]
    print("\nETL pipeline monitoring summary")
    print(f"  Status: {summary['status']}")
    print(f"  Current risk: {summary['current_risk']}")
    print(f"  Recommendation: {summary['recommendation']}")
    print(f"  JSON report: {summary.get('report_path')}")
    print(f"  HTML report: {summary.get('html_report_path')}")


if __name__ == "__main__":
    main()
