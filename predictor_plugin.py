"""
predictor_plugin.py
-------------------
Reusable plugin API for attaching the failure predictor to a running pipeline.

The lower-level PipelineMonitor still owns parsing, feature extraction and model
inference. This wrapper adds the operator-facing contract:

    plugin.ingest_log(line)
    plugin.should_stop()
    plugin.generate_report()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from config import PROJECT_ROOT
from pipeline_monitor import PipelineMonitor
from report_generator import generate_report_files


StopPolicy = Literal["advise", "auto_stop", "review"]


@dataclass
class PredictorPlugin:
    source: str = "hdfs"
    min_lines: int = 5
    stop_policy: StopPolicy = "advise"
    demo_risk_calibration: bool = True
    raw_model_risk: bool = False
    model_path: str | Path | None = None
    log_path: str | Path | None = None
    report_prefix: str = "plugin_session_report"

    alerts: list[dict] = field(default_factory=list)
    logs: list[dict] = field(default_factory=list)
    stopped: bool = False
    review_required: bool = False
    review_reason: str | None = None
    review_process_id: str | None = None
    reviewed_processes: set[str] = field(default_factory=set)
    stop_reason: str | None = None
    recommendation: str | None = None
    report_paths: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.monitor = PipelineMonitor(
            source=self.source,
            model_path=str(self.model_path) if self.model_path else None,
            min_lines=self.min_lines,
            print_alerts=False,
            raw_model_risk=self.raw_model_risk,
            demo_risk_calibration=self.demo_risk_calibration,
        )

    def ingest_log(self, raw_line: str, *, metadata: dict | None = None) -> list[dict]:
        """Feed one pipeline log line into the predictor plugin."""
        metadata = metadata or {}
        log_record = {
            "index": len(self.logs) + 1,
            "line": raw_line.rstrip("\n"),
            **metadata,
        }
        self.logs.append(log_record)

        predictions = self.monitor.ingest_line(raw_line)
        enriched = []
        for prediction in predictions:
            alert = self._enrich_alert(prediction, metadata)
            self.alerts.append(alert)
            enriched.append(alert)
            self._apply_policy(alert)
        return enriched

    def should_stop(self) -> bool:
        """Return True when an auto-stop HIGH-risk decision has been reached."""
        return self.stopped

    def requires_review(self) -> bool:
        """Return True when a HIGH-risk prediction needs operator review."""
        return self.review_required

    def continue_after_review(self) -> None:
        """Release a review hold and continue the pipeline."""
        if self.review_process_id:
            self.reviewed_processes.add(self.review_process_id)
        self.review_required = False
        self.review_reason = None
        self.review_process_id = None
        self.recommendation = "Operator reviewed the HIGH-risk process and continued the pipeline."

    def has_high_risk(self) -> bool:
        return any(alert.get("risk_level") == "HIGH" for alert in self.alerts)

    def latest_alert(self) -> dict | None:
        return self.alerts[-1] if self.alerts else None

    def flush(self) -> list[dict]:
        """Force final predictions for all buffered processes."""
        flushed = self.monitor._flush_all_processes()
        enriched = []
        metadata = {"stage": "Monitor", "component": "pipeline.Monitor"}
        for prediction in flushed:
            alert = self._enrich_alert(prediction, metadata)
            self.alerts.append(alert)
            enriched.append(alert)
            self._apply_policy(alert)
        return enriched

    def finalize(
        self,
        *,
        status: str,
        output_path: str | Path | None = None,
        extra_summary: dict | None = None,
    ) -> dict:
        self.flush()
        payload = self.report_payload(
            status=status,
            output_path=output_path,
            extra_summary=extra_summary,
        )
        self.report_paths = generate_report_files(payload, prefix=self.report_prefix)
        return {
            **payload["summary"],
            "report_paths": self.report_paths,
            "report_path": self.report_paths.get("json"),
            "html_report_path": self.report_paths.get("html"),
        }

    def report_payload(
        self,
        *,
        status: str,
        output_path: str | Path | None = None,
        extra_summary: dict | None = None,
    ) -> dict:
        summary = {
            "status": status,
            "source": self.source,
            "stop_policy": self.stop_policy,
            "stop_reason": self.stop_reason,
            "review_required": self.review_required,
            "review_reason": self.review_reason,
            "reviewed_processes": sorted(self.reviewed_processes),
            "recommendation": self.recommendation or self.default_recommendation(),
            "log_path": str(self.log_path) if self.log_path else None,
            "output_path": str(output_path) if output_path else None,
            "lines_processed": len(self.logs),
            "total_predictions": len(self.alerts),
            "current_risk": self.current_risk(),
            "latest_probability": self.latest_probability(),
            "peak_probability": self.peak_probability(),
            "risk_counts": self.risk_counts(),
            "high_risk_processes": self.high_risk_processes(),
        }
        if extra_summary:
            summary.update(extra_summary)
        return {
            "summary": summary,
            "alerts": self.alerts,
            "logs": self.logs,
        }

    def current_risk(self) -> str:
        if not self.alerts:
            return "LOW"
        return str(self.alerts[-1].get("risk_level", "LOW"))

    def latest_probability(self) -> float:
        if not self.alerts:
            return 0.0
        return float(self.alerts[-1].get("failure_probability", 0.0) or 0.0)

    def peak_probability(self) -> float:
        if not self.alerts:
            return 0.0
        return max(
            float(alert.get("failure_probability", 0.0) or 0.0)
            for alert in self.alerts
        )

    def risk_counts(self) -> dict[str, int]:
        counts = {"LOW": 0, "MEDIUM": 0, "HIGH": 0}
        for alert in self.alerts:
            risk = alert.get("risk_level", "LOW")
            if risk in counts:
                counts[risk] += 1
        return counts

    def high_risk_processes(self) -> list[dict]:
        high = [alert for alert in self.alerts if alert.get("risk_level") == "HIGH"]
        high.sort(key=lambda item: item.get("failure_probability", 0.0), reverse=True)
        return [
            {
                "process_id": item.get("process_id"),
                "failure_probability": item.get("failure_probability"),
                "lines_seen": item.get("lines_seen"),
                "stage": item.get("stage"),
                "action": item.get("action"),
            }
            for item in high[:10]
        ]

    def default_recommendation(self) -> str:
        if self.stopped:
            return "Pipeline was stopped because HIGH failure risk was detected."
        if self.review_required:
            return "Pipeline is waiting for operator review after HIGH failure risk was detected."
        if self.current_risk() == "HIGH":
            return "HIGH risk was detected. Stop the pipeline and inspect the process."
        if self.current_risk() == "MEDIUM":
            return "Continue with close monitoring and inspect warning-heavy stages."
        if self.has_high_risk():
            return "Risk has returned to LOW after an earlier HIGH reading. Continue monitoring closely."
        return "Pipeline can continue. No high-risk process was detected."

    def _enrich_alert(self, prediction: dict, metadata: dict) -> dict:
        risk = str(prediction.get("risk_level", "LOW"))
        action = "CONTINUE"
        if risk == "HIGH":
            if self.stop_policy == "auto_stop":
                action = "AUTO_STOP"
            elif self.stop_policy == "review":
                process_id = str(prediction.get("process_id", "unknown"))
                action = (
                    "CONTINUE_AFTER_REVIEW"
                    if process_id in self.reviewed_processes
                    else "REVIEW_REQUIRED"
                )
            else:
                action = "ADVISE_STOP"
        elif risk == "MEDIUM":
            action = "WATCH"

        return {
            **prediction,
            "stage": metadata.get("stage", "Monitor"),
            "component": metadata.get("component", "pipeline.Monitor"),
            "profile": metadata.get("profile", "plugin"),
            "resource_metrics": metadata.get("resource_metrics"),
            "action": action,
        }

    def _apply_policy(self, alert: dict) -> None:
        risk = str(alert.get("risk_level", "LOW"))
        if risk == "MEDIUM":
            self.recommendation = "MEDIUM risk detected. Continue with close monitoring."
            return
        if risk != "HIGH":
            self.recommendation = "LOW risk detected. The pipeline can continue."
            return

        probability = float(alert.get("failure_probability", 0.0) or 0.0)
        process_id = alert.get("process_id", "unknown")
        if self.stop_policy == "review" and str(process_id) in self.reviewed_processes:
            self.recommendation = (
                f"HIGH risk remains for {process_id} ({probability:.0%}), "
                "but the operator already reviewed this process and chose to continue."
            )
            return
        self.recommendation = (
            f"HIGH risk for {process_id} ({probability:.0%}). "
            "Stop and inspect the process."
        )
        if self.stop_policy == "auto_stop":
            self.stopped = True
            self.stop_reason = (
                f"HIGH risk for {process_id} ({probability:.0%})"
            )
        elif self.stop_policy == "review":
            self.review_required = True
            self.review_process_id = str(process_id)
            self.review_reason = (
                f"HIGH risk for {process_id} ({probability:.0%})"
            )


def create_plugin_for_project(**kwargs) -> PredictorPlugin:
    """Convenience constructor used by demos and external integrations."""
    if "log_path" not in kwargs:
        kwargs["log_path"] = PROJECT_ROOT / "simulated" / "plugin_pipeline.log"
    return PredictorPlugin(**kwargs)
