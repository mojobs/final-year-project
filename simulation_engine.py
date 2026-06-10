"""
simulation_engine.py
--------------------
Visual simulation layer for the pipeline failure predictor plugin.

This module keeps the model and feature logic inside PipelineMonitor. It only
coordinates a demo pipeline run, streams generated logs into the plugin, and
collects operator-facing state for dashboards or orchestration tools.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from config import LIVE_STOP_MARKER, OUTPUTS_DIR, PROJECT_ROOT
from demo_pipeline import (
    PROFILE_NAMES,
    SCENARIOS,
    format_hdfs_line,
    hdfs_timestamp,
    make_block_id,
    write_line,
)
from pipeline_monitor import PipelineMonitor
from report_generator import generate_report_files


StopPolicy = Literal["advise", "auto_stop"]

PIPELINE_STAGES = [
    {"id": "extract", "label": "Extract", "matches": ("Extractor", "DataXceiver")},
    {"id": "validate", "label": "Validate", "matches": ("SchemaValidator", "DFSClient")},
    {"id": "clean", "label": "Clean", "matches": ("Cleaner",)},
    {"id": "transform", "label": "Transform", "matches": ("Transformer", "Aggregator")},
    {"id": "load", "label": "Load", "matches": ("Loader", "BlockManager", "NameNode")},
    {"id": "audit", "label": "Audit", "matches": ("Auditor", "FSNamesystem")},
    {"id": "commit", "label": "Commit", "matches": ("Committer",)},
    {"id": "report", "label": "Report", "matches": ("Reporter", "Orchestrator")},
    {"id": "monitor", "label": "Monitor", "matches": ()},
]


@dataclass
class LogEvent:
    index: int
    line: str
    level: str
    component: str
    message: str
    block: str
    profile: str
    stage_id: str
    stage_label: str


@dataclass
class SimulationSession:
    scenario: str = "mixed"
    loops: int = 1
    stop_policy: StopPolicy = "advise"
    min_lines: int = 5
    seed: int = 42
    log_path: Path = field(
        default_factory=lambda: PROJECT_ROOT / "simulated" / "dashboard_live_pipeline.log"
    )

    status: str = "idle"
    current_index: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    stop_reason: str | None = None
    report_path: str | None = None
    html_report_path: str | None = None
    high_risk_seen: bool = False
    recommendation_pending: bool = False

    events: list[LogEvent] = field(default_factory=list)
    logs: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)
    stage_status: dict[str, str] = field(default_factory=dict)
    stage_counts: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.scenario not in SCENARIOS:
            raise ValueError(f"Unknown scenario: {self.scenario}")
        self.loops = max(1, int(self.loops))
        self.min_lines = max(1, int(self.min_lines))
        self.log_path = Path(self.log_path)
        self.monitor = PipelineMonitor(
            source="hdfs",
            print_alerts=False,
            min_lines=self.min_lines,
            demo_risk_calibration=True,
        )
        self.stage_status = {stage["id"]: "pending" for stage in PIPELINE_STAGES}
        self.stage_counts = {stage["id"]: 0 for stage in PIPELINE_STAGES}
        self.events = build_events(self.scenario, self.loops, self.seed)

    @property
    def total_events(self) -> int:
        return len(self.events)

    @property
    def progress(self) -> float:
        if not self.total_events:
            return 0.0
        return min(1.0, self.current_index / self.total_events)

    @property
    def current_risk(self) -> str:
        if not self.alerts:
            return "LOW"
        return str(self.alerts[-1].get("risk_level", "LOW"))

    @property
    def latest_probability(self) -> float:
        if not self.alerts:
            return 0.0
        return float(self.alerts[-1].get("failure_probability", 0.0) or 0.0)

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.log_path.write_text("", encoding="utf-8")
        self.status = "running"
        self.started_at = utc_now()
        self.finished_at = None
        self.stop_reason = None
        self.report_path = None
        self.html_report_path = None

    def pause(self) -> None:
        if self.status == "running":
            self.status = "paused"

    def resume(self) -> None:
        if self.status == "paused":
            self.status = "running"

    def step(self, events_per_tick: int = 1) -> list[dict]:
        if self.status != "running":
            return []

        new_alerts: list[dict] = []
        for _ in range(max(1, int(events_per_tick))):
            if self.current_index >= self.total_events:
                self.complete("completed")
                break

            event = self.events[self.current_index]
            self.current_index += 1
            write_line(self.log_path, event.line)
            self._record_event(event)

            predictions = self.monitor.ingest_line(event.line)
            for prediction in predictions:
                alert = self._record_alert(prediction, event)
                new_alerts.append(alert)
                if alert["risk_level"] == "HIGH":
                    self.high_risk_seen = True
                    if self.stop_policy == "auto_stop":
                        self.stop_reason = (
                            f"HIGH risk for {alert['process_id']} "
                            f"({alert['failure_probability']:.0%})"
                        )
                        self.complete("stopped")
                        return new_alerts
                    self.recommendation_pending = True

        return new_alerts

    def run_to_end(self, events_per_tick: int = 5, max_ticks: int = 10_000) -> dict:
        if self.status == "idle":
            self.start()
        ticks = 0
        while self.status == "running" and ticks < max_ticks:
            self.step(events_per_tick=events_per_tick)
            ticks += 1
        if self.status == "running":
            self.complete("stopped", reason="Simulation tick limit reached")
        return self.summary()

    def complete(self, final_status: str, reason: str | None = None) -> None:
        if self.status in {"completed", "stopped"}:
            return
        if reason:
            self.stop_reason = reason
        self.status = final_status
        self.finished_at = utc_now()
        write_line(self.log_path, LIVE_STOP_MARKER)
        self._flush_monitor()
        self.stage_status["monitor"] = "done"
        self.report_path = self.save_report()

    def risk_counts(self) -> dict[str, int]:
        counts = {"LOW": 0, "MEDIUM": 0, "HIGH": 0}
        for alert in self.alerts:
            risk = alert.get("risk_level", "LOW")
            if risk in counts:
                counts[risk] += 1
        return counts

    def summary(self) -> dict:
        counts = self.risk_counts()
        return {
            "scenario": self.scenario,
            "status": self.status,
            "stop_policy": self.stop_policy,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "stop_reason": self.stop_reason,
            "log_path": str(self.log_path),
            "report_path": self.report_path,
            "html_report_path": self.html_report_path,
            "lines_processed": len(self.logs),
            "total_lines": self.total_events,
            "total_predictions": len(self.alerts),
            "current_risk": self.current_risk,
            "latest_probability": self.latest_probability,
            "risk_counts": counts,
            "high_risk_processes": self.high_risk_processes(),
            "stage_counts": dict(self.stage_counts),
        }

    def high_risk_processes(self) -> list[dict]:
        high = [a for a in self.alerts if a.get("risk_level") == "HIGH"]
        high.sort(key=lambda item: item.get("failure_probability", 0.0), reverse=True)
        return [
            {
                "process_id": item.get("process_id"),
                "failure_probability": item.get("failure_probability"),
                "lines_seen": item.get("lines_seen"),
                "action": item.get("action"),
                "stage": item.get("stage"),
            }
            for item in high[:10]
        ]

    def report_payload(self) -> dict:
        return {
            "summary": self.summary(),
            "alerts": self.alerts,
            "logs": self.logs,
        }

    def save_report(self) -> str:
        paths = generate_report_files(
            self.report_payload(),
            prefix="dashboard_session_report",
            output_dir=OUTPUTS_DIR,
        )
        self.html_report_path = paths.get("html")
        return str(paths.get("json"))

    def _record_event(self, event: LogEvent) -> None:
        self.logs.append(
            {
                "index": event.index,
                "level": event.level,
                "component": event.component,
                "block": event.block,
                "profile": event.profile,
                "stage": event.stage_label,
                "line": event.line,
            }
        )
        for stage_id, status in list(self.stage_status.items()):
            if status == "running":
                self.stage_status[stage_id] = "done"
        self.stage_status[event.stage_id] = "running"
        self.stage_status["monitor"] = "running"
        self.stage_counts[event.stage_id] += 1

    def _record_alert(self, prediction: dict, event: LogEvent) -> dict:
        alert = dict(prediction)
        risk = alert.get("risk_level", "LOW")
        action = "CONTINUE"
        if risk == "HIGH":
            action = "AUTO_STOP" if self.stop_policy == "auto_stop" else "ADVISE_STOP"
        elif risk == "MEDIUM":
            action = "WATCH"

        alert.update(
            {
                "event_index": event.index,
                "stage": event.stage_label,
                "component": event.component,
                "profile": event.profile,
                "action": action,
            }
        )
        self.alerts.append(alert)
        if risk == "HIGH":
            self.stage_status[event.stage_id] = "risk"
        return alert

    def _flush_monitor(self) -> None:
        try:
            flushed = self.monitor._flush_all_processes()
        except Exception:
            flushed = []

        synthetic_event = self.logs[-1] if self.logs else {}
        for prediction in flushed:
            event = LogEvent(
                index=int(synthetic_event.get("index", self.current_index)),
                line=str(synthetic_event.get("line", "")),
                level=str(synthetic_event.get("level", "INFO")),
                component=str(synthetic_event.get("component", "pipeline.Monitor")),
                message="final flush",
                block=str(prediction.get("process_id", "unknown")),
                profile=str(synthetic_event.get("profile", "final")),
                stage_id="monitor",
                stage_label="Monitor",
            )
            self._record_alert(prediction, event)


def available_scenarios() -> list[str]:
    return sorted(SCENARIOS)


def build_events(scenario: str, loops: int, seed: int = 42) -> list[LogEvent]:
    rng = random.Random(seed)
    templates = SCENARIOS[scenario]
    events: list[LogEvent] = []
    line_no = 0

    for loop_idx in range(1, max(1, int(loops)) + 1):
        for block_idx, template in enumerate(templates, start=1):
            profile_name = PROFILE_NAMES.get(id(template), "custom")
            block = make_block_id(loop_idx * 10 + block_idx, profile_name=profile_name)
            pid = rng.randint(100, 999)

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
                stage = stage_for_component(component)
                events.append(
                    LogEvent(
                        index=line_no + 1,
                        line=line,
                        level=level,
                        component=component,
                        message=message,
                        block=block,
                        profile=profile_name,
                        stage_id=stage["id"],
                        stage_label=stage["label"],
                    )
                )
                line_no += 1
    return events


def stage_for_component(component: str) -> dict:
    for stage in PIPELINE_STAGES:
        if any(match in component for match in stage["matches"]):
            return stage
    return PIPELINE_STAGES[-1]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
