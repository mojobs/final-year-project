"""
live_pipeline_runner.py
-----------------------
Launch a real pipeline command, stream its stdout/stderr into the predictor
plugin, collect runtime resource metrics, and optionally stop the process when
the model raises HIGH risk.

This is the "attach the plugin beside a running pipeline" layer. The pipeline
does not need to import PredictorPlugin. It only needs to print logs.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

from config import PROJECT_ROOT
from data_loader import parse_hdfs_line
from demo_pipeline import format_hdfs_line, hdfs_timestamp
from predictor_plugin import PredictorPlugin, StopPolicy


DEFAULT_RAW_LOG = PROJECT_ROOT / "simulated" / "attached_pipeline.raw.log"
DEFAULT_NORMALIZED_LOG = PROJECT_ROOT / "simulated" / "attached_pipeline.normalized.log"
DEFAULT_COMMAND = [
    sys.executable,
    str(PROJECT_ROOT / "examples" / "real_pipeline_job.py"),
        "--scenario",
        "mixed",
        "--delay",
        "0.35",
        "--records",
        "1200",
        "--batches",
        "12",
]

STAGE_NAMES = ("Extract", "Validate", "Transform", "Load", "Report")
LEVEL_KEYWORDS = {
    "FATAL": ("fatal", "critical", "panic", "abort", "terminate"),
    "ERROR": ("error", "failed", "failure", "exception", "timeout", "corrupt"),
    "WARN": ("warn", "warning", "retry", "slow", "fallback", "threshold", "queue full"),
}


@dataclass
class AdaptedLogLine:
    raw_line: str
    normalized_line: str
    level: str
    stage: str
    component: str
    process_id: str


@dataclass
class ResourceSampler:
    working_dir: Path
    process: subprocess.Popen | None = None
    samples: list[dict] = field(default_factory=list)
    psutil_available: bool = False
    _psutil_process: object | None = None
    _psutil: object | None = None

    def attach(self, process: subprocess.Popen) -> None:
        self.process = process
        try:
            import psutil  # type: ignore

            self._psutil = psutil
            self._psutil_process = psutil.Process(process.pid)
            self._psutil_process.cpu_percent(interval=None)
            self.psutil_available = True
        except Exception:
            self._psutil = None
            self._psutil_process = None
            self.psutil_available = False

    def sample(self) -> dict:
        snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "cpu_percent": 0.0,
            "memory_rss_mb": 0.0,
            "child_processes": 0,
            "disk_used_percent": 0.0,
            "metrics_backend": "psutil" if self.psutil_available else "stdlib",
        }

        if self.psutil_available and self._psutil_process is not None:
            try:
                proc = self._psutil_process
                children = proc.children(recursive=True)
                cpu = proc.cpu_percent(interval=None)
                rss = proc.memory_info().rss
                for child in children:
                    try:
                        cpu += child.cpu_percent(interval=None)
                        rss += child.memory_info().rss
                    except Exception:
                        continue
                snapshot["cpu_percent"] = round(float(cpu), 2)
                snapshot["memory_rss_mb"] = round(float(rss) / (1024 * 1024), 2)
                snapshot["child_processes"] = len(children)
            except Exception:
                pass

        try:
            usage = shutil.disk_usage(self.working_dir)
            snapshot["disk_used_percent"] = round(
                (usage.used / max(usage.total, 1)) * 100,
                2,
            )
        except Exception:
            pass

        self.samples.append(snapshot)
        return snapshot

    def summary(self) -> dict:
        if not self.samples:
            return {
                "samples": 0,
                "metrics_backend": "psutil" if self.psutil_available else "stdlib",
                "max_cpu_percent": 0.0,
                "max_memory_rss_mb": 0.0,
                "max_disk_used_percent": 0.0,
            }
        return {
            "samples": len(self.samples),
            "metrics_backend": self.samples[-1].get("metrics_backend", "stdlib"),
            "max_cpu_percent": max(s.get("cpu_percent", 0.0) for s in self.samples),
            "avg_cpu_percent": round(
                sum(s.get("cpu_percent", 0.0) for s in self.samples) / len(self.samples),
                2,
            ),
            "max_memory_rss_mb": max(s.get("memory_rss_mb", 0.0) for s in self.samples),
            "max_disk_used_percent": max(s.get("disk_used_percent", 0.0) for s in self.samples),
        }

    def suspend_tree(self) -> bool:
        if not self.psutil_available or self._psutil_process is None:
            return False
        try:
            proc = self._psutil_process
            for child in proc.children(recursive=True):
                try:
                    child.suspend()
                except Exception:
                    pass
            proc.suspend()
            return True
        except Exception:
            return False

    def resume_tree(self) -> bool:
        if not self.psutil_available or self._psutil_process is None:
            return False
        try:
            proc = self._psutil_process
            proc.resume()
            for child in proc.children(recursive=True):
                try:
                    child.resume()
                except Exception:
                    pass
            return True
        except Exception:
            return False


class PipelineLogAdapter:
    """Convert ordinary pipeline logs into the HDFS-style lines expected by the model."""

    def __init__(self, *, pid: int, session_name: str = "attached") -> None:
        self.pid = pid
        self.session_name = session_name
        self.line_no = 0
        # Ordinary attached logs describe one batch job even when its ETL stage
        # changes. Keep a stable ID so risk evidence accumulates across the run.
        self._job_block = f"blk_-99{pid:08d}"

    def adapt(self, raw_line: str) -> AdaptedLogLine:
        stripped = raw_line.rstrip("\n")
        parsed = parse_hdfs_line(stripped)
        if parsed and parsed.get("block_ids"):
            process_id = parsed["block_ids"][0]
            return AdaptedLogLine(
                raw_line=stripped,
                normalized_line=stripped,
                level=str(parsed.get("level", "INFO")).upper(),
                stage=self._detect_stage(parsed.get("content", "")),
                component=str(parsed.get("component", "external.Pipeline")),
                process_id=process_id,
            )

        stage = self._detect_stage(stripped)
        level = self._detect_level(stripped)
        component = f"external.{stage}"
        process_id = self._job_block
        date, clock = hdfs_timestamp(self.line_no)
        self.line_no += 1
        message = f"{stage} stage event for {process_id}: {stripped}"
        normalized = format_hdfs_line(
            date=date,
            clock=clock,
            pid=self.pid,
            level=level,
            component=component,
            message=message,
        )
        return AdaptedLogLine(
            raw_line=stripped,
            normalized_line=normalized,
            level=level,
            stage=stage,
            component=component,
            process_id=process_id,
        )

    @staticmethod
    def _detect_stage(text: str) -> str:
        lowered = text.lower()
        for stage in STAGE_NAMES:
            if stage.lower() in lowered:
                return stage

        match = re.search(r"\[(?:INFO|WARN|WARNING|ERROR|FATAL|DEBUG)\]\s+([A-Za-z_]+)", text)
        if match:
            guessed = match.group(1).strip().capitalize()
            if guessed:
                return guessed
        return "External"

    @staticmethod
    def _detect_level(text: str) -> str:
        lowered = text.lower()
        explicit = re.search(r"\[(FATAL|ERROR|WARN|WARNING|INFO|DEBUG)\]", text, re.IGNORECASE)
        if explicit:
            value = explicit.group(1).upper()
            return "WARN" if value == "WARNING" else value

        for level, keywords in LEVEL_KEYWORDS.items():
            if any(keyword in lowered for keyword in keywords):
                return level
        return "INFO"


class LivePipelineRunner:
    def __init__(
        self,
        command: str | Sequence[str],
        *,
        stop_policy: StopPolicy = "advise",
        min_lines: int = 5,
        working_dir: str | Path = PROJECT_ROOT,
        raw_log_path: str | Path = DEFAULT_RAW_LOG,
        normalized_log_path: str | Path = DEFAULT_NORMALIZED_LOG,
        model_path: str | Path | None = None,
        demo_risk_calibration: bool = True,
        timeout_seconds: float | None = None,
        event_callback: Callable[[dict], None] | None = None,
    ) -> None:
        self.command = normalize_command(command)
        self.stop_policy = stop_policy
        self.min_lines = int(min_lines)
        self.working_dir = Path(working_dir)
        self.raw_log_path = Path(raw_log_path)
        self.normalized_log_path = Path(normalized_log_path)
        self.timeout_seconds = timeout_seconds
        self.event_callback = event_callback
        self.stage_status = {stage: "pending" for stage in STAGE_NAMES}
        self.stage_counts = {stage: 0 for stage in STAGE_NAMES}
        self.stage_status["External"] = "pending"
        self.stage_counts["External"] = 0
        self.plugin = PredictorPlugin(
            min_lines=self.min_lines,
            stop_policy=stop_policy,
            demo_risk_calibration=demo_risk_calibration,
            model_path=str(model_path) if model_path else None,
            log_path=self.normalized_log_path,
            report_prefix="attached_pipeline_monitoring_report",
        )
        self.resource_sampler = ResourceSampler(self.working_dir)
        self.return_code: int | None = None
        self.termination_reason: str | None = None
        self.process: subprocess.Popen | None = None
        self.stop_requested = False
        self.review_continue_requested = False
        self.paused_for_review = False
        self.status = "idle"
        self.started_at: str | None = None
        self.finished_at: str | None = None

    def run(self) -> dict:
        self.raw_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.normalized_log_path.parent.mkdir(parents=True, exist_ok=True)
        self.raw_log_path.write_text("", encoding="utf-8")
        self.normalized_log_path.write_text("", encoding="utf-8")

        start_time = time.time()
        self.started_at = datetime.now(timezone.utc).isoformat()
        self.status = "starting"
        process = subprocess.Popen(
            self.command,
            cwd=str(self.working_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        self.process = process
        self.resource_sampler.attach(process)
        adapter = PipelineLogAdapter(pid=process.pid)
        status = "running"
        self.status = status
        self._emit_event("started")

        try:
            assert process.stdout is not None
            for raw in process.stdout:
                adapted = adapter.adapt(raw)
                metrics = self.resource_sampler.sample()
                self._record_line(adapted, metrics)
                self._emit_event("line", adapted=adapted, metrics=metrics)
                if self.stop_requested:
                    status = "stopped"
                    self.termination_reason = "Operator stopped the attached pipeline"
                    self._mark_stopped(adapted.stage)
                    self._terminate(process)
                    break
                if self.plugin.should_stop():
                    status = "stopped"
                    self.termination_reason = self.plugin.stop_reason
                    self._mark_stopped(adapted.stage)
                    self._terminate(process)
                    self._emit_event("stopping", adapted=adapted, metrics=metrics)
                    break
                if self.plugin.requires_review():
                    status = self._wait_for_operator_review(process, adapted, metrics, start_time)
                    if status == "stopped":
                        break
                    status = "running"
                if self.timeout_seconds and time.time() - start_time > self.timeout_seconds:
                    status = "stopped"
                    self.termination_reason = f"timeout after {self.timeout_seconds:.1f}s"
                    self._mark_stopped(adapted.stage)
                    self._terminate(process)
                    self._emit_event("timeout", adapted=adapted, metrics=metrics)
                    break
        finally:
            if process.poll() is None:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self._kill(process)
            self.return_code = process.poll()
            self.process = None

        if status != "stopped":
            status = self._final_status(self.return_code)
        self.status = status
        self.finished_at = datetime.now(timezone.utc).isoformat()
        self._complete_open_stages(status)

        summary = self.plugin.finalize(
            status=status,
            output_path=None,
            extra_summary={
                "pipeline_type": "attached_live_command",
                "command": " ".join(self.command),
                "return_code": self.return_code,
                "termination_reason": self.termination_reason,
                "raw_log_path": str(self.raw_log_path),
                "normalized_log_path": str(self.normalized_log_path),
                "stage_status": dict(self.stage_status),
                "stage_counts": dict(self.stage_counts),
                "resource_summary": self.resource_sampler.summary(),
                "resource_samples": list(self.resource_sampler.samples[-120:]),
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "paused_for_review": self.paused_for_review,
            },
        )
        result = {
            "summary": summary,
            "alerts": list(self.plugin.alerts),
            "logs": list(self.plugin.logs),
            "resource_samples": list(self.resource_sampler.samples),
        }
        self._emit_event("completed", result=result)
        return result

    def request_stop(self) -> None:
        self.stop_requested = True
        self.status = "stopping"
        self.termination_reason = "Operator stopped the attached pipeline"
        self._emit_event("stop_requested")
        if self.process and self.process.poll() is None:
            self._terminate(self.process)

    def continue_after_review(self) -> None:
        self.review_continue_requested = True
        self.plugin.continue_after_review()
        self.status = "resuming"
        self._emit_event("continue_requested")

    def snapshot(self, *, status: str | None = None) -> dict:
        current_status = status or self.status
        summary = {
            "status": current_status,
            "source": "hdfs",
            "stop_policy": self.stop_policy,
            "stop_reason": self.plugin.stop_reason,
            "review_required": self.plugin.requires_review(),
            "review_reason": self.plugin.review_reason,
            "reviewed_processes": sorted(self.plugin.reviewed_processes),
            "paused_for_review": self.paused_for_review,
            "recommendation": self.plugin.recommendation or self.plugin.default_recommendation(),
            "log_path": str(self.normalized_log_path),
            "output_path": None,
            "lines_processed": len(self.plugin.logs),
            "total_predictions": len(self.plugin.alerts),
            "current_risk": self.plugin.current_risk(),
            "latest_probability": self.plugin.latest_probability(),
            "peak_probability": self.plugin.peak_probability(),
            "risk_counts": self.plugin.risk_counts(),
            "high_risk_processes": self.plugin.high_risk_processes(),
            "pipeline_type": "attached_live_command",
            "command": " ".join(self.command),
            "return_code": self.return_code,
            "termination_reason": self.termination_reason,
            "raw_log_path": str(self.raw_log_path),
            "normalized_log_path": str(self.normalized_log_path),
            "stage_status": dict(self.stage_status),
            "stage_counts": dict(self.stage_counts),
            "resource_summary": self.resource_sampler.summary(),
            "resource_samples": list(self.resource_sampler.samples[-120:]),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
        }
        return {
            "summary": summary,
            "alerts": list(self.plugin.alerts),
            "logs": list(self.plugin.logs),
            "resource_samples": list(self.resource_sampler.samples),
        }

    def _emit_event(self, event_type: str, **extra: object) -> None:
        if not self.event_callback:
            return
        payload = self.snapshot()
        payload["event_type"] = event_type
        if extra:
            payload["event"] = {
                key: _event_value(value)
                for key, value in extra.items()
            }
        try:
            self.event_callback(payload)
        except Exception:
            pass

    def _wait_for_operator_review(
        self,
        process: subprocess.Popen,
        adapted: AdaptedLogLine,
        metrics: dict,
        start_time: float,
    ) -> str:
        self.status = "waiting_for_review"
        self.paused_for_review = self.resource_sampler.suspend_tree()
        self._mark_risk(adapted.stage)
        self._emit_event("review_required", adapted=adapted, metrics=metrics)

        while self.plugin.requires_review() and not self.stop_requested:
            if self.timeout_seconds and time.time() - start_time > self.timeout_seconds:
                self.termination_reason = f"timeout after {self.timeout_seconds:.1f}s during operator review"
                self._mark_stopped(adapted.stage)
                self._terminate(process)
                return "stopped"
            time.sleep(0.2)

        if self.stop_requested:
            self.termination_reason = "Operator stopped the attached pipeline during review"
            self._mark_stopped(adapted.stage)
            self._terminate(process)
            return "stopped"

        if self.paused_for_review:
            self.resource_sampler.resume_tree()
        self.paused_for_review = False
        self.review_continue_requested = False
        if self.stage_status.get(adapted.stage) == "risk":
            self.stage_status[adapted.stage] = "running"
        self.status = "running"
        self._emit_event("continued", adapted=adapted, metrics=metrics)
        return "running"

    def _record_line(self, adapted: AdaptedLogLine, metrics: dict) -> None:
        with self.raw_log_path.open("a", encoding="utf-8") as f:
            f.write(adapted.raw_line + "\n")
        with self.normalized_log_path.open("a", encoding="utf-8") as f:
            f.write(adapted.normalized_line + "\n")

        self._mark_running(adapted.stage)
        self.stage_counts[adapted.stage] = self.stage_counts.get(adapted.stage, 0) + 1
        self.plugin.ingest_log(
            adapted.normalized_line,
            metadata={
                "raw_line": adapted.raw_line,
                "level": adapted.level,
                "stage": adapted.stage,
                "component": adapted.component,
                "block": adapted.process_id,
                "profile": "attached_command",
                "resource_metrics": metrics,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _mark_running(self, stage: str) -> None:
        if stage not in self.stage_status:
            self.stage_status[stage] = "pending"
            self.stage_counts[stage] = 0
        for name, state in list(self.stage_status.items()):
            if state == "running" and name != stage:
                self.stage_status[name] = "done"
        if self.stage_status.get(stage) in {"pending", "done"}:
            self.stage_status[stage] = "running"

    def _mark_stopped(self, stage: str) -> None:
        if stage in self.stage_status:
            self.stage_status[stage] = "stopped"

    def _mark_risk(self, stage: str) -> None:
        if stage in self.stage_status:
            self.stage_status[stage] = "risk"

    def _complete_open_stages(self, status: str) -> None:
        for stage, state in list(self.stage_status.items()):
            if state == "running":
                self.stage_status[stage] = "stopped" if status == "stopped" else "done"

    def _final_status(self, return_code: int | None) -> str:
        has_high = self.plugin.has_high_risk()
        if return_code == 0 and has_high:
            return "completed_with_high_risk"
        if return_code == 0:
            return "completed"
        if has_high:
            return "failed_with_high_risk"
        return "failed"

    def _terminate(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        try:
            if self.resource_sampler.psutil_available:
                proc = self.resource_sampler._psutil_process
                for child in proc.children(recursive=True):
                    try:
                        child.terminate()
                    except Exception:
                        pass
                proc.terminate()
            else:
                process.terminate()
            process.wait(timeout=3)
        except Exception:
            self._kill(process)

    def _kill(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        try:
            if self.resource_sampler.psutil_available:
                proc = self.resource_sampler._psutil_process
                for child in proc.children(recursive=True):
                    try:
                        child.kill()
                    except Exception:
                        pass
                proc.kill()
            else:
                process.kill()
        except Exception:
            try:
                process.kill()
            except Exception:
                pass


def normalize_command(command: str | Sequence[str]) -> list[str]:
    if isinstance(command, str):
        parts = shlex.split(command, posix=(os.name != "nt"))
    else:
        parts = [str(part) for part in command if str(part).strip()]
    cleaned = [part.strip("\"'") for part in parts if part.strip("\"'")]
    if cleaned and cleaned[0].lower() in {"python", "python.exe"} and shutil.which(cleaned[0]) is None:
        cleaned[0] = sys.executable
    return cleaned


def _event_value(value: object) -> object:
    if isinstance(value, AdaptedLogLine):
        return asdict(value)
    return value


class LiveAttachSession:
    """Threaded wrapper used by Streamlit to show attach mode while it runs."""

    def __init__(
        self,
        command: str | Sequence[str],
        *,
        stop_policy: StopPolicy = "advise",
        min_lines: int = 5,
        working_dir: str | Path = PROJECT_ROOT,
        raw_log_path: str | Path = DEFAULT_RAW_LOG,
        normalized_log_path: str | Path = DEFAULT_NORMALIZED_LOG,
        model_path: str | Path | None = None,
        demo_risk_calibration: bool = True,
        timeout_seconds: float | None = None,
    ) -> None:
        self._lock = threading.Lock()
        self.runner = LivePipelineRunner(
            command,
            stop_policy=stop_policy,
            min_lines=min_lines,
            working_dir=working_dir,
            raw_log_path=raw_log_path,
            normalized_log_path=normalized_log_path,
            model_path=model_path,
            demo_risk_calibration=demo_risk_calibration,
            timeout_seconds=timeout_seconds,
            event_callback=self._on_event,
        )
        self._latest = self.runner.snapshot(status="idle")
        self._thread: threading.Thread | None = None
        self._error: str | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self.runner.request_stop()

    def continue_pipeline(self) -> None:
        self.runner.continue_after_review()

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def snapshot(self) -> dict:
        with self._lock:
            payload = _copy_payload(self._latest)
            if self._error:
                payload["summary"]["status"] = "error"
                payload["summary"]["recommendation"] = self._error
            payload["running"] = self.is_running()
            return payload

    def _run(self) -> None:
        try:
            result = self.runner.run()
            with self._lock:
                self._latest = _copy_payload(result)
        except Exception as exc:
            with self._lock:
                self._error = str(exc)

    def _on_event(self, payload: dict) -> None:
        with self._lock:
            self._latest = _copy_payload(payload)


def _copy_payload(payload: dict) -> dict:
    return {
        "summary": dict(payload.get("summary", {}) or {}),
        "alerts": list(payload.get("alerts", []) or []),
        "logs": list(payload.get("logs", []) or []),
        "resource_samples": list(payload.get("resource_samples", []) or []),
        "event_type": payload.get("event_type"),
        "event": payload.get("event"),
    }


def run_attached_pipeline(
    command: str | Sequence[str] | None = None,
    **kwargs,
) -> dict:
    runner = LivePipelineRunner(command or DEFAULT_COMMAND, **kwargs)
    return runner.run()
