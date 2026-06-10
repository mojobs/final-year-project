"""
pipeline_monitor.py
-------------------
THE PLUGIN COMPONENT

Watches a live log file (or receives log lines via API) and continuously
predicts which processes/threads are at risk of failure as logs emerge.

Three integration modes
-----------------------
  1. FILE TAIL MODE
       Watches a growing log file (like `tail -f`) and triggers predictions
       as new lines are written by the running pipeline.

       monitor = PipelineMonitor.from_file("path/to/pipeline.log", source="hdfs")
       monitor.start()          ← blocks; prints alerts as they occur

  2. LINE FEED MODE (API integration)
       Call monitor.ingest_line(raw_line) for each log line as it arrives.
       Predictions are returned when enough data has accumulated.

       monitor = PipelineMonitor(source="hdfs")
       for line in log_stream:
           alerts = monitor.ingest_line(line)
           for alert in alerts:
               print(alert)

  3. BATCH FILE MODE
       Process a complete log file at once and return all predictions.

       results = monitor.process_file("path/to/HDFS.log")

Output format (one dict per prediction)
---------------------------------------
{
    "timestamp":           str   (ISO 8601),
    "process_id":          str   (block_id or job_id),
    "source":              str   ("hdfs" | "google"),
    "prediction":          int   (1 = FAILURE predicted, 0 = OK),
    "failure_probability": float (0.0 – 1.0),
    "risk_level":          str   ("HIGH" | "MEDIUM" | "LOW"),
    "lines_seen":          int   (how many log lines accumulated so far),
    "explanation": [
        {
          "rank":      int,
          "feature":   str,
          "value":     float,
          "direction": str,
        }, ...
    ]
}
"""

import os
import sys
import time
import json
import random
import threading
from datetime import datetime, timezone
from collections import defaultdict

from config import (
    MODEL_SAVE_PATH,
    model_path_for_source,
    PREDICT_EVERY_N_LINES,
    FLUSH_TIMEOUT_SECONDS,
    LIVE_STOP_MARKER,
    RISK_HIGH, RISK_MEDIUM,
)
from feature_extractor import StreamingExtractor
from model import PipelineMonitorModel


# ══════════════════════════════════════════════════════════════════
#  FORMATTING HELPERS
# ══════════════════════════════════════════════════════════════════

_RISK_COLORS = {
    "HIGH":   "\033[91m",   # red
    "MEDIUM": "\033[93m",   # yellow
    "LOW":    "\033[92m",   # green
    "RESET":  "\033[0m",
}


def _is_live_stop_marker(raw_line: str) -> bool:
    """Return True when a line explicitly marks pipeline completion."""
    return LIVE_STOP_MARKER in (raw_line or "")


def _fmt_alert(result: dict, use_color: bool = True) -> str:
    """Format a prediction result as a human-readable alert string."""
    risk  = result["risk_level"]
    prob  = result["failure_probability"]
    pid   = result["process_id"]
    lines = result.get("lines_seen", "?")
    pred  = "[WARN] FAILURE PREDICTED" if result["prediction"] else "[OK] OK"
    demo_suffix = "  | DEMO calibration" if result.get("demo_calibrated") else ""

    if use_color:
        c = _RISK_COLORS.get(risk, "")
        r = _RISK_COLORS["RESET"]
    else:
        c = r = ""

    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    header = (f"[{ts}] {c}[{risk}]{r}  {pred}  "
              f"| process: {pid}  "
              f"| prob: {prob:.0%}  "
              f"| lines seen: {lines}"
              f"{demo_suffix}")

    if result.get("explanation"):
        reasons = []
        for item in result["explanation"]:
            val = item["value"]
            reasons.append(
                f"    #{item['rank']}  {item['feature']:<30} "
                f"= {val:>8.4f}  {item['direction']}"
            )
        return header + "\n" + "\n".join(reasons)
    return header


def _apply_demo_risk_calibration(
    result: dict,
    feats: dict,
    *,
    rng: random.Random,
    demo_state: dict,
) -> dict:
    """
    Demo-only calibration for synthetic pipeline logs.

    The saved HDFS model can be overconfident on artificial logs that do not
    match the training distribution. This keeps the real model result in the
    payload, but uses a varied presentation score for synthetic demonstrations.
    The bag includes every risk level plus one level biased by visible log
    signals, so a run is diverse without following a fixed LOW-to-HIGH script.
    """
    calibrated = dict(result)
    raw_prob = float(result.get("failure_probability", 0.0))
    prob = _demo_failure_probability(
        feats,
        rng=rng,
        demo_state=demo_state,
    )

    calibrated["raw_model_failure_probability"] = raw_prob
    calibrated["failure_probability"] = round(prob, 4)
    calibrated["prediction"] = int(prob >= 0.5)
    calibrated["risk_level"] = (
        "HIGH" if prob >= RISK_HIGH
        else "MEDIUM" if prob >= RISK_MEDIUM
        else "LOW"
    )
    calibrated["demo_calibrated"] = True
    calibrated["explanation"] = _demo_explanation(feats)
    return calibrated


def _demo_signal_probability(feats: dict) -> float:
    """Return the baseline score implied by visible synthetic-log signals."""
    warn_count = int(feats.get("warn_count", 0) or 0)
    error_count = int(feats.get("error_count", 0) or 0)
    fatal_count = int(feats.get("fatal_count", 0) or 0)
    retry_count = int(feats.get("retry_count", 0) or 0)
    critical_kw_hits = int(feats.get("critical_kw_hits", 0) or 0)
    max_error_burst = int(feats.get("max_error_burst", 0) or 0)

    if fatal_count > 0 or critical_kw_hits > 0:
        return min(0.98, 0.86 + (0.03 * fatal_count) + (0.01 * error_count))
    if error_count >= 2 or max_error_burst >= 2:
        return min(0.82, 0.70 + (0.03 * error_count) + (0.02 * retry_count))
    if error_count == 1:
        return min(0.68, 0.56 + (0.03 * retry_count))
    if retry_count > 0 or warn_count >= 2:
        return min(0.58, 0.43 + (0.04 * retry_count) + (0.02 * warn_count))
    if warn_count == 1:
        return 0.34
    return 0.12


def _demo_failure_probability(
    feats: dict,
    *,
    rng: random.Random,
    demo_state: dict,
) -> float:
    """Return a varied, unique integer percentage for synthetic presentation runs."""
    risk_bag = demo_state.setdefault("risk_bag", [])
    used_percentages = demo_state.setdefault("used_percentages", set())
    last_risk = demo_state.get("last_risk")
    signal_probability = _demo_signal_probability(feats)
    signal_risk = (
        "HIGH" if signal_probability >= RISK_HIGH
        else "MEDIUM" if signal_probability >= RISK_MEDIUM
        else "LOW"
    )

    if not risk_bag:
        risk_bag.extend(["LOW", "MEDIUM", "HIGH", signal_risk])
        rng.shuffle(risk_bag)

    different_indices = [
        index for index, candidate in enumerate(risk_bag)
        if candidate != last_risk
    ]
    selected_index = rng.choice(different_indices or list(range(len(risk_bag))))
    risk = risk_bag.pop(selected_index)
    demo_state["last_risk"] = risk
    low, high = {
        "LOW": (12, 39),
        "MEDIUM": (40, 69),
        "HIGH": (70, 96),
    }[risk]
    available = [value for value in range(low, high + 1) if value not in used_percentages]
    if not available:
        used_percentages.difference_update(range(low, high + 1))
        available = list(range(low, high + 1))

    percentage = rng.choice(available)
    used_percentages.add(percentage)
    return percentage / 100


def _demo_explanation(feats: dict) -> list[dict]:
    """Build feature explanations that match the demo calibration."""
    feature_order = [
        ("fatal_count", "fatal log entries strongly increase risk"),
        ("error_count", "error log entries increase risk"),
        ("retry_count", "retries indicate instability"),
        ("warn_count", "warnings indicate early pressure"),
        ("max_error_burst", "consecutive errors increase risk"),
        ("failing_comp_hits", "known HDFS failure components increase risk"),
        ("total_lines", "more observations improve confidence"),
    ]

    explanations = []
    for feature, direction in feature_order:
        value = feats.get(feature, 0) or 0
        if value or feature in ("total_lines",):
            explanations.append({
                "rank": len(explanations) + 1,
                "feature": feature,
                "value": round(float(value), 4),
                "direction": direction if value else "no direct risk signal",
            })
        if len(explanations) >= 5:
            break

    if not explanations:
        explanations.append({
            "rank": 1,
            "feature": "error_count",
            "value": 0.0,
            "direction": "no errors observed",
        })
    return explanations


def _safe_ratio(value: float, scale: float) -> float:
    if scale <= 0:
        return 0.0
    return max(0.0, min(1.0, float(value) / float(scale)))


def _failure_prior(metrics: dict | None) -> float:
    metrics = metrics or {}
    tp = int(metrics.get("tp", 0) or 0)
    fn = int(metrics.get("fn", 0) or 0)
    tn = int(metrics.get("tn", 0) or 0)
    fp = int(metrics.get("fp", 0) or 0)
    total = tp + fn + tn + fp
    if total > 0:
        return max(0.05, min(0.35, (tp + fn) / total))
    return 0.10


def _signal_failure_probability(feats: dict) -> float:
    """
    Estimate risk directly from observable stream features.

    This is used to stabilize live-monitor predictions because the saved model
    can be overconfident on partial or shifted log streams.
    """
    warn_count = float(feats.get("warn_count", 0) or 0)
    error_count = float(feats.get("error_count", 0) or 0)
    fatal_count = float(feats.get("fatal_count", 0) or 0)
    retry_count = float(feats.get("retry_count", 0) or 0)
    exception_count = float(feats.get("exception_count", 0) or 0)
    max_error_burst = float(feats.get("max_error_burst", 0) or 0)
    error_rate = float(feats.get("error_rate", 0.0) or 0.0)
    warn_rate = float(feats.get("warn_rate", 0.0) or 0.0)
    critical_rate = float(feats.get("critical_rate", 0.0) or 0.0)
    critical_kw_hits = float(feats.get("critical_kw_hits", 0) or 0)
    failing_comp_hits = float(feats.get("failing_comp_hits", 0) or 0)
    total_lines = float(feats.get("total_lines", 0) or 0)
    strong_critical = fatal_count > 0 or (critical_kw_hits >= 2 and error_count >= 1)
    moderate_error_pattern = (
        error_count >= 2 and (retry_count >= 1 or max_error_burst >= 2)
    )

    weighted_signals = [
        (_safe_ratio(fatal_count, 1.0),        2.4),
        (_safe_ratio(critical_kw_hits, 1.0),   1.8),
        (_safe_ratio(error_count, 2.0),        1.7),
        (_safe_ratio(error_rate, 0.30),        1.7),
        (_safe_ratio(max_error_burst, 2.0),    1.0),
        (_safe_ratio(exception_count, 1.0),    1.0),
        (_safe_ratio(retry_count, 2.0),        0.9),
        (_safe_ratio(warn_count, 3.0),         0.6),
        (_safe_ratio(warn_rate, 0.40),         0.5),
        (_safe_ratio(critical_rate, 0.20),     1.3),
    ]

    signal_sum = sum(value * weight for value, weight in weighted_signals)
    weight_sum = sum(weight for _, weight in weighted_signals) or 1.0
    signal_prob = signal_sum / weight_sum

    if strong_critical:
        signal_prob = max(signal_prob, 0.90)
    elif error_count >= 3 or moderate_error_pattern:
        signal_prob = max(signal_prob, 0.62)
    elif error_count >= 1 or retry_count >= 2 or warn_count >= 3:
        signal_prob = max(signal_prob, 0.40)
    elif retry_count >= 1 or warn_count >= 1:
        signal_prob = max(signal_prob, 0.22)

    if (
        total_lines >= 5
        and error_count == 0
        and fatal_count == 0
        and retry_count == 0
        and critical_kw_hits == 0
        and exception_count == 0
    ):
        signal_prob = min(signal_prob, 0.18)

    if total_lines > 0 and error_count == 0 and fatal_count == 0 and warn_count == 0:
        signal_prob = min(signal_prob, 0.10)

    if error_count == 0 and fatal_count == 0 and total_lines > 0:
        safe_component_bonus = _safe_ratio(failing_comp_hits, max(total_lines, 1.0))
        signal_prob = max(0.0, signal_prob - (0.12 * safe_component_bonus))

    return max(0.02, min(0.98, signal_prob))


def _signal_explanation(feats: dict, raw_prob: float) -> list[dict]:
    feature_order = [
        ("fatal_count", "fatal events strongly increase risk"),
        ("error_count", "error events increase risk"),
        ("critical_kw_hits", "critical failure keywords increase risk"),
        ("retry_count", "retries indicate instability"),
        ("warn_count", "warnings indicate emerging pressure"),
        ("max_error_burst", "consecutive errors increase risk"),
        ("total_lines", "more lines increase confidence in the estimate"),
    ]

    explanations = [
        {
            "rank": 1,
            "feature": "raw_model_probability",
            "value": round(float(raw_prob), 4),
            "direction": "raw model output before stream calibration",
        }
    ]

    for feature, direction in feature_order:
        value = feats.get(feature, 0) or 0
        if value or feature == "total_lines":
            explanations.append({
                "rank": len(explanations) + 1,
                "feature": feature,
                "value": round(float(value), 4),
                "direction": direction if value else "no strong risk signal observed",
            })
        if len(explanations) >= 5:
            break

    return explanations


def _apply_stream_risk_calibration(
    result: dict,
    feats: dict,
    metrics: dict | None,
    predict_every: int,
) -> dict:
    """
    Calibrate live-monitor risk so partial logs do not default to HIGH.

    The returned probability is intended for operator-facing monitoring, not as
    a replacement for offline model evaluation.
    """
    calibrated = dict(result)
    raw_prob = float(result.get("failure_probability", 0.0))
    signal_prob = _signal_failure_probability(feats)
    prior = _failure_prior(metrics)

    total_lines = int(feats.get("total_lines", 0) or 0)
    error_count = int(feats.get("error_count", 0) or 0)
    fatal_count = int(feats.get("fatal_count", 0) or 0)
    critical_kw_hits = int(feats.get("critical_kw_hits", 0) or 0)
    max_error_burst = int(feats.get("max_error_burst", 0) or 0)
    retry_count = int(feats.get("retry_count", 0) or 0)
    strong_critical = fatal_count > 0 or (critical_kw_hits >= 2 and error_count >= 1)
    moderate_error_pattern = (
        error_count >= 2 and (retry_count >= 1 or max_error_burst >= 2)
    )

    confidence_window = max(int(predict_every) * 2, 10)
    line_confidence = min(1.0, total_lines / confidence_window)

    if fatal_count > 0:
        line_confidence = max(line_confidence, 0.90)
    elif strong_critical:
        line_confidence = max(line_confidence, 0.75)
    elif error_count >= 3 or moderate_error_pattern:
        line_confidence = max(line_confidence, 0.60)
    elif error_count >= 1:
        line_confidence = max(line_confidence, 0.45)
    elif retry_count >= 1 or critical_kw_hits >= 1:
        line_confidence = max(line_confidence, 0.35)

    blended_prob = (
        (0.15 * raw_prob) +
        (0.70 * signal_prob) +
        (0.15 * prior)
    )
    calibrated_prob = prior + (line_confidence * (blended_prob - prior))
    calibrated_prob = max(0.02, min(0.98, calibrated_prob))

    calibrated["raw_model_failure_probability"] = round(raw_prob, 4)
    calibrated["signal_failure_probability"] = round(signal_prob, 4)
    calibrated["failure_probability"] = round(calibrated_prob, 4)
    calibrated["prediction"] = int(calibrated_prob >= 0.5)
    calibrated["risk_level"] = (
        "HIGH" if calibrated_prob >= RISK_HIGH
        else "MEDIUM" if calibrated_prob >= RISK_MEDIUM
        else "LOW"
    )
    calibrated["explanation"] = _signal_explanation(feats, raw_prob)
    return calibrated


# ══════════════════════════════════════════════════════════════════
#  CORE MONITOR CLASS
# ══════════════════════════════════════════════════════════════════

class PipelineMonitor:
    """
    Real-time batch job failure prediction plugin.

    Parameters
    ----------
    source       : "hdfs" | "google"
    model_path   : path to trained model .pkl file
    print_alerts : whether to print predictions to stdout
    alert_callback : optional callable(result_dict) called for every prediction
    min_lines    : minimum lines before a prediction is triggered
                   (overrides PREDICT_EVERY_N_LINES for a single run)
    raw_model_risk:
                   use the raw model probability directly without calibration
    demo_risk_calibration:
                   use feature-based demo risk levels for synthetic live demos
    """

    def __init__(
        self,
        source:         str   = "hdfs",
        model_path:     str   = None,
        log_path:       str   = None,
        print_alerts:   bool  = True,
        alert_callback        = None,
        min_lines:      int   = None,
        raw_model_risk: bool  = False,
        demo_risk_calibration: bool = False,
    ):
        self.source         = source
        self.log_path       = log_path
        self.print_alerts   = print_alerts
        self.alert_callback = alert_callback
        self.raw_model_risk = raw_model_risk
        self.demo_risk_calibration = demo_risk_calibration
        if min_lines is not None:
            if int(min_lines) < 1:
                raise ValueError("min_lines must be >= 1")
            self.predict_every = int(min_lines)
        else:
            self.predict_every = PREDICT_EVERY_N_LINES

        if model_path is None:
            candidate = model_path_for_source(source)
            if os.path.exists(candidate):
                model_path = candidate
            # Backward-compatibility: legacy repo artifact was historically HDFS-trained.
            elif source == "hdfs" and os.path.exists(MODEL_SAVE_PATH):
                model_path = MODEL_SAVE_PATH
            else:
                raise FileNotFoundError(
                    f"No trained model found for source='{source}'.\n"
                    f"Expected: {candidate}\n"
                    f"Train it with: python main.py train --source {source}"
                )
        self.model_path = model_path

        self._extractor = StreamingExtractor(
            source=source,
            predict_every_n_lines=self.predict_every,
        )
        self._model     = PipelineMonitorModel.load(model_path, expected_source=source)
        self._results   = []                      # all predictions made so far
        self._demo_rng = random.Random()
        self._demo_state = {
            "risk_bag": [],
            "used_percentages": set(),
            "last_risk": None,
        }

        print(
            f"[OK] PipelineMonitor ready  [source={source}  "
            f"predict_every={self.predict_every} lines  "
            f"model={model_path}]"
        )
        if not self.raw_model_risk and not self.demo_risk_calibration:
            print("  Stream risk calibration enabled for live monitoring.")
        if self.demo_risk_calibration:
            print("  Demo risk calibration enabled for synthetic pipeline logs.")

    # ── Single-line ingestion ─────────────────────────────────────────────

    def ingest_line(self, raw_line: str) -> list[dict]:
        """
        Feed one log line into the monitor.
        Returns a (possibly empty) list of prediction dicts for processes
        that crossed the prediction threshold with this line.
        """
        ready_ids = self._extractor.ingest(raw_line)
        results   = []

        for pid in ready_ids:
            result = self._predict_process(pid)
            if result:
                results.append(result)
                self._results.append(result)
                if self.print_alerts:
                    print(_fmt_alert(result))
                if self.alert_callback:
                    self.alert_callback(result)

        return results

    # ── Batch file processing ─────────────────────────────────────────────

    def process_file(self, log_path: str,
                     show_progress: bool = True,
                     max_lines: int = None) -> list[dict]:
        """
        Process a complete log file. Returns all predictions made.
        Suitable for offline analysis of historical logs.

        max_lines limits how many lines are read, which is useful for demos
        against very large log files.
        """
        if not os.path.exists(log_path):
            print(f"File not found: {log_path}")
            return []

        print(f"\nProcessing: {log_path}")
        results = []
        line_count = 0
        batch_size = 10_000

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            for raw in f:
                if max_lines is not None and line_count >= max_lines:
                    break
                batch_results = self.ingest_line(raw)
                results.extend(batch_results)
                line_count += 1
                if show_progress and line_count % batch_size == 0:
                    print(f"  ... {line_count:,} lines  |  "
                          f"{len(results)} predictions so far", end="\r")

        if max_lines is not None and line_count >= max_lines:
            print(f"\n  Stopped after max_lines={max_lines:,}")

        # Final flush — predict for any process with data but below threshold
        print(f"\n  Flushing {len(self._extractor.get_all_process_ids())} "
              f"remaining buffers ...")
        results.extend(self._flush_all_processes())

        print(f"\n[OK] Processed {line_count:,} lines  |  "
              f"{len(results)} total predictions")
        return results

    # ── Live file tail mode ───────────────────────────────────────────────

    def start(self, log_path: str = None, poll_interval: float = 0.2) -> None:
        """
        Watch a growing log file and predict in real-time (blocks until
        interrupted with Ctrl-C or until a completion marker is written).

        Parameters
        ----------
        log_path      : path to the live log file being written by the pipeline
        poll_interval : seconds between file reads (default 0.2s)
        """
        log_path = log_path or self.log_path
        if not log_path:
            raise ValueError("Provide a log file path (log_path) to start monitoring.")

        print(f"\n{'='*58}")
        print(f"  PIPELINE MONITOR - LIVE MODE")
        print(f"  Watching : {log_path}")
        print(f"  Source   : {self.source.upper()}")
        print(f"  Press Ctrl-C to stop manually")
        print(f"{'='*58}\n")

        if not os.path.exists(log_path):
            # Wait for the file to be created
            print(f"Waiting for {log_path} to appear ...")
            while not os.path.exists(log_path):
                time.sleep(1)

        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            # Seek to end so we only read NEW lines
            f.seek(0, 2)
            try:
                while True:
                    raw = f.readline()
                    if raw:
                        if _is_live_stop_marker(raw):
                            print("\n  Pipeline completion marker detected.")
                            print("  Flushing remaining buffers and stopping monitor ...")
                            self._flush_all_processes()
                            self._print_session_summary()
                            return
                        self.ingest_line(raw)
                    else:
                        self._flush_timed_out_processes()
                        time.sleep(poll_interval)
            except KeyboardInterrupt:
                print("\n\n  Monitor stopped.")
                self._print_session_summary()

    # ── Internal helpers ──────────────────────────────────────────────────

    def _predict_process(self, process_id: str) -> dict | None:
        feats = self._extractor.get_features(process_id)
        if feats is None:
            return None

        result = self._model.predict(feats)
        if self.demo_risk_calibration:
            result = _apply_demo_risk_calibration(
                result,
                feats,
                rng=self._demo_rng,
                demo_state=self._demo_state,
            )
        elif not self.raw_model_risk:
            result = _apply_stream_risk_calibration(
                result,
                feats,
                self._model.metrics,
                self.predict_every,
            )
        result["lines_seen"] = len(
            self._extractor._buffers.get(process_id, [])
        )
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        return result

    def _flush_timed_out_processes(self) -> None:
        """Predict for processes that haven't received a new line recently."""
        now = time.time()
        for pid in list(self._extractor.get_all_process_ids()):
            last = self._extractor.get_last_seen(pid) or now
            if now - last > FLUSH_TIMEOUT_SECONDS:
                result = self._predict_process(pid)
                if result:
                    self._results.append(result)
                    if self.print_alerts:
                        print(_fmt_alert(result))
                    if self.alert_callback:
                        self.alert_callback(result)
                self._extractor.flush(pid)

    def _flush_all_processes(self) -> list[dict]:
        """Predict once for every buffered process and clear all buffers."""
        results = []
        for pid in list(self._extractor.get_all_process_ids()):
            result = self._predict_process(pid)
            if result:
                results.append(result)
                self._results.append(result)
                if self.print_alerts:
                    print(_fmt_alert(result))
                if self.alert_callback:
                    self.alert_callback(result)
            self._extractor.flush(pid)
        return results

    def _print_session_summary(self) -> None:
        if not self._results:
            print("  No predictions made.")
            return
        total    = len(self._results)
        failures = sum(1 for r in self._results if r["prediction"] == 1)
        high     = sum(1 for r in self._results if r["risk_level"] == "HIGH")
        medium   = sum(1 for r in self._results if r["risk_level"] == "MEDIUM")
        low      = sum(1 for r in self._results if r["risk_level"] == "LOW")
        print(f"\n{'='*45}")
        print(f"  SESSION SUMMARY")
        print(f"  Total predictions : {total}")
        print(f"  Predicted FAIL    : {failures}  ({failures/total*100:.1f}%)")
        print(f"  HIGH risk         : {high}")
        print(f"  MEDIUM risk       : {medium}")
        print(f"  LOW risk          : {low}")
        print(f"{'='*45}")

    # ── Results access ────────────────────────────────────────────────────

    def get_results(self) -> list[dict]:
        """Return all predictions made in this session."""
        return list(self._results)

    def save_results(self, path: str) -> None:
        """Save all predictions to a JSON file."""
        with open(path, "w") as f:
            json.dump(self._results, f, indent=2)
        print(f"  [OK] Results saved -> {path}")

    def get_high_risk_processes(self) -> list[dict]:
        return [r for r in self._results if r["risk_level"] == "HIGH"]

    # ── Alternative constructors ──────────────────────────────────────────

    @classmethod
    def from_file(cls, log_path: str, source: str = "hdfs", **kwargs):
        """Convenience constructor that remembers a default log path."""
        return cls(source=source, log_path=log_path, **kwargs)


# ══════════════════════════════════════════════════════════════════
#  STANDALONE USAGE
# ══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser(description="Pipeline Monitor Plugin")
    ap.add_argument("log_path",  help="Path to the log file to monitor")
    ap.add_argument("--source",  choices=["hdfs", "google"], default="hdfs")
    ap.add_argument("--live",    action="store_true",
                    help="Live tail mode (for running pipelines)")
    ap.add_argument("--output",  default=None,
                    help="Save prediction results to this JSON file")
    ap.add_argument("--max-lines", type=int, default=None,
                    help="Stop after this many input lines in batch mode")
    ap.add_argument("--min-lines", type=int, default=None,
                    help="Override how many lines are needed before each prediction")
    ap.add_argument("--raw-model-risk", action="store_true",
                    help="Use the raw model probability without stream calibration")
    ap.add_argument("--demo-risk-calibration", action="store_true",
                    help="Use feature-based demo risk levels for synthetic logs")
    args = ap.parse_args()

    monitor = PipelineMonitor(
        source=args.source,
        min_lines=args.min_lines,
        raw_model_risk=args.raw_model_risk,
        demo_risk_calibration=args.demo_risk_calibration,
    )

    if args.live:
        monitor.start(args.log_path)
    else:
        results = monitor.process_file(args.log_path, max_lines=args.max_lines)
        if args.output:
            monitor.save_results(args.output)
        # Print summary
        monitor._print_session_summary()
