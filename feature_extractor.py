"""
feature_extractor.py
--------------------
Converts the unified intermediate records (from data_loader.py) into a flat
feature vector per process/job that XGBoost can train on and predict from.

Works for BOTH sources:
  • HDFS  – derives features from log line content, log levels, components,
             error keywords, timing patterns, and block event sequences
  • Google Cluster – derives features from job event metadata, resource
             requests, scheduling attributes, and event sequence patterns

Also works INCREMENTALLY: the StreamingExtractor class accepts one log line
at a time (for real-time pipeline monitoring) and produces a feature snapshot
at any point during log accumulation — this is the core of the plugin.
"""

import re
import math
import time
from collections import Counter, defaultdict
from datetime import datetime
from typing import Optional


# ══════════════════════════════════════════════════════════════════
#  KEYWORD SIGNALS
# ══════════════════════════════════════════════════════════════════

ERROR_KEYWORDS = [
    "exception", "error", "failed", "failure", "timeout", "refused",
    "denied", "corrupt", "lost", "unreachable", "killed", "oom",
    "outofmemory", "diskfull", "no space", "connection reset",
    "nullpointer", "classnotfound", "ioexception",
]

WARNING_KEYWORDS = [
    "warn", "retry", "slow", "delay", "high latency", "threshold",
    "deprecated", "fallback", "reconnect", "queue full",
]

CRITICAL_KEYWORDS = [
    "fatal", "critical", "abort", "terminate", "crash", "panic",
    "unrecoverable", "data loss", "corruption",
]

# HDFS-specific component names that appear in failing blocks
FAILING_COMPONENTS = [
    "DataNode", "NameNode", "BlockManager", "DataXceiver",
    "DFSClient", "FSNamesystem",
]


# ══════════════════════════════════════════════════════════════════
#  HDFS FEATURE EXTRACTION
# ══════════════════════════════════════════════════════════════════

def _extract_hdfs_features(lines: list[dict]) -> dict:
    """
    Extract features from a list of parsed HDFS log line dicts.
    Each dict has: date, time, pid, level, component, content, block_ids
    """
    if not lines:
        return _empty_features()

    level_counts = Counter(l.get("level", "OTHER") for l in lines)
    components   = Counter(l.get("component", "") for l in lines)
    pids         = {l.get("pid", "") for l in lines}

    # Keyword scanning
    error_kw_hits    = 0
    warning_kw_hits  = 0
    critical_kw_hits = 0
    retry_count      = 0
    exception_count  = 0

    for line in lines:
        content = line.get("content", "").lower()

        for kw in ERROR_KEYWORDS:
            if kw in content:
                error_kw_hits += 1
                break
        for kw in WARNING_KEYWORDS:
            if kw in content:
                warning_kw_hits += 1
                break
        for kw in CRITICAL_KEYWORDS:
            if kw in content:
                critical_kw_hits += 1
                break
        if "retry" in content or "retrying" in content:
            retry_count += 1
        if "exception" in content:
            exception_count += 1

    # Consecutive error burst
    max_error_burst = _max_consecutive_errors(lines)

    # Level-based counts
    info_count     = level_counts.get("INFO",  0)
    warn_count     = level_counts.get("WARN",  0) + level_counts.get("WARNING", 0)
    error_count    = level_counts.get("ERROR", 0)
    debug_count    = level_counts.get("DEBUG", 0)
    fatal_count    = level_counts.get("FATAL", 0)
    total_lines    = len(lines)

    # Component diversity
    unique_components = len(components)
    failing_comp_hits = sum(
        1 for line in lines
        if any(fc in line.get("component", "") for fc in FAILING_COMPONENTS)
    )

    # Process / thread diversity (more PIDs = more parallel activity)
    unique_pids = len(pids)

    # Timing
    duration_s, hour_of_day, is_weekend = _hdfs_timing(lines)

    # Rates (guard zero division)
    tl = max(total_lines, 1)
    error_rate    = round(error_count    / tl, 4)
    warn_rate     = round(warn_count     / tl, 4)
    critical_rate = round(fatal_count    / tl, 4)
    error_kw_rate = round(error_kw_hits  / tl, 4)

    return {
        # Log level counts
        "total_lines":        total_lines,
        "info_count":         info_count,
        "warn_count":         warn_count,
        "error_count":        error_count,
        "debug_count":        debug_count,
        "fatal_count":        fatal_count,
        # Rates
        "error_rate":         error_rate,
        "warn_rate":          warn_rate,
        "critical_rate":      critical_rate,
        "error_kw_rate":      error_kw_rate,
        # Keyword signals
        "error_kw_hits":      error_kw_hits,
        "warning_kw_hits":    warning_kw_hits,
        "critical_kw_hits":   critical_kw_hits,
        "retry_count":        retry_count,
        "exception_count":    exception_count,
        # Structure
        "unique_components":  unique_components,
        "failing_comp_hits":  failing_comp_hits,
        "unique_pids":        unique_pids,
        "max_error_burst":    max_error_burst,
        # Temporal
        "duration_s":         round(duration_s, 2),
        "hour_of_day":        hour_of_day,
        "is_weekend":         is_weekend,
        # Resource signals (not available in raw HDFS log — zero-filled)
        "cpu_request":        0.0,
        "memory_request":     0.0,
        "disk_request":       0.0,
        "priority":           0,
        "scheduling_class":   0,
        "task_count":         0,
        "failed_task_count":  0,
        "event_count":        total_lines,
        # Derived
        "err_x_retry":        error_count * retry_count,
        "has_exception":      int(exception_count > 0),
        "has_fatal":          int(fatal_count > 0),
        "has_retry":          int(retry_count > 0),
    }


def _max_consecutive_errors(lines: list[dict]) -> int:
    max_run = 0
    run = 0
    for l in lines:
        lvl = l.get("level", "")
        if lvl in ("ERROR", "FATAL"):
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return max_run


def _hdfs_timing(lines: list[dict]) -> tuple[float, int, int]:
    """Parse HDFS timestamps (date=081109, time=203518) and return duration, hour, is_weekend."""
    timestamps = []
    for line in lines:
        try:
            dt_str = line["date"] + line["time"]
            ts = datetime.strptime(dt_str, "%y%m%d%H%M%S")
            timestamps.append(ts)
        except (ValueError, KeyError):
            continue

    timestamps.sort()

    if len(timestamps) >= 2:
        duration_s = (timestamps[-1] - timestamps[0]).total_seconds()
    else:
        duration_s = 0.0

    if timestamps:
        hour_of_day = timestamps[0].hour
        is_weekend  = int(timestamps[0].weekday() >= 5)
    else:
        hour_of_day = 0
        is_weekend  = 0

    return max(0.0, duration_s), hour_of_day, is_weekend


# ══════════════════════════════════════════════════════════════════
#  GOOGLE CLUSTER FEATURE EXTRACTION
# ══════════════════════════════════════════════════════════════════

def _extract_google_features(events: list[dict]) -> dict:
    """
    Extract features from a list of Google Cluster event dicts.
    Each dict has: time, event_type, event_name, scheduling_class,
                   priority, cpu_request, memory_request, disk_request
    """
    if not events:
        return _empty_features()

    from config import GOOGLE_FAIL_EVENTS

    event_types   = [e.get("event_type", -1) for e in events]
    event_counter = Counter(event_types)

    # Terminal event analysis
    fail_event_count = sum(1 for et in event_types if et in GOOGLE_FAIL_EVENTS)
    evict_count      = event_counter.get(2, 0)
    fail_count       = event_counter.get(3, 0)
    kill_count       = event_counter.get(5, 0)
    lost_count       = event_counter.get(6, 0)

    # Resource requests (from first SUBMIT event, or max across all events)
    cpu_vals  = [e.get("cpu_request",    0.0) for e in events if e.get("cpu_request",    0) > 0]
    mem_vals  = [e.get("memory_request", 0.0) for e in events if e.get("memory_request", 0) > 0]
    disk_vals = [e.get("disk_request",   0.0) for e in events if e.get("disk_request",   0) > 0]

    cpu_request  = max(cpu_vals,  default=0.0)
    mem_request  = max(mem_vals,  default=0.0)
    disk_request = max(disk_vals, default=0.0)

    # Schedule metadata (from first event)
    first = events[0]
    sched_class = first.get("scheduling_class", 0)
    priority    = first.get("priority", 0)

    # Timing
    times = [e.get("time", 0) for e in events if e.get("time", 0) > 0]
    if len(times) >= 2:
        duration_s = max(times) - min(times)
        # Convert microseconds to seconds if needed
        if duration_s > 1e9:
            duration_s /= 1e6
    else:
        duration_s = 0.0

    # Retry heuristic: SCHEDULE events after a FAIL/EVICT indicate retries
    seq = [e.get("event_type") for e in sorted(events, key=lambda x: x.get("time", 0))]
    retry_count = _count_reschedules(seq)

    # Compute an "error burst" analog: consecutive non-SUBMIT non-SCHEDULE events
    error_burst = _google_error_burst(seq)

    total_events = len(events)
    tl = max(total_events, 1)

    # OOM/timeout signals from resource over-utilisation heuristics
    oom_signal     = int(mem_request > 0.80)
    timeout_signal = int(duration_s > 14400)

    return {
        "total_lines":        total_events,
        "info_count":         event_counter.get(0, 0) + event_counter.get(1, 0),  # SUBMIT+SCHEDULE
        "warn_count":         event_counter.get(7, 0) + event_counter.get(8, 0),  # UPDATE events
        "error_count":        fail_event_count,
        "debug_count":        0,
        "fatal_count":        kill_count + lost_count,
        "error_rate":         round(fail_event_count / tl, 4),
        "warn_rate":          round((event_counter.get(7, 0) + event_counter.get(8, 0)) / tl, 4),
        "critical_rate":      round((kill_count + lost_count) / tl, 4),
        "error_kw_rate":      round(fail_event_count / tl, 4),
        "error_kw_hits":      fail_event_count,
        "warning_kw_hits":    event_counter.get(7, 0),
        "critical_kw_hits":   kill_count + lost_count,
        "retry_count":        retry_count,
        "exception_count":    evict_count + fail_count,
        "unique_components":  len(set(e.get("user", "") for e in events)),
        "failing_comp_hits":  fail_event_count,
        "unique_pids":        1,
        "max_error_burst":    error_burst,
        "duration_s":         round(duration_s, 2),
        "hour_of_day":        0,
        "is_weekend":         0,
        "cpu_request":        round(cpu_request,  4),
        "memory_request":     round(mem_request,  4),
        "disk_request":       round(disk_request, 4),
        "priority":           priority,
        "scheduling_class":   sched_class,
        "task_count":         total_events,
        "failed_task_count":  fail_event_count,
        "event_count":        total_events,
        "err_x_retry":        fail_event_count * retry_count,
        "has_exception":      int(evict_count + fail_count > 0),
        "has_fatal":          int(kill_count + lost_count > 0),
        "has_retry":          int(retry_count > 0),
        # OOM/timeout signals
        "oom_signal":         oom_signal,
        "timeout_signal":     timeout_signal,
    }


def _count_reschedules(event_seq: list[int]) -> int:
    """Count how many times a SCHEDULE follows a FAIL/EVICT (= retry)."""
    from config import GOOGLE_FAIL_EVENTS
    count = 0
    for i in range(1, len(event_seq)):
        if event_seq[i-1] in GOOGLE_FAIL_EVENTS and event_seq[i] == 1:
            count += 1
    return count


def _google_error_burst(seq: list[int]) -> int:
    from config import GOOGLE_FAIL_EVENTS
    max_run = run = 0
    for et in seq:
        if et in GOOGLE_FAIL_EVENTS:
            run += 1
            max_run = max(max_run, run)
        else:
            run = 0
    return max_run


# ============================================================================
# COMPRESSED PRODUCTION TRACE FEATURE EXTRACTION
# ============================================================================

def _extract_alibaba_features(record: dict) -> dict:
    events = record.get("events", [])
    if not events:
        return _empty_features()

    instance_count = sum(float(event.get("instance_count", 0.0) or 0.0) for event in events)
    start_time = float(record.get("job_start_time", 0.0) or 0.0)
    task_start_times = [
        float(event.get("time", 0.0) or 0.0)
        for event in events
        if float(event.get("time", 0.0) or 0.0) > 0
    ]
    # Use only task starts visible during execution, never terminal end_time.
    duration_s = (
        max(0.0, max(task_start_times) - start_time)
        if start_time and task_start_times
        else 0.0
    )
    scheduling_delay_s = (
        max(0.0, min(task_start_times) - start_time)
        if start_time and task_start_times
        else 0.0
    )
    cpu_request = max((float(event.get("cpu_request", 0.0) or 0.0) for event in events), default=0.0)
    memory_request = max((float(event.get("memory_request", 0.0) or 0.0) for event in events), default=0.0)
    gpu_request = max((float(event.get("gpu_request", 0.0) or 0.0) for event in events), default=0.0)
    unique_tasks = len({event.get("task_name", "") for event in events})
    total_events = len(events)

    features = _empty_features()
    features.update(
        {
            "total_lines": total_events,
            "info_count": total_events,
            "unique_components": unique_tasks,
            "unique_pids": int(instance_count),
            "duration_s": round(duration_s, 2),
            "cpu_request": round(cpu_request, 4),
            "memory_request": round(memory_request, 4),
            "gpu_request": round(gpu_request, 4),
            "task_count": total_events,
            "instance_count": round(instance_count, 4),
            "event_count": total_events,
            "scheduling_delay_s": round(scheduling_delay_s, 4),
            "timeout_signal": int(duration_s > 14_400),
        }
    )
    return features


def _extract_google2019_features(record: dict) -> dict:
    events = record.get("events", [])
    if not events:
        return _empty_features()

    event = events[0]
    start_time = float(event.get("time", 0.0) or 0.0)
    cpu_request = float(event.get("cpu_request", 0.0) or 0.0)
    memory_request = float(event.get("memory_request", 0.0) or 0.0)
    cpu_usage_sample = float(event.get("cpu_usage_sample", 0.0) or 0.0)
    memory_usage_sample = float(event.get("memory_usage_sample", 0.0) or 0.0)
    assigned_memory = float(event.get("assigned_memory", 0.0) or 0.0)
    page_cache_memory = float(event.get("page_cache_memory", 0.0) or 0.0)

    features = _empty_features()
    features.update(
        {
            "total_lines": 1,
            "info_count": 1,
            "unique_components": 1,
            "unique_pids": 1,
            # No terminal duration feature: end_time is not known during a run.
            "duration_s": 0.0,
            "cpu_request": round(cpu_request, 6),
            "memory_request": round(memory_request, 6),
            "priority": int(event.get("priority", 0) or 0),
            "scheduling_class": int(event.get("scheduling_class", 0) or 0),
            "task_count": 1,
            "event_count": 1,
            "oom_signal": int(memory_usage_sample > max(memory_request, assigned_memory, 0.0)),
            "cpu_usage_sample": round(cpu_usage_sample, 6),
            "memory_usage_sample": round(memory_usage_sample, 6),
            "assigned_memory": round(assigned_memory, 6),
            "page_cache_memory": round(page_cache_memory, 6),
            "cycles_per_instruction": round(float(event.get("cycles_per_instruction", 0.0) or 0.0), 6),
            "memory_accesses_per_instruction": round(float(event.get("memory_accesses_per_instruction", 0.0) or 0.0), 6),
            "sample_rate": round(float(event.get("sample_rate", 0.0) or 0.0), 6),
            "cluster": int(event.get("cluster", 0) or 0),
        }
    )
    return features


# ══════════════════════════════════════════════════════════════════
#  UNIFIED EXTRACTOR
# ══════════════════════════════════════════════════════════════════

def extract_features(record: dict) -> dict:
    """
    Main entry point. Accepts a unified intermediate record and dispatches
    to the correct extractor based on record["source"].

    Returns a flat feature dict with a consistent set of keys
    suitable for pandas / XGBoost.
    """
    source = record.get("source", "hdfs")
    if source == "hdfs":
        feats = _extract_hdfs_features(record.get("log_lines", []))
    elif source == "alibaba":
        feats = _extract_alibaba_features(record)
    elif source == "google2019":
        feats = _extract_google2019_features(record)
    else:
        feats = _extract_google_features(record.get("events", []))

    # Add log1p transforms for skewed numeric features
    for col in LOG_TRANSFORM_COLS:
        if col in feats:
            feats[f"log1p_{col}"] = round(math.log1p(max(0, feats[col])), 6)

    feats["process_id"] = record.get("process_id", "unknown")
    feats["group_id"]   = record.get("group_id", record.get("process_id", "unknown"))
    feats["source"]     = source
    feats["label"]      = record.get("label", -1)
    return feats


def extract_early_warning_features(
    records: list[dict],
    fractions: tuple[float, ...] = (0.25, 0.50, 0.75, 1.0),
) -> list[dict]:
    """
    Build training samples from partial process/job histories.

    A failed job should be learnable before the final crash line appears, so
    each record is converted into prefix snapshots. For example, a block with
    100 log lines becomes samples using the first 25, 50, 75, and 100 lines.
    Each snapshot keeps the original label.
    """
    features = []

    for record in records:
        source = record.get("source", "hdfs")
        sequence_key = "log_lines" if source == "hdfs" else "events"
        sequence = record.get(sequence_key, [])

        if not sequence:
            features.append(extract_features(record))
            continue

        total = len(sequence)
        seen_sizes: set[int] = set()
        for fraction in fractions:
            fraction = max(0.0, min(1.0, float(fraction)))
            size = max(1, min(total, math.ceil(total * fraction)))
            if size in seen_sizes:
                continue
            seen_sizes.add(size)

            snapshot = dict(record)
            snapshot[sequence_key] = sequence[:size]
            snapshot["group_id"] = record.get("process_id", "unknown")
            snapshot["process_id"] = (
                f"{record.get('process_id', 'unknown')}@{size}_of_{total}"
            )
            features.append(extract_features(snapshot))

    return features


def extract_all(
    records: list[dict],
    early_warning: bool = False,
    fractions: tuple[float, ...] = (0.25, 0.50, 0.75, 1.0),
) -> list[dict]:
    """Extract features from every record in a list."""
    if early_warning:
        features = extract_early_warning_features(records, fractions=fractions)
        mode = "early-warning snapshots"
    else:
        features = [extract_features(r) for r in records]
        mode = "full histories"

    labeled  = [f for f in features if f.get("label", -1) != -1]
    print(f"[OK] Features extracted: {len(labeled)} labeled samples  "
          f"({mode}; "
          f"failures: {sum(1 for f in labeled if f['label'] == 1)}  "
          f"successes: {sum(1 for f in labeled if f['label'] == 0)})")
    return labeled


# ══════════════════════════════════════════════════════════════════
#  STREAMING EXTRACTOR  – used by pipeline_monitor.py
# ══════════════════════════════════════════════════════════════════

class StreamingExtractor:
    """
    Maintains per-process state during live log streaming.

    Usage
    -----
        extractor = StreamingExtractor(source="hdfs")

        for raw_line in live_log_stream:
            process_ids = extractor.ingest(raw_line)
            # Returns list of process_ids that now have enough data to predict

        snapshot = extractor.get_features("blk_1234567890")
    """

    def __init__(self, source: str = "hdfs", predict_every_n_lines: int | None = None):
        self.source = source
        self._buffers: dict[str, list] = defaultdict(list)  # process_id → [entries]
        self._line_counts: dict[str, int] = defaultdict(int)
        self._last_seen: dict[str, float] = defaultdict(float)

        # Allow a per-instance threshold override (useful for monitor runs).
        if predict_every_n_lines is None:
            from config import PREDICT_EVERY_N_LINES as _DEFAULT_PREDICT_EVERY_N_LINES
            predict_every_n_lines = _DEFAULT_PREDICT_EVERY_N_LINES
        self.predict_every_n_lines = max(1, int(predict_every_n_lines))

    def ingest(self, raw_line: str) -> list[str]:
        """
        Ingest one raw log line.
        Returns a list of process_ids that crossed the PREDICT_EVERY_N_LINES
        threshold with this line (i.e. ready for prediction).
        """
        from data_loader import parse_hdfs_line, parse_google_event_line

        ready = []

        if self.source == "hdfs":
            parsed = parse_hdfs_line(raw_line)
            if not parsed:
                return []
            for blk_id in parsed.get("block_ids", []):
                self._buffers[blk_id].append(parsed)
                self._line_counts[blk_id] += 1
                self._last_seen[blk_id] = time.time()
                if self._line_counts[blk_id] % self.predict_every_n_lines == 0:
                    ready.append(blk_id)

        elif self.source == "google":
            parsed = parse_google_event_line(raw_line)
            if not parsed or not parsed.get("job_id"):
                return []
            jid = parsed["job_id"]
            self._buffers[jid].append(parsed)
            self._line_counts[jid] += 1
            self._last_seen[jid] = time.time()
            if self._line_counts[jid] % self.predict_every_n_lines == 0:
                ready.append(jid)

        return ready

    def get_features(self, process_id: str) -> dict | None:
        """
        Return the current feature snapshot for a process_id.
        Returns None if no data has been ingested for that id.
        """
        if process_id not in self._buffers:
            return None

        if self.source == "hdfs":
            feats = _extract_hdfs_features(self._buffers[process_id])
        else:
            feats = _extract_google_features(self._buffers[process_id])

        for col in LOG_TRANSFORM_COLS:
            if col in feats:
                feats[f"log1p_{col}"] = round(math.log1p(max(0, feats[col])), 6)

        feats["process_id"] = process_id
        feats["group_id"]   = process_id
        feats["source"]     = self.source
        return feats

    def get_all_process_ids(self) -> list[str]:
        return list(self._buffers.keys())

    def get_last_seen(self, process_id: str) -> float:
        """Last ingest time (seconds since epoch) for a process_id, or 0.0 if unknown."""
        return float(self._last_seen.get(process_id, 0.0))

    def flush(self, process_id: str) -> None:
        """Remove a process from the buffer (call after prediction is delivered)."""
        self._buffers.pop(process_id, None)
        self._line_counts.pop(process_id, None)
        self._last_seen.pop(process_id, None)


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

LOG_TRANSFORM_COLS = [
    "total_lines", "error_count", "warn_count", "fatal_count",
    "error_kw_hits", "retry_count", "exception_count",
    "duration_s", "err_x_retry", "max_error_burst",
]


def _empty_features() -> dict:
    """Return a zeroed-out feature dict for processes with no parseable data."""
    return {
        "total_lines": 0, "info_count": 0, "warn_count": 0,
        "error_count": 0, "debug_count": 0, "fatal_count": 0,
        "error_rate": 0.0, "warn_rate": 0.0, "critical_rate": 0.0,
        "error_kw_rate": 0.0, "error_kw_hits": 0, "warning_kw_hits": 0,
        "critical_kw_hits": 0, "retry_count": 0, "exception_count": 0,
        "unique_components": 0, "failing_comp_hits": 0, "unique_pids": 0,
        "max_error_burst": 0, "duration_s": 0.0, "hour_of_day": 0,
        "is_weekend": 0, "cpu_request": 0.0, "memory_request": 0.0,
        "disk_request": 0.0, "priority": 0, "scheduling_class": 0,
        "task_count": 0, "failed_task_count": 0, "event_count": 0,
        "err_x_retry": 0, "has_exception": 0, "has_fatal": 0,
        "has_retry": 0, "oom_signal": 0, "timeout_signal": 0,
        "gpu_request": 0.0, "instance_count": 0.0,
        "failed_instance_count": 0.0, "scheduling_delay_s": 0.0,
        "cpu_usage_avg": 0.0, "cpu_usage_max": 0.0,
        "memory_usage_avg": 0.0, "memory_usage_max": 0.0,
        "cpu_usage_sample": 0.0, "memory_usage_sample": 0.0,
        "assigned_memory": 0.0, "page_cache_memory": 0.0,
        "cycles_per_instruction": 0.0,
        "memory_accesses_per_instruction": 0.0, "sample_rate": 0.0,
        "cluster": 0,
    }
