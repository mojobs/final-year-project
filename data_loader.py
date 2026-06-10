"""
data_loader.py
--------------
Loads real-world log datasets and normalises them into a unified per-process
record list that feature_extractor.py can consume.

Supported datasets
------------------
  1. LogHub HDFS  – raw text log + anomaly_label.csv
       Groups log lines by block_id.
       Label: 1 = Anomaly (from anomaly_label.csv), 0 = Normal

  2. Google Cluster Trace 2019  – job_events.csv
       Groups events by job_id.
       Label: 1 = job ended with EVICT/FAIL/KILL/LOST, 0 = FINISH

Both loaders return a list of dicts with the schema expected by
feature_extractor.extract_features().

Unified intermediate schema (one dict per process/job)
-------------------------------------------------------
{
    "process_id"  : str,          # block_id or job_id
    "source"      : str,          # "hdfs" | "google"
    "label"       : int,          # 1 = failure, 0 = success
    "log_lines"   : list[dict],   # for HDFS: parsed line dicts
    "events"      : list[dict],   # for Google: event row dicts
}
"""

import os
import re
import csv
import ast
import io
import zipfile
from contextlib import contextmanager
from config import (
    HDFS_LOG_PATH, HDFS_LABEL_PATH,
    GOOGLE_JOB_EVENTS_PATH,
    ALIBABA_TRACE_ZIP_PATH, GOOGLE_2019_TRACE_ZIP_PATH,
    GOOGLE_FAIL_EVENTS, GOOGLE_SUCCESS_EVENT,
    GOOGLE_EVENT_TYPES,
)


# ══════════════════════════════════════════════════════════════════
#  HDFS LOADER
# ══════════════════════════════════════════════════════════════════

# Log line regex:
# 081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608 ...
_HDFS_LINE_RE = re.compile(
    r"^(?P<date>\d{6})\s+"
    r"(?P<time>\d{6})\s+"
    r"(?P<pid>\d+)\s+"
    r"(?P<level>\w+)\s+"
    r"(?P<component>\S+):\s+"
    r"(?P<content>.+)$"
)

# Block IDs appear inline in log content
_BLOCK_RE = re.compile(r"(blk_-?\d+)")


def _parse_hdfs_line(raw: str) -> dict | None:
    """Parse one HDFS log line. Returns None if the line doesn't match."""
    m = _HDFS_LINE_RE.match(raw.strip())
    if not m:
        return None
    d = m.groupdict()
    # Deduplicate while preserving order (some lines may mention the same block twice).
    d["block_ids"] = list(dict.fromkeys(_BLOCK_RE.findall(d["content"])))
    return d


def load_hdfs(
    log_path:   str = HDFS_LOG_PATH,
    label_path: str = HDFS_LABEL_PATH,
    max_lines:  int = None,
) -> list[dict]:
    """
    Load the LogHub HDFS dataset.

    Reads HDFS.log line by line, groups entries by block_id, then attaches
    the anomaly label from anomaly_label.csv.

    Parameters
    ----------
    log_path   : Path to HDFS.log
    label_path : Path to anomaly_label.csv (columns: BlockId, Label)
    max_lines  : Optional limit on lines read (useful during development)

    Returns
    -------
    List of process dicts in the unified intermediate schema.
    """
    _check_file(log_path,   "HDFS.log",          "https://github.com/logpai/loghub")
    _check_file(label_path, "anomaly_label.csv",  "https://github.com/logpai/loghub")

    # ── Load ground-truth labels ──────────────────────────────────────────
    labels: dict[str, int] = {}
    with open(label_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            block_id = row.get("BlockId", row.get("block_id", "")).strip()
            lbl_str  = row.get("Label",   row.get("label",   "Normal")).strip()
            labels[block_id] = 1 if lbl_str.lower() == "anomaly" else 0

    print(f"  Labels loaded: {len(labels)} blocks  "
          f"(anomalies: {sum(labels.values())}  "
          f"normal: {sum(1 for v in labels.values() if v == 0)})")

    # ── Parse log lines and group by block_id ─────────────────────────────
    blocks: dict[str, list] = {}   # block_id → [parsed_line, ...]

    print(f"  Reading {log_path} …")
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        for i, raw in enumerate(f):
            if max_lines and i >= max_lines:
                break
            parsed = _parse_hdfs_line(raw)
            if not parsed:
                continue
            for blk in parsed["block_ids"]:
                if blk not in blocks:
                    blocks[blk] = []
                blocks[blk].append(parsed)

    print(f"  Parsed {len(blocks)} unique blocks from log")

    # ── Build unified records (only blocks with known labels) ─────────────
    records = []
    unknown = 0
    for blk_id, lines in blocks.items():
        if blk_id not in labels:
            unknown += 1
            continue
        records.append({
            "process_id": blk_id,
            "source":     "hdfs",
            "label":      labels[blk_id],
            "log_lines":  lines,
            "events":     [],
        })

    print(f"  ✓ {len(records)} labeled records  "
          f"(failures: {sum(r['label'] for r in records)}  "
          f"skipped unlabeled: {unknown})")
    return records


# ══════════════════════════════════════════════════════════════════
#  GOOGLE CLUSTER LOADER
# ══════════════════════════════════════════════════════════════════

def load_google_cluster(
    job_events_path: str = GOOGLE_JOB_EVENTS_PATH,
    max_rows: int = None,
) -> list[dict]:
    """
    Load the Google Cluster Trace 2019 job_events CSV.

    Expected CSV columns (BigQuery export):
        time, missing_info, job_id, event_type, user,
        scheduling_class, job_name, logical_job_name

    Also supports the minimal schema used in research papers:
        time, job_id, event_type, scheduling_class, priority,
        cpu_request, memory_request, disk_space_request

    Label: 1 if the job's terminal event is EVICT/FAIL/KILL/LOST
           0 if FINISH
           rows with no terminal event are dropped.

    Parameters
    ----------
    job_events_path : Path to job_events.csv
    max_rows        : Optional row limit for development

    Returns
    -------
    List of process dicts in the unified intermediate schema.
    """
    _check_file(job_events_path, "job_events.csv",
                "https://research.google/tools/datasets/"
                "google-cluster-workload-traces-2019/")

    jobs: dict[str, dict] = {}   # job_id → aggregated job dict

    print(f"  Reading {job_events_path} …")
    with open(job_events_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cols   = {c.lower().strip() for c in (reader.fieldnames or [])}

        for i, row in enumerate(reader):
            if max_rows and i >= max_rows:
                break

            # Normalise column names (BigQuery exports can contain missing values).
            lrow = {
                k.lower().strip(): (v.strip() if isinstance(v, str) else "")
                for k, v in row.items()
                if k is not None
            }

            job_id = lrow.get("job_id", lrow.get("jobid", "")).strip()
            if not job_id:
                continue

            try:
                etype = int(float(lrow.get("event_type", lrow.get("type", -1))))
            except ValueError:
                continue

            ts = _safe_float(lrow.get("time", lrow.get("timestamp", 0)))

            event_dict = {
                "time":             ts,
                "event_type":       etype,
                "event_name":       GOOGLE_EVENT_TYPES.get(etype, "UNKNOWN"),
                "scheduling_class": _safe_int(lrow.get("scheduling_class", 0)),
                "priority":         _safe_int(lrow.get("priority", 0)),
                "cpu_request":      _safe_float(lrow.get("cpu_request",      lrow.get("cpu", 0))),
                "memory_request":   _safe_float(lrow.get("memory_request",   lrow.get("memory", 0))),
                "disk_request":     _safe_float(lrow.get("disk_space_request", lrow.get("disk", 0))),
                "user":             lrow.get("user", ""),
            }

            if job_id not in jobs:
                jobs[job_id] = {
                    "process_id":       job_id,
                    "source":           "google",
                    "label":            -1,
                    "log_lines":        [],
                    "events":           [],
                    "submit_time":      ts,
                    "terminal_event":   None,
                }

            jobs[job_id]["events"].append(event_dict)

            # Track terminal state
            if etype in GOOGLE_FAIL_EVENTS:
                jobs[job_id]["label"]          = 1
                jobs[job_id]["terminal_event"] = etype
            elif etype == GOOGLE_SUCCESS_EVENT and jobs[job_id]["label"] != 1:
                jobs[job_id]["label"]          = 0
                jobs[job_id]["terminal_event"] = etype

    # Keep only jobs with a determined label
    records = [j for j in jobs.values() if j["label"] != -1]
    failures = sum(r["label"] for r in records)
    print(f"  ✓ {len(records)} labeled jobs  "
          f"(failures: {failures}  "
          f"success: {len(records) - failures}  "
          f"skipped no-terminal: {len(jobs) - len(records)})")
    return records


# ============================================================================
# COMPRESSED PRODUCTION TRACE LOADERS
# ============================================================================

def load_alibaba_pai(
    archive_path: str = ALIBABA_TRACE_ZIP_PATH,
    max_rows: int | None = 20_000,
) -> list[dict]:
    """
    Load a bounded sample of the Alibaba PAI GPU trace directly from its ZIP.

    The archive is the official cluster-trace-gpu-v2020 format. A job with
    status=Terminated is successful; status=Failed is a failure. Running and
    Waiting jobs are incomplete and are skipped.

    pai_task_table.csv is joined to the selected jobs to provide task counts,
    requested CPU, memory and GPU values without unpacking the full archive.
    """
    _check_file(
        archive_path,
        "Alibaba PAI GPU trace ZIP",
        "https://github.com/alibaba/clusterdata/tree/master/cluster-trace-gpu-v2020",
    )
    print(f"  Reading compressed Alibaba PAI trace: {archive_path}")
    selected: dict[str, dict] = {}
    scanned = 0

    with zipfile.ZipFile(archive_path) as archive:
        with _zip_csv_reader(archive, "pai_job_table.csv") as reader:
            for row in reader:
                scanned += 1
                status = (row.get("status") or "").strip().lower()
                if status not in {"terminated", "failed"}:
                    continue
                job_name = (row.get("job_name") or "").strip()
                if not job_name:
                    continue
                label = int(status == "failed")
                selected[job_name] = {
                    "process_id": job_name,
                    "source": "alibaba",
                    "label": label,
                    "log_lines": [],
                    "events": [],
                    "job_start_time": _safe_float(row.get("start_time")),
                    "job_end_time": _safe_float(row.get("end_time")),
                    "job_status": status,
                    "user": row.get("user", ""),
                }
                if max_rows and len(selected) >= max_rows:
                    break

        with _zip_csv_reader(archive, "pai_task_table.csv") as reader:
            for row in reader:
                job_name = (row.get("job_name") or "").strip()
                if job_name not in selected:
                    continue
                status = (row.get("status") or "").strip().lower()
                selected[job_name]["events"].append(
                    {
                        "event_type": "task",
                        "task_name": row.get("task_name", ""),
                        "status": status,
                        "time": _safe_float(row.get("start_time")),
                        "end_time": _safe_float(row.get("end_time")),
                        "instance_count": _safe_float(row.get("inst_num")),
                        "cpu_request": _safe_float(row.get("plan_cpu")) / 100.0,
                        "memory_request": _safe_float(row.get("plan_mem")),
                        "gpu_request": _safe_float(row.get("plan_gpu")) / 100.0,
                        "gpu_type": row.get("gpu_type", ""),
                    }
                )

    records = list(selected.values())
    for record in records:
        if not record["events"]:
            record["events"].append(
                {
                    "event_type": "job",
                    "task_name": "job",
                    "status": record["job_status"],
                    "time": record["job_start_time"],
                    "end_time": record["job_end_time"],
                    "instance_count": 1.0,
                    "cpu_request": 0.0,
                    "memory_request": 0.0,
                    "gpu_request": 0.0,
                }
            )

    failures = sum(record["label"] for record in records)
    print(
        f"  [OK] Alibaba jobs: {len(records)} labeled "
        f"(failures: {failures}  success: {len(records) - failures}  "
        f"job rows scanned: {scanned})"
    )
    return records


def load_google2019_borg(
    archive_path: str = GOOGLE_2019_TRACE_ZIP_PATH,
    max_rows: int | None = 100_000,
) -> list[dict]:
    """
    Load a bounded sample from the compressed prejoined Borg trace CSV.

    This local archive already contains a `failed` label and joined resource
    request / usage fields. Each row becomes one labeled instance record.
    """
    _check_file(
        archive_path,
        "Google Borg traces ZIP",
        "https://github.com/google/cluster-data/blob/master/ClusterData2019.md",
    )
    print(f"  Reading compressed Google Borg trace: {archive_path}")
    records = []

    with zipfile.ZipFile(archive_path) as archive:
        with _zip_csv_reader(archive, "borg_traces_data.csv") as reader:
            for index, row in enumerate(reader):
                if max_rows and index >= max_rows:
                    break
                label = _safe_binary_label(row.get("failed"))
                if label is None:
                    continue
                collection_id = (row.get("collection_id") or "unknown").strip()
                instance_index = (row.get("instance_index") or str(index)).strip()
                request = _parse_resource_dict(row.get("resource_request"))
                sampled_usage = _parse_resource_dict(row.get("random_sample_usage"))
                event_name = (row.get("event") or "").strip().upper()
                start_time = _safe_float(row.get("start_time"))
                end_time = _safe_float(row.get("end_time"))
                records.append(
                    {
                        "process_id": f"{collection_id}:{instance_index}:{index}",
                        "group_id": collection_id,
                        "source": "google2019",
                        "label": label,
                        "log_lines": [],
                        "events": [
                            {
                                "event_type": event_name,
                                "event_name": event_name,
                                "status": "failed" if label else "finished",
                                "time": start_time,
                                "end_time": end_time,
                                "cpu_request": _safe_float(request.get("cpus")),
                                "memory_request": _safe_float(request.get("memory")),
                                "cpu_usage_sample": _safe_float(sampled_usage.get("cpus")),
                                "memory_usage_sample": _safe_float(sampled_usage.get("memory")),
                                "assigned_memory": _safe_float(row.get("assigned_memory")),
                                "page_cache_memory": _safe_float(row.get("page_cache_memory")),
                                "cycles_per_instruction": _safe_float(row.get("cycles_per_instruction")),
                                "memory_accesses_per_instruction": _safe_float(row.get("memory_accesses_per_instruction")),
                                "sample_rate": _safe_float(row.get("sample_rate")),
                                "scheduling_class": _safe_int(row.get("scheduling_class")),
                                "priority": _safe_int(row.get("priority")),
                                "cluster": _safe_int(row.get("cluster")),
                            }
                        ],
                    }
                )

    failures = sum(record["label"] for record in records)
    print(
        f"  [OK] Google Borg instances: {len(records)} labeled "
        f"(failures: {failures}  success: {len(records) - failures})"
    )
    return records


# ══════════════════════════════════════════════════════════════════
#  STREAMING LINE PARSER  (used by pipeline_monitor.py)
# ══════════════════════════════════════════════════════════════════

def parse_hdfs_line(raw_line: str) -> dict | None:
    """
    Public wrapper — parse a single raw HDFS log line.
    Returns a dict with keys: date, time, pid, level, component,
                              content, block_ids
    Returns None if the line does not match the HDFS format.
    """
    return _parse_hdfs_line(raw_line)


def parse_google_event_line(raw_line: str,
                             fieldnames: list[str] = None) -> dict | None:
    """
    Parse a single CSV line from a Google Cluster job_events file.
    `fieldnames` should be the CSV header row (list of column names).
    Returns None on parse failure.
    """
    try:
        if fieldnames is not None:
            reader = csv.DictReader([raw_line], fieldnames=fieldnames)
            row    = next(reader)
            # DictReader stores extra columns under key None; ignore those.
            lrow = {
                k.lower().strip(): (v.strip() if isinstance(v, str) else "")
                for k, v in row.items()
                if k is not None
            }
        else:
            # Heuristics for common schemas when streaming raw CSV lines.
            cols = next(csv.reader([raw_line]))
            cols = [(c.strip() if isinstance(c, str) else "") for c in cols]

            # BigQuery-like export (common ordering): at least 12 columns.
            if len(cols) >= 12:
                keys = [
                    "time", "missing_info", "job_id", "event_type",
                    "user", "scheduling_class", "job_name", "logical_job_name",
                    "priority", "cpu_request", "memory_request", "disk_space_request",
                ]
                lrow = {k: (cols[i] if i < len(cols) else "") for i, k in enumerate(keys)}
            # Minimal schema used in some research code (exactly 8 columns).
            elif len(cols) == 8:
                lrow = {
                    "time": cols[0],
                    "job_id": cols[1],
                    "event_type": cols[2],
                    "scheduling_class": cols[3],
                    "priority": cols[4],
                    "cpu_request": cols[5],
                    "memory_request": cols[6],
                    "disk_space_request": cols[7],
                }
            # Fallback: assume the first 8 columns follow the docstring ordering.
            else:
                keys = [
                    "time", "missing_info", "job_id", "event_type",
                    "user", "scheduling_class", "job_name", "logical_job_name",
                ]
                lrow = {k: (cols[i] if i < len(cols) else "") for i, k in enumerate(keys)}

        etype  = int(float(lrow.get("event_type", -1)))
        return {
            "time":             _safe_float(lrow.get("time", 0)),
            "job_id":           (lrow.get("job_id", "") or "").strip(),
            "event_type":       etype,
            "event_name":       GOOGLE_EVENT_TYPES.get(etype, "UNKNOWN"),
            "scheduling_class": _safe_int(lrow.get("scheduling_class", 0)),
            "priority":         _safe_int(lrow.get("priority", 0)),
            "cpu_request":      _safe_float(lrow.get("cpu_request", 0)),
            "memory_request":   _safe_float(lrow.get("memory_request", 0)),
            "disk_request":     _safe_float(lrow.get("disk_space_request", 0)),
        }
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════

def _check_file(path: str, name: str, url: str) -> None:
    if not os.path.exists(path):
        abs_path = os.path.abspath(path)
        raise FileNotFoundError(
            f"File not found: {path}\n"
            f"Download '{name}' from: {url}\n"
            f"Then place it at: {abs_path}"
        )


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val, default: int = 0) -> int:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _safe_binary_label(val) -> int | None:
    text = str(val or "").strip().lower()
    if text in {"1", "1.0", "true", "yes", "failed", "fail"}:
        return 1
    if text in {"0", "0.0", "false", "no", "finished", "success"}:
        return 0
    return None


def _parse_resource_dict(val) -> dict:
    if not val:
        return {}
    try:
        parsed = ast.literal_eval(str(val))
        return parsed if isinstance(parsed, dict) else {}
    except (ValueError, SyntaxError):
        return {}


@contextmanager
def _zip_csv_reader(archive: zipfile.ZipFile, member_name: str):
    try:
        raw = archive.open(member_name)
    except KeyError as exc:
        raise FileNotFoundError(
            f"Archive is missing required member: {member_name}"
        ) from exc
    text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace", newline="")
    try:
        yield csv.DictReader(text)
    finally:
        text.close()


# ══════════════════════════════════════════════════════════════════
#  COMBINED LOADER
# ══════════════════════════════════════════════════════════════════

def load_dataset(source: str, **kwargs) -> list[dict]:
    """
    Unified entry point.

    Parameters
    ----------
    source : "hdfs" | "google" | "alibaba" | "google2019" | "both"
    kwargs : Passed through to the individual loaders
             (e.g. max_lines=500_000 for HDFS during development)
    """
    source = (source or "").lower().strip()

    if source == "hdfs":
        allowed = {"log_path", "label_path", "max_lines"}
        return load_hdfs(**{k: v for k, v in kwargs.items() if k in allowed})
    elif source == "google":
        allowed = {"job_events_path", "max_rows"}
        return load_google_cluster(**{k: v for k, v in kwargs.items() if k in allowed})
    elif source == "alibaba":
        allowed = {"archive_path", "max_rows"}
        return load_alibaba_pai(**{k: v for k, v in kwargs.items() if k in allowed})
    elif source == "google2019":
        allowed = {"archive_path", "max_rows"}
        return load_google2019_borg(**{k: v for k, v in kwargs.items() if k in allowed})
    elif source == "both":
        records = []
        try:
            records += load_hdfs(**{k: v for k, v in kwargs.items()
                                    if k in ("log_path", "label_path", "max_lines")})
        except FileNotFoundError:
            print("  Skipping HDFS (file not found)")
        try:
            records += load_google_cluster(**{k: v for k, v in kwargs.items()
                                              if k in ("job_events_path", "max_rows")})
        except FileNotFoundError:
            print("  Skipping Google Cluster (file not found)")
        return records
    else:
        raise ValueError(
            f"Unknown source '{source}'. "
            "Use 'hdfs', 'google', 'alibaba', 'google2019', or 'both'."
        )


if __name__ == "__main__":
    # Quick sanity check — prints dataset stats without training
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--source",
        choices=["hdfs", "google", "alibaba", "google2019", "both"],
        default="hdfs",
    )
    ap.add_argument("--max_lines", type=int, default=200_000,
                    help="Max HDFS lines to read (for quick testing)")
    ap.add_argument("--max_rows", type=int, default=5_000,
                    help="Max compressed trace rows/jobs to read")
    args = ap.parse_args()

    records = load_dataset(
        args.source,
        max_lines=args.max_lines,
        max_rows=args.max_rows,
    )
    print(f"\nLoaded {len(records)} records from source='{args.source}'")
    if records:
        print(f"Sample process_id : {records[0]['process_id']}")
        print(f"Source            : {records[0]['source']}")
        print(f"Label             : {records[0]['label']}")
        if records[0]["log_lines"]:
            print(f"First log line    : {records[0]['log_lines'][0]}")
        if records[0]["events"]:
            print(f"First event       : {records[0]['events'][0]}")
