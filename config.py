"""
config.py
---------
Central configuration for the pipeline failure prediction plugin.
Edit the DATA section to point to your downloaded dataset files.
"""

import os
from pathlib import Path

# ═══════════════════════════════════════════════════════════════
#  DATA PATHS  ← Edit these to match your downloaded files
# ═══════════════════════════════════════════════════════════════

# -- LogHub HDFS Dataset --
# Download from: https://github.com/logpai/loghub  (HDFS folder)
# You need two files:
#   HDFS.log            – the raw text log (~1.5 GB)
#   anomaly_label.csv   – ground-truth labels per block_id
PROJECT_ROOT = Path(__file__).resolve().parent
MODELS_DIR   = PROJECT_ROOT / "models"
OUTPUTS_DIR  = str(PROJECT_ROOT / "outputs")


def _first_existing(*paths: Path) -> Path:
    """Return the first path that exists; otherwise return the first candidate."""
    for p in paths:
        try:
            if p.exists():
                return p
        except OSError:
            # Ignore invalid paths and keep searching.
            continue
    return paths[0]


_env_hdfs_dir = os.environ.get("HDFS_DATA_DIR")
HDFS_DATA_DIR = Path(_env_hdfs_dir) if _env_hdfs_dir else (PROJECT_ROOT / "data")

_env_hdfs_log = os.environ.get("HDFS_LOG_PATH")
HDFS_LOG_PATH = str(
    Path(_env_hdfs_log) if _env_hdfs_log else _first_existing(
        HDFS_DATA_DIR / "HDFS.log",
        PROJECT_ROOT / "data" / "HDFS.log",
    )
)

_env_hdfs_label = os.environ.get("HDFS_LABEL_PATH")
HDFS_LABEL_PATH = str(
    Path(_env_hdfs_label) if _env_hdfs_label else _first_existing(
        HDFS_DATA_DIR / "anomaly_label.csv",
        HDFS_DATA_DIR / "preprocessed" / "anomaly_label.csv",
        PROJECT_ROOT / "data" / "anomaly_label.csv",
        PROJECT_ROOT / "data" / "preprocessed" / "anomaly_label.csv",
    )
)

# -- Google Cluster Trace 2019 --
# Download from BigQuery: research.google/tools/datasets/google-cluster-workload-traces-2019
# Export the job_events table as CSV
GOOGLE_JOB_EVENTS_PATH = os.environ.get(
    "GOOGLE_JOB_EVENTS_PATH",
    str(PROJECT_ROOT / "data" / "job_events.csv"),
)

# -- Compressed production traces added for source-specific training --
ALIBABA_TRACE_ZIP_PATH = os.environ.get(
    "ALIBABA_TRACE_ZIP_PATH",
    str(PROJECT_ROOT / "alibaba.zip"),
)

GOOGLE_2019_TRACE_ZIP_PATH = os.environ.get(
    "GOOGLE_2019_TRACE_ZIP_PATH",
    str(PROJECT_ROOT / "google cluster trace.zip"),
)

# ═══════════════════════════════════════════════════════════════
#  MODEL SETTINGS
# ═══════════════════════════════════════════════════════════════

MODEL_SAVE_PATH = str(MODELS_DIR / "xgboost_pipeline_monitor.pkl")
ETL_MODEL_SAVE_PATH = str(MODELS_DIR / "xgboost_pipeline_monitor_etl.pkl")
MODEL_SAVE_PATHS = {
    "hdfs":       str(MODELS_DIR / "xgboost_pipeline_monitor_hdfs.pkl"),
    "google":     str(MODELS_DIR / "xgboost_pipeline_monitor_google.pkl"),
    "alibaba":    str(MODELS_DIR / "xgboost_pipeline_monitor_alibaba.pkl"),
    "google2019": str(MODELS_DIR / "xgboost_pipeline_monitor_google2019.pkl"),
    # "both" falls back to MODEL_SAVE_PATH (single combined artifact).
}


def model_path_for_source(source: str) -> str:
    """Return the preferred model path for a given dataset source."""
    return MODEL_SAVE_PATHS.get((source or "").lower().strip(), MODEL_SAVE_PATH)

XGBOOST_PARAMS = {
    "n_estimators":     300,
    "max_depth":        5,
    "learning_rate":    0.08,
    "subsample":        0.8,
    "colsample_bytree": 0.8,
    "random_state":     42,
    # scale_pos_weight is set automatically from class distribution at train time
}

TEST_SIZE       = 0.25
CV_FOLDS        = 5

# ═══════════════════════════════════════════════════════════════
#  STREAMING / PLUGIN SETTINGS
# ═══════════════════════════════════════════════════════════════

# How many log lines to accumulate per process before triggering a prediction
PREDICT_EVERY_N_LINES = 20

# After this many seconds without new lines for a process, flush & predict
FLUSH_TIMEOUT_SECONDS = 30

# Special line written by live pipelines to tell the monitor the run ended.
LIVE_STOP_MARKER = "__PIPELINE_MONITOR_STOP__"

# Risk thresholds for classification
RISK_HIGH   = 0.70   # >= 70% failure probability → HIGH
RISK_MEDIUM = 0.40   # >= 40% failure probability → MEDIUM
                     # <  40% failure probability → LOW

# How many top features to show in the explanation
EXPLANATION_TOP_N = 5

# ═══════════════════════════════════════════════════════════════
#  HDFS LOG FORMAT
# ═══════════════════════════════════════════════════════════════
# Example line:
# 081109 203518 143 INFO dfs.DataNode$DataXceiver: Receiving block blk_-1608 ...
HDFS_LOG_FIELDS = ["date", "time", "pid", "level", "component", "content"]

# ═══════════════════════════════════════════════════════════════
#  GOOGLE CLUSTER EVENT TYPE CODES
# ═══════════════════════════════════════════════════════════════
GOOGLE_EVENT_TYPES = {
    0: "SUBMIT",
    1: "SCHEDULE",
    2: "EVICT",
    3: "FAIL",
    4: "FINISH",
    5: "KILL",
    6: "LOST",
    7: "UPDATE_PENDING",
    8: "UPDATE_RUNNING",
}
GOOGLE_FAIL_EVENTS  = {2, 3, 5, 6}   # EVICT, FAIL, KILL, LOST
GOOGLE_SUCCESS_EVENT = 4              # FINISH
