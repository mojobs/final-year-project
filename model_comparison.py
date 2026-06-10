"""
model_comparison.py
-------------------
Compare multiple machine-learning algorithms for pipeline failure prediction.

This supports the proposal objective that requires comparative analysis before
selecting XGBoost as the final model.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    matthews_corrcoef,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import (
    StratifiedGroupKFold,
    StratifiedKFold,
    cross_val_score,
    train_test_split,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier

from config import OUTPUTS_DIR, TEST_SIZE, XGBOOST_PARAMS
from demo_pipeline import (
    ERROR_MESSAGES,
    FATAL_MESSAGES,
    RECOVERING_MESSAGES,
    STABLE_MESSAGES,
    WARNING_MESSAGES,
    format_hdfs_line,
    hdfs_timestamp,
    make_block_id,
)
from feature_extractor import extract_all
from model import NON_FEATURE_COLS


try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None


@dataclass
class ComparisonArtifacts:
    csv_path: str
    json_path: str
    chart_path: str


def compare_models(
    feature_records: list[dict],
    *,
    output_dir: str = OUTPUTS_DIR,
    dataset_name: str = "dataset",
) -> dict:
    """Train/evaluate candidate models on the same feature split."""
    os.makedirs(output_dir, exist_ok=True)
    df = pd.DataFrame(feature_records)
    if df.empty or "label" not in df:
        raise ValueError("No labeled feature records were provided for comparison.")

    df = df[df["label"] != -1].copy()
    if df["label"].nunique() < 2:
        raise ValueError("Model comparison requires both success and failure labels.")

    y = df["label"].astype(int)
    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLS]
    X = df[feature_cols].fillna(0)
    X_train, X_test, y_train, y_test = _split_features(df, X, y)
    group_col = "group_id" if "group_id" in df.columns else "process_id"
    train_groups = df.loc[X_train.index, group_col]

    models = _candidate_models(y_train)
    rows = []
    for name, estimator in models.items():
        rows.append(
            _evaluate_model(
                name,
                estimator,
                X_train,
                X_test,
                y_train,
                y_test,
                train_groups=train_groups,
            )
        )

    rows.sort(key=lambda r: (r["f1"], r["pr_auc"], r["recall"]), reverse=True)
    artifacts = _save_outputs(rows, output_dir=output_dir)
    result = {
        "dataset": dataset_name,
        "samples": int(len(X)),
        "features": int(X.shape[1]),
        "failures": int(y.sum()),
        "successes": int((y == 0).sum()),
        "top_ranked_model": rows[0]["model"],
        "deployed_model": "XGBoost" if any(r["model"] == "XGBoost" for r in rows) else rows[0]["model"],
        "ranking": rows,
        "artifacts": artifacts.__dict__,
    }

    with open(artifacts.json_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    return result


def load_comparison_features(
    *,
    source: str,
    max_lines: int | None = None,
    max_rows: int | None = None,
    early_warning: bool = True,
    snapshot_fractions: tuple[float, ...] = (0.25, 0.5, 0.75, 1.0),
    synthetic_loops: int = 10,
) -> tuple[list[dict], str]:
    """Load records and extract features for comparison."""
    source = source.lower().strip()
    if source == "synthetic":
        records = build_synthetic_comparison_records(loops=synthetic_loops)
        features = extract_all(
            records,
            early_warning=early_warning,
            fractions=snapshot_fractions,
        )
        return features, "synthetic_pipeline_logs"
    if source == "etl":
        records = build_etl_comparison_records(loops=synthetic_loops)
        features = extract_all(
            records,
            early_warning=early_warning,
            fractions=snapshot_fractions,
        )
        return features, "etl_style_pipeline_logs"

    from data_loader import load_dataset

    kwargs = {}
    if source == "hdfs" and max_lines:
        kwargs["max_lines"] = max_lines
    if source in {"google", "alibaba", "google2019"} and max_rows:
        kwargs["max_rows"] = max_rows

    records = load_dataset(source, **kwargs)
    features = extract_all(
        records,
        early_warning=early_warning,
        fractions=snapshot_fractions,
    )
    return features, source


def build_synthetic_comparison_records(loops: int = 10) -> list[dict]:
    """
    Build a labeled benchmark from the existing demo log templates.

    This is meant for immediate report/demo generation when the external HDFS
    label file is not present. Use HDFS/Google labels for the final experiment
    when available.
    """
    templates = [
        ("stable", STABLE_MESSAGES, 0),
        ("warning", WARNING_MESSAGES, 0),
        ("recovering", RECOVERING_MESSAGES, 0),
        ("error", ERROR_MESSAGES, 1),
        ("fatal", FATAL_MESSAGES, 1),
    ]
    records = []
    line_no = 0
    for loop_idx in range(1, max(1, int(loops)) + 1):
        for template_idx, (profile, messages, label) in enumerate(templates, start=1):
            block = make_block_id(loop_idx * 100 + template_idx, profile)
            pid = 200 + template_idx
            parsed_lines = []
            for level, component, message_template in messages:
                date, clock = hdfs_timestamp(line_no)
                message = message_template.format(block=block)
                raw = format_hdfs_line(
                    date=date,
                    clock=clock,
                    pid=pid,
                    level=level,
                    component=component,
                    message=message,
                )
                parsed_lines.append(
                    {
                        "date": date,
                        "time": clock,
                        "pid": str(pid),
                        "level": level,
                        "component": component,
                        "content": message,
                        "block_ids": [block],
                        "raw": raw,
                    }
                )
                line_no += 1
            records.append(
                {
                    "process_id": block,
                    "source": "hdfs",
                    "label": label,
                    "log_lines": parsed_lines,
                    "events": [],
                }
            )
    return records


def build_etl_comparison_records(loops: int = 10) -> list[dict]:
    """
    Build labeled ETL-style records that match the live attach demo.

    These are not a replacement for an external labeled benchmark, but they
    let the project demonstrate model selection on pipeline-like stages instead
    of only the raw HDFS dataset.
    """
    scenarios = [
        ("normal", 0),
        ("warning", 0),
        ("recovering", 0),
        ("risky", 1),
        ("mixed", 1),
    ]
    stage_templates = {
        "normal": [
            ("INFO", "pipeline.Extract", "Extracted orders for block {block}"),
            ("INFO", "pipeline.Validate", "Schema validation passed for block {block}"),
            ("INFO", "pipeline.Transform", "Currency normalization completed for block {block}"),
            ("INFO", "pipeline.Load", "Loaded curated rows for block {block}"),
            ("INFO", "pipeline.Report", "Completion report generated for block {block}"),
        ],
        "warning": [
            ("INFO", "pipeline.Extract", "Extracted orders for block {block}"),
            ("WARN", "pipeline.Extract", "Source latency near threshold for block {block}"),
            ("INFO", "pipeline.Validate", "Schema validation passed for block {block}"),
            ("WARN", "pipeline.Transform", "Slow transform observed for block {block}"),
            ("INFO", "pipeline.Load", "Loaded curated rows for block {block}"),
        ],
        "recovering": [
            ("INFO", "pipeline.Extract", "Extracted orders for block {block}"),
            ("WARN", "pipeline.Validate", "Optional column missing; fallback default applied for block {block}"),
            ("WARN", "pipeline.Transform", "Retry scheduled after transient mapping error for block {block}"),
            ("INFO", "pipeline.Transform", "Retry completed successfully for block {block}"),
            ("INFO", "pipeline.Load", "Loaded curated rows for block {block}"),
        ],
        "risky": [
            ("INFO", "pipeline.Extract", "Extracted orders for block {block}"),
            ("WARN", "pipeline.Validate", "Non-positive amount detected for block {block}"),
            ("WARN", "pipeline.Load", "Queue full while buffering block {block}; retrying once"),
            ("ERROR", "pipeline.Load", "Timeout while loading block {block}; retrying"),
            ("ERROR", "pipeline.Load", "Connection reset while processing block {block}"),
            ("ERROR", "pipeline.Load", "Disk write refused after repeated retry attempts for block {block}"),
            ("FATAL", "pipeline.Load", "Critical corruption detected for block {block}; terminate job"),
        ],
        "mixed": [
            ("INFO", "pipeline.Extract", "Extracted orders for block {block}"),
            ("WARN", "pipeline.Extract", "Source latency near threshold for block {block}"),
            ("WARN", "pipeline.Validate", "Optional column missing; fallback default applied for block {block}"),
            ("WARN", "pipeline.Transform", "Retry scheduled after transient mapping error for block {block}"),
            ("ERROR", "pipeline.Load", "Timeout while loading block {block}; retrying"),
            ("ERROR", "pipeline.Load", "Disk write refused after repeated retry attempts for block {block}"),
            ("FATAL", "pipeline.Load", "Critical corruption detected for block {block}; terminate job"),
        ],
    }

    records = []
    line_no = 0
    for loop_idx in range(1, max(1, int(loops)) + 1):
        for scenario_idx, (scenario, label) in enumerate(scenarios, start=1):
            sign = "-" if label else ""
            block = f"blk_{sign}{7000 + loop_idx * 100 + scenario_idx}"
            parsed_lines = []
            for level, component, message_template in stage_templates[scenario]:
                date, clock = hdfs_timestamp(line_no)
                message = message_template.format(block=block)
                raw = format_hdfs_line(
                    date=date,
                    clock=clock,
                    pid=500 + scenario_idx,
                    level=level,
                    component=component,
                    message=message,
                )
                parsed_lines.append(
                    {
                        "date": date,
                        "time": clock,
                        "pid": str(500 + scenario_idx),
                        "level": level,
                        "component": component,
                        "content": message,
                        "block_ids": [block],
                        "raw": raw,
                    }
                )
                line_no += 1
            records.append(
                {
                    "process_id": block,
                    "source": "hdfs",
                    "label": label,
                    "log_lines": parsed_lines,
                    "events": [],
                    "group_id": f"etl_{loop_idx}_{scenario}",
                }
            )
    return records


def _candidate_models(y_train: pd.Series) -> dict:
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    scale_pos_weight = round(neg / pos, 2) if pos else 1.0

    models = {
        "Majority Baseline": DummyClassifier(strategy="most_frequent"),
        "Logistic Regression": Pipeline(
            [
                ("scale", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        max_iter=1000,
                        class_weight="balanced",
                        random_state=42,
                    ),
                ),
            ]
        ),
        "Decision Tree": DecisionTreeClassifier(
            max_depth=6,
            class_weight="balanced",
            random_state=42,
        ),
        "Random Forest": RandomForestClassifier(
            n_estimators=200,
            max_depth=8,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        ),
        "HistGradientBoosting": HistGradientBoostingClassifier(
            max_iter=200,
            max_depth=5,
            learning_rate=0.08,
            random_state=42,
        ),
    }

    if XGBClassifier is not None:
        params = dict(XGBOOST_PARAMS)
        params.update(
            {
                "scale_pos_weight": scale_pos_weight,
                "eval_metric": "logloss",
                "verbosity": 0,
                "random_state": 42,
            }
        )
        models["XGBoost"] = XGBClassifier(**params)
    return models


def _split_features(df: pd.DataFrame, X: pd.DataFrame, y: pd.Series):
    group_col = "group_id" if "group_id" in df.columns else "process_id"
    group_labels = (
        df[[group_col, "label"]]
        .groupby(group_col, as_index=False)["label"]
        .max()
    )
    can_group_split = (
        len(group_labels) >= 4
        and group_labels["label"].nunique() == 2
        and group_labels["label"].value_counts().min() >= 2
    )
    if can_group_split:
        try:
            train_groups, test_groups = train_test_split(
                group_labels[group_col],
                test_size=TEST_SIZE,
                stratify=group_labels["label"],
                random_state=42,
            )
            train_mask = df[group_col].isin(set(train_groups))
            test_mask = df[group_col].isin(set(test_groups))
            return X.loc[train_mask], X.loc[test_mask], y.loc[train_mask], y.loc[test_mask]
        except ValueError:
            pass
    return train_test_split(X, y, test_size=TEST_SIZE, stratify=y, random_state=42)


def _evaluate_model(
    name,
    estimator,
    X_train,
    X_test,
    y_train,
    y_test,
    *,
    train_groups=None,
) -> dict:
    cv_folds = min(5, int(y_train.value_counts().min()))
    if cv_folds >= 2:
        use_group_cv = (
            train_groups is not None
            and train_groups.nunique() < len(train_groups)
            and train_groups.nunique() >= cv_folds
        )
        if use_group_cv:
            cv = StratifiedGroupKFold(n_splits=cv_folds, shuffle=True, random_state=42)
            cv_scores = cross_val_score(
                estimator,
                X_train,
                y_train,
                cv=cv,
                groups=train_groups,
                scoring="f1",
            )
        else:
            cv = StratifiedKFold(n_splits=cv_folds, shuffle=True, random_state=42)
            cv_scores = cross_val_score(estimator, X_train, y_train, cv=cv, scoring="f1")
        cv_f1_mean = float(cv_scores.mean())
        cv_f1_std = float(cv_scores.std())
    else:
        cv_f1_mean = 0.0
        cv_f1_std = 0.0

    estimator.fit(X_train, y_train)
    t0 = time.perf_counter()
    y_pred = estimator.predict(X_test)
    y_prob = _predict_probability(estimator, X_test)
    inference_ms = (time.perf_counter() - t0) / max(len(X_test), 1) * 1000

    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    specificity = tn / (tn + fp) if (tn + fp) else 0.0
    return {
        "model": name,
        "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
        "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "roc_auc": round(_safe_roc_auc(y_test, y_prob), 4),
        "pr_auc": round(float(average_precision_score(y_test, y_prob)), 4),
        "mcc": round(float(matthews_corrcoef(y_test, y_pred)), 4),
        "specificity": round(float(specificity), 4),
        "cv_f1_mean": round(cv_f1_mean, 4),
        "cv_f1_std": round(cv_f1_std, 4),
        "inference_ms": round(float(inference_ms), 4),
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
    }


def _predict_probability(estimator, X_test) -> pd.Series:
    if hasattr(estimator, "predict_proba"):
        return estimator.predict_proba(X_test)[:, 1]
    if hasattr(estimator, "decision_function"):
        scores = estimator.decision_function(X_test)
        scores = (scores - scores.min()) / max(scores.max() - scores.min(), 1e-9)
        return scores
    return estimator.predict(X_test)


def _safe_roc_auc(y_true, y_prob) -> float:
    try:
        return float(roc_auc_score(y_true, y_prob))
    except ValueError:
        return 0.0


def _save_outputs(rows: list[dict], *, output_dir: str) -> ComparisonArtifacts:
    csv_path = os.path.join(output_dir, "model_comparison.csv")
    json_path = os.path.join(output_dir, "model_comparison.json")
    chart_path = os.path.join(output_dir, "model_comparison.png")

    df = pd.DataFrame(rows)
    df.to_csv(csv_path, index=False)
    _plot_comparison(df, chart_path)
    return ComparisonArtifacts(csv_path=csv_path, json_path=json_path, chart_path=chart_path)


def _plot_comparison(df: pd.DataFrame, chart_path: str) -> None:
    metrics = ["precision", "recall", "f1", "pr_auc", "roc_auc"]
    plot_df = df.set_index("model")[metrics]
    ax = plot_df.plot(kind="bar", figsize=(12, 6), width=0.78)
    ax.set_ylim(0, 1.1)
    ax.set_ylabel("Score")
    ax.set_title("Model Comparison for Pipeline Failure Prediction")
    ax.legend(loc="lower right")
    ax.grid(axis="y", alpha=0.25)
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()
    plt.savefig(chart_path, dpi=150)
    plt.close()


def print_comparison(result: dict) -> None:
    print("\nMODEL COMPARISON SUMMARY")
    print(f"  Dataset    : {result['dataset']}")
    print(f"  Samples    : {result['samples']}")
    print(f"  Features   : {result['features']}")
    print(f"  Failures   : {result['failures']}")
    print(f"  Successes  : {result['successes']}")
    print(f"  Top ranked : {result['top_ranked_model']}")
    print(f"  Deployed   : {result['deployed_model']}")
    print("\n  Ranking:")
    for row in result["ranking"]:
        print(
            f"  - {row['model']:<22} "
            f"F1={row['f1']:.4f} "
            f"Recall={row['recall']:.4f} "
            f"PR-AUC={row['pr_auc']:.4f} "
            f"ROC-AUC={row['roc_auc']:.4f}"
        )
    print("\n  Artifacts:")
    for key, path in result["artifacts"].items():
        print(f"  - {key}: {path}")
