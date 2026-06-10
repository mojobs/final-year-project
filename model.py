"""
model.py
--------
XGBoost failure prediction model with:
  • Full evaluation suite: Accuracy, Precision, Recall, F1, ROC-AUC,
    PR-AUC, MCC, Specificity, Log Loss, inference latency
  • 5-fold stratified cross-validation
  • Per-prediction explanation (top N features driving the prediction)
  • Evaluation plots: confusion matrix, ROC curve, PR curve,
    feature importance, metrics bar chart, probability distribution
  • Save / load model artifact
"""

import os
import time
import pickle
import warnings
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import (
    train_test_split,
    StratifiedKFold,
    StratifiedGroupKFold,
    cross_val_score,
)
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, average_precision_score, matthews_corrcoef,
    log_loss, confusion_matrix, classification_report,
    roc_curve, precision_recall_curve,
)

from config import (
    MODEL_SAVE_PATH, XGBOOST_PARAMS, TEST_SIZE, CV_FOLDS,
    RISK_HIGH, RISK_MEDIUM, EXPLANATION_TOP_N,
)

warnings.filterwarnings("ignore")


# ── Algorithm selection ───────────────────────────────────────────────────────
try:
    from xgboost import XGBClassifier
    _BACKEND = "xgboost"
    print("[OK] XGBoost backend loaded")
except ImportError:
    from sklearn.ensemble import HistGradientBoostingClassifier as _HGB
    class XGBClassifier(_HGB):
        """Fallback to sklearn HistGradientBoostingClassifier."""
        def __init__(self, n_estimators=200, max_depth=5, learning_rate=0.1,
                     l2_regularization=1.0, random_state=42, **_):
            self.n_estimators      = n_estimators
            self.max_depth         = max_depth
            self.learning_rate     = learning_rate
            self.l2_regularization = l2_regularization
            self.random_state      = random_state
            super().__init__(max_iter=n_estimators, max_depth=max_depth,
                             learning_rate=learning_rate,
                             l2_regularization=l2_regularization,
                             random_state=random_state)
    _BACKEND = "sklearn_fallback"
    print("[WARN] xgboost not installed - using sklearn HistGradientBoostingClassifier")
    print("   Run: pip install xgboost")


PALETTE = {
    "primary": "#2563EB", "danger": "#DC2626",
    "success": "#16A34A", "warn":   "#D97706", "purple": "#7C3AED",
}

# Columns that are metadata, not features
NON_FEATURE_COLS = {"process_id", "group_id", "source", "label"}


def _get_feature_cols(df: pd.DataFrame) -> list[str]:
    return [c for c in df.columns if c not in NON_FEATURE_COLS]


# ══════════════════════════════════════════════════════════════════
#  TRAINING
# ══════════════════════════════════════════════════════════════════

def train(
    feature_records: list[dict],
    output_dir: str = "outputs",
    model_path: str = MODEL_SAVE_PATH,
) -> dict:
    """
    Train an XGBoost model on extracted feature records.

    Parameters
    ----------
    feature_records : output of feature_extractor.extract_all()
    output_dir      : where plots and metrics.json are written

    Returns
    -------
    metrics dict with all evaluation scores
    """
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)

    df = pd.DataFrame(feature_records)
    df = df[df["label"] != -1]

    y = df["label"].astype(int)
    X = df[_get_feature_cols(df)].fillna(0)
    feature_names = list(X.columns)

    print(f"\n{'='*58}")
    print(f"  Dataset   : {len(X)} samples  |  {X.shape[1]} features")
    print(f"  Failures  : {y.sum()}  ({y.mean()*100:.1f}%)")
    print(f"  Successes : {(y==0).sum()}  ({(1-y.mean())*100:.1f}%)")
    print(f"{'='*58}")

    # ── Train/test split ──────────────────────────────────────────
    # Hold out whole process/job groups when early-warning snapshots are used.
    # This prevents snapshots from the same job appearing in both train and test.
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
            train_groups = set(train_groups)
            test_groups = set(test_groups)
            train_mask = df[group_col].isin(train_groups)
            test_mask = df[group_col].isin(test_groups)
            X_train, X_test = X.loc[train_mask], X.loc[test_mask]
            y_train, y_test = y.loc[train_mask], y.loc[test_mask]
            print(
                f"  Split     : group-held-out "
                f"({len(train_groups)} train groups / {len(test_groups)} test groups)"
            )
        except ValueError:
            can_group_split = False

    if not can_group_split:
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=TEST_SIZE, stratify=y, random_state=42
        )
        print("  Split     : stratified row split")

    # ── Build model ───────────────────────────────────────────────
    neg, pos = (y_train == 0).sum(), (y_train == 1).sum()
    spw = round(neg / pos, 2) if pos else 1.0

    params = dict(XGBOOST_PARAMS)
    if _BACKEND == "xgboost":
        params.update({
            "scale_pos_weight": spw,
            "use_label_encoder": False,
            "eval_metric": "logloss",
            "verbosity": 0,
        })
    else:
        params = {k: v for k, v in params.items()
                  if k in ("n_estimators", "max_depth", "learning_rate", "random_state")}

    model = XGBClassifier(**params)

    # ── Cross-validation ──────────────────────────────────────────
    train_group_values = df.loc[X_train.index, group_col]
    use_group_cv = (
        train_group_values.nunique() < len(train_group_values)
        and train_group_values.nunique() >= CV_FOLDS
    )
    if use_group_cv:
        cv = StratifiedGroupKFold(n_splits=CV_FOLDS, shuffle=True, random_state=42)
        cv_f1 = cross_val_score(
            model,
            X_train,
            y_train,
            cv=cv,
            groups=train_group_values,
            scoring="f1",
        )
        print("  CV split  : stratified group folds")
    else:
        cv = StratifiedKFold(n_splits=CV_FOLDS, shuffle=True, random_state=42)
        cv_f1 = cross_val_score(model, X_train, y_train, cv=cv, scoring="f1")
        print("  CV split  : stratified row folds")
    print(f"\n  {CV_FOLDS}-Fold CV F1 : {cv_f1.mean():.4f} ± {cv_f1.std():.4f}")

    # ── Fit ───────────────────────────────────────────────────────
    model.fit(X_train, y_train)

    # ── Evaluate ──────────────────────────────────────────────────
    t0     = time.perf_counter()
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    latency_ms = (time.perf_counter() - t0) / len(X_test) * 1000

    tn, fp, fn, tp = confusion_matrix(y_test, y_pred).ravel()
    specificity = round(tn / (tn + fp), 4) if (tn + fp) > 0 else 0.0

    metrics = {
        "accuracy":          round(accuracy_score(y_test, y_pred), 4),
        "precision":         round(precision_score(y_test, y_pred, zero_division=0), 4),
        "recall":            round(recall_score(y_test, y_pred, zero_division=0), 4),
        "f1":                round(f1_score(y_test, y_pred, zero_division=0), 4),
        "roc_auc":           round(roc_auc_score(y_test, y_prob), 4),
        "pr_auc":            round(average_precision_score(y_test, y_prob), 4),
        "mcc":               round(matthews_corrcoef(y_test, y_pred), 4),
        "specificity":       specificity,
        "log_loss":          round(log_loss(y_test, y_prob), 4),
        "inference_ms":      round(latency_ms, 4),
        "cv_f1_mean":        round(cv_f1.mean(), 4),
        "cv_f1_std":         round(cv_f1.std(),  4),
        "tp": int(tp), "tn": int(tn), "fp": int(fp), "fn": int(fn),
    }

    # ── Print results ─────────────────────────────────────────────
    print(f"\n{'='*58}")
    print(f"  {'Metric':<22}  {'Score':>8}")
    print(f"  {'-'*32}")
    for k in ["accuracy","precision","recall","f1","roc_auc",
              "pr_auc","mcc","specificity","log_loss"]:
        print(f"  {k.capitalize():<22}  {metrics[k]:>8.4f}")
    print(f"  {'Inference (ms/sample)':<22}  {metrics['inference_ms']:>8.4f}")
    print(f"{'='*58}")
    print(f"\nClassification Report:\n")
    print(classification_report(y_test, y_pred,
                                 target_names=["Success", "Failure"]))

    # ── Plots ─────────────────────────────────────────────────────
    _plot_confusion_matrix(y_test, y_pred, output_dir)
    _plot_roc_curve(y_test, y_prob, metrics["roc_auc"], output_dir)
    _plot_pr_curve(y_test, y_prob, metrics["pr_auc"], output_dir)
    _plot_feature_importance(model, feature_names, output_dir)
    _plot_metrics_bar(metrics, output_dir)
    _plot_prob_distribution(y_test, y_prob, output_dir)

    # ── Save model ────────────────────────────────────────────────
    sources = sorted({str(s).strip().lower() for s in df.get("source", []) if str(s).strip()})
    trained_source = sources[0] if len(sources) == 1 else ("both" if len(sources) > 1 else "unknown")
    artifact = {
        "model":         model,
        "feature_names": feature_names,
        "metrics":       metrics,
        "backend":       _BACKEND,
        "trained_source": trained_source,
    }
    with open(model_path, "wb") as f:
        pickle.dump(artifact, f)
    print(f"\n  [OK] Model saved -> {model_path}")

    # ── Save metrics JSON ─────────────────────────────────────────
    import json
    with open(os.path.join(output_dir, "metrics.json"), "w") as f:
        json.dump(metrics, f, indent=2)

    return metrics


# ══════════════════════════════════════════════════════════════════
#  PREDICTION + EXPLANATION
# ══════════════════════════════════════════════════════════════════

class PipelineMonitorModel:
    """
    Loaded model that produces predictions + human-readable explanations.
    """

    def __init__(self):
        self.model         = None
        self.feature_names = None
        self.metrics       = {}
        self.trained_source = None

    @classmethod
    def load(cls, path: str = MODEL_SAVE_PATH, expected_source: str | None = None):
        if not os.path.exists(path):
            raise FileNotFoundError(
                f"No trained model at '{path}'. Run:  python main.py train"
            )
        with open(path, "rb") as f:
            artifact = pickle.load(f)

        trained_source = artifact.get("trained_source")
        if expected_source and trained_source:
            exp = (expected_source or "").lower().strip()
            ts  = str(trained_source).lower().strip()
            if exp and ts not in ("both", "unknown", exp):
                raise ValueError(
                    f"Model '{path}' was trained for source='{trained_source}', "
                    f"but this monitor is configured for source='{expected_source}'."
                )
        inst = cls()
        inst.model         = artifact["model"]
        inst.feature_names = artifact["feature_names"]
        inst.metrics       = artifact.get("metrics", {})
        inst.trained_source = trained_source
        return inst

    def predict(self, feature_dict: dict) -> dict:
        """
        Predict failure risk for one process snapshot.

        Parameters
        ----------
        feature_dict : output of feature_extractor.extract_features()
                       or feature_extractor.StreamingExtractor.get_features()

        Returns
        -------
        {
          process_id         : str,
          prediction         : int   (1=FAILURE, 0=SUCCESS),
          failure_probability: float,
          risk_level         : "HIGH" | "MEDIUM" | "LOW",
          explanation        : list of {feature, value, direction, rank} dicts,
          raw_features       : dict of feature values used
        }
        """
        X = self._align(feature_dict)
        prob  = float(self.model.predict_proba(X)[0, 1])
        pred  = int(prob >= 0.5)
        risk  = ("HIGH" if prob >= RISK_HIGH
                 else "MEDIUM" if prob >= RISK_MEDIUM
                 else "LOW")

        explanation = self._explain(X, prob)

        return {
            "process_id":          feature_dict.get("process_id", "unknown"),
            "source":              feature_dict.get("source", "unknown"),
            "prediction":          pred,
            "failure_probability": round(prob, 4),
            "risk_level":          risk,
            "explanation":         explanation,
            "raw_features":        {k: feature_dict.get(k)
                                    for k in self.feature_names
                                    if k in feature_dict},
        }

    def predict_batch(self, feature_records: list[dict]) -> list[dict]:
        return [self.predict(r) for r in feature_records]

    def _align(self, feature_dict: dict) -> pd.DataFrame:
        """Build a one-row DataFrame aligned to training feature columns."""
        row = {col: feature_dict.get(col, 0) for col in self.feature_names}
        return pd.DataFrame([row]).fillna(0)

    def _explain(self, X: pd.DataFrame, prob: float) -> list[dict]:
        """
        Produce a ranked list of the top N features driving this prediction.
        Uses model feature importances as a proxy when SHAP is unavailable.
        """
        if not hasattr(self.model, "feature_importances_"):
            return []

        importances = self.model.feature_importances_
        feat_vals   = X.iloc[0]

        scored = []
        for fname, imp in zip(self.feature_names, importances):
            val = feat_vals.get(fname, 0)
            scored.append({
                "feature":   fname,
                "value":     round(float(val), 4),
                "importance": round(float(imp), 6),
                "direction": _direction(fname, val, prob),
            })

        scored.sort(key=lambda x: x["importance"], reverse=True)
        top = scored[:EXPLANATION_TOP_N]
        for i, item in enumerate(top):
            item["rank"] = i + 1
        return top


def _direction(feature_name: str, value: float, prob: float) -> str:
    """
    Heuristic: features whose high value correlates with failure
    are marked as INCREASING risk; others as DECREASING risk.
    """
    high_risk_features = {
        "error_count", "error_rate", "error_kw_hits", "error_kw_rate",
        "fatal_count", "critical_rate", "retry_count", "exception_count",
        "max_error_burst", "err_x_retry", "failing_comp_hits",
        "has_exception", "has_fatal", "has_retry",
        "memory_request", "oom_signal", "timeout_signal",
        "log1p_error_count", "log1p_fatal_count",
        "log1p_retry_count", "log1p_exception_count",
        "log1p_err_x_retry", "log1p_max_error_burst",
    }
    if feature_name in high_risk_features:
        return "↑ increases failure risk" if value > 0 else "neutral"
    return "↓ reduces failure risk" if value > 0 else "neutral"


# ══════════════════════════════════════════════════════════════════
#  PLOTS
# ══════════════════════════════════════════════════════════════════

def _plot_confusion_matrix(y_true, y_pred, out_dir):
    cm = confusion_matrix(y_true, y_pred)
    fig, ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=["Success", "Failure"],
                yticklabels=["Success", "Failure"],
                linewidths=0.5, ax=ax)
    ax.set_xlabel("Predicted", fontsize=12)
    ax.set_ylabel("Actual",    fontsize=12)
    ax.set_title("Confusion Matrix", fontsize=13)
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "confusion_matrix.png"), dpi=150)
    plt.close(fig)
    print("  [OK] confusion_matrix.png")


def _plot_roc_curve(y_true, y_prob, auc, out_dir):
    fpr, tpr, _ = roc_curve(y_true, y_prob)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(fpr, tpr, color=PALETTE["primary"], lw=2, label=f"AUC = {auc:.4f}")
    ax.plot([0,1],[0,1], "k--", lw=1, label="Random")
    ax.fill_between(fpr, tpr, alpha=0.08, color=PALETTE["primary"])
    ax.set_xlabel("False Positive Rate", fontsize=12)
    ax.set_ylabel("True Positive Rate",  fontsize=12)
    ax.set_title("ROC Curve", fontsize=13)
    ax.legend()
    ax.set_xlim([0,1]); ax.set_ylim([0, 1.02])
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "roc_curve.png"), dpi=150)
    plt.close(fig)
    print("  [OK] roc_curve.png")


def _plot_pr_curve(y_true, y_prob, pr_auc, out_dir):
    prec, rec, _ = precision_recall_curve(y_true, y_prob)
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(rec, prec, color=PALETTE["danger"], lw=2,
            label=f"PR-AUC = {pr_auc:.4f}")
    baseline = y_true.mean()
    ax.axhline(baseline, color="grey", linestyle="--", lw=1,
               label=f"Baseline = {baseline:.3f}")
    ax.fill_between(rec, prec, alpha=0.08, color=PALETTE["danger"])
    ax.set_xlabel("Recall",    fontsize=12)
    ax.set_ylabel("Precision", fontsize=12)
    ax.set_title("Precision-Recall Curve", fontsize=13)
    ax.legend()
    ax.set_xlim([0,1]); ax.set_ylim([0, 1.02])
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "pr_curve.png"), dpi=150)
    plt.close(fig)
    print("  [OK] pr_curve.png")


def _plot_feature_importance(model, feature_names, out_dir):
    if not hasattr(model, "feature_importances_"):
        return
    imp = pd.Series(model.feature_importances_, index=feature_names)
    imp = imp.sort_values(ascending=True).tail(20)
    fig, ax = plt.subplots(figsize=(9, 6))
    colors  = [PALETTE["danger"] if "log1p" in n else PALETTE["primary"]
               for n in imp.index]
    imp.plot(kind="barh", ax=ax, color=colors, edgecolor="white", lw=0.4)
    ax.set_xlabel("Feature Importance (Gain)", fontsize=12)
    ax.set_title("Top 20 Feature Importances", fontsize=13)
    ax.axvline(imp.mean(), color="grey", linestyle="--", lw=1,
               label=f"Mean = {imp.mean():.4f}")
    ax.legend()
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "feature_importance.png"), dpi=150)
    plt.close(fig)
    print("  [OK] feature_importance.png")


def _plot_metrics_bar(metrics, out_dir):
    keys   = ["accuracy","precision","recall","f1","roc_auc","pr_auc","mcc","specificity"]
    labels = ["Accuracy","Precision","Recall","F1","ROC-AUC","PR-AUC","MCC","Specificity"]
    vals   = [metrics[k] for k in keys]
    colors = [PALETTE["primary"], PALETTE["warn"], PALETTE["danger"],
              PALETTE["success"], PALETTE["purple"], "#0891B2",
              "#059669", "#B45309"]
    fig, ax = plt.subplots(figsize=(10, 4))
    bars = ax.bar(labels, vals, color=colors, width=0.55,
                  edgecolor="white", lw=0.6)
    ax.set_ylim(0, 1.18)
    ax.set_ylabel("Score", fontsize=12)
    ax.set_title("XGBoost Model Evaluation Metrics", fontsize=13)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2,
                bar.get_height() + 0.02,
                f"{val:.3f}", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "evaluation_metrics.png"), dpi=150)
    plt.close(fig)
    print("  [OK] evaluation_metrics.png")


def _plot_prob_distribution(y_true, y_prob, out_dir):
    df = pd.DataFrame({"prob": y_prob, "label": y_true})
    fig, ax = plt.subplots(figsize=(8, 5))
    for lbl, col, name in [(0, PALETTE["success"], "Success"),
                            (1, PALETTE["danger"],  "Failure")]:
        ax.hist(df[df["label"] == lbl]["prob"], bins=40, alpha=0.6,
                color=col, label=name, edgecolor="white", lw=0.3)
    ax.axvline(0.5, color="black", linestyle="--", lw=1.2,
               label="Threshold = 0.5")
    ax.set_xlabel("Predicted Failure Probability", fontsize=12)
    ax.set_ylabel("Count", fontsize=12)
    ax.set_title("Distribution of Predicted Probabilities", fontsize=13)
    ax.legend()
    plt.tight_layout()
    fig.savefig(os.path.join(out_dir, "probability_distribution.png"), dpi=150)
    plt.close(fig)
    print("  [OK] probability_distribution.png")
