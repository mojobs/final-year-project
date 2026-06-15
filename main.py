"""
main.py
-------
CLI entry point for PipePulse Sentinel.

Commands
--------
  python main.py train    --source hdfs   [--max-lines 500000]
  python main.py train    --source google [--max-rows 100000]
  python main.py train    --source both

  python main.py monitor  --file  pipeline.log --source hdfs --max-lines 5000
  python main.py monitor  --file  pipeline.log --source hdfs --live
  python main.py monitor  --file  job_events.csv --source google

  python main.py evaluate              ← prints stored metrics
"""

import os
import sys
import json
import argparse


def _configure_stdout() -> None:
    # Avoid UnicodeEncodeError on some Windows terminals when printing symbols.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass


def _parse_snapshot_fractions(value: str) -> tuple[float, ...]:
    try:
        fractions = tuple(
            float(part.strip()) for part in value.split(",") if part.strip()
        )
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Use comma-separated numbers between 0 and 1, e.g. 0.25,0.5,0.75,1"
        ) from exc

    if not fractions or any(f <= 0 or f > 1 for f in fractions):
        raise argparse.ArgumentTypeError(
            "Snapshot fractions must be greater than 0 and less than or equal to 1."
        )
    return fractions


def cmd_train(args):
    from data_loader      import load_dataset
    from feature_extractor import extract_all
    from model             import train
    from config            import ETL_MODEL_SAVE_PATH, model_path_for_source, OUTPUTS_DIR

    def _train_one(source: str) -> None:
        print(f"\n[TRAIN] source={source}")
        output_dir = (
            os.path.join(OUTPUTS_DIR, source)
            if source in {"alibaba", "google2019"}
            else OUTPUTS_DIR
        )
        if source == "etl":
            from model_comparison import build_etl_comparison_records

            records = build_etl_comparison_records(loops=args.synthetic_loops)
            features = extract_all(
                records,
                early_warning=not args.full_log_only,
                fractions=args.snapshot_fractions,
            )
            train(features, output_dir=output_dir, model_path=ETL_MODEL_SAVE_PATH)
            print(f"\n[OK] ETL-style model saved for live demo: {ETL_MODEL_SAVE_PATH}")
            return

        kwargs = {}
        if source == "hdfs" and args.max_lines:
            kwargs["max_lines"] = args.max_lines
        if source in {"google", "alibaba", "google2019"} and args.max_rows:
            kwargs["max_rows"] = args.max_rows

        records  = load_dataset(source, **kwargs)
        features = extract_all(
            records,
            early_warning=not args.full_log_only,
            fractions=args.snapshot_fractions,
        )
        train(features, output_dir=output_dir, model_path=model_path_for_source(source))

    try:
        if args.source == "both":
            trained_any = False
            for src in ("hdfs", "google"):
                try:
                    _train_one(src)
                    trained_any = True
                except FileNotFoundError as e:
                    print(f"\n[TRAIN] source={src} skipped:\n{e}\n")
            if not trained_any:
                sys.exit(1)
        else:
            _train_one(args.source)
    except FileNotFoundError as e:
        print(f"\n{e}\n")
        sys.exit(1)

    print(f"\n[OK] Training complete. Outputs written to {OUTPUTS_DIR}")


def cmd_monitor(args):
    from pipeline_monitor import PipelineMonitor

    try:
        monitor = PipelineMonitor(
            source=args.source,
            min_lines=args.min_lines,
            raw_model_risk=args.raw_model_risk,
            demo_risk_calibration=args.demo_risk_calibration,
        )
    except (FileNotFoundError, ValueError) as e:
        print(f"\n{e}\n")
        sys.exit(1)

    if args.live:
        # Real-time tail mode
        monitor.start(args.file)
    else:
        # Batch file mode
        results = monitor.process_file(args.file, max_lines=args.max_lines)
        monitor._print_session_summary()

        if args.output:
            monitor.save_results(args.output)
        elif results:
            # Print top high-risk predictions
            high_risk = monitor.get_high_risk_processes()
            if high_risk:
                print(f"\n{'='*55}")
                print(f"  HIGH-RISK PROCESSES  ({len(high_risk)} found)")
                print(f"{'='*55}")
                for r in high_risk[:10]:
                    pid  = r["process_id"]
                    prob = r["failure_probability"]
                    print(f"\n  Process : {pid}")
                    print(f"  Risk    : {r['risk_level']}  ({prob:.0%} failure prob)")
                    if r.get("explanation"):
                        print("  Why:")
                        for item in r["explanation"]:
                            print(f"    #{item['rank']}  {item['feature']:<28} "
                                  f"= {item['value']:.4f}  {item['direction']}")


def cmd_evaluate(args):
    from config import OUTPUTS_DIR
    metrics_dir = (
        os.path.join(OUTPUTS_DIR, args.source)
        if args.source in {"alibaba", "google2019"}
        else OUTPUTS_DIR
    )
    metrics_path = os.path.join(metrics_dir, "metrics.json")
    if not os.path.exists(metrics_path):
        print("No evaluation results found. Run:  python main.py train")
        sys.exit(1)

    with open(metrics_path) as f:
        m = json.load(f)

    print(f"\n{'='*50}")
    print("  MODEL EVALUATION RESULTS")
    print(f"{'='*50}")
    ordered = [
        ("Accuracy",            "accuracy"),
        ("Precision",           "precision"),
        ("Recall (Sensitivity)","recall"),
        ("Specificity",         "specificity"),
        ("F1 Score",            "f1"),
        ("ROC-AUC",             "roc_auc"),
        ("PR-AUC",              "pr_auc"),
        ("MCC",                 "mcc"),
        ("Log Loss",            "log_loss"),
        ("Inference (ms/sample)","inference_ms"),
        ("CV F1 Mean",          "cv_f1_mean"),
        ("CV F1 Std",           "cv_f1_std"),
    ]
    for label, key in ordered:
        val = m.get(key)
        if val is not None:
            print(f"  {label:<26} {val:.4f}")

    print(f"\n  Confusion Matrix:")
    print(f"    TP={m.get('tp',0)}  FP={m.get('fp',0)}")
    print(f"    FN={m.get('fn',0)}  TN={m.get('tn',0)}")
    print(f"{'='*50}\n")


def cmd_etl_demo(args):
    from etl_pipeline_demo import run_monitored_etl

    result = run_monitored_etl(
        scenario=args.scenario,
        stop_policy=args.stop_policy,
        min_lines=args.min_lines,
        input_path=args.input,
        output_path=args.output,
        log_path=args.log_file,
    )
    summary = result["summary"]
    print("\nETL PIPELINE MONITORING SUMMARY")
    print(f"  Status          : {summary['status']}")
    print(f"  Current risk    : {summary['current_risk']}")
    print(f"  Predictions     : {summary['total_predictions']}")
    print(f"  Recommendation  : {summary['recommendation']}")
    print(f"  Output file     : {summary.get('output_path')}")
    print(f"  JSON report     : {summary.get('report_path')}")
    print(f"  HTML report     : {summary.get('html_report_path')}")


def cmd_compare(args):
    from model_comparison import (
        compare_models,
        load_comparison_features,
        print_comparison,
    )
    from config import OUTPUTS_DIR

    try:
        features, dataset_name = load_comparison_features(
            source=args.source,
            max_lines=args.max_lines,
            max_rows=args.max_rows,
            early_warning=not args.full_log_only,
            snapshot_fractions=args.snapshot_fractions,
            synthetic_loops=args.synthetic_loops,
        )
    except FileNotFoundError as exc:
        print(f"\n{exc}\n")
        print("Tip: run a quick comparison with:")
        print("  python main.py compare --source synthetic")
        raise SystemExit(1) from exc

    comparison_output_dir = (
        os.path.join(OUTPUTS_DIR, f"comparison_{dataset_name}")
        if args.source in {"alibaba", "google2019"}
        else OUTPUTS_DIR
    )
    result = compare_models(
        features,
        output_dir=comparison_output_dir,
        dataset_name=dataset_name,
    )
    print_comparison(result)


def cmd_attach(args):
    from live_pipeline_runner import DEFAULT_COMMAND, run_attached_pipeline

    command = args.pipeline_command or DEFAULT_COMMAND
    if args.pipeline_command and args.pipeline_command[0] == "--":
        command = args.pipeline_command[1:]

    result = run_attached_pipeline(
        command,
        stop_policy=args.stop_policy,
        min_lines=args.min_lines,
        raw_log_path=args.raw_log_file,
        normalized_log_path=args.log_file,
        model_path=args.model_path,
        demo_risk_calibration=not args.raw_model_risk,
        timeout_seconds=args.timeout,
    )
    summary = result["summary"]
    print("\nATTACHED PIPELINE MONITORING SUMMARY")
    print(f"  Command         : {summary.get('command')}")
    print(f"  Status          : {summary.get('status')}")
    print(f"  Return code     : {summary.get('return_code')}")
    print(f"  Current risk    : {summary.get('current_risk')}")
    print(f"  Predictions     : {summary.get('total_predictions')}")
    print(f"  Recommendation  : {summary.get('recommendation')}")
    print(f"  Raw log         : {summary.get('raw_log_path')}")
    print(f"  Normalized log  : {summary.get('normalized_log_path')}")
    print(f"  JSON report     : {summary.get('report_path')}")
    print(f"  HTML report     : {summary.get('html_report_path')}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    _configure_stdout()
    parser = argparse.ArgumentParser(
        description="PipePulse Sentinel - XGBoost pipeline failure monitor",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  python main.py train   --source hdfs   --max-lines 500000
  python main.py train   --source hdfs   --snapshot-fractions 0.25,0.5,0.75,1
  python main.py train   --source google
  python main.py train   --source alibaba    --max-rows 10000 --full-log-only
  python main.py train   --source google2019 --max-rows 50000 --full-log-only
  python main.py monitor --file data/HDFS.log   --source hdfs --max-lines 5000 --min-lines 5
  python main.py monitor --file data/HDFS.log   --source hdfs --live
  python main.py monitor --file simulated/live_pipeline.log --source hdfs --live --min-lines 5 --demo-risk-calibration
  python main.py etl-demo --scenario mixed --stop-policy advise
  python main.py attach --stop-policy auto_stop --min-lines 3 -- python examples/real_pipeline_job.py --scenario risky
  python main.py compare --source etl
  python main.py compare --source synthetic
  python main.py compare --source hdfs --max-lines 500000
  python main.py monitor --file data/job_events.csv --source google --output results.json
  python main.py evaluate
        """
    )
    sub = parser.add_subparsers(dest="command")

    # train
    p_train = sub.add_parser("train", help="Train XGBoost model on log data")
    p_train.add_argument("--source", choices=["hdfs", "google", "alibaba", "google2019", "both", "etl"],
                         default="hdfs", help="Dataset source")
    p_train.add_argument("--max_lines", "--max-lines", dest="max_lines",
                         type=int, default=None,
                         help="Max HDFS log lines to read (for development)")
    p_train.add_argument("--max_rows", "--max-rows", dest="max_rows",
                         type=int, default=None,
                         help="Max trace rows/jobs to read for bounded training")
    p_train.add_argument("--full-log-only", action="store_true",
                         help="Train on complete histories instead of early-warning snapshots")
    p_train.add_argument("--snapshot-fractions", type=_parse_snapshot_fractions,
                         default=(0.25, 0.50, 0.75, 1.0),
                         help="Comma-separated partial-history fractions for early training")
    p_train.add_argument("--synthetic-loops", type=int, default=20,
                         help="How many ETL-style labeled runs to generate when --source etl")

    # monitor
    p_mon = sub.add_parser("monitor",
                            help="Monitor a log file and predict failures")
    p_mon.add_argument("--file",   required=True, help="Path to log file")
    p_mon.add_argument("--source", choices=["hdfs", "google"], default="hdfs")
    p_mon.add_argument("--live",   action="store_true",
                       help="Live tail mode for a running pipeline")
    p_mon.add_argument("--output", default=None,
                       help="Save predictions to this JSON file")
    p_mon.add_argument("--max-lines", type=int, default=None,
                       help="Stop after this many input lines in batch mode")
    p_mon.add_argument("--min-lines", type=int, default=None,
                       help="Override how many lines are needed before each prediction")
    p_mon.add_argument("--raw-model-risk", action="store_true",
                       help="Use the raw model probability without stream calibration")
    p_mon.add_argument("--demo-risk-calibration", action="store_true",
                       help="Use feature-based demo risk levels for synthetic logs")

    # evaluate
    p_eval = sub.add_parser("evaluate", help="Print stored evaluation metrics")
    p_eval.add_argument(
        "--source",
        choices=["default", "alibaba", "google2019"],
        default="default",
        help="Metrics artifact to print",
    )

    # etl-demo
    p_etl = sub.add_parser("etl-demo", help="Run monitored CSV ETL demo")
    p_etl.add_argument("--scenario", choices=["normal", "warning", "recovering", "risky", "mixed"],
                       default="mixed")
    p_etl.add_argument("--stop-policy", choices=["advise", "auto_stop"], default="advise")
    p_etl.add_argument("--min-lines", type=int, default=5)
    p_etl.add_argument("--input", default="data/sample_orders.csv")
    p_etl.add_argument("--output", default="outputs/curated_orders.csv")
    p_etl.add_argument("--log-file", default="simulated/etl_pipeline.log")

    # attach
    p_attach = sub.add_parser(
        "attach",
        help="Run a real pipeline command and attach the predictor to its logs",
    )
    p_attach.add_argument("--stop-policy", choices=["advise", "auto_stop"], default="advise")
    p_attach.add_argument("--min-lines", type=int, default=3)
    p_attach.add_argument("--log-file", default="simulated/attached_pipeline.normalized.log")
    p_attach.add_argument("--raw-log-file", default="simulated/attached_pipeline.raw.log")
    p_attach.add_argument("--model-path", default=None,
                          help="Optional model artifact, e.g. models/xgboost_pipeline_monitor_etl.pkl")
    p_attach.add_argument("--raw-model-risk", action="store_true",
                          help="Use raw model probabilities without demo calibration")
    p_attach.add_argument("--timeout", type=float, default=None,
                          help="Stop the attached command after this many seconds")
    p_attach.add_argument(
        "pipeline_command",
        nargs=argparse.REMAINDER,
        help="Pipeline command after --, e.g. -- python examples/real_pipeline_job.py --scenario risky",
    )

    # compare
    p_cmp = sub.add_parser("compare", help="Compare ML algorithms for failure prediction")
    p_cmp.add_argument("--source", choices=["synthetic", "etl", "hdfs", "google", "alibaba", "google2019", "both"],
                       default="synthetic",
                       help="Dataset source for comparison")
    p_cmp.add_argument("--max-lines", type=int, default=None,
                       help="Max HDFS log lines to read")
    p_cmp.add_argument("--max-rows", type=int, default=None,
                       help="Max trace rows/jobs to read")
    p_cmp.add_argument("--full-log-only", action="store_true",
                       help="Compare on complete histories instead of early-warning snapshots")
    p_cmp.add_argument("--snapshot-fractions", type=_parse_snapshot_fractions,
                       default=(0.25, 0.50, 0.75, 1.0),
                       help="Comma-separated partial-history fractions")
    p_cmp.add_argument("--synthetic-loops", type=int, default=10,
                       help="How many synthetic labeled runs to generate for quick comparison")

    args = parser.parse_args()

    if args.command == "train":
        cmd_train(args)
    elif args.command == "monitor":
        cmd_monitor(args)
    elif args.command == "evaluate":
        cmd_evaluate(args)
    elif args.command == "etl-demo":
        cmd_etl_demo(args)
    elif args.command == "attach":
        cmd_attach(args)
    elif args.command == "compare":
        cmd_compare(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
