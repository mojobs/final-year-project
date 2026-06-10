# Log-Based Failure Prediction for Data Pipelines

This project is a predictive monitoring plugin for batch data pipelines. It reads logs while a job is running, extracts numerical warning signals, and uses an XGBoost classifier to estimate whether the job is likely to fail before the final crash or failure line appears.

## What It Does

- Loads historical pipeline logs such as LogHub HDFS logs or Google Cluster job events.
- Groups log lines by process, block, or job id.
- Converts each running job into structured features such as warning counts, retry counts, error rates, duration, failure keywords, and resource signals.
- Trains an XGBoost model to classify `success` vs `failure`.
- Monitors a live or saved log file and emits risk predictions as logs arrive.
- Returns a failure probability, risk level, and top feature-based explanation for each prediction.

## Project Flow

```text
Pipeline logs
  -> data_loader.py
  -> feature_extractor.py
  -> model.py
  -> pipeline_monitor.py
  -> predictor_plugin.py
  -> alerts / stop advice / reports / dashboard / Airflow
```

The product-facing demo now has three layers:

```text
Real pipeline command or monitored ETL pipeline
  -> LivePipelineRunner
  -> PredictorPlugin
  -> XGBoost monitor
  -> LOW / MEDIUM / HIGH intervention decision
  -> JSON + HTML monitoring report
  -> Streamlit dashboard / Airflow DAG
```

## Important Design Choice

The training command now uses early-warning snapshots by default. That means each historical job is split into partial prefixes, such as the first 25%, 50%, 75%, and 100% of its logs. This teaches the model to recognize failure risk before the final failure point.

Use complete logs only when you explicitly want a baseline:

```powershell
python main.py train --source hdfs --full-log-only
```

## Setup

Install Python 3.10+ and then run:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

## Data

The repository currently contains:

```text
data/HDFS.log
alibaba.zip
google cluster trace.zip
models/xgboost_pipeline_monitor.pkl
outputs/metrics.json
```

To retrain on HDFS, add the LogHub label file:

```text
data/anomaly_label.csv
```

The expected label columns are `BlockId` and `Label`, where labels are `Normal` or `Anomaly`.

The project can stream the compressed Alibaba and Google archives directly.
They do not need to be unpacked.

Local compressed trace support:

```text
alibaba.zip
  -> Alibaba PAI GPU trace v2020
  -> pai_job_table.csv + pai_task_table.csv
  -> successful jobs: Terminated
  -> failed jobs: Failed

google cluster trace.zip
  -> prejoined Google Borg trace CSV
  -> borg_traces_data.csv
  -> label column: failed
```

## Train the Model

Train on HDFS using early-warning snapshots:

```powershell
python main.py train --source hdfs --max-lines 500000
```

Customize the partial-log windows:

```powershell
python main.py train --source hdfs --snapshot-fractions 0.2,0.4,0.6,0.8,1
```

Train on Google Cluster job events:

```powershell
python main.py train --source google --max-rows 100000
```

Train source-specific models directly from the compressed production traces:

```powershell
python main.py train --source alibaba --max-rows 10000 --full-log-only
python main.py train --source google2019 --max-rows 50000 --full-log-only
```

These write separate model artifacts and output folders:

```text
models/xgboost_pipeline_monitor_alibaba.pkl
models/xgboost_pipeline_monitor_google2019.pkl
outputs/alibaba/
outputs/google2019/
```

The compressed-trace adapters deliberately exclude terminal outcome fields
from predictor inputs. Alibaba uses planned-task and observed-start features.
Google uses a random sampled runtime usage snapshot. Final status is used only
as the supervised learning label.

## Compare Machine Learning Algorithms

Use this to satisfy the project objective that compares multiple models before
settling on the deployed XGBoost predictor.

Quick comparison using the built-in labeled synthetic pipeline benchmark:

```powershell
python main.py compare --source synthetic
```

Comparison using ETL-style logs that match the live attach demo:

```powershell
python main.py compare --source etl --synthetic-loops 20
```

Comparison on HDFS, after adding `data/anomaly_label.csv`:

```powershell
python main.py compare --source hdfs --max-lines 500000
```

Comparison on the compressed production traces:

```powershell
python main.py compare --source alibaba --max-rows 5000 --full-log-only
python main.py compare --source google2019 --max-rows 10000 --full-log-only
```

Their comparison artifacts are kept separately:

```text
outputs/comparison_alibaba/
outputs/comparison_google2019/
```

The dashboard Model Comparison tab includes a dataset selector for the demo,
Alibaba, and Google comparison artifacts.

The comparison evaluates:

```text
Majority baseline
Logistic Regression
Decision Tree
Random Forest
HistGradientBoosting
XGBoost
```

Outputs are written to:

```text
outputs/model_comparison.csv
outputs/model_comparison.json
outputs/model_comparison.png
```

The synthetic comparison is useful for demonstration and dashboard evidence.
For the final report, rerun the comparison on a labeled benchmark dataset such
as LogHub HDFS, Google Cluster Trace, or another labeled pipeline failure
dataset.

## Run a Demo Without Reading the Whole 1.5 GB Log

```powershell
python main.py monitor --file data/HDFS.log --source hdfs --max-lines 5000 --min-lines 5 --output outputs/demo_predictions.json
```

`--max-lines` stops after a small number of log lines.

`--min-lines` makes the monitor predict earlier, which is useful for demos.

## Live Pipeline Simulation Demo

Use this when you want to show the project working like a real-time monitoring
plugin instead of only processing a saved dataset.

Open two PowerShell terminals in the project root.

Terminal 1 starts the monitor:

```powershell
python main.py monitor --file simulated/live_pipeline.log --source hdfs --live --min-lines 5 --demo-risk-calibration
```

Terminal 2 starts a small simulated data pipeline that writes HDFS-style logs:

```powershell
python demo_pipeline.py --scenario mixed
```

The simulator creates `simulated/live_pipeline.log`, writes log lines slowly,
and includes normal, recovering, and risky pipeline blocks. The monitor watches
the same file and emits failure-risk predictions as new log lines arrive.
When the simulator finishes, it writes a completion marker so the live monitor
flushes any remaining predictions and stops automatically.

`--demo-risk-calibration` is only for the synthetic presentation demo. It uses
demo-specific alert mapping so the full LOW/MEDIUM/HIGH behavior is visible on
the simulated pipeline.

For normal monitoring, run without that flag:

```powershell
python main.py monitor --file simulated/live_pipeline.log --source hdfs --live --min-lines 5
```

Normal monitoring now uses stream risk calibration by default. If you want the
old raw model probability behavior for comparison, add `--raw-model-risk`.

The simulator appends by default so it is safe to start after the monitor. If
you want to clear the old demo log, run the simulator once with `--reset-log`
before starting the monitor.

Useful scenarios:

```powershell
python demo_pipeline.py --scenario normal
python demo_pipeline.py --scenario warning
python demo_pipeline.py --scenario recovering
python demo_pipeline.py --scenario risky
python demo_pipeline.py --scenario mixed
```

`mixed` is best for presentation because it writes stable, warning, recovering,
error, and fatal blocks into the same live log file. This gives the monitor more
varied feature patterns instead of one repeated result.

For a faster demo:

```powershell
python demo_pipeline.py --scenario mixed --delay 0.2 --loops 2
```

## Visual Simulation Dashboard

Use this for the presentation view instead of the console. The dashboard starts
a simulated pipeline or a real CSV ETL demo, streams each generated log line
into the predictor plugin, shows LOW/MEDIUM/HIGH risk updates, optionally
auto-stops on HIGH risk, and generates final JSON/HTML reports.

```powershell
python -m streamlit run dashboard_app.py
```

Dashboard controls:

- `Demo type`: attach live command, monitored ETL pipeline, or visual simulator.
- `Scenario`: normal, warning, recovering, risky, or mixed.
- `Pipeline command`: the external job to run when using attach mode.
- `HIGH risk policy`: review gate, advise the operator, or auto-stop the pipeline.
- `Risk Timeline`: live failure-probability changes as log evidence accumulates.
- `Live playback`: run continuously, or pause and step through one log event at
  a time.
- `Generate report`: save/download a complete session report.

In `Review gate` mode, a HIGH-risk prediction holds the attached pipeline and
shows operator choices in the dashboard:

```text
Check process -> inspect the risky process, recent logs, explanation, and metrics
Continue pipeline -> resume the running process
Stop pipeline -> terminate the process
```

The dashboard reuses:

```text
live_pipeline_runner.py / simulation_engine.py -> PredictorPlugin -> report output
```

This keeps the visual demo aligned with the same predictor plugin used by the
console monitor.

## Attach the Plugin to a Live Pipeline Command

This is the closest version to the final defence vision. A normal pipeline
process runs as a separate command. It does not import the predictor. The
project attaches to the process, captures stdout/stderr logs, converts them
into the predictor's parseable log format, collects CPU/memory/disk metrics,
and stops or advises when HIGH risk is detected.

Default attached demo:

```powershell
python main.py attach --stop-policy advise --min-lines 3
```

Auto-stop HIGH risk demo:

```powershell
python main.py attach --stop-policy auto_stop --min-lines 3 -- python examples/real_pipeline_job.py --scenario risky --delay 0.25
```

For the visual defence flow, prefer the dashboard with `HIGH risk policy` set
to `Review gate`. That gives the operator the realistic choice to inspect the
HIGH-risk process, continue the pipeline, or stop it.

Use the `mixed` attached scenario to demonstrate varied live assessments:

```text
MEDIUM 52% -> LOW 27% -> HIGH 84% -> MEDIUM 63% -> LOW 19%
```

The attached adapter keeps one stable process ID for the whole batch job, so
warnings, retries, and errors accumulate naturally as the job moves through
Extract, Validate, Transform, and Load. Synthetic dashboard runs add varied,
unique integer presentation scores while preserving the raw model probability
inside each alert payload.

The unmonitored demo pipeline lives at:

```text
examples/real_pipeline_job.py
```

The attaching layer lives at:

```text
live_pipeline_runner.py
```

It writes:

```text
simulated/attached_pipeline.raw.log
simulated/attached_pipeline.normalized.log
outputs/attached_pipeline_monitoring_report_*.json
outputs/attached_pipeline_monitoring_report_*.html
```

To train an ETL-style demo model for this attached flow:

```powershell
python main.py train --source etl --synthetic-loops 20
```

Then run the attached demo with that model artifact:

```powershell
python main.py attach --model-path models/xgboost_pipeline_monitor_etl.pkl --stop-policy auto_stop --min-lines 3 -- python examples/real_pipeline_job.py --scenario risky
```

## Monitored ETL Pipeline Demo

Use this when you want to show the project behaving like a real data pipeline
instead of only a synthetic log stream.

```powershell
python main.py etl-demo --scenario normal --stop-policy advise
python main.py etl-demo --scenario risky --stop-policy auto_stop
```

The ETL job reads:

```text
data/sample_orders.csv
```

Then it runs:

```text
Extract -> Validate -> Transform -> Load -> Report
```

Every stage emits HDFS-style logs. `PredictorPlugin` ingests those logs while
the job is running. If the plugin sees HIGH risk:

- `--stop-policy advise` finishes the pipeline but reports that the operator
  should stop and inspect the process.
- `--stop-policy auto_stop` stops the pipeline before the load completes.

Reports are written to:

```text
outputs/etl_pipeline_monitoring_report_*.json
outputs/etl_pipeline_monitoring_report_*.html
```

The curated ETL output is written to:

```text
outputs/curated_orders.csv
```

## Predictor Plugin API

The reusable plugin wrapper lives in:

```text
predictor_plugin.py
```

Minimal integration example:

```python
from predictor_plugin import PredictorPlugin

plugin = PredictorPlugin(min_lines=5, stop_policy="auto_stop")

for line in running_pipeline_logs:
    alerts = plugin.ingest_log(line, metadata={"stage": "Load"})
    if plugin.should_stop():
        break

summary = plugin.finalize(status="stopped" if plugin.should_stop() else "completed")
```

This is the interface to describe in the report as the project plugin. The
older `PipelineMonitor` remains the lower-level model monitor.

## Airflow Orchestration Demo

Airflow is useful for showing how the plugin fits into a production-style data
pipeline. The repo includes a DAG at:

```text
dags/pipeline_failure_monitor_demo.py
```

To display it in Airflow, make sure Airflow can see this `dags` directory and
can import the project files. One option is to point Airflow at this repo:

```powershell
$env:PIPELINE_PROJECT_ROOT = "C:\Users\HP\Downloads\Final year project"
$env:AIRFLOW__CORE__DAGS_FOLDER = "C:\Users\HP\Downloads\Final year project\dags"
```

Then start Airflow and open:

```text
http://localhost:8080
```

Look for:

```text
pipeline_failure_monitor_demo
```

Trigger the DAG and choose `demo_type=etl` or `demo_type=synthetic`, then choose
the scenario, stop policy, and prediction threshold. Use `demo_type=attached`
to run a real command through `LivePipelineRunner`; the `pipeline_command`
parameter controls what Airflow launches. The Airflow DAG runs the same
attach/ETL/simulation engines and predictor plugin, then prints the
intervention decision and report path in task logs.

## Live Monitoring

Use live mode when another process is actively writing to a log file:

```powershell
python main.py monitor --file path\to\pipeline.log --source hdfs --live
```

If the writer appends the line `__PIPELINE_MONITOR_STOP__`, the monitor treats
that as pipeline completion, flushes remaining buffers, prints the session
summary, and exits automatically.

## Plugin-Style Usage

```python
from pipeline_monitor import PipelineMonitor

monitor = PipelineMonitor(source="hdfs", print_alerts=False, min_lines=10)

for raw_line in pipeline_log_stream:
    alerts = monitor.ingest_line(raw_line)
    for alert in alerts:
        if alert["risk_level"] == "HIGH":
            print("High failure risk:", alert)
```

## Evaluate Saved Metrics

```powershell
python main.py evaluate
python main.py evaluate --source alibaba
python main.py evaluate --source google2019
```

The stored evaluation metrics are in:

```text
outputs/metrics.json
```

Charts are saved in:

```text
outputs/
```

## Current Model Snapshot

The saved metrics currently show:

- Accuracy: 0.9305
- Recall: 0.5995
- F1 score: 0.4537
- ROC-AUC: 0.7919
- PR-AUC: 0.6218

For a final report, emphasize that recall and PR-AUC matter more than accuracy because failures are usually much rarer than successful jobs.
