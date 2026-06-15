from copy import deepcopy
from pathlib import Path
import re

from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.shared import Inches, Pt


ROOT = Path(r"C:\Users\HP\Downloads\Final year project")
SOURCE = ROOT / "not_useful" / "documents" / "expanded_final_defence" / "Ajibola Final Defence Report Adjusted.docx"
OUTPUT = ROOT / "not_useful" / "documents" / "expanded_final_defence" / "Ajibola Final Defence Report Expanded APA Clickable.docx"


PARAGRAPH_REPLACEMENTS = {
    "Although the classic data pipeline designs are resilient in their respective design environments, they are under increasingly straining pressure to support modern data streams that are of high velocity, volume and variety. These pipelines, which are typically based on batch-oriented models of processing, cannot satisfy the requirements of real-time analytics, and often must be actively monitored and manually interfered with to address errors, optimization, and maintenance. The data environments of the present day have been further complicated by the fact that data pipelines have to accommodate data of both structured and unstructured formats, schema evolution, adhere to the ever-stricter data protection regulations, and ensure data quality regardless of whether the data source is homogeneous (Cambronero et al., 2024).":
    "Although classic data pipeline designs remain useful in many batch-oriented environments, they are under increasing pressure to support high-volume, high-velocity, and diverse data streams. Modern pipelines often combine structured and semi-structured sources, changing schemas, cloud storage, scheduled transformations, and downstream analytics dependencies. These conditions make manual monitoring less reliable because a small ingestion, cleaning, or transformation defect can move silently into other stages of the pipeline before the issue becomes visible to users (Chanda, 2024).",

    "Recent studies have proven that the errors related to data in pipelines are mainly due to the wrong types of data (33%), and that these errors take place mainly during the data cleaning phase of pipelines (35%), then during the ingestion phase (34%) (Foidl et al., 2024). The analysis of 600 issues across 11 GitHub projects demonstrated that the problem of compatibility is another area of concern, despite the presence of issues related to the conventional data pipeline processing domains, such as data loading, ingestion, integration, cleaning and transformation. These results highlight the immense importance of automated monitoring and failure prediction systems to ensure that before a failure of the pipeline can occur and affect other systems and businesses downstream, the possible failure is forecasted and mitigated. Data engineering is seeing a fundamental change as a result of this shift from learning-based to rule-based ETL (Khan, 2025). One of the paradigms shifts in overcoming these challenges is the introduction of machine learning methods into the data pipeline management.":
    "Recent studies show that data-related pipeline problems often emerge around type mismatches, cleaning, ingestion, integration, and compatibility issues (Foidl et al., 2024). These findings support the need for automated monitoring and prediction because a failure can affect multiple downstream systems before it is discovered by a data engineer. The present study therefore treats machine learning as a support mechanism for early warning, not as a replacement for engineering judgement or sound pipeline design.",

    "The major processes that can be automated by Artificial Intelligence (AI) based data pipelines include resource allocation, anomaly detection, real-time analytics, and error handling, which significantly enhance scalability, cost-efficiency, and performance. It has been revealed that AI-based ETL systems are 94% accurate in automated schema mapping with a wide range of data source types, as opposed to 61% accuracy in traditional rule-based ETL systems (Gaurav, 2025). Moreover, machine learning models could forecast and evade 89% of possible data quality problems before they affect downstream systems, and AI-driven ETL pipelines automatically fix 94% of data transformation anomalies (Seenivasan, 2025).":
    "Artificial intelligence and machine learning can support pipeline management in areas such as anomaly detection, error prioritization, schema monitoring, resource awareness, and operational decision support. In this report, the role of machine learning is narrowed to log analysis and batch-job failure prediction so that the implementation remains measurable and defensible. The project does not claim full autonomous pipeline repair; it demonstrates how predictive evidence can be presented to an operator before or during a risky execution state (Seenivasan, 2025).",

    "Machine learning-based log analysis and failure prediction in data pipelines is one of the new directions in data engineering. Conventional methods of pipeline monitoring presuppose the use of reactive methods, where failures are solved once they have been identified instead of implementing preventative actions (Popović, et al., 2024). In comparison, those methods based on machine learning can process historical logs, detect trends that might signal potential failures and proactive intervention measures. It has been shown that organizations with automated ETL pipelines, which include built-in intelligence, can deliver 63% faster time-to-insight as compared to organizations that rely on manual ETL development (Gangarapu & Chilukoori, 2024). Furthermore, automated pipelines that are enhanced with the use of Machine Learning (ML) have demonstrated a 40% decrease in latency during peak load situations and a 25% reduction in resource utilization (Joshi et al., 2023).":
    "Machine learning-based log analysis and failure prediction in data pipelines is therefore an important direction in data engineering. Conventional monitoring is often reactive because errors are handled after failure messages, missing outputs, or downstream quality issues appear. A predictive monitoring layer can instead summarize historical and live execution evidence, detect patterns associated with risk, and support proactive intervention before a batch job causes wider disruption (Popovic et al., 2024). Automated ETL research also supports the need for stronger data quality and governance controls within pipeline workflows (Ogunsola et al., 2022).",

    "Secondly, this study is underscored by the substantial economic costs associated with data pipeline failures and the operational burden of maintaining complex data ecosystems. There are poor data quality costs organizations an average of $12.9 million annually, with data-related incidents affecting business operations, regulatory compliance, and customer satisfaction (Zdrok, 2024). Organizations implementing AI-driven enterprise architecture solutions have experienced an average 56% reduction in data integration errors, 41% improvement in processing speeds, and 38% decrease in operational costs.":
    "Secondly, this study is underscored by the operational cost of pipeline failure and the burden of maintaining complex data ecosystems. Data-related incidents can affect reporting accuracy, compliance activities, customer-facing services, and downstream analytics (Foidl et al., 2024). The value of the proposed system is therefore not only in model accuracy, but also in the way it converts logs, metrics, and alerts into an interpretable monitoring workflow that helps engineers respond earlier (Ogunsola et al., 2022).",

    "The resource usage patterns of failed and completed jobs have sizable measurably different qualities, and these differences immediately tend to grow prior to the termination of the relevant jobs. The failures of jobs are also characterized by the level of task resubmission, nonstandard sequences of resources usage, and their untimely depletion (Alwhbi, et al., 2024). Research has discovered that unsuccessful tasks are strongly associated with the resources specified in them, indicating that one of the common patterns is that resource misallocation can predict upcoming failure.":
    "The resource usage patterns of failed and completed jobs can differ before termination, especially where retry activity, task resubmission, scheduling delay, and abnormal consumption appear in the execution history. Large-scale studies of job failures show that failed jobs may consume considerable computation before failure, which makes early detection valuable for cost control and operational reliability (Yuan et al., 2012).",

    "The time-varying frequency of failures adheres to very different distribution trends, where some windows in the history of a system are characterized by a higher rate of failures caused by resource contention, maintenance of a system or dependency on external systems. Cloud computing workload analysis suggests that the largest portion of job types in large-scale clusters is represented by batch jobs, in particular, the operational type of multi-task batches, and such jobs is highly vulnerable to particular modes of failures (Alwhbi et al., 2022).":
    "The frequency of failures may also vary across time windows because of resource contention, maintenance activities, dependency failures, and workload changes. Workload analysis in large heterogeneous GPU clusters shows that production traces can contain diverse job types and resource requirements, which supports the use of source-specific models rather than assuming that one feature schema can represent every execution environment (Weng et al., 2022).",

    "Batch job processing is a very cost-effective technique when processing large quantities of data at a time, since it is done at intervals. This in turn means that when a particular batch job fails the entire data pipeline shuts down. For large organizations, this is a huge issue, because often times batch processing is done off hours and there may not be engineers on demand that can mitigate the situation. In terms of costs, stream processing is easily more detectable than batch, but lambda architecture allows users to optimize their costs of data processing by understanding which parts of the data need online or batch processing (Mariam et al., 2015).":
    "Batch job processing remains useful because large volumes of data can be processed at scheduled intervals, often during periods of lower system demand. The disadvantage is that a failed batch job can delay an entire reporting or analytics workflow, especially when the process runs outside normal working hours. This makes early warning important because the objective is not simply to record that a job failed, but to give engineers a chance to inspect abnormal conditions while the process is still running (Chanda, 2024).",

    "These failures of data are associated with the lack of input data, corrupt files, mismatched schema, data problems, and too big volumes of data that are beyond processing capabilities. These failures are challenging, especially when a data pipeline is involved and a failure in one of the upstream jobs propagates to the other jobs. The current studies on cloud failure prediction stress data-related matters present a high percentage of job loss instances (Azmoon et al., 2022).":
    "Data failures are associated with missing input files, corrupt records, mismatched schema, invalid values, duplicated records, and volumes that exceed expected processing capacity. These failures are challenging because an upstream defect can propagate into transformation, loading, reporting, and machine-learning stages. Data pipeline quality studies therefore emphasize that failure analysis should consider where in the pipeline the issue occurs and how it affects later processing areas (Foidl et al., 2024).",

    "Problems that can occur in the system are hardware problems, network snafus, bugs in the software, platform instability, and infrastructure maintenance disruptions. The existing cloud computing clusters are based on commercial off-the-shelf products and, hence, show high hardware and software component failure rates leading to higher levels of node and application failures (Jassas, et al., 2023).":
    "System failures can include hardware faults, network interruptions, software bugs, platform instability, dependency timeouts, and infrastructure maintenance disruptions. In cloud and cluster environments, the interaction between applications, nodes, schedulers, and resource constraints makes failure prediction difficult because a job may fail for reasons that are only partially visible in application logs (Jassas & Mahmoud, 2022).",

    "Semi-supervised methods use both labeled and unlabeled data, and it takes advantage of small amounts of labeled data and large volumes of unlabeled data. This technique is used in situations that are not inexpensive or expensive in time to label data but have much unlabeled data. The main objective of semi supervised learning is to overcome the drawbacks of both supervised and unsupervised learning (Reddy & Viswanath, 2018). Semi-supervised approaches can also be used in pipeline monitoring to learn using few failure samples, but with significant logs of normally functioning pipelines. It has been demonstrated that semi-supervised methods do not provide as high detection accuracy as fully supervised methods do, but they have convenient advantages in those cases when labeled data is not available in large amounts, their effectiveness tends to be worse than the latter due to lack of knowledge on historical anomalies (Yang et al., 2021).":
    "Semi-supervised methods use both labeled and unlabeled data and are useful where labeled failure examples are limited but large quantities of normal logs are available. The main objective of semi-supervised learning is to reduce dependence on fully labeled datasets while still using the structure of available unlabeled data (Reddy et al., 2018). Semi-supervised log-based anomaly detection can be relevant to pipeline monitoring, but its results must be interpreted carefully because the absence of labeled failures can make validation difficult (Yang et al., 2021).",

    "Reinforcement learning conditions agents to make successive judgements by engaging with an environment and is rewarded or punished. It is a learning method where a software agent interacts within unknown environment, selects actions, and progressively discovers the environment dynamics (Naeem, et al,. 2020). It actively allows the model to learn based on its mistakes and success. In reinforcement learning, unlike unsupervised learning, a helping hand is available in the form of critic. Thus, reinforcement learning constitutes four elements namely critic, environment, reward or punishment, and action (Shwartz et al., 2014). Although reinforcement learning is less widely used in failure prediction, it has the potential to be used in real-time resource deployment and in dynamic scheduling to predictive failure risks.":
    "Reinforcement learning trains agents to make sequential decisions by interacting with an environment and receiving rewards or penalties. It is less central to this study than supervised learning because the present project is based on labeled success and failure outcomes rather than online reward optimization. However, reinforcement learning remains relevant to future work because it could support dynamic scheduling, resource allocation, and intervention policies after a failure-prediction system has been validated (Naeem et al., 2020).",

    "Figure 2.1: Workflow for Log Analysis and Prediction (Guy et al, 2021)":
    "Figure 2.3: Workflow for Log Analysis and Prediction",
}


INLINE_REPLACEMENTS = {
    "(Salesforce Inc., 2025)": "(Informatica, 2025)",
    "(Nishanth R.M., 2019)": "(Nishanth R. M., 2019)",
    "(Popovic et al., 2024; Ogunsola et al., 2022)": "(Popovic et al., 2024; Ogunsola et al., 2022)",
}


EXPANSIONS = {
    "3.1 Preamble": [
        "The methodology is also framed around the practical problem that batch job failure prediction must be useful before a job reaches its terminal state. For that reason, the system was not designed only as an offline notebook experiment. It was designed to connect offline training artifacts with a live monitoring surface where log lines, feature summaries, risk decisions, and operator actions can be observed during execution. This makes the methodology suitable for a final-year software engineering project because it evaluates both model behavior and the usability of the monitoring workflow.",
        "A further methodological concern was traceability. Each major claim in the implementation is connected to an artifact that can be inspected, such as source code, saved model files, metrics files, comparison charts, generated reports, dashboard views, or the Airflow DAG. This reduces the risk of presenting the dashboard as a visual mock-up. The dashboard is instead treated as the interface through which stored evidence and runtime evidence are made understandable to an examiner.",
        "The project therefore uses a layered evidence strategy. Public and local datasets support model development; the ETL demonstration supports live execution; the predictor plugin supports reusable ingestion and risk classification; the dashboard supports visualization; and the report generator supports post-run auditability. This layered strategy is consistent with the study aim because batch job failure prediction is not only a statistical problem but also an operational decision-support problem.",
    ],
    "3.2 Research Design": [
        "The applied experimental part of the design followed a supervised-learning workflow because the available data can be organized into successful and failed outcomes. Supervised learning is suitable when historical labels exist and the objective is to learn a mapping between execution features and a failure outcome. This justified the use of precision, recall, F1-score, ROC-AUC, PR-AUC, confusion-matrix counts, and related metrics, since the research question concerns the detection of failure risk rather than only the description of log events.",
        "The prototype-development part of the design followed an incremental build-and-test approach. At each stage, a working software capability was added and then checked against the project objectives. The first capability was offline feature extraction and model training. The next was model comparison. After that, the work progressed to the predictor plugin, live ETL runner, dashboard, report generator, and Airflow DAG. This sequence helped ensure that the final system was not built around a single screen but around a complete monitoring flow.",
        "XGBoost was selected as the main deployed model because gradient boosted decision trees are effective for tabular operational features and can model nonlinear interactions among counts, rates, timing indicators, resource variables, and failure keywords (Chen & Guestrin, 2016). Review literature also supports the continued relevance of XGBoost in applied machine-learning classification tasks (Arif et al., 2023). The selection was methodologically cautious because XGBoost was not treated as automatically superior. It was compared with logistic regression, decision tree, random forest, histogram gradient boosting, and a majority baseline, so that the final interpretation could distinguish the selected implementation from the broader comparison evidence.",
        "The comparison design also reflects the diversity of machine-learning methods. Logistic regression provides a simple linear baseline (Maalouf, 2011). Decision-tree methods provide interpretable rule-based splits (Quinlan, 1986), and applied predictive work continues to show their usefulness in classification tasks (Matzavela & Alepis, 2021). Recent survey evidence also describes decision trees as a continuing foundation for interpretable machine-learning applications (Mienye & Jere, 2024). Random forests provide ensemble averaging over trees (Breiman, 2001), while support-vector approaches represent margin-based classification traditions (Cortes & Vapnik, 1995). K-nearest neighbor was considered in the wider literature review as a supervised-learning method, but it was not selected for the implemented comparison because the project focused on models better suited to the saved tabular and tree-based workflow (Suyal & Goyal, 2022).",
    ],
    "3.3 Population of the Study and Data Sources": [
        "The population was not treated as human respondents, survey participants, or manually selected opinions. It was treated as operational evidence generated by computational jobs and data pipeline processes. This distinction is important because the unit of analysis is a job, task, log sequence, or observation window rather than a person. The study therefore uses records that describe execution behavior, severity levels, resource usage, task outcomes, and observable progress through pipeline stages.",
        "The public workload traces were used to broaden the evidence base beyond the small ETL demonstration. The Alibaba trace provides heterogeneous GPU-cluster workload evidence and is appropriate for studying large-scale job behavior (Alibaba Group, 2022). Workload analysis of large heterogeneous GPU clusters further supports the importance of examining source-specific execution behavior (Weng et al., 2022). The Google Borg 2019 trace provides another production-style workload source with a different schema and operational context (Google, n.d.). These sources support the methodological decision to retain source-specific models rather than force all observations into one artificial schema.",
        "The HDFS-compatible source and the ETL demonstration have a different role. They support log-line grouping, streaming feature extraction, and visible risk movement inside the dashboard. Public log collections such as Loghub show why system logs are useful for AI-driven log analytics, but the live demonstration still needed a controllable pipeline that could run safely during defence (Zhu et al., 2023). The ETL demonstration therefore acts as the bridge between research evidence and examiner-facing software behavior.",
    ],
    "3.4 Sampling Technique": [
        "Purposive bounded sampling was used because the purpose of the project was to build and demonstrate a defensible prototype, not to exhaust every row in very large public traces. This decision made the experiments feasible on a local development machine and prevented the deployed repository from depending on multi-gigabyte raw archives. The bounded samples were selected through code-based loaders, which means the sampling logic can be reviewed and repeated instead of being hidden in manual spreadsheet editing.",
        "The sampling approach also considered class imbalance. Failure records are often less frequent than successful records, and this can make a model appear strong if accuracy is the only metric used. For this reason, the study preserved the failure counts for each bounded sample and used metrics that are more sensitive to the minority class. Precision, recall, F1-score, PR-AUC, and MCC were therefore included alongside accuracy.",
        "The ETL demonstration sample had a different sampling purpose. It was not intended to represent all production pipelines. It was designed to create repeatable scenarios that show LOW, MEDIUM, and HIGH risk states, stage transitions, warning messages, recovery attempts, and report generation. This makes the demonstration sample valuable for showing system behavior, while the public traces provide broader benchmark evidence.",
    ],
    "3.5 Instrumentation and Development Tools": [
        "The implemented software prototype served as the main research instrument because it produced the model artifacts, monitoring outputs, dashboard views, and reports used in the analysis. Python was appropriate because the ecosystem includes mature data and machine-learning libraries, including pandas, NumPy, scikit-learn, and XGBoost. Scikit-learn was especially useful for splitting data, training comparison models, and computing metrics (Pedregosa et al., 2011).",
        "The evaluation tools were selected to match the project objectives. F1-score was included because it balances precision and recall and is more informative than accuracy when failures are less common (scikit-learn developers, n.d.-a). Stratified and grouped validation concepts were also important because related snapshots from the same job should not be carelessly treated as fully independent records across training and testing partitions (scikit-learn developers, n.d.-b).",
        "Streamlit was used as the dashboard framework because it allows Python-based data applications to present controls, charts, logs, tables, and downloadable outputs in a browser interface (Streamlit, n.d.). Apache Airflow was included as orchestration evidence because its DAG model represents a pipeline as ordered tasks with configurable execution behavior (Apache Software Foundation, n.d.). These tools helped convert the machine-learning experiment into a practical monitoring workflow.",
    ],
    "3.6 Data Collection and Preprocessing": [
        "Data collection was performed through reproducible loaders and generated runtime evidence. This distinction matters because the public traces and the live ETL demonstration entered the system through different paths. The public traces were read as stored datasets, while the ETL evidence was produced dynamically by a running process. Both forms of evidence were useful, but they required different preprocessing decisions.",
        "For stored datasets, preprocessing focused on schema interpretation, missing-value handling, label construction, type conversion, and feature normalization where needed. For live logs, preprocessing focused on line parsing, timestamp capture, severity extraction, job identification, component detection, and message normalization. The system therefore separates workload-trace preprocessing from streaming log preprocessing instead of pretending that every source is the same type of data.",
        "The avoidance of label leakage was an important preprocessing control. Terminal fields that directly reveal whether a job succeeded or failed were used for labels during supervised training, but they were not treated as live predictive inputs. This protects the experiment from producing artificially high scores that would not be available during real monitoring. In a prediction system, features should represent warning evidence available before or during execution, not the final answer after the job ends.",
        "Preprocessing also made the project more reproducible. The loaders, feature extractors, and demonstration scripts document how each observation becomes a model-ready record. This makes the research stronger than a manual workflow because another reviewer can inspect the code path, repeat the preparation, and understand why particular fields were included or excluded.",
    ],
    "3.6.1 HDFS-Compatible Log Preparation": [
        "The HDFS-compatible preparation was built around grouped execution evidence. Each log line was associated with a stable identifier so that the feature extractor could summarize a job or observation window over time. This grouping is important because isolated log lines may be harmless, while a sequence of repeated warnings, retries, and errors can indicate a growing failure condition.",
        "The live ETL adapter used the same principle. It converted external pipeline messages into a predictable structure containing timestamp, severity, component, stage, message, and job identity information. This allowed the monitoring logic to update features continuously without requiring the ETL pipeline itself to be rewritten as a machine-learning script.",
    ],
    "3.6.2 Alibaba PAI Preparation": [
        "The Alibaba preparation focused on workload-level records rather than natural-language log messages. Job and task information was summarized into fields such as task counts, unsuccessful task counts, scheduling delay, resource requests, and terminal status. These fields are suitable for tabular prediction because they describe the structure and outcome of computational work in a cluster environment.",
        "The preparation also respected the size of the raw trace. Large compressed archives were not committed to the deployable repository. Instead, the project retained model artifacts and comparison summaries so that the dashboard could remain lightweight while the training evidence remained explainable in the report.",
    ],
    "3.6.3 Google 2019 Preparation": [
        "The Google 2019 preparation treated the Borg trace as a separate benchmark source because its schema differs from the HDFS-compatible and Alibaba sources. The preprocessing mapped available workload fields into numeric features and retained labels suitable for failure prediction. This approach avoided the methodological error of claiming that one universal model had been trained across incompatible schemas.",
        "The Google sample also served as a stress test for interpretation. Its results were more modest than the controlled ETL experiment, which helped the report present a balanced conclusion. The model should therefore be understood as a prototype and benchmarked implementation, not as a universally validated production predictor.",
    ],
    "3.6.4 ETL Demonstration Preparation": [
        "The ETL demonstration was prepared to be small, repeatable, and safe to run during a defence session. It reads a compact sample order file, executes recognizable ETL stages, and emits log messages that represent normal progress, warnings, recoveries, and risky conditions. This gives the examiner a concrete pipeline to observe instead of only a static dataset.",
        "Because the ETL demonstration is controlled, it is also transparent. The scenarios are intentionally selectable so that the dashboard can show different operational states. The report therefore separates demonstration behavior from model-evaluation evidence, which is important for academic honesty and for avoiding overstated production claims.",
    ],
    "3.7 Feature Engineering": [
        "Feature engineering was treated as a central research activity because the quality of the model depends on the quality of the operational evidence supplied to it. Raw log text can be difficult for a tabular model to interpret directly, so the system converts logs and workload records into numeric signals. These signals represent severity, repetition, timing, resource context, and workload structure.",
        "For log-based evidence, the strongest features are those that can be observed while a process is still running. Counts of warnings and errors, ratios of abnormal messages, repeated exception keywords, timeout indicators, retry messages, and error bursts are useful because they can change before a final failure occurs. This supports early warning rather than post-mortem classification.",
        "For workload-trace evidence, the important features are different. Task counts, unsuccessful task counts, scheduling delay, requested resources, usage snapshots, and priority fields describe how a job interacts with a cluster. These features are consistent with studies showing that job outcomes can be influenced by resource use, scheduling behavior, and task-level instability (Yuan et al., 2012). Cloud-job failure prediction research also supports the value of relating job behavior to machine-learning features (Jassas & Mahmoud, 2022).",
        "The implementation deliberately kept feature sets source-specific. A feature such as failed task count is meaningful in a workload trace, while a feature such as maximum consecutive ERROR lines is meaningful in a log stream. Keeping these feature sets separate improved methodological validity because it allowed each model to learn from the evidence type that actually exists in its data source.",
    ],
    "3.8 Model Development and Validation": [
        "The model-development process began with a baseline expectation: the selected model should perform better than a majority-class classifier and should also be interpreted against alternative machine-learning methods. This was necessary because failure prediction can be distorted by class imbalance. A majority baseline can produce high accuracy by always predicting success, but such a model would be operationally useless because it would miss the failure class.",
        "Validation therefore emphasized failure-class performance. Recall shows how many actual failures are detected; precision shows how many predicted failures are truly failures; and F1-score provides a combined view when both missed failures and false alarms matter. PR-AUC was included because it is useful in imbalanced classification settings where the positive class is operationally important.",
        "ROC-AUC, specificity, MCC, log loss, and inference latency were also retained because each metric answers a different question. ROC-AUC describes ranking ability, specificity describes how well successful jobs are not falsely flagged, MCC gives a balanced single score from the confusion matrix, log loss reflects probability quality, and latency shows whether prediction is fast enough for monitoring use.",
        "The saved model artifacts are part of the validation evidence because they make the experiment repeatable. The project stores the trained model files, metrics, comparison outputs, and charts rather than relying only on screenshots. This helps distinguish the implemented model pipeline from a presentation-only dashboard.",
    ],
    "3.9 System Architecture and Implementation": [
        "The architecture follows a separation-of-concerns pattern. The ETL process generates operational events, the runner captures those events, the adapter normalizes them, the feature extractor summarizes them, the model estimates risk, the plugin stores alerts and reports, and the dashboard presents the session. This separation makes the system easier to reason about because each component has a defined role.",
        "The attachable monitoring idea is important because many real pipelines already exist before a prediction system is introduced. Requiring every pipeline to be rewritten would reduce adoption. The live runner therefore demonstrates how a predictor can sit beside an external command and observe stdout, stderr, and runtime information without turning the original process into a notebook.",
        "The architecture also supports auditability. A prediction without supporting logs, stage information, or a report is difficult for an operator to trust. The implemented system therefore stores risk counts, high-risk items, recent logs, resource summaries, stage summaries, and report outputs so that the decision can be reviewed after the session.",
    ],
    "3.9.1 External ETL Batch Process": [
        "The external ETL batch process was designed around recognizable data-engineering stages: extraction, validation, transformation, loading, and reporting. Each stage emits progress messages so that the monitoring layer can observe normal movement and abnormal behavior. This makes the demonstration more realistic than a random event generator because it follows a process structure that data engineers can recognize.",
        "The scenario controls provide repeatability. A normal scenario can show stable execution, while warning, recovering, risky, and mixed scenarios can show different alert paths. These controls help the examiner understand the intervention logic without requiring an unpredictable production incident to occur during the defence.",
    ],
    "3.9.2 Live Attachment Runner": [
        "The live attachment runner is responsible for connecting the external process to the monitoring layer. It starts the command, captures output streams, preserves recent lines, and forwards normalized records to the predictor plugin. This design shows that the prediction system can receive evidence from a process as it runs, which is closer to real monitoring than training and testing only from a saved CSV file.",
        "Runtime context is also included where available. CPU, memory, and disk samples help the dashboard show whether abnormal messages are accompanied by resource pressure. These samples are not treated as a complete infrastructure observability platform, but they improve the practical value of the prototype by adding context around the prediction.",
    ],
    "3.9.3 Predictor Plugin": [
        "The predictor plugin is the point where the monitoring workflow becomes reusable. Instead of scattering prediction logic across the dashboard, ETL script, and report generator, the plugin receives normalized records, updates the monitor, stores alerts, tracks high-risk states, and prepares report payloads. This gives the project a clearer software boundary.",
        "The plugin design also supports policy-based behavior. The same prediction can be used in advisory mode, review mode, or auto-stop mode depending on the operational setting. This is important because not every HIGH-risk prediction should automatically terminate a process; some environments require human approval, while others may prefer automated stopping when the cost of continuing is too high.",
    ],
    "3.9.4 Streamlit Dashboard": [
        "The Streamlit dashboard is the examiner-facing and operator-facing layer of the prototype. It brings together controls, stage indicators, logs, metrics, risk cards, charts, reports, and system maps in one interface. This supports the study objective of representing the prediction workflow visually, not merely printing a model output in the terminal.",
        "The dashboard was designed to make evidence visible. A user can see the selected scenario, the current stage, the risk level, recent alerts, and report outputs. This is important because trust in a failure-prediction tool depends on whether the operator can understand why the system is flagging a run as risky.",
    ],
    "3.9.5 Airflow DAG": [
        "The Airflow DAG was included to show how the monitoring process can be placed inside an orchestrated workflow. The DAG structure makes the sequence explicit: prepare a run, execute the pipeline with the predictor, evaluate the intervention decision, and publish the report. This supports the claim that the prototype can be represented as a workflow rather than as an isolated script.",
        "The DAG is also carefully scoped. It is evidence of orchestration design and local workflow integration, but it is not described as part of the Render web service. This distinction prevents deployment overclaiming while still showing that the project aligns with common pipeline orchestration practice.",
    ],
    "3.9.6 Render Deployment Preparation": [
        "Render deployment preparation focused on the web-facing Streamlit application because that is the part of the system most suitable for a lightweight hosted demonstration. The repository includes the files needed for Render to install dependencies and launch the dashboard with the correct host and port configuration.",
        "Large raw traces, local report drafts, and generated files were excluded from deployment because they increase repository size without improving the hosted demonstration. The deployed prototype is therefore intentionally lean: it uses saved model artifacts, sample data, and output charts to demonstrate the workflow while keeping heavy retraining tasks local.",
    ],
    "3.10 Risk Classification and Intervention Logic": [
        "The LOW, MEDIUM, and HIGH categories translate model probability into an operator-friendly scale. This is necessary because raw probabilities can be difficult to interpret quickly during monitoring. The categories provide a simple risk language that can be used in the dashboard, report, and intervention decision.",
        "The threshold values are intentionally transparent. A HIGH threshold of 0.70 and a MEDIUM threshold of 0.40 make the logic easy to explain during defence. However, the report also recognizes that production thresholds should be calibrated using operational cost assumptions. A missed failure and a false alarm do not have equal consequences, so final thresholds should reflect the priorities of the deployment environment.",
        "The intervention design also avoids an unsafe assumption that the model should always make the final operational decision. Review mode allows the system to flag HIGH risk while giving the operator control over whether to continue or stop. This reflects real data-engineering practice, where termination can have business consequences and may require human approval.",
    ],
    "3.11 Method of Data Analysis": [
        "Data analysis was performed at two levels. The first level was model analysis, where held-out metrics and comparison outputs were interpreted. The second level was system analysis, where implemented behavior was assessed through the dashboard, runner, plugin, report generator, and Airflow DAG. Both levels were necessary because a strong metric alone would not prove that the monitoring workflow works.",
        "The analysis also distinguishes controlled and production-style evidence. The ETL/HDFS-compatible result is expected to be stronger because the demonstration scenarios are structured and repeatable. The Alibaba and Google results are more difficult because they are heterogeneous, imbalanced, and source-specific. This distinction is carried into Chapter Four so that the findings are not overstated.",
        "The output artifacts were used as evidence rather than decoration. Metrics files, charts, generated reports, and dashboard outputs are all connected to implemented code paths. This improves the credibility of the analysis because the results are traceable to stored project files.",
    ],
    "3.12 Validity, Reliability and Reproducibility": [
        "Internal validity was supported by avoiding label leakage, retaining source-specific schemas, and comparing the selected model against alternatives. These steps reduce the risk that the results are caused by an artificial preprocessing shortcut or by an unexamined model choice. The project also reports limitations openly where evidence is bounded or demonstrative.",
        "Reliability was supported through modular implementation. Data loading, feature extraction, model training, comparison, live running, prediction, reporting, and dashboard rendering are handled by separate modules. This makes the system easier to test and revise because changes in one part of the system do not need to rewrite the entire prototype.",
        "Reproducibility was supported by saving model artifacts, metrics, charts, sample data, and configuration files. A future reviewer can inspect the files used in the dashboard and compare them with the methodology described in the report. Although full-trace retraining remains outside the deployable demonstration, the bounded experiments and saved artifacts provide a reproducible basis for the final-year project.",
    ],
    "3.13 Ethical Considerations": [
        "The study does not use human participant data, but ethical considerations still apply because operational logs can contain sensitive information in real deployments. A production version would need procedures for masking credentials, limiting access to reports, protecting identifiers, and controlling who can view or export logs. The prototype avoids these risks by using public traces and synthetic demonstration data.",
        "The report also treats model limitations as an ethical issue. A failure-prediction system can influence whether a pipeline is stopped, delayed, or escalated. For this reason, the project avoids claiming that the model is production-ready without broader validation. It presents the system as a decision-support prototype that should assist human judgement rather than replace it.",
    ],
    "3.14 Summary": [
        "In summary, the methodology connects supervised learning, prototype development, source-specific modeling, live ETL monitoring, dashboard visualization, report generation, and orchestration evidence. The chapter also explains why the system uses bounded samples, why the public traces are not merged into one universal model, and why the dashboard separates demonstration behavior from measured evaluation evidence. This provides the foundation for the result presentation in Chapter Four.",
    ],
    "4.1 Preamble": [
        "The chapter presents the results in a way that separates implementation evidence from model-performance evidence. Implementation evidence shows whether the software functions as a monitoring workflow. Model-performance evidence shows how the trained classifiers behaved on the available evaluation data. This separation is important because a dashboard may look complete even when a model is weak, and a model may score well without being integrated into a usable system.",
        "The analysis also respects the limits of each evidence source. The controlled ETL experiment is useful for demonstrating the complete monitoring flow, while the Alibaba and Google samples are useful for showing how the approach behaves on more heterogeneous workload evidence. The results are therefore interpreted as prototype and benchmark evidence, not as a claim of enterprise-wide production validation.",
    ],
    "4.2 Implemented System Output": [
        "The implemented system output demonstrates that the project moved beyond static prediction. A user can start a run, select a scenario, observe stage movement, inspect logs, watch risk counts change, review HIGH-risk events, and generate a report. These actions show that the model is embedded in a workflow that resembles how an operator might inspect a batch job.",
        "The three demonstration paths also serve different evaluation purposes. Synthetic simulation tests whether the dashboard and risk categories respond to controlled event streams. The monitored ETL path tests whether a real process can produce logs and reports while the plugin observes it. The attached command path tests whether the monitoring idea can be generalized to an external command. Together, they support the claim that the predictor is not locked to a single script.",
        "The output is especially useful for defence because it allows the examiner to see cause and effect. When warning or error messages increase, the feature extractor updates the evidence, the risk level changes, and the dashboard presents the state to the user. This makes the project easier to explain than an isolated metrics table because the relationship between pipeline behavior and prediction output is visible.",
    ],
    "4.3 Model Evaluation Results": [
        "The ETL/HDFS-compatible model achieved strong held-out results, especially on recall and F1-score. High recall is valuable in failure prediction because missed failures can be more damaging than false alarms. However, high recall must still be considered alongside precision because too many false alarms would reduce operator trust. The reported F1-score therefore provides a more balanced view than accuracy alone.",
        "The Alibaba and Google benchmark results provide a more conservative view of the model family. Their lower scores show that production-style workload traces are more difficult than a controlled ETL demonstration. This is an important finding because it prevents the report from presenting the strongest result as if it automatically applies to every environment.",
        "The classifier-comparison evidence also shows that the best model may differ by source. XGBoost was strong for the implemented live demonstration, but histogram gradient boosting and random forest performed better on the bounded Alibaba and Google comparisons respectively. This supports a data-source-specific model-selection strategy rather than a rigid rule that one algorithm should always be used.",
    ],
    "4.4 Interpretation of the Evaluation": [
        "The results should be interpreted through the purpose of the project. The objective was to build a working prototype that predicts and visualizes batch-job failure risk, not to publish a universal model for all cluster workloads. From that perspective, the ETL/HDFS-compatible result shows that the implemented live demonstration is technically consistent, while the benchmark results show the need for further validation before production use.",
        "The controlled ETL result is valuable because it proves the end-to-end path from logs to features, prediction, risk classification, review controls, and report generation. At the same time, the bounded production-trace results remind the reader that more diverse environments can reduce model performance. The correct interpretation is therefore constructive rather than exaggerated: the prototype works, but broader validation remains necessary.",
        "The comparison also highlights the danger of relying only on accuracy. In imbalanced failure-prediction problems, a model can classify many successful jobs correctly and still miss failures. For that reason, recall, precision, F1-score, PR-AUC, and MCC are more relevant to the failure-class question. This supports the metric choices described in the methodology.",
    ],
    "4.5 Dashboard Analysis": [
        "The dashboard provides the clearest evidence that the project has an operator-facing workflow. The run-control area allows the user to select a mode and scenario, while the monitoring panels show the current state of the process. This makes the system interactive and inspectable, which is important for a final-year defence because the examiner can follow the pipeline as it moves through its stages.",
        "The log stream and stage board help connect the prediction to observable behavior. A HIGH-risk state is more understandable when it appears beside warning lines, failed validation messages, retry attempts, or stage delays. This reduces the black-box feeling of the model because the operator can see the operational signals surrounding the prediction.",
        "The review controls are also a significant result. They show that the project does not treat model output as an automatic command in every case. Instead, the system can pause for human review, allow continuation, or stop according to policy. This is important in real operations because stopping a pipeline can protect data quality but may also delay downstream business processes.",
        "The model-comparison and report sections extend the dashboard beyond live viewing. They allow the user to inspect stored evidence and export a monitoring report. This supports accountability because the session can be reviewed after the dashboard run ends.",
    ],
    "4.6 Airflow Orchestration Evidence": [
        "The Airflow DAG demonstrates that the monitoring logic can be represented as a sequence of workflow tasks. This is important because data pipelines are often orchestrated rather than executed as one manual command. By defining preparation, monitored execution, intervention evaluation, and report publishing as tasks, the project shows how the predictor can fit into a broader workflow model.",
        "The DAG parameters also support experimentation. Values such as demo type, command, scenario, loop count, minimum lines, stop policy, and failure-on-high behavior can be configured for different runs. This gives the workflow flexibility and makes the orchestration evidence more than a static file.",
        "The interpretation remains deliberately cautious. The DAG is included as local orchestration evidence, while the Render deployment focuses on the Streamlit dashboard. This distinction is important because deploying Airflow as a production scheduler would require additional infrastructure, security, persistence, and worker configuration.",
    ],
    "4.7 Report Generation Evidence": [
        "Report generation is a key part of the system because operational monitoring should leave a reviewable record. A dashboard session is temporary, but a JSON or HTML report can preserve what happened during the run. This supports accountability and makes the system more useful for post-run diagnosis.",
        "The JSON report is valuable for structured inspection because it stores fields that can later be loaded by another tool or analyzed programmatically. The HTML report is valuable for human review because it presents session status, risk counts, high-risk items, logs, and resource summaries in a readable format. Together, they support both machine-readable and operator-readable evidence.",
        "The report payload also connects the prediction to the intervention decision. When HIGH risk appears, the report can preserve the alert details and the policy context. This helps distinguish between a model score, an operator decision, and a final pipeline outcome.",
    ],
    "4.8 Deployment Readiness Analysis": [
        "Deployment readiness was assessed from the perspective of a lightweight defence demonstration. The repository contains the files needed for a Streamlit service to run on Render, including dependency declarations, a Python version file, and a Render configuration. This means the visual dashboard can be hosted without requiring the examiner to install the full local environment.",
        "The deployment decision is intentionally conservative. Raw public traces, generated reports, installer files, and local document drafts were excluded because they increase size and complexity. A deployable prototype should include only the artifacts required for demonstration: source code, model files, sample data, and saved charts. This conservative approach is also consistent with broader data-warehousing discussions that emphasize cloud readiness, scalability, and maintainable data architecture (Srikanth & Vishnu, 2024).",
        "The project is not described as a complete enterprise deployment. Production readiness would require authentication, authorization, secret management, persistent storage, logging retention, model registry support, continuous monitoring, retry handling, and infrastructure-level observability. The current deployment evidence is therefore suitable for a final-year prototype but not for unsupported production use.",
    ],
    "4.9 Summary of Findings": [
        "The findings show that the project achieved a practical integration of model evidence and system behavior. The implemented workflow can run a pipeline, collect logs, update risk, show intervention controls, and generate reports. The evaluation results show strong controlled ETL performance and more modest benchmark performance, which together provide a balanced view of the prototype.",
        "The strongest contribution is the end-to-end monitoring concept. Instead of presenting failure prediction as an isolated classifier, the project presents it as a plugin-style workflow that supports observation, decision-making, and auditability. The main weakness is that full production-scale validation and multi-user deployment remain outside the present scope.",
    ],
    "5.1 Summary": [
        "The study addressed the problem of detecting and presenting failure risk in data pipeline batch jobs. It combined machine-learning model development with a software prototype that can observe logs, extract features, classify risk, display evidence, and generate reports. This combination is important because failure prediction is useful only when the prediction reaches an operator in a form that can guide action.",
        "The project also improved the defensibility of the work by separating evidence layers. Offline training and comparison outputs provide model evidence, the ETL demonstration provides runtime evidence, the predictor plugin provides reusable monitoring behavior, the Streamlit dashboard provides visual evidence, and the Airflow DAG provides orchestration evidence. These layers make the project stronger than a single notebook or static interface.",
        "The results show that the controlled ETL/HDFS-compatible model performed strongly, while the bounded Alibaba and Google benchmarks were more challenging. This balanced outcome is important because it shows that the prototype is promising but still requires broader validation before stronger generalization claims can be made.",
    ],
    "5.2 Conclusion by Objectives": [
        "The first objective, which concerned the identification of failure patterns, was achieved through feature engineering. The system extracts severity counts, error and warning rates, failure keywords, retry evidence, timing features, and workload-structure features. These features make raw operational evidence usable for supervised learning and live monitoring.",
        "The second objective, which concerned model comparison, was achieved through saved comparison outputs. The study evaluated multiple classifiers and retained the results so that the selected implementation could be discussed against alternatives. This is important because the best model was not assumed without comparison.",
        "The third objective, which concerned the development of a monitoring predictor, was achieved through the predictor plugin, live runner, and dashboard integration. The system can receive log evidence, update risk classification, store alerts, and present the state to the user. The fourth objective, which concerned evaluation, was achieved using metrics such as precision, recall, F1-score, ROC-AUC, PR-AUC, MCC, specificity, and confusion-matrix counts. The fifth objective, which concerned visualization, was achieved through the Streamlit dashboard and Airflow workflow evidence.",
    ],
    "5.3 Recommendations": [
        "The first recommendation is to treat the current system as a validated prototype rather than a finished enterprise platform. It is suitable for demonstration, research discussion, and further development, but a production deployment should include stronger operational controls such as authentication, persistent storage, model registry support, and structured logging.",
        "The second recommendation is to continue separating raw model probability from demonstration presentation values. This distinction protects the academic honesty of the work. The dashboard can remain visually clear during defence while the report still preserves the raw model evidence used for evaluation.",
        "The third recommendation is to expand validation before deployment in a real organization. Larger and repeated experiments should be carried out on additional traces, real pipeline logs, and different failure categories. This will help determine whether the thresholds, features, and selected model remain effective outside the controlled demonstration.",
        "The fourth recommendation is to improve explainability only after the base monitoring workflow remains stable. Methods such as SHAP can help show which features influenced a prediction, but explanations should be presented as model explanations rather than proof of the true root cause. Root-cause confirmation still requires engineering investigation.",
    ],
    "5.4 Limitations of the Study": [
        "One limitation is the controlled nature of the live ETL demonstration. Although it includes recognizable stages and realistic log messages, it does not include all the dependencies of a real enterprise pipeline, such as distributed storage, message queues, authentication systems, external service failures, or multiple simultaneous workloads. This limits the extent to which the live demonstration can be generalized.",
        "Another limitation is the bounded size of the benchmark samples. The Alibaba and Google traces are useful because they broaden the evidence base, but the current experiments do not exhaust the full public datasets. Larger repeated experiments would be required to make stronger claims about general performance across those traces.",
        "A further limitation is threshold calibration. The LOW, MEDIUM, and HIGH thresholds are explainable and useful for demonstration, but production environments should calibrate them using actual business costs. For example, the cost of missing a failure may be higher than the cost of investigating a false alarm in some systems, while the reverse may be true in others.",
        "The deployment limitation is also important. Render deployment supports the Streamlit dashboard, but it does not provide a complete production architecture with Airflow workers, persistent databases, user access management, or continuous retraining. The hosted version should therefore be understood as a web demonstration of the prototype.",
    ],
    "5.5 Suggestions for Further Work": [
        "Future work should evaluate the system with real organizational pipeline logs after privacy and access controls have been addressed. Such evaluation would help confirm whether the feature engineering approach remains useful when logs include inconsistent message formats, missing fields, operational noise, and business-specific failure modes.",
        "Another direction is to package the predictor as a formally documented plugin or service. A clear API, configuration file, installable package, and versioned model artifacts would make it easier to connect the monitor to other pipelines without copying project scripts manually.",
        "Future work should also add persistent monitoring history. Storing alerts, decisions, reports, and outcomes in a database would allow trend analysis, threshold tuning, model drift detection, and post-incident review. This would move the prototype closer to an operations console.",
        "A final direction is to compare engineered-feature methods with sequence-aware log models. Deep learning approaches such as log-sequence anomaly detection can capture order and context in log streams (Du et al., 2017). However, future research should compare such methods carefully because greater complexity must be justified by better operational performance, interpretability, and maintainability.",
    ],
    "5.6 Final Conclusion": [
        "The final conclusion is that the project successfully demonstrates a defensible prototype for log analysis and failure prediction in data pipeline batch jobs. It combines supervised learning, source-specific model artifacts, live ETL monitoring, dashboard visualization, HIGH-risk review logic, report generation, and orchestration evidence. This combination directly supports the project aim because it shows not only that failure risk can be estimated, but also that the estimate can be presented within a practical monitoring workflow.",
        "The project should be understood as a foundation for further development. Its strongest academic value is the clear integration of model evidence and operational workflow, while its main practical limitation is the need for broader validation and production hardening. With larger datasets, calibrated thresholds, persistent storage, authentication, and extended plugin packaging, the system could become a stronger tool for proactive data-pipeline reliability management.",
    ],
}


ADDITIONAL_EXPANSIONS = {
    "3.8 Model Development and Validation": [
        "The validation process was also designed to examine whether the model is useful in an operational setting, not only whether it can produce a high aggregate score. In batch-job monitoring, a false negative can allow a failing job to continue unnoticed, while a false positive can interrupt work unnecessarily. The evaluation therefore considered the confusion matrix as a decision-support object. True positives represent correctly detected risky jobs, false negatives represent missed risk, false positives represent unnecessary warnings, and true negatives represent stable executions that were correctly left alone.",
        "The use of a majority baseline was important because failure prediction datasets are often imbalanced. If most jobs succeed, a naive classifier can appear accurate by predicting success for nearly every case. Such a classifier would not satisfy the aim of this project because it would provide little support during the rare but important moments when failure is likely. Comparing against the majority baseline therefore helped show whether the trained models learned meaningful warning patterns rather than only the dominant class distribution.",
        "The validation outputs were saved as project artifacts so that the results could be revisited during writing, defence, and future improvement. This includes metrics, comparison tables, charts, and model files. Retaining the artifacts makes the study more transparent because the reported values are not isolated numbers typed into the report after the fact. They are connected to reproducible files created by the implementation.",
        "Model validation was also treated as source-specific. The ETL/HDFS-compatible model, Alibaba model, and Google 2019 model were not collapsed into one overall score because each source has different fields and different operational meaning. This decision improves the honesty of the evaluation. A strong result in the controlled ETL setting supports the live demonstration, while weaker benchmark results identify where additional data and tuning are still needed.",
    ],
    "3.10 Risk Classification and Intervention Logic": [
        "The risk-classification layer was necessary because raw probability values alone are not always actionable. A dashboard user may not immediately know whether a probability of 0.61 should be ignored, watched, or escalated. By converting probability into LOW, MEDIUM, and HIGH categories, the system creates a common operational language that can be understood quickly during a run. This is especially useful in a defence demonstration because the examiner can follow how model output becomes a visible monitoring decision.",
        "The threshold design also supports reviewability. Because the threshold values are stated explicitly, another researcher or engineer can adjust them and observe how the number of warnings changes. In a production setting, these thresholds should be tuned using historical incident cost, expected downtime, and tolerance for false alarms. The present project uses explainable thresholds to support demonstration and evaluation, while recognizing that production calibration would require broader operational data.",
        "The intervention logic separates prediction from action. A HIGH-risk classification identifies a state that requires attention, but the policy configuration decides whether the system advises, pauses for review, or stops the process. This separation is important because different organizations may have different risk tolerance. A financial reporting pipeline may require cautious stopping, while a low-impact experimental pipeline may prefer review before interruption.",
    ],
    "3.12 Validity, Reliability and Reproducibility": [
        "Construct validity was supported by aligning the features with recognizable operational warning signs. Severity counts, retry indicators, exception keywords, timing features, resource signals, and failed-task counts are meaningful because they describe conditions that engineers can understand. This does not prove root cause automatically, but it makes the model inputs easier to defend than arbitrary or hidden variables.",
        "External validity remains limited because the live ETL demonstration is controlled and the public trace experiments are bounded. The study therefore avoids claiming that the model will generalize to every pipeline without retraining or recalibration. The correct claim is that the implemented approach demonstrates a workable failure-prediction and monitoring architecture that can be extended to new logs and larger traces.",
        "Reliability was further supported by preserving the distinction between training artifacts and presentation artifacts. The dashboard may display rounded or calibrated values for readability during a demonstration, but the raw model probability and saved evaluation metrics remain available for academic interpretation. This distinction prevents the visual layer from being mistaken for the measured evidence.",
    ],
    "4.3 Model Evaluation Results": [
        "The strong recall recorded for the ETL/HDFS-compatible model is meaningful because the project is concerned with early recognition of risky execution. In a monitoring environment, missing a failure can be more serious than raising a warning that later proves unnecessary. However, recall must not be interpreted alone. Precision is also necessary because repeated false alarms can make operators ignore the system. The F1-score therefore provides a balanced measure of the failure-class result.",
        "The ROC-AUC and PR-AUC values add another layer to the interpretation. ROC-AUC describes the ability of the model to rank positive and negative cases across thresholds, while PR-AUC is particularly useful when the failure class is less common. This is why PR-AUC was retained in the project artifacts. It helps show whether the model remains useful when attention is focused on the minority failure class rather than the majority successful class.",
        "The Alibaba and Google results are important precisely because they are not as strong as the controlled ETL result. They show that workload traces contain more variation and that the model may need source-specific tuning. This finding strengthens the report because it demonstrates that the project does not hide difficult evidence. Instead, it uses the benchmark results to identify realistic limits and future improvement areas.",
    ],
    "4.4 Interpretation of the Evaluation": [
        "The evaluation also shows the difference between demonstration validity and generalization validity. Demonstration validity asks whether the implemented system can run, observe logs, classify risk, present evidence, and generate reports. Generalization validity asks whether the same model will perform well on a broader set of production pipelines. The project provides stronger evidence for the first question and preliminary benchmark evidence for the second.",
        "This interpretation is consistent with the selected research design. The prototype is intended to show an end-to-end monitoring concept for batch jobs using machine learning, not to replace enterprise observability platforms. The results therefore support the claim that machine learning can be integrated into a visual pipeline-monitoring workflow. They do not support a claim that the current thresholds or model files should be used unchanged in every production environment.",
    ],
    "4.8 Deployment Readiness Analysis": [
        "A useful part of deployment readiness is the separation between files needed for demonstration and files needed for research training. The hosted dashboard does not need raw multi-gigabyte trace archives to show the monitoring workflow. It needs the trained artifacts, sample data, configuration, and visual components. This distinction keeps the deployment manageable while still preserving the research explanation inside the report.",
        "The deployment analysis also identifies what would be required beyond the final-year prototype. A production version would need secure handling of logs, authentication for users, persistent storage for reports, audit trails, monitoring of the monitoring service itself, and a policy for model updates. These requirements are deliberately stated so that the report does not confuse a successful hosted demo with a complete enterprise system.",
    ],
    "5.2 Conclusion by Objectives": [
        "Overall, the objectives were achieved in a connected manner rather than as isolated tasks. Data preparation made feature extraction possible, feature extraction supported model training, model comparison justified the selected implementation, the predictor plugin connected the model to streaming logs, and the dashboard made the results visible. This chain of work is important because it shows that the project moved from data to decision support.",
        "The objective related to visualization was particularly important for the final project because it transformed the work from a technical model into a demonstrable system. The dashboard, report output, and Airflow DAG make the system easier to inspect and defend. They show not only what the model predicted, but also how the prediction fits into a recognizable data pipeline workflow.",
    ],
    "5.3 Recommendations": [
        "The system should be extended with stronger report storage. At present, reports can be generated for inspection, but a production-oriented version should store them in a database with session identifiers, timestamps, model versions, risk counts, operator decisions, and final job outcomes. This would support trend analysis and make it possible to compare predictions against later confirmed failures.",
        "Another recommendation is to introduce model-version governance. Each monitoring report should record the model name, training date, feature schema, threshold configuration, and evaluation summary used during that session. This will make future audits easier because an engineer can determine exactly which model produced a particular warning.",
        "The dashboard should also include clearer separation between operational controls and research evidence. Operational users need simple risk cards, alerts, and actions, while researchers and examiners may need metrics, comparison charts, and feature explanations. Separating these views would make the interface cleaner for each audience while preserving the evidence needed for academic review.",
    ],
    "5.4 Limitations of the Study": [
        "The study is also limited by the absence of long-term monitoring history. The prototype can demonstrate a session and generate a report, but it does not yet analyze patterns across weeks or months of repeated pipeline execution. Long-term history would be necessary to identify seasonal failure trends, recurring dependency problems, model drift, and the effect of threshold changes over time.",
        "Another limitation is that the system does not yet include formal explainability in the live dashboard. The risk explanations are based on feature evidence and alert summaries, but methods such as SHAP were not integrated into the final deployed workflow. This limits the depth of model interpretation available to the operator, although it also keeps the prototype simpler and easier to defend.",
        "A final limitation concerns operational integration. The live runner demonstrates attachment to an external command, and the Airflow DAG shows orchestration evidence, but the system has not been integrated with enterprise schedulers, data warehouses, incident-management tools, or notification channels. Such integration would be required before the system could support a real operations team.",
    ],
    "5.6 Final Conclusion": [
        "The completed work also shows that a final-year project can combine research evidence with a functioning software artifact. The literature review explains why pipeline failure prediction matters, the methodology explains how data and models were handled, the results chapter presents the implemented workflow and evaluation outputs, and the conclusion identifies the boundaries of the prototype. This progression makes the project coherent because every chapter supports the same central idea: pipeline failures should be detected early and presented in a way that supports action.",
        "The most important lesson from the study is that failure prediction should not be treated as a single model score. A useful monitoring system must connect data preparation, feature engineering, model evaluation, live observation, risk interpretation, intervention policy, and reporting. The implemented prototype brings these parts together in a clear and defensible manner. Although further validation and production hardening are still required, the work provides a strong basis for future research and practical development in intelligent data pipeline monitoring.",
        "The project is also valuable because it makes the boundary between automation and human judgement visible. The model estimates failure risk, but the operator still needs supporting evidence before making a decision. By showing logs, stages, alerts, resource samples, and reports together, the system encourages informed review rather than blind trust in automation. This is an important principle for data engineering because stopping a pipeline, allowing it to continue, or escalating it to another engineer can affect reporting schedules, business users, and downstream systems.",
        "In practical terms, the prototype demonstrates a path for improving batch-job reliability without requiring an organization to discard its existing pipelines. The live attachment design shows that a predictor can observe an external process, translate the evidence into structured features, classify the risk, and preserve the session for later inspection. This makes the work extensible. Future versions can replace the sample ETL process with organization-specific commands, connect reports to persistent storage, and tune thresholds using real incident history.",
        "For academic purposes, the study remains defensible because it states both its contribution and its limits. The contribution is the integration of predictive modeling with a visible monitoring and intervention workflow. The limit is that broader validation, stronger security controls, persistent storage, and enterprise integration are still required before production use. This balanced conclusion protects the report from overclaiming while still showing that the implemented system satisfies the core objectives of the final-year project.",
    ],
}


REFERENCES = [
    {
        "key": "Alibaba Group, 2022",
        "bookmark": "ref_alibaba_2022",
        "patterns": ["(Alibaba Group, 2022)"],
        "parts": [
            ("Alibaba Group. (2022). ", False),
            ("Cluster trace GPU v2020", True),
            (" [Data set]. GitHub. https://github.com/alibaba/clusterdata/tree/master/cluster-trace-gpu-v2020", False),
        ],
    },
    {
        "key": "Akash & Charate, 2025",
        "bookmark": "ref_akash_charate_2025",
        "patterns": ["(Akash & Charate, 2025)"],
        "parts": [
            ("Akash, V. C., & Charate, P. C. (2025). Proactive data pipeline maintenance via machine learning-driven anomaly detection. ", False),
            ("International Journal of Scientific Research in Science and Technology, 12", True),
            ("(2), 1041-1053. https://doi.org/10.32628/ijsrst251222663", False),
        ],
    },
    {
        "key": "Apache Software Foundation, n.d.",
        "bookmark": "ref_apache_nd",
        "patterns": ["(Apache Software Foundation, n.d.)"],
        "parts": [
            ("Apache Software Foundation. (n.d.). ", False),
            ("DAGs", True),
            (". Apache Airflow documentation. Retrieved June 2, 2026, from https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html", False),
        ],
    },
    {
        "key": "Arif et al., 2023",
        "bookmark": "ref_arif_2023",
        "patterns": ["(Arif et al., 2023)"],
        "parts": [
            ("Arif, A. Z., Abduljabbar, H. Z., Tahir, A. H., Bibo, S. A., & Almufti, S. M. (2023). eXtreme gradient boosting algorithm with machine learning: A review. ", False),
            ("Academic Journal of Nawroz University, 12", True),
            ("(2), 320-334. https://doi.org/10.25007/ajnu.v12n2a1612", False),
        ],
    },
    {
        "key": "Breiman, 2001",
        "bookmark": "ref_breiman_2001",
        "patterns": ["(Breiman, 2001)"],
        "parts": [
            ("Breiman, L. (2001). Random forests. ", False),
            ("Machine Learning, 45", True),
            ("(1), 5-32. https://doi.org/10.1023/A:1010933404324", False),
        ],
    },
    {
        "key": "Chanda, 2024",
        "bookmark": "ref_chanda_2024",
        "patterns": ["(Chanda, 2024)"],
        "parts": [
            ("Chanda, D. (2024). Automated ETL pipelines for modern data warehousing: Architectures, challenges, and emerging solutions. ", False),
            ("The Eastasouth Journal of Information System and Computer Science, 1", True),
            ("(3), 209-212. https://doi.org/10.58812/esiscs.v1i03", False),
        ],
    },
    {
        "key": "Chen & Guestrin, 2016",
        "bookmark": "ref_chen_guestrin_2016",
        "patterns": ["(Chen & Guestrin, 2016)"],
        "parts": [
            ("Chen, T., & Guestrin, C. (2016). XGBoost: A scalable tree boosting system. In ", False),
            ("Proceedings of the 22nd ACM SIGKDD International Conference on Knowledge Discovery and Data Mining", True),
            (" (pp. 785-794). https://doi.org/10.1145/2939672.2939785", False),
        ],
    },
    {
        "key": "Cortes & Vapnik, 1995",
        "bookmark": "ref_cortes_vapnik_1995",
        "patterns": ["(Cortes & Vapnik, 1995)"],
        "parts": [
            ("Cortes, C., & Vapnik, V. (1995). Support-vector networks. ", False),
            ("Machine Learning, 20", True),
            ("(3), 273-297. https://doi.org/10.1007/BF00994018", False),
        ],
    },
    {
        "key": "Du et al., 2017",
        "bookmark": "ref_du_2017",
        "patterns": ["(Du et al., 2017)"],
        "parts": [
            ("Du, M., Li, F., Zheng, G., & Srikumar, V. (2017). DeepLog: Anomaly detection and diagnosis from system logs through deep learning. In ", False),
            ("Proceedings of the 2017 ACM SIGSAC Conference on Computer and Communications Security", True),
            (" (pp. 1285-1298). https://doi.org/10.1145/3133956.3134015", False),
        ],
    },
    {
        "key": "Foidl et al., 2024",
        "bookmark": "ref_foidl_2024",
        "patterns": ["(Foidl et al., 2024)"],
        "parts": [
            ("Foidl, H., Golendukhina, V., Ramler, R., & Felderer, M. (2024). Data pipeline quality: Influencing factors, root causes of data-related issues, and processing problem areas for developers. ", False),
            ("Journal of Systems and Software, 207", True),
            (", Article 111855. https://doi.org/10.1016/j.jss.2023.111855", False),
        ],
    },
    {
        "key": "Google, n.d.",
        "bookmark": "ref_google_nd",
        "patterns": ["(Google, n.d.)"],
        "parts": [
            ("Google. (n.d.). ", False),
            ("Borg cluster workload traces from 2019", True),
            (" [Data set documentation]. GitHub. Retrieved June 2, 2026, from https://github.com/google/cluster-data/blob/master/ClusterData2019.md", False),
        ],
    },
    {
        "key": "Informatica, 2025",
        "bookmark": "ref_informatica_2025",
        "patterns": ["(Informatica, 2025)"],
        "parts": [
            ("Informatica. (2025). ", False),
            ("What is an ETL pipeline?", True),
            (" https://www.informatica.com/resources/articles/what-is-etl-pipeline.html", False),
        ],
    },
    {
        "key": "Jassas & Mahmoud, 2022",
        "bookmark": "ref_jassas_mahmoud_2022",
        "patterns": ["(Jassas & Mahmoud, 2022)"],
        "parts": [
            ("Jassas, M. S., & Mahmoud, Q. H. (2022). Analysis of job failure and prediction model for cloud computing using machine learning. ", False),
            ("Sensors, 22", True),
            ("(5), Article 2035. https://doi.org/10.3390/s22052035", False),
        ],
    },
    {
        "key": "Maalouf, 2011",
        "bookmark": "ref_maalouf_2011",
        "patterns": ["(Maalouf, 2011)"],
        "parts": [
            ("Maalouf, M. (2011). Logistic regression in data analysis: An overview. ", False),
            ("International Journal of Data Analysis Techniques and Strategy, 3", True),
            ("(3), 281-299.", False),
        ],
    },
    {
        "key": "Matzavela & Alepis, 2021",
        "bookmark": "ref_matzavela_alepis_2021",
        "patterns": ["(Matzavela & Alepis, 2021)"],
        "parts": [
            ("Matzavela, V., & Alepis, E. (2021). Decision tree learning through a predictive model for student academic performance in intelligent m-learning environments. ", False),
            ("Computers and Education: Artificial Intelligence, 2", True),
            (", Article 100035. https://doi.org/10.1016/j.caeai.2021.100035", False),
        ],
    },
    {
        "key": "Mienye & Jere, 2024",
        "bookmark": "ref_mienye_jere_2024",
        "patterns": ["(Mienye & Jere, 2024)"],
        "parts": [
            ("Mienye, I. D., & Jere, N. (2024). A survey of decision trees: Concepts, algorithms, and applications. ", False),
            ("IEEE Access, 12", True),
            (", 86716-86727. https://doi.org/10.1109/ACCESS.2024.3416838", False),
        ],
    },
    {
        "key": "Naeem et al., 2020",
        "bookmark": "ref_naeem_m_2020",
        "patterns": ["(Naeem et al., 2020)"],
        "parts": [
            ("Naeem, M., Rizvi, S., & Coronato, A. (2020). A gentle introduction to reinforcement learning and its application in different fields. ", False),
            ("IEEE Access, 8", True),
            (", 209320-209344. https://doi.org/10.1109/ACCESS.2020.3038605", False),
        ],
    },
    {
        "key": "Naeem et al., 2023",
        "bookmark": "ref_naeem_s_2023",
        "patterns": ["(Naeem et al., 2023)"],
        "parts": [
            ("Naeem, S., Ali, A., Anam, S., & Ahmed, M. M. (2023). Unsupervised machine learning algorithms: Comprehensive review. ", False),
            ("International Journal of Computing and Digital Systems, 13", True),
            ("(1), 911-921. https://doi.org/10.12785/ijcds/130172", False),
        ],
    },
    {
        "key": "Nishanth R. M., 2019",
        "bookmark": "ref_nishanth_2019",
        "patterns": ["(Nishanth R. M., 2019)"],
        "parts": [
            ("Nishanth R. M. (2019). The evolution of ETL architecture: From traditional data warehousing to real-time data integration. ", False),
            ("World Journal of Advanced Research and Reviews, 1", True),
            ("(3), 073-084. https://doi.org/10.30574/wjarr.2019.1.3.0033", False),
        ],
    },
    {
        "key": "Ogunsola et al., 2022",
        "bookmark": "ref_ogunsola_2022",
        "patterns": ["(Ogunsola et al., 2022)"],
        "parts": [
            ("Ogunsola, K. O., Balogun, E. D., & Ogunmokun, A. S. (2022). Developing an automated ETL pipeline model for enhanced data quality and governance in analytics. ", False),
            ("International Journal of Multidisciplinary Research and Growth Evaluation, 3", True),
            ("(1), 791-796. https://doi.org/10.54660/.ijmrge.2022.3.1.791-796", False),
        ],
    },
    {
        "key": "Pedregosa et al., 2011",
        "bookmark": "ref_pedregosa_2011",
        "patterns": ["(Pedregosa et al., 2011)"],
        "parts": [
            ("Pedregosa, F., Varoquaux, G., Gramfort, A., Michel, V., Thirion, B., Grisel, O., Blondel, M., Prettenhofer, P., Weiss, R., Dubourg, V., VanderPlas, J., Passos, A., Cournapeau, D., Brucher, M., Perrot, M., & Duchesnay, E. (2011). Scikit-learn: Machine learning in Python. ", False),
            ("Journal of Machine Learning Research, 12", True),
            (", 2825-2830. https://www.jmlr.org/papers/v12/pedregosa11a.html", False),
        ],
    },
    {
        "key": "Popovic et al., 2024",
        "bookmark": "ref_popovic_2024",
        "patterns": ["(Popovic et al., 2024)", "(Popović et al., 2024)"],
        "parts": [
            ("Popovic, A., Ivkovic, V., Trajkovic, N., & Lukovic, I. (2024). A domain-specific language for managing ETL processes. ", False),
            ("PeerJ Computer Science, 10", True),
            (", Article e1835. https://doi.org/10.7717/peerj-cs.1835", False),
        ],
    },
    {
        "key": "Quinlan, 1986",
        "bookmark": "ref_quinlan_1986",
        "patterns": ["(Quinlan, 1986)"],
        "parts": [
            ("Quinlan, J. R. (1986). Induction of decision trees. ", False),
            ("Machine Learning, 1", True),
            ("(1), 81-106. https://doi.org/10.1007/BF00116251", False),
        ],
    },
    {
        "key": "Reddy et al., 2018",
        "bookmark": "ref_reddy_2018",
        "patterns": ["(Reddy et al., 2018)"],
        "parts": [
            ("Reddy, P., Viswanath, P., & Reddy, B. E. (2018). Semi-supervised learning: A brief review. ", False),
            ("International Journal of Engineering & Technology, 7", True),
            ("(1), 81-85.", False),
        ],
    },
    {
        "key": "scikit-learn developers, n.d.-a",
        "bookmark": "ref_sklearn_f1",
        "patterns": ["(scikit-learn developers, n.d.-a)"],
        "parts": [
            ("scikit-learn developers. (n.d.-a). ", False),
            ("f1_score", True),
            (". Scikit-learn documentation. Retrieved June 2, 2026, from https://scikit-learn.org/stable/modules/generated/sklearn.metrics.f1_score.html", False),
        ],
    },
    {
        "key": "scikit-learn developers, n.d.-b",
        "bookmark": "ref_sklearn_stratifiedgroupkfold",
        "patterns": ["(scikit-learn developers, n.d.-b)"],
        "parts": [
            ("scikit-learn developers. (n.d.-b). ", False),
            ("StratifiedGroupKFold", True),
            (". Scikit-learn documentation. Retrieved June 2, 2026, from https://scikit-learn.org/stable/modules/generated/sklearn.model_selection.StratifiedGroupKFold.html", False),
        ],
    },
    {
        "key": "Seenivasan, 2025",
        "bookmark": "ref_seenivasan_2025",
        "patterns": ["(Seenivasan, 2025)"],
        "parts": [
            ("Seenivasan, D. (2025). AI-driven enhancement of ETL workflows for scalable and efficient cloud data engineering. ", False),
            ("International Journal of Engineering and Computer Science, 13", True),
            ("(20), 26837-26848. https://doi.org/10.18535/ijecs/v14i02.4824", False),
        ],
    },
    {
        "key": "Shaveta, 2023",
        "bookmark": "ref_shaveta_2023",
        "patterns": ["(Shaveta, 2023)"],
        "parts": [
            ("Shaveta. (2023). A review on machine learning. ", False),
            ("International Journal of Science and Research Archive, 9", True),
            ("(1), 281-285. https://doi.org/10.30574/ijsra.2023.9.1.0410", False),
        ],
    },
    {
        "key": "Srikanth & Vishnu, 2024",
        "bookmark": "ref_srikanth_vishnu_2024",
        "patterns": ["(Srikanth & Vishnu, 2024)"],
        "parts": [
            ("Srikanth, G., & Vishnu, C. (2024). The future of data warehousing: Trends, technologies, and challenges in the era of big data, cloud computing, and artificial intelligence. ", False),
            ("International Journal of Scientific Research in Computer Science, Engineering and Information Technology, 10", True),
            ("(5), 470-479. https://doi.org/10.32628/cseit241051029", False),
        ],
    },
    {
        "key": "Streamlit, n.d.",
        "bookmark": "ref_streamlit_nd",
        "patterns": ["(Streamlit, n.d.)"],
        "parts": [
            ("Streamlit. (n.d.). ", False),
            ("st.fragment", True),
            (". Streamlit documentation. Retrieved June 2, 2026, from https://docs.streamlit.io/develop/api-reference/execution-flow/st.fragment", False),
        ],
    },
    {
        "key": "Suyal & Goyal, 2022",
        "bookmark": "ref_suyal_goyal_2022",
        "patterns": ["(Suyal & Goyal, 2022)"],
        "parts": [
            ("Suyal, M., & Goyal, P. (2022). A review on analysis of k-nearest neighbor classification machine learning algorithms based on supervised learning. ", False),
            ("International Journal of Engineering Trends and Technology, 70", True),
            ("(7), 43-48. https://doi.org/10.14445/22315381/IJETT-V70I7P205", False),
        ],
    },
    {
        "key": "Udousoro, 2020",
        "bookmark": "ref_udousoro_2020",
        "patterns": ["(Udousoro, 2020)"],
        "parts": [
            ("Udousoro, I. C. (2020). Machine learning: A review. ", False),
            ("Semiconductor Science and Information Devices, 2", True),
            ("(2), 5-14. https://doi.org/10.30564/ssid.v2i2.1931", False),
        ],
    },
    {
        "key": "Weng et al., 2022",
        "bookmark": "ref_weng_2022",
        "patterns": ["(Weng et al., 2022)"],
        "parts": [
            ("Weng, Q., Xiao, L., Yu, C., Wang, W., Wang, C., He, J., Li, Y., Zhang, L., & Ding, W. (2022). MLaaS in the wild: Workload analysis and scheduling in large-scale heterogeneous GPU clusters. In ", False),
            ("19th USENIX Symposium on Networked Systems Design and Implementation (NSDI 22)", True),
            (" (pp. 945-960). https://www.usenix.org/conference/nsdi22/presentation/weng", False),
        ],
    },
    {
        "key": "Yang et al., 2021",
        "bookmark": "ref_yang_2021",
        "patterns": ["(Yang et al., 2021)"],
        "parts": [
            ("Yang, L., Chen, J., Wang, Z., Wang, W., Jiang, J., Dong, X., & Zhang, W. (2021). Semi-supervised log-based anomaly detection via probabilistic label estimation. In ", False),
            ("Proceedings of the International Conference on Software Engineering", True),
            (" (pp. 1448-1460). https://doi.org/10.1109/ICSE43902.2021.00130", False),
        ],
    },
    {
        "key": "Yuan et al., 2012",
        "bookmark": "ref_yuan_2012",
        "patterns": ["(Yuan et al., 2012)"],
        "parts": [
            ("Yuan, Y., Wu, Y., Wang, Q., Yang, G., & Zheng, W. (2012). Job failures in high performance computing systems: A large-scale empirical study. ", False),
            ("Computers and Mathematics with Applications, 63", True),
            ("(2), 365-377. https://doi.org/10.1016/j.camwa.2011.07.040", False),
        ],
    },
    {
        "key": "Zhu et al., 2023",
        "bookmark": "ref_zhu_2023",
        "patterns": ["(Zhu et al., 2023)"],
        "parts": [
            ("Zhu, J., He, S., Liu, P., He, Q., Xu, Y., & Lyu, M. R. (2023). Loghub: A large collection of system log datasets for AI-driven log analytics. In ", False),
            ("2023 IEEE 34th International Symposium on Software Reliability Engineering (ISSRE)", True),
            (" (pp. 355-366). https://doi.org/10.1109/ISSRE59848.2023.00071", False),
        ],
    },
    {
        "key": "Zou & Petrosian, 2020",
        "bookmark": "ref_zou_petrosian_2020",
        "patterns": ["(Zou & Petrosian, 2020)"],
        "parts": [
            ("Zou, J., & Petrosian, O. (2020). ", False),
            ("Explainable AI: Using Shapely value to explain complex anomaly detection ML-based systems", True),
            (". ResearchGate. https://www.researchgate.net/figure/DeepLog-architecture_fig1_347324639", False),
        ],
    },
]


def normalize_text(text: str) -> str:
    return " ".join(text.split())


def set_paragraph_text(paragraph, text: str) -> None:
    paragraph.text = text


def find_paragraph(doc: Document, text: str):
    for paragraph in doc.paragraphs:
        if normalize_text(paragraph.text) == text:
            return paragraph
    raise ValueError(f"Could not find paragraph: {text}")


def insert_expansion(doc: Document, heading_text: str, paragraphs: list[str]) -> None:
    paras = doc.paragraphs
    start = None
    for idx, paragraph in enumerate(paras):
        if normalize_text(paragraph.text) == heading_text:
            start = idx
            break
    if start is None:
        raise ValueError(f"Missing heading: {heading_text}")

    target = None
    for paragraph in paras[start + 1:]:
        style_name = paragraph.style.name if paragraph.style else ""
        if style_name.startswith("Heading"):
            target = paragraph
            break
    if target is None:
        target = doc.paragraphs[-1]

    for text in paragraphs:
        new_p = target.insert_paragraph_before(text, style="Body Text")
        new_p.paragraph_format.space_after = Pt(6)


def delete_paragraph(paragraph) -> None:
    element = paragraph._element
    element.getparent().remove(element)


def add_bookmark(paragraph, name: str, bookmark_id: int) -> None:
    start = OxmlElement("w:bookmarkStart")
    start.set(qn("w:id"), str(bookmark_id))
    start.set(qn("w:name"), name)
    end = OxmlElement("w:bookmarkEnd")
    end.set(qn("w:id"), str(bookmark_id))

    children = list(paragraph._p)
    insert_at = 1 if children and children[0].tag == qn("w:pPr") else 0
    paragraph._p.insert(insert_at, start)
    paragraph._p.append(end)


def add_external_hyperlink(paragraph, text: str, url: str) -> None:
    r_id = paragraph.part.relate_to(url, RT.HYPERLINK, is_external=True)
    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("r:id"), r_id)
    run = OxmlElement("w:r")
    rpr = OxmlElement("w:rPr")
    color = OxmlElement("w:color")
    color.set(qn("w:val"), "0563C1")
    underline = OxmlElement("w:u")
    underline.set(qn("w:val"), "single")
    rpr.append(color)
    rpr.append(underline)
    run.append(rpr)
    t = OxmlElement("w:t")
    if text.startswith(" ") or text.endswith(" "):
        t.set(qn("xml:space"), "preserve")
    t.text = text
    run.append(t)
    hyperlink.append(run)
    paragraph._p.append(hyperlink)


def add_internal_hyperlink(paragraph, text: str, anchor: str) -> None:
    hyperlink = OxmlElement("w:hyperlink")
    hyperlink.set(qn("w:anchor"), anchor)
    hyperlink.set(qn("w:history"), "1")
    run = OxmlElement("w:r")
    rpr = OxmlElement("w:rPr")
    color = OxmlElement("w:color")
    color.set(qn("w:val"), "0563C1")
    underline = OxmlElement("w:u")
    underline.set(qn("w:val"), "single")
    rpr.append(color)
    rpr.append(underline)
    run.append(rpr)
    t = OxmlElement("w:t")
    if text.startswith(" ") or text.endswith(" "):
        t.set(qn("xml:space"), "preserve")
    t.text = text
    run.append(t)
    hyperlink.append(run)
    paragraph._p.append(hyperlink)


def add_plain_run(paragraph, text: str) -> None:
    run = paragraph.add_run(text)
    if text.startswith(" ") or text.endswith(" "):
        for text_node in run._r.iter(qn("w:t")):
            text_node.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")


def clear_paragraph_content(paragraph) -> None:
    for child in list(paragraph._p):
        if child.tag != qn("w:pPr"):
            paragraph._p.remove(child)


def rebuild_with_citation_links(paragraph, citation_map: dict[str, str]) -> bool:
    original = paragraph.text
    if not original:
        return False
    patterns = sorted(citation_map, key=len, reverse=True)
    regex = re.compile("|".join(re.escape(pattern) for pattern in patterns))
    matches = list(regex.finditer(original))
    if not matches:
        return False

    clear_paragraph_content(paragraph)
    cursor = 0
    for match in matches:
        if match.start() > cursor:
            add_plain_run(paragraph, original[cursor:match.start()])
        citation_text = match.group(0)
        add_internal_hyperlink(paragraph, citation_text, citation_map[citation_text])
        cursor = match.end()
    if cursor < len(original):
        add_plain_run(paragraph, original[cursor:])
    return True


def add_reference_paragraph(doc: Document, ref: dict, bookmark_id: int):
    paragraph = doc.add_paragraph(style="Body Text")
    paragraph.paragraph_format.left_indent = Inches(0.5)
    paragraph.paragraph_format.first_line_indent = Inches(-0.5)
    paragraph.paragraph_format.space_after = Pt(6)
    paragraph.paragraph_format.line_spacing = 2

    for text, italic in ref["parts"]:
        url_match = re.search(r"https?://\S+", text)
        if url_match and text.strip() == url_match.group(0):
            add_external_hyperlink(paragraph, text, text.strip())
        else:
            run = paragraph.add_run(text)
            run.italic = italic
    add_bookmark(paragraph, ref["bookmark"], bookmark_id)
    return paragraph


def main() -> None:
    doc = Document(str(SOURCE))

    for paragraph in doc.paragraphs:
        normalized = normalize_text(paragraph.text)
        if normalized in PARAGRAPH_REPLACEMENTS:
            set_paragraph_text(paragraph, PARAGRAPH_REPLACEMENTS[normalized])
        else:
            updated = paragraph.text
            for old, new in INLINE_REPLACEMENTS.items():
                updated = updated.replace(old, new)
            if updated != paragraph.text:
                set_paragraph_text(paragraph, updated)

    combined_expansions = {heading: list(paragraphs) for heading, paragraphs in EXPANSIONS.items()}
    for heading, paragraphs in ADDITIONAL_EXPANSIONS.items():
        combined_expansions.setdefault(heading, []).extend(paragraphs)

    for heading, paragraphs in combined_expansions.items():
        insert_expansion(doc, heading, paragraphs)

    ref_index = next(i for i, p in enumerate(doc.paragraphs) if normalize_text(p.text) == "REFERENCES")
    for paragraph in list(doc.paragraphs)[ref_index + 1:]:
        delete_paragraph(paragraph)

    citation_map = {}
    bookmark_id = 1000
    for ref in REFERENCES:
        add_reference_paragraph(doc, ref, bookmark_id)
        bookmark_id += 1
        for pattern in ref["patterns"]:
            citation_map[pattern] = ref["bookmark"]

    ref_index = next(i for i, p in enumerate(doc.paragraphs) if normalize_text(p.text) == "REFERENCES")
    linked_count = 0
    for paragraph in doc.paragraphs[:ref_index]:
        if rebuild_with_citation_links(paragraph, citation_map):
            linked_count += 1

    doc.save(str(OUTPUT))
    print(f"saved={OUTPUT}")
    print(f"linked_paragraphs={linked_count}")


if __name__ == "__main__":
    main()
