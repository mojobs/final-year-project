$ErrorActionPreference = "Stop"

$Template = "C:\Users\HP\Downloads\Telegram Desktop\Project Student Slide Template.pptx"
$Output = "C:\Users\HP\Downloads\Final year project\Final_Year_Project_Presentation_Updated.pptx"
$Workspace = "C:\Users\HP\Downloads\Final year project\outputs\manual-20260614-ajibola-slides\presentations\proposal-slide-update"
$FeatureImportance = "C:\Users\HP\Downloads\Final year project\outputs\feature_importance.png"
$ModelComparison = "C:\Users\HP\Downloads\Final year project\outputs\model_comparison.png"

New-Item -ItemType Directory -Force -Path $Workspace | Out-Null
Copy-Item -LiteralPath $Template -Destination $Output -Force

$slides = @(
    @{
        Source=1; Kind="cover"; Title="Log Analysis and Failure Prediction for Data Pipeline Batch Jobs Using XGBoost Algorithm";
        Subtitle="Ajibola, Mojolaoluwa David | 22CG031820";
        Supervisor="Supervisor: Dr Iheanetu Olamma | Department of Computer and Information Sciences";
    },
    @{
        Source=2; Title="Presentation Outline"; Font=18;
        Body=@(
            "1. Background and problem context",
            "2. Aim, five objectives, and literature gap",
            "3. Methodology, data sources, and model validation",
            "4. System architecture, risk logic, and implementation",
            "5. Feature-importance findings and evaluation results",
            "6. Dashboard evidence, discussion, limitations, recommendations, and conclusion"
        )
    },
    @{
        Source=4; Title="Background"; Font=18;
        Body=@(
            "Modern ETL pipelines move large volumes of data across cloud storage, logs, scheduled transformations, and downstream analytics.",
            "Batch jobs often run unattended, so failures can remain hidden until reports, analytics, or dependent jobs are affected.",
            "Machine learning can support early warning by converting historical and live execution evidence into interpretable risk signals.",
            "This project narrows that idea to log analysis and batch-job failure prediction using XGBoost and supporting model comparisons."
        )
    },
    @{
        Source=5; Title="Statement of the Problem"; Font=18;
        Body=@(
            "Current monitoring often shows logs or completed failure alerts after a batch job has already degraded.",
            "Manual log analysis keeps engineers in reactive maintenance rather than proactive intervention.",
            "Pipeline defects can cascade into data-quality issues, missing outputs, analytics delays, and business decision errors.",
            "The gap: an integrated workflow that learns failure patterns, classifies live risk, and presents evidence for timely action."
        )
    },
    @{
        Source=7; Title="Aim and Objectives"; Font=15;
        Body=@(
            "Aim: Design and develop a log analysis and failure prediction system for ETL batch jobs using XGBoost.",
            "1. Identify and characterize common failure patterns in data pipeline batch jobs.",
            "2. Compare multiple machine learning algorithms for failure prediction.",
            "3. Develop a prediction model that continuously monitors batch pipeline execution.",
            "4. Evaluate the system using Precision, Recall, F1-score and supporting metrics.",
            "5. Visually represent the monitoring workflow using a dashboard and orchestration evidence."
        )
    },
    @{
        Source=9; Title="Literature Gap"; Font=16;
        Body=@(
            "Existing studies show that machine learning can support job-failure and log-anomaly detection.",
            "DeepLog demonstrates sequence-based log anomaly detection, but not an ETL-facing monitoring workflow.",
            "Cloud job-failure prediction studies show model value, but performance depends on workload source and features.",
            "Recent log-anomaly research emphasizes usability, trust, and source adaptation.",
            "Project response: source-specific models plus live ETL attachment, dashboard review controls, and generated reports."
        )
    },
    @{
        Source=8; Title="Methodological Alignment"; Font=16;
        Body=@(
            "Research design: applied experimental machine learning plus prototype-based software development.",
            "Objective 1: extract severity, keyword, retry, timing, resource, and task-failure signals.",
            "Objective 2: compare baseline, Logistic Regression, Decision Tree, Random Forest, HistGradientBoosting, and XGBoost.",
            "Objective 3: integrate the selected model with a live runner and predictor plugin.",
            "Objective 4-5: evaluate metrics and visualize workflow through Streamlit, reports, and Airflow evidence."
        )
    },
    @{
        Source=8; Title="Data Sources and Sampling"; Font=16;
        Body=@(
            "HDFS / ETL-compatible logs: primary live dashboard schema and controlled ETL demonstration.",
            "Alibaba PAI GPU trace: bounded production-style benchmark with 5,000 jobs and 273 failures.",
            "Google Borg 2019 trace: bounded benchmark with 10,000 records and 2,299 failures.",
            "Synthetic ETL evidence: 100 ETL records and 400 early-warning snapshots for repeatable defence demonstration.",
            "Large raw traces are kept out of deployment; saved models and comparison artifacts are retained."
        )
    },
    @{
        Source=8; Title="Feature Engineering"; Font=16;
        Body=@(
            "Raw logs are converted into structured features instead of being passed directly into XGBoost.",
            "Severity profile: INFO, WARN, ERROR, FATAL counts and rates.",
            "Failure indicators: exception, failed, timeout, retry, stopped, invalid, and error-burst signals.",
            "Workload context: task counts, failed-task counts, scheduling delay, requested CPU/memory/GPU, and runtime samples.",
            "Feature sets remain source-specific to avoid false merged-model claims."
        )
    },
    @{
        Source=8; Title="Model Development and Validation"; Font=15;
        Body=@(
            "XGBoost configuration: 300 estimators, max_depth 5, learning_rate 0.08, subsample 0.8, colsample_bytree 0.8, random_state 42.",
            "Split strategy: 25% held-out testing with group-held-out splitting where process/job groups exist.",
            "Cross-validation: 5-fold StratifiedGroupKFold when grouped snapshots are available; otherwise stratified row folds.",
            "Evaluation: accuracy, precision, recall, F1, ROC-AUC, PR-AUC, MCC, specificity, log loss, inference latency, and confusion matrix.",
            "Artifacts: saved model files, metrics, charts, and comparison outputs."
        )
    },
    @{
        Source=8; Title="System Architecture"; Font=16;
        Body=@(
            "External ETL process emits extract, validate, transform, load, and report logs.",
            "Live attachment runner captures stdout, stderr, and resource samples.",
            "Log adapter normalizes records for streaming feature extraction.",
            "Predictor plugin applies the trained artifact, stores alerts, and prepares report payloads.",
            "Streamlit dashboard presents stages, logs, risks, charts, review controls, and downloads."
        )
    },
    @{
        Source=8; Title="Risk Classification and Intervention"; Font=16;
        Body=@(
            "LOW risk: probability below 0.40; continue monitoring.",
            "MEDIUM risk: probability from 0.40 to below 0.70; review warnings and context.",
            "HIGH risk: probability at or above 0.70; trigger intervention review.",
            "Review mode keeps human judgement in the loop before stopping an execution.",
            "Auto-stop remains configurable for environments where the cost of continuation is too high."
        )
    },
    @{
        Source=8; Title="Feature-Importance Findings"; Font=15; Image=$FeatureImportance; ImageMode="right";
        Body=@(
            "Objective 1 is closed by the saved XGBoost feature-importance evidence.",
            "Top warning patterns: error_rate, info_count, error_count, max_error_burst, warn_count, and warn_rate.",
            "Interpretation: failure risk is driven by error concentration, abnormal bursts, warning density, and execution-volume signals.",
            "These features are observable while a batch job is still running."
        )
    },
    @{
        Source=8; Title="Model Evaluation Results"; Font=16;
        Body=@(
            "ETL/HDFS-compatible held-out model:",
            "Accuracy = 0.9250 | Precision = 0.8421 | Recall = 1.0000 | F1-score = 0.9143",
            "ROC-AUC = 0.9883 | PR-AUC = 0.9704 | MCC = 0.8584 | Log loss = 0.1222",
            "Confusion matrix: TP 16, TN 21, FP 3, FN 0.",
            "Interpretation: strong controlled-demo evidence, but not a universal production guarantee."
        )
    },
    @{
        Source=8; Title="Model Comparison"; Font=15; Image=$ModelComparison; ImageMode="right";
        Body=@(
            "The deployed model was compared against simpler and ensemble alternatives.",
            "Comparison set: Majority Baseline, Logistic Regression, Decision Tree, Random Forest, HistGradientBoosting, and XGBoost.",
            "XGBoost remains the implemented monitor model because it fits the engineered-feature artifact path.",
            "Benchmark evidence shows model choice should remain source-specific."
        )
    },
    @{
        Source=8; Title="Dashboard and Output Evidence"; Font=16;
        Body=@(
            "Dashboard evidence: run controls, stage board, log stream, risk KPIs, resource samples, and model comparison views.",
            "HIGH-risk review controls allow the operator to continue or stop a risky run.",
            "Report generator produces JSON and HTML summaries with alerts, logs, risk counts, high-risk items, and resource evidence.",
            "Airflow DAG shows orchestration evidence through prepare, run, evaluate, and publish tasks.",
            "Render deployment hosts the Streamlit dashboard as a lightweight web demonstration."
        )
    },
    @{
        Source=10; Title="Discussion of Findings"; Font=16;
        Body=@(
            "The ETL/HDFS-compatible result supports the live demonstration because the scenarios are controlled and observable.",
            "Alibaba and Google benchmark results are more modest, confirming that workload traces are harder and source-dependent.",
            "Accuracy alone is insufficient under class imbalance; recall, precision, F1-score, PR-AUC and MCC are more useful for failure detection.",
            "Compared with DeepLog and cloud job-failure studies, this project adds an ETL-facing dashboard, intervention logic, and audit reports."
        )
    },
    @{
        Source=10; Title="Limitations"; Font=16;
        Body=@(
            "The live ETL process is controlled and does not reproduce all enterprise dependencies.",
            "Alibaba and Google experiments use bounded samples rather than full-trace production validation.",
            "Risk thresholds are explainable for defence but require production cost calibration.",
            "Render deployment covers the Streamlit dashboard, not Airflow workers, authentication, persistent storage, or continuous retraining.",
            "Formal explainability such as SHAP is future work."
        )
    },
    @{
        Source=10; Title="Recommendations and Future Work"; Font=16;
        Body=@(
            "Package the predictor as a formal plugin or service with a documented API and configuration.",
            "Add persistent storage for monitoring sessions, reports, thresholds, and model-version tracking.",
            "Validate on larger and real organizational pipeline logs after privacy review.",
            "Add authentication and access control before any non-defence deployment.",
            "Compare sequence-aware log models against the current engineered-feature approach."
        )
    },
    @{
        Source=10; Title="Final Conclusion"; Font=17;
        Body=@(
            "The project delivers a defensible final-year prototype for log analysis and failure prediction in ETL batch jobs.",
            "Its main contribution is the integration of machine learning evidence with an operational workflow: logs are observed, features are extracted, risk is classified, HIGH risk triggers review, and reports are generated.",
            "The system is not presented as a finished production platform, but it is a strong foundation for proactive data-pipeline reliability management."
        )
    },
    @{
        Source=12; Title="References"; Font=12;
        Body=@(
            "Akash, V. C., & Charate, P. C. (2025). Proactive data pipeline maintenance via ML-driven anomaly detection.",
            "Chen, T., & Guestrin, C. (2016). XGBoost: A scalable tree boosting system.",
            "Du, M., Li, F., Zheng, G., & Srikumar, V. (2017). DeepLog.",
            "Foidl, H., Golendukhina, V., Ramler, R., & Felderer, M. (2024). Data pipeline quality.",
            "Jassas, M. S., & Mahmoud, Q. H. (2022). Job failure prediction in cloud computing.",
            "Ma, X. et al. (2024). Practitioners' expectations on log anomaly detection.",
            "Wang, Y. et al. (2024). Cross-system software log-based anomaly detection.",
            "Zhang, L. et al. (2024). Multivariate log-based anomaly detection for distributed database."
        )
    },
    @{
        Source=14; Kind="thanks"; Title="Thank You for Listening";
        Subtitle="Questions, comments, and suggestions";
    }
)

$frameMap = [ordered]@{
    outputSlides = @()
    omittedSourceSlides = @(
        @{ sourceSlide = 3; reason = "Optional scenario prompt not needed for final defence narrative." },
        @{ sourceSlide = 13; reason = "Appreciation content is covered verbally; final deck ends with references and Q&A." }
    )
}
for ($i = 0; $i -lt $slides.Count; $i++) {
    $frameMap.outputSlides += [ordered]@{
        outputSlide = $i + 1
        sourceSlide = $slides[$i].Source
        narrativeRole = if ($slides[$i].Title) { $slides[$i].Title } else { $slides[$i].Kind }
        reuseMode = "duplicate-slide"
        editTargets = @("Title placeholder", "Content/subtitle placeholder", "Slide number placeholder")
    }
}
$frameMap | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $Workspace "template-frame-map.json") -Encoding UTF8

@"
Task mode: template-following
Primary profile: template-following
Secondary gate: engineering-platform academic defence
Template source: Project Student Slide Template.pptx
Content source: Final YEAR PROPOSAL Revised.docx and existing custom presentation
Required proof objects: five objectives, method/data/model validation, feature importance, model evaluation, dashboard/report evidence, limitations, recommendations, final conclusion.
Font gate: all authored slide text is set to 12 pt or larger.
Known environment note: packaged artifact inspector expected Unix unzip on Windows; PowerPoint COM was used for template duplication, authoring, repagination, and export.
"@ | Set-Content -LiteralPath (Join-Path $Workspace "template-audit.txt") -Encoding UTF8

@"
Intentional deviations:
- Student template source slides are duplicated and filled with updated project content.
- Additional content slides are created by reusing the Methodology, Literature Review, Significance, Conclusion, References, and Thank You patterns to cover the revised proposal/report.
- Feature-importance and model-comparison proof images are inserted where the template had only text placeholders, because the user requested charts/figures where relevant.
- Dense slides are split across multiple template slides to keep all text at 12 pt or larger.
"@ | Set-Content -LiteralPath (Join-Path $Workspace "deviation-log.txt") -Encoding UTF8

function FirstTextShape($slide, [string[]]$namePatterns) {
    foreach ($pattern in $namePatterns) {
        foreach ($shape in $slide.Shapes) {
            if ($shape.HasTextFrame -and $shape.TextFrame.HasText -and $shape.Name -like $pattern) {
                return $shape
            }
        }
    }
    foreach ($shape in $slide.Shapes) {
        if ($shape.HasTextFrame) { return $shape }
    }
    return $null
}

function ShapeByNameLike($slide, [string]$pattern) {
    foreach ($shape in $slide.Shapes) {
        if ($shape.Name -like $pattern) { return $shape }
    }
    return $null
}

function SetShapeText($shape, [string]$text, [double]$fontSize, [bool]$bold=$false) {
    if ($null -eq $shape) { return }
    $shape.TextFrame.TextRange.Text = $text
    $shape.TextFrame.WordWrap = -1
    $shape.TextFrame.AutoSize = 0
    $shape.TextFrame.MarginLeft = 8
    $shape.TextFrame.MarginRight = 8
    $shape.TextFrame.MarginTop = 5
    $shape.TextFrame.MarginBottom = 5
    $shape.TextFrame.TextRange.Font.Size = [Math]::Max($fontSize, 12)
    $shape.TextFrame.TextRange.Font.Color.RGB = 0
    try { $shape.TextFrame.TextRange.HighlightColorIndex = 0 } catch {}
    if ($bold) { $shape.TextFrame.TextRange.Font.Bold = -1 } else { $shape.TextFrame.TextRange.Font.Bold = 0 }
}

function ResizeContentForImage($shape) {
    if ($null -eq $shape) { return }
    $shape.Left = 30
    $shape.Top = 92
    $shape.Width = 262
    $shape.Height = 260
}

function ResizeContentFull($shape) {
    if ($null -eq $shape) { return }
    $shape.Left = 30
    $shape.Top = 92
    $shape.Width = 660
    $shape.Height = 265
}

$pp = New-Object -ComObject PowerPoint.Application
$pp.Visible = [Microsoft.Office.Core.MsoTriState]::msoTrue
$pres = $pp.Presentations.Open($Output, $false, $false, $false)

try {
    $originalCount = $pres.Slides.Count
    foreach ($spec in $slides) {
        $range = $pres.Slides.Item([int]$spec.Source).Duplicate()
        $dup = $range.Item(1)
        $dup.MoveTo($pres.Slides.Count)
    }
    for ($i = $originalCount; $i -ge 1; $i--) {
        $pres.Slides.Item($i).Delete()
    }

    for ($i = 1; $i -le $pres.Slides.Count; $i++) {
        $slide = $pres.Slides.Item($i)
        $spec = $slides[$i - 1]

        if ($spec.Kind -eq "cover") {
            $title = FirstTextShape $slide @("*Title*")
            $subs = @()
            foreach ($shape in $slide.Shapes) {
                if ($shape.HasTextFrame -and $shape.Name -like "*Subtitle*") { $subs += $shape }
            }
            SetShapeText $title $spec.Title 25 $true
            if ($subs.Count -ge 1) { SetShapeText $subs[0] $spec.Subtitle 18 $false }
            if ($subs.Count -ge 2) { SetShapeText $subs[1] $spec.Supervisor 14 $false }
            continue
        }

        if ($spec.Kind -eq "thanks") {
            $title = FirstTextShape $slide @("*Title*")
            $sub = FirstTextShape $slide @("*Subtitle*")
            SetShapeText $title $spec.Title 42 $true
            SetShapeText $sub $spec.Subtitle 20 $false
            continue
        }

        $titleShape = FirstTextShape $slide @("*Title*")
        $contentShape = ShapeByNameLike $slide "*Content Placeholder*"
        $slideNumber = ShapeByNameLike $slide "*Slide Number*"

        SetShapeText $titleShape $spec.Title 34 $true
        SetShapeText $slideNumber ([string]$i) 16 $false

        $bodyLines = @($spec.Body)
        $bodyText = ($bodyLines -join "`r")
        if ($spec.ContainsKey("Image")) {
            ResizeContentForImage $contentShape
            SetShapeText $contentShape $bodyText ([double]$spec.Font) $false
            if (Test-Path -LiteralPath $spec.Image) {
                $pic = $slide.Shapes.AddPicture($spec.Image, 0, -1, 310, 104, 378, 212)
                $pic.Name = "Evidence image - " + $spec.Title
                $pic.Line.Visible = 0
            }
        } else {
            ResizeContentFull $contentShape
            SetShapeText $contentShape $bodyText ([double]$spec.Font) $false
        }
    }

    foreach ($slide in $pres.Slides) {
        foreach ($shape in $slide.Shapes) {
            if ($shape.HasTextFrame) {
                try {
                    if ($shape.TextFrame.TextRange.Font.Size -lt 12 -and $shape.TextFrame.TextRange.Text.Trim().Length -gt 0) {
                        $shape.TextFrame.TextRange.Font.Size = 12
                    }
                } catch {}
            }
        }
    }

    $pres.SaveAs($Output)
}
finally {
    $pres.Close()
    $pp.Quit()
}

Write-Output $Output
