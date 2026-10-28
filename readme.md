# Why Am I Waiting? Data-Driven Analysis of Waiting Times in Business Processes

A Python replication and evaluation pipeline based on **Lashkevich et al. (2023)**, *"Why Am I Waiting? Data-Driven Analysis of Waiting Times in Business Processes"* (CAiSE 2023).

## Overview

This tool ingests activity-instance event logs (CSV) and performs end-to-end waiting-time decomposition:

1. **Log Validation & Quality Reporting:** Checks schema completeness, timestamp formatting, negative durations, and same-resource overlaps.
2. **Transition Discovery:** Infers activity-pair concurrency via a Heuristics-Miner-style bidirectional temporal oracle (`closest_non_concurrent`) or sequential adjacency (`adjacent`) to link each target activity instance with its enabling predecessor(s).
3. **Mutually Exclusive Cause Decomposition:** Partitions each transition's waiting interval $[t_{\text{enabled}}, t_{\text{start}})$, without double counting, into five causes in strict priority order:
   - **Batching** (`wt_batching`)
   - **Resource Contention** (`wt_contention`)
   - **Prioritization** (`wt_prioritization`)
   - **Resource Unavailability** (`wt_unavailability`)
   - **Extraneous Factors** (`wt_extraneous`)
4. **Cycle Time Efficiency (CTE) & Counterfactual Impact:** Computes overall CTE and the counterfactual CTE achieved if each waiting-time cause were eliminated.
5. **Automated Gold-Standard Evaluation:** Compares aggregated transition reports against expected ground-truth CSVs (`manual_log_1`–`manual_log_5` and custom artificial logs), reporting full-row match rates, duration errors, Precision, Recall, and F1 scores.

---

## Requirements & Installation

- **Python:** 3.9+
- **Dependencies:** `pandas`, `numpy`, `PyYAML`, `matplotlib`

Install the required packages inside a virtual environment:

```bash
python -m venv .venv

# Windows PowerShell:
.\.venv\Scripts\Activate.ps1

# Linux / macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Usage & CLI Commands

The script `ProcessMiningAssignment.py` supports four subcommands: `validate`, `init-config`, `evaluate`, and `analyze`.

### 1. Reproduce Gold-Standard Evaluation on Manual Logs (`manual_log_1` to `manual_log_5`)
Run the evaluation pipeline against the authors' expected outputs using the default configuration (`config.yaml`):

```bash
python ProcessMiningAssignment.py evaluate --input manual_log_1.csv --expected manual_log_1_expected.csv --output-dir results_log_1 --config config.yaml
python ProcessMiningAssignment.py evaluate --input manual_log_2.csv --expected manual_log_2_expected.csv --output-dir results_log_2 --config config.yaml
python ProcessMiningAssignment.py evaluate --input manual_log_3.csv --expected manual_log_3_expected.csv --output-dir results_log_3 --config config.yaml
python ProcessMiningAssignment.py evaluate --input manual_log_4.csv --expected manual_log_4_expected.csv --output-dir results_log_4 --config config.yaml
python ProcessMiningAssignment.py evaluate --input manual_log_5.csv --expected manual_log_5_expected.csv --output-dir results_log_5 --config config.yaml
```

### 2. Reproduce Evaluation on Custom Artificial Gold-Standard Log

```bash
python ProcessMiningAssignment.py evaluate --input artificial_test_log.csv --expected artificial_test_log_expected.csv --output-dir results_artificial --config config.yaml
```

### 3. Run Real-World Analysis on `Production.csv`
For real-life manufacturing logs where shift schedules vary across resources, use `config_production.yaml` (or pass `--calendar-method inferred`):

```bash
python ProcessMiningAssignment.py analyze --input Production.csv --output-dir results_production --config config_production.yaml
```

### 4. Validate an Event Log or Generate a Default Config

```bash
python ProcessMiningAssignment.py validate --input Production.csv
python ProcessMiningAssignment.py init-config --output default_config.yaml
```

---

## Summary of Replication Results

### Synthetic Gold-Standard Benchmarks (`manual_log_1` – `manual_log_5` & `artificial_test_log`)

| Dataset | Transitions | Transition Groups | Full Row Matches | Mean Max Error (s) | Precision | Recall | F1 Score |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `manual_log_1` | 8 | 8 | 8 / 8 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |
| `manual_log_2` | 10 | 8 | 8 / 8 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |
| `manual_log_3` | 45 | 14 | 14 / 14 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |
| `manual_log_4` | 18 | 3 | 3 / 3 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |
| `manual_log_5` | 42 | 7 | 7 / 7 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |
| `artificial_test_log` | 42 | 2 | 2 / 2 (100%) | 0.0 s | 1.000 | 1.000 | 1.000 |

### Real-World Manufacturing Log (`Production.csv`)

- **Total Processing Time:** 13,743.75 hours (6.02% of total cycle time)
- **Total Waiting Time:** 214,482.98 hours (93.98% of total cycle time)
- **Baseline Cycle Time Efficiency (CTE):** 6.02% (vs. 6.81% reported in Lashkevich et al., 2023)

**Waiting Time Breakdown & Counterfactual CTE:**
*   **Resource Unavailability:** 60.73% of waiting time (130,250.97 h) → Eliminating raises CTE to 14.03% (+8.01 pp)
*   **Batching:** 14.19% of waiting time (30,442.60 h) → Eliminating raises CTE to 6.95% (+0.93 pp)
*   **Extraneous Factors:** 13.43% of waiting time (28,811.23 h) → Eliminating raises CTE to 6.89% (+0.87 pp)
*   **Prioritization:** 6.32% of waiting time (13,548.23 h) → Eliminating raises CTE to 6.40% (+0.38 pp)
*   **Resource Contention:** 5.33% of waiting time (11,429.95 h) → Eliminating raises CTE to 6.34% (+0.32 pp)

---

## Output Files Generated Per Run

Each output directory contains:

*   `transition_summary.csv`: Aggregated waiting-time decomposition per `(source_activity, source_resource, target_activity, target_resource)` transition.
*   `transition_instances.csv`: Instance-level decomposition with exact ISO interval timestamps and conservation verification.
*   `cte_summary.json`: Baseline CTE and counterfactual CTE impact per cause.
*   `data_quality_report.json`: Log statistics, null checks, and overlap counts.
*   `evaluation_metrics.json` & `expected_comparison_details.csv`: Row-by-row comparison metrics (when run in `evaluate` mode).
*   `run_metadata.json` & `run.log`: SHA-256 input hash, runtime, environment versions, and execution logs for full reproducibility.