# CSI500 Strategy Dashboard

This repository contains a local-first workflow for a CSI500 strategy dashboard, daily data updates, and backtesting.

## What this project does

- Builds and maintains `strategy_data.csv` from CSI500 index data, component stock data, and ETF turnover data
- Runs a Streamlit dashboard for current strategy status and market context
- Runs a backtest based on the same strategy engine used by the dashboard
- Supports daily incremental updates and full rebuilds

## Core files

- `dashboard.py`
  Streamlit dashboard for current strategy state, metrics, and explanations
- `backtest.py`
  Backtest script and chart output
- `strategy_engine.py`
  Shared strategy logic used by both dashboard and backtest
- `daily_update.py`
  Daily incremental update pipeline
- `build_strategy_data.py`
  Full rebuild pipeline for `strategy_data.csv`

## Core data files

- `strategy_data.csv`
  Main daily strategy input table
- `csi500_components_schedule.csv`
  Component schedule used to determine the active stock set for each date
- `adj_factor_base.csv`
  Base adjustment factor table for post-adjusted price handling
- `update_status.json`
  Status file used by the dashboard to display the latest update result

## Data directories

- `stocks_data/`
  Current CSI500 component stock files
- `stocks_archive/`
  Archived non-current component stock files

The rebuild flow reads from both directories so historical reconstruction still works after archiving.

## Strategy summary

The current strategy engine uses:

- breadth
- MA5 / MA10 / MA30
- heat_z
- ETF turnover

Two main entry modes are used:

- `Composite`: triggered when breadth is extremely weak
- `FirstNeg`: triggered on a trend pullback after consecutive gains

Signals are generated on day `T` and executed on the next trading day open (`T+1`).

## Local run

Create and use a virtual environment:

```powershell
.\.venv\Scripts\python -m pip install -r requirements.txt
```

Run the dashboard:

```powershell
.\.venv\Scripts\python -m streamlit run dashboard.py --server.port 3000
```

Run the backtest:

```powershell
.\.venv\Scripts\python backtest.py
```

Run the daily update:

```powershell
.\.venv\Scripts\python daily_update.py
```

Run a full rebuild:

```powershell
.\.venv\Scripts\python build_strategy_data.py
```

## Safety tools

Behavior baseline:

```powershell
.\.venv\Scripts\python tools\behavior_baseline.py snapshot
.\.venv\Scripts\python tools\behavior_baseline.py verify
```

Archive old component files:

```powershell
.\.venv\Scripts\python tools\archive_old_component_files.py plan
.\.venv\Scripts\python tools\archive_old_component_files.py apply
.\.venv\Scripts\python tools\archive_old_component_files.py restore
```

## Update and deployment notes

- Daily updates can be triggered by GitHub Actions
- The Streamlit app can be deployed separately on Streamlit Community Cloud
- Local-only notes and operational memory are intentionally kept out of the repository

## Repository intent

This repository keeps the project code, data workflow, and reusable tooling.

Local operational notes, personal rollback records, and local-only helper documents are not intended to be tracked here.
