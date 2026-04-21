# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

WPP Venture LM is an AI-powered financial intelligence platform that predicts stock price movements for media holding companies (WPP, Publicis, Omnicom). It combines Gemini-powered evolutionary model optimization (EvolveEngine), a Financial JEPA transformer architecture, and an interactive web dashboard.

## Commands

### Dashboard
```bash
python app/server.py                    # Serves on :8080, auth: admin / $APP_PASSWORD
```

### EvolveEngine
```bash
pip install -r evolve/requirements.txt
export GOOGLE_API_KEY=<key>

# Real data run (recommended)
python -m evolve.run --example xgb_wpp --bigquery --iterations 100

# Smoke test (synthetic, no API key needed)
python -m evolve.run --example xgb_wpp --difficulty medium --iterations 20

# Resume from checkpoint
python -m evolve.run --example xgb_wpp --seed-from evolve_results/final_population.json
```

### JEPA Training
```bash
docker build -t jepa jepa_training && docker run --gpus all jepa
```

### Cloud Functions Deploy
```bash
gcloud functions deploy retrain_model --runtime python311 --trigger-topic=retrain-schedule
gcloud functions deploy update_stock_data --runtime python311 --trigger-topic=daily-data-update
```

There is no formal test suite. Use `--difficulty medium --iterations 20` for smoke tests and `--bigquery` for real validation.

## Architecture

**`evolve/`** — EvolveEngine: Gemini-powered evolutionary optimizer (AlphaEvolve-inspired). Core loop in `engine.py`, MAP-elites population with island diversity in `population.py`, safe candidate evaluation with 300s timeout in `evaluator.py`. Seed programs use `# --- EVOLVE-BLOCK: <name> ---` markers to define mutable regions. Config in `config.py` (200 max iterations, 5 islands, 4 concurrent evals). Model-agnostic — works on XGBoost, PyTorch, sklearn.

**`evolve/examples/xgb_wpp_daily.py`** — XGBoost seed program with 16 features (returns, benchmark, relative, technical, domain, sentiment). Three evolution blocks: HYPERPARAMETERS, FEATURE_ENGINEERING, TRAINING_PROCEDURE. Baseline: 58% accuracy, AUC 0.587 on real BigQuery data.

**`jepa_training/`** — Financial JEPA (Joint Embedding Predictive Architecture) adapted for 1D financial time series. 5-day patch windows, transformer encoders, VICReg anti-collapse regularization. Multi-task: direction + return prediction.

**`app/`** — Dashboard served by `server.py` (HTTP Basic auth). Static HTML/JS using Chart.js and the PASE design system. Tabs: Overview, Competition, Win Streaks, Models, Simulator, Detail.

**`functions/`** — GCP Cloud Functions. `update_stock_data` runs daily (yfinance → BigQuery → ARIMA forecasts → GCS). `retrain_model` triggers on performance degradation (MAPE > 5% for 10 days, CI < 60%, or direction accuracy < 45%) or quarterly schedule.

**`evolve_results/`** — Output from evolutionary runs: best/top programs, population checkpoints, summary metrics.

## Critical: Synthetic vs Real Data

Models evolved on synthetic data scored well but performed WORSE on real data. The synthetic distribution does not match real market dynamics. Always evaluate on real BigQuery data (`--bigquery` flag). See `evolve/README.md` for details on what worked vs failed.

## GCP Integration

- **BigQuery**: Market data source and ARIMA forecasting
- **GCS**: Model storage at `gs://na-analytics-media-stocks/models/`
- **Cloud Functions**: Automated data pipelines and retraining

## Key Dependencies

Python 3.8+. Core: `torch`, `xgboost`, `scikit-learn`, `google-generativeai` (Gemini), `google-cloud-bigquery`, `google-cloud-storage`, `yfinance`, `pandas`.
