"""
XGBoost WPP Daily — EvolveEngine example for the xgb_wpp_specific model.

REAL BASELINE (on BigQuery data):
  58.0% accuracy, AUC 0.587, 274 trades, 59.5% win rate, +82.5% P&L, Sharpe 0.37

LESSON LEARNED: Models evolved on synthetic data failed on real data. Aggressive
regularization (gamma=10, reg_alpha=15) that won on synthetic makes models too
conservative on real market data — zero trades or negative P&L. Always evaluate
on real data. Domain constraints (monotonic on net_wins) transfer; numeric
hyperparameter tuning does not.

This module uses BigQuery by default. Synthetic is only for smoke-testing the pipeline.
"""

import gc
import numpy as np

# ---------------------------------------------------------------------------
# 1. SEED CODE — baseline config matching the real 58% model
# ---------------------------------------------------------------------------

SEED_CODE = '''
import xgboost as xgb
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score


# EVOLVE-BLOCK-START: HYPERPARAMETERS
# Baseline hyperparameters — proven 58% accuracy on real data.
# WARNING: Aggressive regularization (gamma>2, reg_alpha>5) kills trading signal.
# The baseline moderate config is strong. Evolve cautiously.
PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "max_depth": 6,
    "learning_rate": 0.03,
    "n_estimators": 300,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 1,
    "gamma": 0.0,
    "reg_alpha": 0.0,
    "reg_lambda": 1.0,
    "scale_pos_weight": 1.0,
    "max_delta_step": 0,
    "random_state": 42,
    # Monotonic constraint: net_wins (index 12) must increase prediction.
    # This is the one evolved idea proven to transfer to real data.
    "monotone_constraints": (0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0),
}
NUM_BOOST_ROUND = 300
EARLY_STOPPING_ROUNDS = 15
THRESHOLD = 0.5
# EVOLVE-BLOCK-END: HYPERPARAMETERS


# EVOLVE-BLOCK-START: FEATURE_ENGINEERING
def engineer_features(X, feature_names):
    """
    Feature engineering on the 16 raw features.
    Returns (X_new, new_feature_names).

    Feature indices:
        0: ret_1d, 1: ret_5d, 2: ret_20d, 3: ret_60d,
        4: sp_ret_1d, 5: sp_ret_5d, 6: sp_ret_20d,
        7: rel_5d, 8: rel_20d,
        9: vol_20d, 10: beta_60d, 11: rsi_14,
        12: net_wins, 13: net_spend_bn,
        14: gdelt_tone, 15: gdelt_volume

    HIGH IMPACT ideas (these create new signal, not just tune existing):
    - Interaction: net_wins * ret_20d (competition + momentum alignment)
    - Interaction: gdelt_tone * gdelt_volume (volume-weighted sentiment)
    - Volatility-adjusted returns: ret_5d / (vol_20d + 1e-6)
    - RSI regime flags: overbought (rsi > 70), oversold (rsi < 30)
    - Momentum crossover: ret_5d - ret_20d (short vs medium trend)
    - Competition intensity: net_wins * net_spend_bn
    - Winsorize extreme values (clip at 1st/99th percentile)

    IMPORTANT: New features must help the model find signal it can't find
    from the raw features alone. XGBoost can already do splits on individual
    features, so polynomial/binning of single features adds little.
    Focus on CROSS-FEATURE interactions.
    """
    return X, feature_names
# EVOLVE-BLOCK-END: FEATURE_ENGINEERING


# EVOLVE-BLOCK-START: TRAINING_PROCEDURE
def train_model(X_train, y_train, X_val, y_val):
    """
    Train XGBoost model. Returns (model, best_iteration).

    Ideas that may help on REAL data:
    - Monotonic constraints on additional features (vol_20d negative, sp_ret_20d positive)
    - Feature interaction constraints: group correlated features
    - Sample weighting: upweight recent data (market regime changes over 10 years)
    - Two-stage: train on all data, fine-tune on recent 3 years
    - Custom objective for asymmetric loss (missed opportunities vs false signals)

    Ideas that FAILED on real data (avoid):
    - DART booster with aggressive dropout — too conservative, zero trades
    - gamma > 2, reg_alpha > 5, reg_lambda > 5 — kills signal
    - max_depth < 4 — too shallow for feature interactions
    - Very low learning rate (< 0.01) with high rounds — overfits to noise
    """
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dval = xgb.DMatrix(X_val, label=y_val)

    params = {k: v for k, v in PARAMS.items() if k != "n_estimators"}

    model = xgb.train(
        params,
        dtrain,
        num_boost_round=NUM_BOOST_ROUND,
        evals=[(dval, "val")],
        early_stopping_rounds=EARLY_STOPPING_ROUNDS,
        verbose_eval=False,
    )
    return model, model.best_iteration
# EVOLVE-BLOCK-END: TRAINING_PROCEDURE


# EVOLVE-BLOCK-START: PREDICTION
def predict(model, X):
    """
    Generate predictions from trained model.

    The baseline threshold of 0.5 with confidence filtering at 0.55/0.45
    produces 274 trades with 59.5% win rate. Changes here directly affect
    trade count and quality — test carefully.

    Ideas:
    - Dynamic threshold tuned on validation set (maximize Sharpe, not accuracy)
    - Wider confidence band (0.57/0.43) for fewer but higher-quality trades
    - Platt scaling for better-calibrated probabilities
    """
    dtest = xgb.DMatrix(X)
    probs = model.predict(dtest)
    preds = (probs >= THRESHOLD).astype(int)
    return preds, probs
# EVOLVE-BLOCK-END: PREDICTION
'''


# ---------------------------------------------------------------------------
# 2. SYSTEM PROMPT — grounded in real-data results
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are evolving an XGBoost classifier that predicts WPP stock 5-day direction.

## Real Baseline Performance (this is what you must beat)
- Accuracy: 58.0% | AUC: 0.587 | Trades: 274 | Win rate: 59.5% | P&L: +82.5% | Sharpe: 0.37
- Config: max_depth=6, lr=0.03, 300 rounds, moderate regularization, gbtree booster

## What FAILED in previous evolution (DO NOT repeat)
- Aggressive regularization (gamma=10, reg_alpha=15, reg_lambda=15) → model too conservative, zero trades
- DART booster with high dropout → predicts "up" for everything, no signal
- max_depth=3 → too shallow, can't capture feature interactions
- Very low learning_rate (<0.01) with 800+ rounds → overfits to noise patterns
- These ALL scored well on synthetic data but FAILED on real market data

## What WORKED
- Monotonic constraint on net_wins (index 12) → domain knowledge that transfers
- The baseline moderate config is genuinely strong — don't blow it up

## High-Value Directions (focus here)

### Feature Engineering (HIGHEST IMPACT — creates new signal)
- net_wins * ret_20d — competition momentum alignment
- gdelt_tone * gdelt_volume — volume-weighted sentiment
- ret_5d / (vol_20d + 1e-6) — volatility-adjusted momentum
- ret_5d - ret_20d — short vs medium momentum crossover
- net_wins * net_spend_bn — competition intensity
- RSI regime flags: (rsi > 70) as overbought, (rsi < 30) as oversold
- Winsorize features at 1st/99th percentile to reduce outlier impact

### Structural Constraints (transfers to real data)
- Additional monotonic constraints: vol_20d should be negative (high vol = bearish)
- Feature interaction constraints between correlated features
- Sample weighting: upweight recent 3 years (market regimes change)

### Careful Hyperparameter Adjustments (SMALL moves only)
- max_depth: stay in 5-8 range (not below 4, not above 10)
- learning_rate: 0.02-0.05 range (not below 0.01)
- subsample/colsample: 0.6-0.9 range
- reg_alpha/reg_lambda: 0-2 range MAXIMUM (not higher)
- gamma: 0-1 range MAXIMUM
- early_stopping: 10-20 range

### Prediction Refinement
- Tune threshold on validation Sharpe ratio, not just accuracy
- Wider confidence bands (0.57/0.43) for higher-quality trades
- Must produce at least 100 trades to be useful (zero trades = fail)

## Hard Constraints
- Must use xgboost (not sklearn GradientBoosting)
- Available: xgboost, numpy, sklearn, scipy
- Train in under 60 seconds
- Must produce >= 50 trades on the validation set (models with zero trades score -1)
"""


# ---------------------------------------------------------------------------
# 3. EVALUATION FUNCTION — real data by default, baseline-relative scoring
# ---------------------------------------------------------------------------

# Baseline metrics from real data (the bar to beat)
BASELINE = {
    "accuracy": 0.580,
    "roc_auc": 0.587,
    "win_rate": 0.595,
    "n_trades": 274,
    "pnl_pct": 82.5,
    "sharpe": 0.37,
}


def make_eval_fn(difficulty: str = "medium", use_bigquery: bool = False):
    """
    Returns evaluation function for the XGBoost WPP model.

    IMPORTANT: use_bigquery=True loads real data (default for production).
    Synthetic data is only for smoke-testing the pipeline.
    """
    if use_bigquery:
        X_train, y_train, X_val, y_val, X_test, y_test, feature_names, val_returns = _load_bigquery_data()
    else:
        X_train, y_train, X_val, y_val, X_test, y_test, feature_names, val_returns = _generate_synthetic_data(difficulty)

    def evaluate(candidate_code: str) -> dict:
        """Train candidate and return metrics with baseline-relative scoring."""
        try:
            import xgboost as xgb
            from sklearn.metrics import (
                accuracy_score, f1_score, roc_auc_score,
                precision_score, recall_score,
            )

            ns = {"xgb": xgb, "np": np, "xgboost": xgb}
            import sklearn.metrics
            import sklearn.calibration
            ns["sklearn"] = __import__("sklearn")
            ns["accuracy_score"] = accuracy_score
            ns["f1_score"] = f1_score
            ns["roc_auc_score"] = roc_auc_score

            exec(candidate_code, ns)

            # Feature engineering
            eng_fn = ns.get("engineer_features")
            if eng_fn:
                X_tr_eng, feat_eng = eng_fn(X_train.copy(), list(feature_names))
                X_va_eng, _ = eng_fn(X_val.copy(), list(feature_names))
            else:
                X_tr_eng, X_va_eng = X_train, X_val

            # Train
            train_fn = ns.get("train_model")
            if train_fn is None:
                return {"score": -1.0, "error": "train_model not found"}

            model, best_iter = train_fn(X_tr_eng, y_train, X_va_eng, y_val)

            # Predict
            predict_fn = ns.get("predict")
            if predict_fn:
                preds, probs = predict_fn(model, X_va_eng)
            else:
                dval = xgb.DMatrix(X_va_eng)
                probs = model.predict(dval)
                threshold = float(ns.get("THRESHOLD", 0.5))
                preds = (probs >= threshold).astype(int)

            # --- Core metrics ---
            acc = accuracy_score(y_val, preds)
            try:
                roc = roc_auc_score(y_val, probs)
            except Exception:
                roc = 0.5

            precision = precision_score(y_val, preds, zero_division=0)
            recall = recall_score(y_val, preds, zero_division=0)

            # --- Trading simulation (realistic) ---
            trades_pnl = []
            for i in range(len(probs)):
                if probs[i] > 0.55:
                    # Long signal: profit if stock actually went up
                    trades_pnl.append(val_returns[i] if val_returns is not None else
                                      (1.0 if y_val[i] == 1 else -1.0))
                elif probs[i] < 0.45:
                    # Short signal: profit if stock actually went down
                    trades_pnl.append(-val_returns[i] if val_returns is not None else
                                      (1.0 if y_val[i] == 0 else -1.0))

            n_trades = len(trades_pnl)
            if n_trades > 0:
                total_pnl = sum(trades_pnl)
                win_rate = sum(1 for t in trades_pnl if t > 0) / n_trades
                sharpe = (float(np.mean(trades_pnl) / np.std(trades_pnl) * np.sqrt(52))
                          if np.std(trades_pnl) > 0 else 0.0)
            else:
                total_pnl = 0.0
                win_rate = 0.0
                sharpe = 0.0

            # --- HARD FAIL: models with zero/too few trades are useless ---
            if n_trades < 50:
                return {
                    "score": -1.0,
                    "accuracy": float(acc),
                    "roc_auc": float(roc),
                    "n_trades": n_trades,
                    "fail_reason": f"Only {n_trades} trades (need >= 50). Model too conservative.",
                }

            # --- Baseline-relative scoring ---
            # Score = how much better than baseline across key metrics
            # Each component is (candidate - baseline) / baseline, capped
            acc_delta = (acc - BASELINE["accuracy"]) / BASELINE["accuracy"]
            auc_delta = (roc - BASELINE["roc_auc"]) / BASELINE["roc_auc"]
            wr_delta = (win_rate - BASELINE["win_rate"]) / BASELINE["win_rate"]
            sharpe_delta = (sharpe - BASELINE["sharpe"]) / max(BASELINE["sharpe"], 0.01)

            # Trade count penalty: fewer trades than baseline is penalized
            trade_ratio = min(n_trades / BASELINE["n_trades"], 1.5)  # cap at 1.5x
            trade_penalty = 0.0 if trade_ratio >= 0.5 else -0.5 * (0.5 - trade_ratio)

            # Weighted baseline-relative score
            # Positive = better than baseline, negative = worse
            score = (
                0.30 * acc_delta
                + 0.25 * auc_delta
                + 0.20 * wr_delta
                + 0.15 * sharpe_delta
                + 0.10 * trade_penalty
            )

            return {
                "score": float(score),
                "accuracy": float(acc),
                "roc_auc": float(roc),
                "precision": float(precision),
                "recall": float(recall),
                "n_trades": n_trades,
                "win_rate": float(win_rate),
                "total_pnl": float(total_pnl),
                "sharpe": float(sharpe),
                "best_iteration": int(best_iter) if best_iter else 0,
                # Deltas for transparency
                "vs_baseline_acc": f"{acc_delta:+.1%}",
                "vs_baseline_auc": f"{auc_delta:+.1%}",
                "vs_baseline_wr": f"{wr_delta:+.1%}",
                "vs_baseline_sharpe": f"{sharpe_delta:+.1%}",
            }

        except Exception as e:
            import traceback
            return {"score": -1.0,
                    "error": f"{type(e).__name__}: {e}\n{traceback.format_exc()[-300:]}"}

        finally:
            gc.collect()

    return evaluate


def _generate_synthetic_data(difficulty: str = "medium"):
    """
    Synthetic data for smoke-testing only. NOT for evolution.
    Returns (X_train, y_train, X_val, y_val, X_test, y_test, feature_names, val_returns).
    """
    np.random.seed(42)
    n_samples = 2481
    feature_names = [
        "ret_1d", "ret_5d", "ret_20d", "ret_60d",
        "sp_ret_1d", "sp_ret_5d", "sp_ret_20d",
        "rel_5d", "rel_20d",
        "vol_20d", "beta_60d", "rsi_14",
        "net_wins", "net_spend_bn",
        "gdelt_tone", "gdelt_volume",
    ]

    X = np.zeros((n_samples, 16), dtype=np.float32)
    X[:, 0] = np.random.normal(0.0, 1.5, n_samples)
    X[:, 1] = np.random.normal(0.05, 3.0, n_samples)
    X[:, 2] = np.random.normal(0.2, 6.0, n_samples)
    X[:, 3] = np.random.normal(0.5, 12.0, n_samples)
    X[:, 4] = 0.6 * X[:, 0] + np.random.normal(0, 0.8, n_samples)
    X[:, 5] = 0.5 * X[:, 1] + np.random.normal(0, 1.5, n_samples)
    X[:, 6] = 0.4 * X[:, 2] + np.random.normal(0, 3.0, n_samples)
    X[:, 7] = X[:, 1] - X[:, 5]
    X[:, 8] = X[:, 2] - X[:, 6]
    X[:, 9] = np.abs(np.random.normal(25, 8, n_samples))
    X[:, 10] = np.random.normal(0.85, 0.25, n_samples)
    X[:, 11] = np.clip(np.random.normal(50, 15, n_samples), 10, 90)
    X[:, 12] = np.random.choice([-2, -1, 0, 1, 2, 3], n_samples,
                                 p=[0.1, 0.15, 0.3, 0.25, 0.12, 0.08])
    X[:, 13] = np.abs(np.random.normal(0.5, 0.3, n_samples))
    X[:, 14] = np.random.normal(0.0, 1.0, n_samples)
    X[:, 15] = np.abs(np.random.normal(100, 50, n_samples))

    noise_scale = {"easy": 0.3, "medium": 0.6, "hard": 1.0}[difficulty]
    signal = (0.15 * X[:, 12] - 0.008 * X[:, 9] + 0.01 * X[:, 6]
              + 0.08 * X[:, 13] + 0.005 * X[:, 2] + 0.02 * X[:, 14])
    noise = noise_scale * np.random.randn(n_samples)
    y = (signal + noise > np.median(signal)).astype(np.float32)

    # Simulated 5-day returns for trading sim
    val_returns = (signal * 0.02 + 0.01 * np.random.randn(n_samples)).astype(np.float32)

    # 70/15/15 split
    s1 = int(n_samples * 0.7)
    s2 = int(n_samples * 0.85)
    return (
        X[:s1], y[:s1],
        X[s1:s2], y[s1:s2],
        X[s2:], y[s2:],
        feature_names,
        val_returns[s1:s2],
    )


def _load_bigquery_data():
    """
    Load real WPP data from BigQuery.
    Split: 70% train / 15% validation (for evolution) / 15% test (held out).
    Returns (X_train, y_train, X_val, y_val, X_test, y_test, feature_names, val_returns).
    """
    from google.cloud import bigquery
    client = bigquery.Client(project="na-analytics")
    query = """
        SELECT * FROM `na-analytics.media_stocks.daily_features`
        WHERE ticker = 'WPP'
        ORDER BY date
    """
    df = client.query(query).to_dataframe()

    feature_cols = [
        "ret_1d", "ret_5d", "ret_20d", "ret_60d",
        "sp_ret_1d", "sp_ret_5d", "sp_ret_20d",
        "rel_5d", "rel_20d",
        "vol_20d", "beta_60d", "rsi_14",
        "net_wins", "net_spend_bn",
        "gdelt_tone", "gdelt_volume",
    ]
    X = df[feature_cols].values.astype(np.float32)
    y = df["target_direction"].values.astype(np.float32)

    # Use 5-day return for realistic trading simulation
    val_returns = df["target_5d_return"].values.astype(np.float32) if "target_5d_return" in df.columns else None

    # Sequential split: 70 / 15 / 15
    n = len(X)
    s1 = int(n * 0.7)
    s2 = int(n * 0.85)

    return (
        X[:s1], y[:s1],
        X[s1:s2], y[s1:s2],
        X[s2:], y[s2:],
        feature_cols,
        val_returns[s1:s2] if val_returns is not None else None,
    )
