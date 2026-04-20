"""
XGBoost WPP Daily — EvolveEngine example for evolving the xgb_wpp_specific model.

Baseline: 60.6% accuracy, ROC AUC 0.61 (BigQuery ML XGBoost on WPP-only data).
16 features, 2,481 daily observations, 5-day forward direction target.

To use with real BigQuery data, set USE_BIGQUERY=True and ensure gcloud auth.
"""

import gc
import numpy as np

# ---------------------------------------------------------------------------
# 1. SEED CODE — XGBoost classifier replicating the BigQuery ML baseline
# ---------------------------------------------------------------------------

SEED_CODE = '''
import xgboost as xgb
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score


# EVOLVE-BLOCK-START: HYPERPARAMETERS
# XGBoost hyperparameters — baseline matches BigQuery ML defaults
PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "max_depth": 6,
    "learning_rate": 0.3,
    "n_estimators": 100,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 1,
    "gamma": 0.0,
    "reg_alpha": 0.0,
    "reg_lambda": 1.0,
    "scale_pos_weight": 1.0,
    "max_delta_step": 0,
    "random_state": 42,
}
NUM_BOOST_ROUND = 100
EARLY_STOPPING_ROUNDS = 10
THRESHOLD = 0.5
# EVOLVE-BLOCK-END: HYPERPARAMETERS


# EVOLVE-BLOCK-START: FEATURE_ENGINEERING
def engineer_features(X, feature_names):
    """
    Feature engineering on raw features. Returns (X_new, new_feature_names).
    Input features (16):
        ret_1d, ret_5d, ret_20d, ret_60d,
        sp_ret_1d, sp_ret_5d, sp_ret_20d,
        rel_5d, rel_20d,
        vol_20d, beta_60d, rsi_14,
        net_wins, net_spend_bn,
        gdelt_tone, gdelt_volume

    Ideas to explore:
    - Interaction features (net_wins * ret_20d, gdelt_tone * vol_20d)
    - Ratio features (ret_5d / vol_20d, rel_5d / beta_60d)
    - Polynomial features on top predictors
    - Rolling z-scores or rank transforms
    - Binning continuous features
    - Momentum regime indicators (rsi > 70, rsi < 30)
    - Volatility-adjusted returns (ret_Xd / vol_20d)
    """
    return X, feature_names
# EVOLVE-BLOCK-END: FEATURE_ENGINEERING


# EVOLVE-BLOCK-START: TRAINING_PROCEDURE
def train_model(X_train, y_train, X_val, y_val):
    """
    Train XGBoost model and return (model, best_iteration).

    Ideas to explore:
    - Custom objective functions (focal loss, asymmetric loss)
    - Different booster types (gbtree, dart, gblinear)
    - Sample weighting strategies
    - Monotonic constraints on key features
    - Feature interaction constraints
    - Two-stage training (coarse then fine)
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

    Ideas to explore:
    - Calibrated probabilities (Platt scaling, isotonic regression)
    - Dynamic threshold tuning (optimize F1 or profit on val set)
    - Ensemble with prediction from different iterations
    - Margin-based confidence filtering
    """
    dtest = xgb.DMatrix(X)
    probs = model.predict(dtest)
    preds = (probs >= THRESHOLD).astype(int)
    return preds, probs
# EVOLVE-BLOCK-END: PREDICTION
'''


# ---------------------------------------------------------------------------
# 2. SYSTEM PROMPT — domain guidance for Gemini
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert ML engineer evolving an XGBoost classifier for stock direction prediction.
The model predicts whether WPP stock will go up or down over the next 5 trading days.

## Current Model (baseline: 60.6% accuracy, ROC AUC 0.61)
- XGBoost with default BigQuery ML parameters
- 16 features, 2,481 daily observations (WPP only, 10 years)
- Binary classification: 5-day forward direction

## Key Domain Knowledge
- #1 predictor is net_wins (account competition) at 29% importance
- Market risk (vol_20d) is #2
- Stock-specific models significantly outperform multi-stock models
- Returns are very noisy at daily frequency — regularization is critical
- Class balance is roughly 55/45 (slight upward bias)

## Directions to Explore

### Hyperparameter Tuning
- max_depth: try 3-10 (deeper risks overfitting on 2,481 samples)
- learning_rate: try 0.01-0.3 with higher n_estimators for lower rates
- subsample: try 0.5-1.0
- colsample_bytree: try 0.3-1.0
- min_child_weight: try 1-10 (higher = more regularization)
- gamma: try 0-5 (minimum loss reduction for split)
- reg_alpha (L1): try 0-10
- reg_lambda (L2): try 0-10
- scale_pos_weight: try ratio of negative/positive class counts
- max_delta_step: try 1-10 for imbalanced data
- n_estimators: try 50-500 (with appropriate early stopping)

### Feature Engineering (HIGH IMPACT)
- Interaction: net_wins * ret_20d (competition momentum)
- Interaction: gdelt_tone * gdelt_volume (weighted sentiment)
- Volatility-adjusted returns: ret_Xd / vol_20d
- RSI regime: binary indicators for overbought (>70) / oversold (<30)
- Momentum crossover: ret_5d - ret_20d (short vs medium momentum)
- Beta-adjusted relative strength: rel_20d / beta_60d
- Competition intensity: net_wins * net_spend_bn
- Polynomial: net_wins^2, vol_20d^2
- Rolling rank transforms on continuous features
- Clipping extreme values (winsorize at 1st/99th percentile)

### Training Procedure
- Custom objective: focal loss for hard-to-classify samples
- Asymmetric loss (penalize false negatives differently from false positives)
- DART booster instead of gbtree (dropout regularization)
- Monotonic constraints: enforce net_wins increases probability of up
- Feature interaction constraints: group related features
- Two-stage: first train on all data, then fine-tune on recent 2 years
- Bagging: train multiple models on bootstrap samples, majority vote

### Prediction
- Calibrate probabilities with Platt scaling
- Dynamic threshold: optimize on validation set
- Confidence filtering: only predict when probability > 0.55 or < 0.45
- Ensemble ntree_limit values for robust prediction

## Constraints
- Must use xgboost library
- Available: xgboost, numpy, sklearn, scipy
- Train in under 60 seconds
- Don't introduce external data dependencies
"""


# ---------------------------------------------------------------------------
# 3. EVALUATION FUNCTION FACTORY
# ---------------------------------------------------------------------------

def make_eval_fn(difficulty: str = "medium", use_bigquery: bool = False):
    """
    Returns an evaluation function for the XGBoost WPP model.

    With use_bigquery=True, loads real data from BigQuery.
    Otherwise uses synthetic data that mimics the real distribution.
    """
    if use_bigquery:
        X_train, y_train, X_val, y_val, feature_names = _load_bigquery_data()
    else:
        X_train, y_train, X_val, y_val, feature_names = _generate_synthetic_data(difficulty)

    def evaluate(candidate_code: str) -> dict:
        """Train candidate XGBoost model and return metrics."""
        try:
            import xgboost as xgb
            from sklearn.metrics import (
                accuracy_score, f1_score, roc_auc_score,
                precision_score, recall_score,
            )

            ns = {"xgb": xgb, "np": np, "xgboost": xgb}
            # Inject sklearn into namespace
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
                return {"accuracy": -1.0, "error": "train_model not found"}

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

            # Metrics
            acc = accuracy_score(y_val, preds)
            f1 = f1_score(y_val, preds, average="binary", zero_division=0)
            macro_f1 = f1_score(y_val, preds, average="macro", zero_division=0)
            precision = precision_score(y_val, preds, zero_division=0)
            recall = recall_score(y_val, preds, zero_division=0)

            try:
                roc = roc_auc_score(y_val, probs)
            except Exception:
                roc = 0.0

            # Trading simulation
            trades = []
            for i in range(len(probs)):
                if probs[i] > 0.55:
                    trades.append(1 if y_val[i] == 1 else -1)
                elif probs[i] < 0.45:
                    trades.append(1 if y_val[i] == 0 else -1)
            win_rate = sum(1 for t in trades if t > 0) / len(trades) if trades else 0

            # Composite score: weighted combination emphasizing accuracy + AUC
            composite = 0.4 * acc + 0.3 * roc + 0.2 * macro_f1 + 0.1 * precision

            return {
                "accuracy": float(acc),
                "roc_auc": float(roc),
                "f1": float(f1),
                "macro_f1": float(macro_f1),
                "precision": float(precision),
                "recall": float(recall),
                "win_rate": float(win_rate),
                "n_trades": len(trades),
                "best_iteration": int(best_iter) if best_iter else 0,
                "composite": float(composite),
            }

        except Exception as e:
            import traceback
            return {"accuracy": -1.0, "composite": -1.0,
                    "error": f"{type(e).__name__}: {e}\n{traceback.format_exc()[-300:]}"}

        finally:
            gc.collect()

    return evaluate


def _generate_synthetic_data(difficulty: str = "medium"):
    """
    Generate synthetic data mimicking the WPP daily features distribution.
    2,481 samples, 16 features, binary target (5-day direction).
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

    # Simulate realistic feature distributions
    X = np.zeros((n_samples, 16), dtype=np.float32)

    # Returns: mean ~0, std varies by horizon
    X[:, 0] = np.random.normal(0.0, 1.5, n_samples)     # ret_1d
    X[:, 1] = np.random.normal(0.05, 3.0, n_samples)     # ret_5d
    X[:, 2] = np.random.normal(0.2, 6.0, n_samples)      # ret_20d
    X[:, 3] = np.random.normal(0.5, 12.0, n_samples)     # ret_60d

    # S&P returns (correlated with stock returns)
    X[:, 4] = 0.6 * X[:, 0] + np.random.normal(0, 0.8, n_samples)  # sp_ret_1d
    X[:, 5] = 0.5 * X[:, 1] + np.random.normal(0, 1.5, n_samples)  # sp_ret_5d
    X[:, 6] = 0.4 * X[:, 2] + np.random.normal(0, 3.0, n_samples)  # sp_ret_20d

    # Relative strength
    X[:, 7] = X[:, 1] - X[:, 5]   # rel_5d
    X[:, 8] = X[:, 2] - X[:, 6]   # rel_20d

    # Volatility, beta, RSI
    X[:, 9] = np.abs(np.random.normal(25, 8, n_samples))   # vol_20d
    X[:, 10] = np.random.normal(0.85, 0.25, n_samples)     # beta_60d
    X[:, 11] = np.random.normal(50, 15, n_samples)         # rsi_14
    X[:, 11] = np.clip(X[:, 11], 10, 90)

    # Competition features (most important predictors)
    X[:, 12] = np.random.choice([-2, -1, 0, 1, 2, 3], n_samples,
                                 p=[0.1, 0.15, 0.3, 0.25, 0.12, 0.08])  # net_wins
    X[:, 13] = np.abs(np.random.normal(0.5, 0.3, n_samples))  # net_spend_bn

    # Sentiment
    X[:, 14] = np.random.normal(0.0, 1.0, n_samples)       # gdelt_tone
    X[:, 15] = np.abs(np.random.normal(100, 50, n_samples)) # gdelt_volume

    # Target: nonlinear function of features + noise
    # Matches observed feature importance: net_wins >> vol_20d >> sp_ret_20d
    noise_scale = {"easy": 0.3, "medium": 0.6, "hard": 1.0}[difficulty]

    signal = (
        0.15 * X[:, 12]                          # net_wins (strongest)
        - 0.008 * X[:, 9]                         # vol_20d (high vol = bearish)
        + 0.01 * X[:, 6]                          # sp_ret_20d (market trend)
        + 0.08 * X[:, 13]                          # net_spend_bn
        + 0.005 * X[:, 2]                          # ret_20d (momentum)
        + 0.02 * X[:, 14]                          # gdelt_tone
        + 0.003 * X[:, 3]                          # ret_60d
        + 0.01 * (X[:, 1] - X[:, 5])              # rel_5d
        - 0.005 * np.abs(X[:, 11] - 50)           # RSI distance from neutral
        + 0.05 * X[:, 12] * (X[:, 2] > 0).astype(float)  # interaction: wins + momentum
    )

    noise = noise_scale * np.random.randn(n_samples)
    y = (signal + noise > np.median(signal)).astype(np.float32)

    # Slight upward bias (55/45 like real data)
    bias_flip = (y == 0) & (np.random.rand(n_samples) < 0.05)
    y[bias_flip] = 1.0

    # Sequential split (80/20, temporal)
    split = int(n_samples * 0.8)
    return (
        X[:split], y[:split],
        X[split:], y[split:],
        feature_names,
    )


def _load_bigquery_data():
    """Load real WPP data from BigQuery."""
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

    split = int(len(X) * 0.8)
    return X[:split], y[:split], X[split:], y[split:], feature_cols
