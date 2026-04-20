
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
    # --- Evolved Change: Combine Sample Weighting and a New Monotonic Constraint ---
    
    # 1. Sample Weighting: Upweight recent data to adapt to changing market regimes.
    n_samples = X_train.shape[0]
    weights = np.ones(n_samples)
    recent_period_len = 3 * 252  # Approx 3 years of trading days
    if n_samples > recent_period_len:
        start_index = n_samples - recent_period_len
        # Linearly increase weight from 1.0 to 2.0 over the last 3 years
        ramp = np.linspace(start=1.0, stop=2.0, num=recent_period_len)
        weights[start_index:] = ramp
    
    dtrain = xgb.DMatrix(X_train, label=y_train, weight=weights)
    dval = xgb.DMatrix(X_val, label=y_val)

    # 2. Structural Constraints: Create a local copy of parameters to modify them.
    params = {k: v for k, v in PARAMS.items() if k != "n_estimators"}
    
    # Add a new monotonic constraint based on domain knowledge.
    # High volatility (vol_20d, index 9) is often bearish, so we enforce
    # a negative relationship (-1). This builds on the existing positive constraint
    # for net_wins (index 12).
    # Original: (0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0)
    # New:      (0,0,0,0,0,0,0,0,0,-1,0,0,1,0,0,0)
    num_features = X_train.shape[1]
    # Start with the constraints from the global PARAMS
    current_constraints = list(params.get("monotone_constraints", (0,) * num_features))
    # Ensure list is long enough before assignment
    while len(current_constraints) < num_features:
        current_constraints.append(0)
    
    if num_features > 9:
        current_constraints[9] = -1 # vol_20d -> negative constraint
    
    params["monotone_constraints"] = tuple(current_constraints)

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
