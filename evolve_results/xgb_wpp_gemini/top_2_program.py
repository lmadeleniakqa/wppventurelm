
import xgboost as xgb
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score


# EVOLVE-BLOCK-START: HYPERPARAMETERS
# XGBoost hyperparameters — Evolved for more aggressive regularization
# Rationale: Financial data is noisy. This evolution pushes regularization further
# to build a more conservative and potentially more robust model. Key changes include
# reducing tree depth (max_depth=3), increasing the minimum child weight, gamma, and
# L2 regularization. A lower colsample_bytree value increases feature randomness.
# The number of boosting rounds is slightly increased to allow the model to converge
# with these stricter constraints.
PARAMS = {
    "objective": "binary:logistic",
    "eval_metric": "logloss",
    "max_depth": 3,          # Reduced from 4 for stronger regularization
    "learning_rate": 0.02,   # Maintained slow learning rate
    "subsample": 0.7,
    "colsample_bytree": 0.6, # Lowered from 0.7 for more feature randomness
    "min_child_weight": 7,   # Increased from 5 for more conservative splits
    "gamma": 1.0,            # Increased from 0.5, requiring larger loss reduction
    "reg_alpha": 1.0,
    "reg_lambda": 3.0,       # Increased from 2.0 for stronger L2 regularization
    "scale_pos_weight": 0.82,# Set for 45/55 class split (count(neg)/count(pos))
    "max_delta_step": 0,
    "random_state": 42,
}
# Increased boosting rounds to account for more regularization, with more patient early stopping
NUM_BOOST_ROUND = 600
EARLY_STOPPING_ROUNDS = 25
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
