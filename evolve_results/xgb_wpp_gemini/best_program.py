
import xgboost as xgb
import numpy as np
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score


# EVOLVE-BLOCK-START: HYPERPARAMETERS
# XGBoost hyperparameters — Evolved for aggressive regularization, DART booster, and monotonic constraints
# Rationale: This iteration combines several high-probability strategies to break the current performance plateau.
# It pushes regularization parameters (gamma, min_child_weight, reg_alpha, reg_lambda, subsample, colsample_bytree)
# to more extreme values as recommended, recognizing the noisy nature of financial data.
# A positive monotonic constraint is added for 'net_wins', injecting crucial domain knowledge for the top predictor
# and acting as a powerful regularizer. The booster is switched to 'dart' to introduce a different regularization
# mechanism (dropout), fostering solution diversity. Deprecated parameters like 'scale_pos_weight' and
# 'max_delta_step' are removed as Focal Loss (implicitly used in TRAINING_PROCEDURE) is the superior method for
# handling class imbalance and they were deemed unimpactful.
PARAMS = {
    "objective": "binary:logistic", # This will be overridden by the Focal Loss custom objective in TRAINING_PROCEDURE.
    "eval_metric": "logloss",
    "booster": "dart",             # Switched to DART for diversity and dropout regularization.
    "max_depth": 3,                # Tightly constrained to prevent overfitting, as suggested.
    "learning_rate": 0.02,         # Maintained slow learning rate for stability with aggressive regularization.
    "subsample": 0.55,             # Aggressively lowered for more data randomness (Tier 1 guidance).
    "colsample_bytree": 0.45,      # Aggressively lowered for more feature randomness (Tier 1 guidance).
    "min_child_weight": 15,        # Significantly increased for more conservative splits (Tier 1 guidance).
    "gamma": 10.0,                 # Significantly increased, requiring larger loss reduction for splits (Tier 1 guidance).
    "reg_alpha": 15.0,             # Stronger L1 regularization (Tier 1 guidance).
    "reg_lambda": 15.0,            # Stronger L2 regularization (Tier 1 guidance).
    # Positive monotonic constraint on 'net_wins' (index 12 out of 16 features).
    # This assumes 'net_wins' is the 13th feature in the input list, as per the problem description.
    "monotone_constraints": (0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0),
    "random_state": 42,
    # DART specific parameters (using reasonable defaults for initial exploration).
    "rate_drop": 0.1, # Fraction of trees to drop at each iteration.
    "skip_drop": 0.5, # Probability of skipping the dropout procedure.
}
# Boosting rounds increased to allow for convergence with more aggressive regularization and DART's behavior.
# Early stopping rounds also increased to be more patient.
NUM_BOOST_ROUND = 800      # Increased from 600
EARLY_STOPPING_ROUNDS = 50 # Increased from 25
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
