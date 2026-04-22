"""
Model Retrain Cloud Function

Triggered by:
  1. Performance monitor (MAPE > 5% for 10 days, CI < 60%, direction < 45%)
  2. Quarterly schedule (Jan, Apr, Jul, Oct first Monday)
  3. Manual trigger (POST with reason)

Retrains the Evolved XGBoost v2 on latest BigQuery data,
saves to GCS, and updates monitoring status.
"""
import functions_framework
import json
import numpy as np
import xgboost as xgb
from datetime import datetime
from google.cloud import bigquery, storage
from sklearn.metrics import accuracy_score, roc_auc_score


PROJECT = "na-analytics"
BUCKET = "na-analytics-media-stocks"
MODEL_PATH = "models/xgb_wpp_evolved_v2/model.bst"
FEATURE_COLS = [
    "ret_1d", "ret_5d", "ret_20d", "ret_60d",
    "sp_ret_1d", "sp_ret_5d", "sp_ret_20d",
    "rel_5d", "rel_20d", "vol_20d", "beta_60d", "rsi_14",
    "net_wins", "net_spend_bn", "gdelt_tone", "gdelt_volume",
    "comv_net_wins_90d", "comv_net_spend_90d_bn",
    "comv_market_share_pct", "comv_digital_share_pct",
    "comv_competitive_pressure", "comv_concentration_hhi",
]


def train_evolved_v2(X_train, y_train, X_val, y_val):
    """Evolved XGBoost v2 — recency-weighted + monotonic constraint on net_wins"""
    n = X_train.shape[0]
    weights = np.ones(n)
    recent = 3 * 252
    if n > recent:
        start = n - recent
        weights[start:] = np.linspace(1.0, 2.0, recent)

    dtrain = xgb.DMatrix(X_train, label=y_train, weight=weights)
    dval = xgb.DMatrix(X_val, label=y_val)

    params = {
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "max_depth": 6,
        "learning_rate": 0.03,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 1,
        "gamma": 0.0,
        "reg_alpha": 0.0,
        "reg_lambda": 1.0,
        "random_state": 42,
        # 22 features: net_wins(12)+1, comv_net_wins_90d(16)+1, comv_net_spend_90d_bn(17)+1,
        # comv_market_share_pct(18)+1, comv_competitive_pressure(20)-1, comv_concentration_hhi(21)-1
        "monotone_constraints": (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, -1, -1),
    }

    model = xgb.train(
        params, dtrain, num_boost_round=300,
        evals=[(dval, "val")], early_stopping_rounds=15, verbose_eval=False,
    )
    return model


@functions_framework.http
def retrain_model(request):
    """Retrain and deploy the production model."""
    req_data = request.get_json(silent=True) or {}
    trigger = req_data.get("trigger", "manual")
    reason = req_data.get("reason", "Manual retrain")

    results = {
        "status": "ok",
        "trigger": trigger,
        "reason": reason,
        "timestamp": datetime.utcnow().isoformat(),
    }

    # 1. Load data from BigQuery
    bq = bigquery.Client(project=PROJECT)
    df = bq.query(f"""
        SELECT d.*,
            COALESCE(c.comv_net_wins_90d, 0) AS comv_net_wins_90d,
            COALESCE(c.comv_net_spend_90d_bn, 0) AS comv_net_spend_90d_bn,
            COALESCE(c.comv_market_share_pct, 0) AS comv_market_share_pct,
            COALESCE(c.comv_digital_share_pct, 0) AS comv_digital_share_pct,
            COALESCE(c.comv_competitive_pressure, 0) AS comv_competitive_pressure,
            COALESCE(c.comv_concentration_hhi, 0) AS comv_concentration_hhi
        FROM `{PROJECT}.media_stocks.daily_features` d
        LEFT JOIN `{PROJECT}.media_stocks.comvergence_daily_features` c
            ON d.date = c.date AND d.ticker = c.ticker
        WHERE d.ticker = 'WPP' ORDER BY d.date
    """).result().to_dataframe()

    X = df[FEATURE_COLS].values.astype(np.float32)
    y = df["target_direction"].values.astype(int)
    split = int(len(X) * 0.8)

    results["data_rows"] = len(X)
    results["train_rows"] = split
    results["test_rows"] = len(X) - split

    # 2. Train
    model = train_evolved_v2(X[:split], y[:split], X[split:], y[split:])

    # 3. Evaluate
    dtest = xgb.DMatrix(X[split:])
    probs = model.predict(dtest)
    preds = (probs >= 0.5).astype(int)
    acc = accuracy_score(y[split:], preds)
    auc = roc_auc_score(y[split:], probs)

    # Trading sim
    trades = []
    for i in range(len(probs)):
        ret = float(df["target_5d_return"].iloc[split + i])
        if probs[i] > 0.55:
            trades.append(ret)
        elif probs[i] < 0.45:
            trades.append(-ret)
    wr = sum(1 for t in trades if t > 0) / len(trades) * 100 if trades else 0
    pnl = sum(trades)
    sharpe = np.mean(trades) / np.std(trades) * np.sqrt(52) if trades and np.std(trades) > 0 else 0

    results["metrics"] = {
        "accuracy": round(acc * 100, 1),
        "roc_auc": round(float(auc), 3),
        "trades": len(trades),
        "win_rate": round(wr, 1),
        "total_pnl": round(pnl, 1),
        "sharpe": round(float(sharpe), 2),
    }

    # 4. Save to GCS
    sc = storage.Client(project=PROJECT)
    bucket = sc.get_bucket(BUCKET)

    # Archive previous model
    old_blob = bucket.blob(MODEL_PATH)
    if old_blob.exists():
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        bucket.copy_blob(old_blob, bucket, f"models/archive/model_{ts}.bst")

    # Save new model
    model.save_model("/tmp/model.bst")
    bucket.blob(MODEL_PATH).upload_from_filename("/tmp/model.bst")

    # Update monitoring status
    status = {
        "model_version": f"evolved_v2_{datetime.utcnow().strftime('%Y%m%d')}",
        "last_trained": datetime.utcnow().strftime("%Y-%m-%d"),
        "last_retrain_trigger": trigger,
        "last_retrain_reason": reason,
        "metrics_at_retrain": results["metrics"],
        "consecutive_high_mape_days": 0,
        "retrains_this_quarter": req_data.get("retrains_this_quarter", 0) + 1,
    }
    bucket.blob("monitoring/status.json").upload_from_string(json.dumps(status))

    results["model_saved"] = MODEL_PATH
    results["status_updated"] = True

    return json.dumps(results), 200, {"Content-Type": "application/json"}
