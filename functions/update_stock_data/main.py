import functions_framework
import yfinance as yf
import json
import subprocess
from datetime import datetime, timedelta
from google.cloud import storage, bigquery


BUCKET = "na-analytics-media-stocks"
PROJECT = "na-analytics"
TICKERS = {"WPP": "WPP", "Publicis": "PUB.PA", "Omnicom": "OMC", "SP500": "^GSPC"}


@functions_framework.http
def update_stock_data(request):
    """Fetches latest stock data, runs BQ forecasts, writes to GCS, triggers redeploy."""
    results = {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

    # 1. Fetch 10 years of daily prices
    end = datetime.now()
    start = end - timedelta(days=10 * 365 + 60)
    stock_data = {}
    for name, ticker in TICKERS.items():
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"), progress=False)
        dates = [d.strftime("%Y-%m-%d") for d in df.index]
        closes = df["Close"].values.flatten().tolist()
        clean = {"dates": [], "closes": []}
        for d, c in zip(dates, closes):
            if c == c:
                clean["dates"].append(d)
                clean["closes"].append(round(c, 2))
        stock_data[name] = clean

    stock_data["_last_updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    results["data_points"] = {k: len(v.get("dates", [])) for k, v in stock_data.items() if k != "_last_updated"}

    # 2. Run BQ ARIMA+ forecasts
    bq = bigquery.Client(project=PROJECT)
    forecasts = {}
    try:
        rows = bq.query("""
            SELECT ticker, FORMAT_DATE('%Y-%m-%d', forecast_timestamp) as date,
                   ROUND(forecast_value, 2) as forecast,
                   ROUND(prediction_interval_lower_bound, 2) as lower,
                   ROUND(prediction_interval_upper_bound, 2) as upper
            FROM ML.FORECAST(MODEL `na-analytics.media_stocks.arima_stocks`, STRUCT(90 AS horizon, 0.9 AS confidence_level))
            ORDER BY ticker, forecast_timestamp
        """).result()
        for row in rows:
            t = row["ticker"]
            if t not in forecasts:
                forecasts[t] = {"dates": [], "forecast": [], "lower": [], "upper": []}
            forecasts[t]["dates"].append(row["date"])
            forecasts[t]["forecast"].append(float(row["forecast"]))
            forecasts[t]["lower"].append(float(row["lower"]))
            forecasts[t]["upper"].append(float(row["upper"]))
        results["forecasts"] = {k: len(v["dates"]) for k, v in forecasts.items()}
    except Exception as e:
        results["forecast_error"] = str(e)

    # 3. Write to GCS
    client = storage.Client(project=PROJECT)
    try:
        bucket = client.get_bucket(BUCKET)
    except Exception:
        bucket = client.create_bucket(BUCKET, location="us-central1")

    # stock_data.js
    # Also need decomposition — skip for now, use existing
    blob = bucket.blob("dashboard/stock_data_live.js")
    blob.upload_from_string(f"const STOCK_DATA_LIVE = {json.dumps(stock_data)};")

    # forecast_data_live.js
    blob = bucket.blob("dashboard/forecast_live.js")
    blob.upload_from_string(f"const FORECAST_LIVE = {json.dumps(forecasts)};")

    # 4. Build forecast tracking (compare predictions vs actuals)
    tracking = {}
    from datetime import datetime as dt_cls
    for ticker in ["WPP", "Publicis", "Omnicom"]:
        if ticker not in forecasts or ticker not in stock_data:
            continue
        actual_map = dict(zip(stock_data[ticker]["dates"], stock_data[ticker]["closes"]))
        fc = forecasts[ticker]
        daily = []
        errors = []
        n_within = 0
        for i, fd in enumerate(fc["dates"]):
            # Skip weekends
            if dt_cls.strptime(fd, "%Y-%m-%d").weekday() >= 5:
                continue
            fp = fc["forecast"][i]
            lo = fc["lower"][i]
            hi = fc["upper"][i]
            actual = actual_map.get(fd)
            if actual is not None:
                err = actual - fp
                err_pct = (err / actual) * 100
                within = lo <= actual <= hi
                if within:
                    n_within += 1
                errors.append(abs(err_pct))
                daily.append({"date": fd, "forecast": round(fp, 2), "actual": round(actual, 2),
                              "error": round(err, 2), "error_pct": round(err_pct, 2),
                              "lower": round(lo, 2), "upper": round(hi, 2),
                              "within_ci": within, "status": "actual"})
            else:
                daily.append({"date": fd, "forecast": round(fp, 2), "actual": None,
                              "error": None, "error_pct": None,
                              "lower": round(lo, 2), "upper": round(hi, 2),
                              "within_ci": None, "status": "pending"})
        n_actual = len(errors)
        tracking[ticker] = {
            "daily": daily,
            "summary": {
                "days_with_actuals": n_actual,
                "days_pending": len(daily) - n_actual,
                "days_within_ci": n_within,
                "ci_hit_rate": round(n_within / n_actual * 100, 1) if n_actual > 0 else None,
                "mape": round(sum(errors) / len(errors), 2) if errors else None,
                "forecast_start": fc["dates"][0] if fc["dates"] else None,
                "last_actual_date": stock_data[ticker]["dates"][-1],
                "last_actual_price": stock_data[ticker]["closes"][-1],
            },
        }

    blob = bucket.blob("dashboard/forecast_tracking.js")
    blob.upload_from_string(f"const FORECAST_TRACKING = {json.dumps(tracking)};")

    results["tracking"] = {k: v["summary"]["days_with_actuals"] for k, v in tracking.items()}

    # 5. Performance monitoring — check retrain triggers
    retrain_needed = False
    retrain_reason = None

    # Load current monitoring status
    try:
        status_blob = bucket.blob("monitoring/status.json")
        if status_blob.exists():
            mon_status = json.loads(status_blob.download_as_string())
        else:
            mon_status = {"consecutive_high_mape_days": 0, "retrains_this_quarter": 0, "last_trained": "2026-04-20"}
    except Exception:
        mon_status = {"consecutive_high_mape_days": 0, "retrains_this_quarter": 0, "last_trained": "2026-04-20"}

    # Check WPP tracking (primary model)
    wpp_t = tracking.get("WPP", {}).get("summary", {})
    mape = wpp_t.get("mape")
    ci_rate = wpp_t.get("ci_hit_rate")
    dir_acc = wpp_t.get("direction_accuracy")
    n_days = wpp_t.get("days_with_actuals", 0)

    # Rule 1: MAPE > 5% for 10+ consecutive days
    if mape is not None and mape > 5.0:
        mon_status["consecutive_high_mape_days"] = mon_status.get("consecutive_high_mape_days", 0) + 1
    else:
        mon_status["consecutive_high_mape_days"] = 0

    if mon_status["consecutive_high_mape_days"] >= 10:
        retrain_needed = True
        retrain_reason = f"MAPE={mape:.1f}% for {mon_status['consecutive_high_mape_days']} consecutive days"

    # Rule 2: CI hit rate < 60% over 20+ days
    if ci_rate is not None and n_days >= 20 and ci_rate < 60.0:
        retrain_needed = True
        retrain_reason = f"CI hit rate={ci_rate:.1f}% over {n_days} days (threshold: 60%)"

    # Rule 3: Direction accuracy < 45% over 20+ days
    if dir_acc is not None and n_days >= 20 and dir_acc < 45.0:
        retrain_needed = True
        retrain_reason = f"Direction accuracy={dir_acc:.1f}% over {n_days} days (below coin flip)"

    # Rule 4: Quarterly schedule (first Monday of Jan, Apr, Jul, Oct)
    today = datetime.utcnow()
    if today.day <= 7 and today.weekday() == 0 and today.month in [1, 4, 7, 10]:
        retrain_needed = True
        retrain_reason = f"Quarterly scheduled retrain ({today.strftime('%Y-%m-%d')})"

    # Guard: minimum 7 days between retrains, max 4 per quarter
    last_trained = mon_status.get("last_trained", "2026-01-01")
    days_since = (today - datetime.strptime(last_trained, "%Y-%m-%d")).days
    if days_since < 7:
        retrain_needed = False
    if mon_status.get("retrains_this_quarter", 0) >= 4:
        retrain_needed = False

    # Trigger retrain if needed
    if retrain_needed and retrain_reason:
        try:
            import urllib.request
            retrain_url = "https://retrain-model-z6uoagg43a-uc.a.run.app"
            payload = json.dumps({
                "trigger": "auto_monitor",
                "reason": retrain_reason,
                "retrains_this_quarter": mon_status.get("retrains_this_quarter", 0),
            }).encode()
            req = urllib.request.Request(retrain_url, data=payload, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=300)
            results["retrain_triggered"] = True
            results["retrain_reason"] = retrain_reason
        except Exception as e:
            results["retrain_trigger_failed"] = str(e)
    else:
        results["retrain_triggered"] = False

    # Save monitoring status
    mon_status["last_checked"] = today.strftime("%Y-%m-%d %H:%M UTC")
    mon_status["current_mape"] = mape
    mon_status["current_ci_rate"] = ci_rate
    mon_status["current_dir_acc"] = dir_acc
    mon_status["days_tracked"] = n_days
    bucket.blob("monitoring/status.json").upload_from_string(json.dumps(mon_status))

    results["monitoring"] = {
        "mape": mape, "ci_rate": ci_rate, "dir_acc": dir_acc,
        "consecutive_high_mape": mon_status["consecutive_high_mape_days"],
        "retrain_needed": retrain_needed,
    }
    results["gcs_updated"] = True

    # 6. Check COMvergence daily features view is accessible (no-op if not loaded yet)
    try:
        comv_check = bq.query(f"""
            SELECT COUNT(*) as n FROM `{PROJECT}.media_stocks.comvergence_daily_features`
            WHERE date = CURRENT_DATE()
        """).result()
        for row in comv_check:
            results["comvergence_features_today"] = row["n"]
    except Exception as e:
        results["comvergence_note"] = f"comvergence_daily_features not available yet: {e}"

    return json.dumps(results), 200, {"Content-Type": "application/json"}
