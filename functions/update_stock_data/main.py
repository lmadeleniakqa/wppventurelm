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

    results["gcs_updated"] = True
    return json.dumps(results), 200, {"Content-Type": "application/json"}
