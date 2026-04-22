"""Background updater: fetches latest stock prices via yfinance, updates
stock_data.js and forecast_tracking.js in-place so the served dashboard
always reflects the latest market close. Runs on startup + daily at 22:30 ET."""

import builtins
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import partial

# Ensure all prints flush immediately (Cloud Run buffers stdout)
print = partial(builtins.print, flush=True)

TICKERS = {"WPP": "WPP", "Publicis": "PUB.PA", "Omnicom": "OMC", "SP500": "^GSPC"}
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
STOCK_DATA_JS = os.path.join(STATIC_DIR, "stock_data.js")
FORECAST_TRACKING_JS = os.path.join(STATIC_DIR, "forecast_tracking.js")

# US Eastern (UTC-4 during EDT, UTC-5 during EST)
ET_OFFSET = timezone(timedelta(hours=-4))
UPDATE_HOUR_ET = 22   # 10:30 PM ET — after market close + settlement
UPDATE_MINUTE_ET = 30


def _parse_js_const(path, var_name):
    """Read a JS file like 'const FOO = {...};' and return the parsed dict."""
    with open(path) as f:
        content = f.read().strip()
    prefix = f"const {var_name} = "
    if content.startswith(prefix):
        content = content[len(prefix):]
    if content.endswith(";"):
        content = content[:-1]
    return json.loads(content)


def _write_js_const(path, var_name, data):
    """Write a JS file like 'const FOO = {...};'."""
    with open(path, "w") as f:
        f.write(f"const {var_name} = {json.dumps(data)};")


def fetch_prices():
    """Fetch last 14 days of prices via yfinance."""
    import yfinance as yf

    end = datetime.now() + timedelta(days=1)
    start = end - timedelta(days=14)
    prices = {}

    for name, ticker in TICKERS.items():
        try:
            df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                             end=end.strftime("%Y-%m-%d"), progress=False)
            day_prices = {}
            for d in df.index:
                val = df.loc[d, "Close"]
                close = float(val.iloc[0]) if hasattr(val, "iloc") else float(val)
                day_prices[d.strftime("%Y-%m-%d")] = round(close, 2)
            prices[name] = day_prices
        except Exception as e:
            print(f"[updater] WARNING: failed to fetch {name}/{ticker}: {e}")
            prices[name] = {}

    return prices


def update_stock_data(prices):
    """Append new dates to stock_data.js."""
    data = _parse_js_const(STOCK_DATA_JS, "STOCK_DATA")

    added = 0
    for name in TICKERS:
        if name not in data or name not in prices:
            continue
        existing = set(data[name]["dates"])
        for date_str in sorted(prices[name].keys()):
            if date_str not in existing:
                data[name]["dates"].append(date_str)
                data[name]["closes"].append(prices[name][date_str])
                added += 1

    _write_js_const(STOCK_DATA_JS, "STOCK_DATA", data)
    return added


def update_forecast_tracking(prices):
    """Fill in actuals for pending forecast dates, recompute summaries."""
    tracking = _parse_js_const(FORECAST_TRACKING_JS, "FORECAST_TRACKING")

    filled = 0
    for name in ["WPP", "Publicis", "Omnicom"]:
        if name not in tracking or name not in prices:
            continue

        actual_prices = prices[name]
        prev_actual = None
        correct_dirs = 0
        total_dirs = 0
        errors = []
        n_within = 0

        for entry in tracking[name]["daily"]:
            # Fill pending entries that now have actuals
            if entry["status"] == "pending" and entry["date"] in actual_prices:
                actual = actual_prices[entry["date"]]
                forecast = entry["forecast"]
                error = round(actual - forecast, 2)
                error_pct = round((error / actual) * 100, 2)
                within = entry["lower"] <= actual <= entry["upper"]

                entry.update({
                    "actual": actual, "error": error, "error_pct": error_pct,
                    "within_ci": within, "status": "actual",
                })
                filled += 1

            # Accumulate stats
            if entry["status"] == "actual":
                errors.append(abs(entry["error_pct"]))
                if entry["within_ci"]:
                    n_within += 1
                if prev_actual is not None:
                    actual_dir = entry["actual"] - prev_actual
                    forecast_dir = entry["forecast"] - prev_actual
                    if (actual_dir >= 0) == (forecast_dir >= 0):
                        correct_dirs += 1
                    total_dirs += 1
                prev_actual = entry["actual"]

        # Recompute summary
        n_actual = len(errors)
        s = tracking[name]["summary"]
        s["days_with_actuals"] = n_actual
        s["days_pending"] = len(tracking[name]["daily"]) - n_actual
        s["days_within_ci"] = n_within
        s["ci_hit_rate"] = round(n_within / n_actual * 100, 1) if n_actual else None
        s["mape"] = round(sum(errors) / len(errors), 2) if errors else None
        s["direction_accuracy"] = round(correct_dirs / total_dirs * 100, 1) if total_dirs else None

        actuals = [e for e in tracking[name]["daily"] if e["status"] == "actual"]
        if actuals:
            s["last_actual_date"] = actuals[-1]["date"]
            s["last_actual_price"] = actuals[-1]["actual"]

    _write_js_const(FORECAST_TRACKING_JS, "FORECAST_TRACKING", tracking)
    return filled


def run_update():
    """Run a full update cycle. Safe to call from any thread."""
    now = datetime.now(ET_OFFSET)
    print(f"[updater] Starting update at {now.strftime('%Y-%m-%d %H:%M ET')}...")

    try:
        prices = fetch_prices()
        for name, dp in prices.items():
            latest = max(dp.keys()) if dp else "N/A"
            price = dp.get(latest, "N/A")
            print(f"[updater]   {name}: latest={latest}, ${price}")

        added = update_stock_data(prices)
        filled = update_forecast_tracking(prices)

        print(f"[updater] Done: {added} price points added, {filled} forecasts filled")
    except Exception as e:
        print(f"[updater] ERROR: {e}")


def _seconds_until_next_run():
    """Seconds until next 22:30 ET."""
    now = datetime.now(ET_OFFSET)
    target = now.replace(hour=UPDATE_HOUR_ET, minute=UPDATE_MINUTE_ET, second=0, microsecond=0)
    if now >= target:
        target += timedelta(days=1)
    # Skip weekends (Sat=5, Sun=6) — advance to Monday
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _scheduler_loop():
    """Background loop: sleep until next run time, execute, repeat."""
    while True:
        wait = _seconds_until_next_run()
        hrs = wait / 3600
        print(f"[updater] Next update in {hrs:.1f}h")
        time.sleep(wait)
        run_update()


def start_background_updater():
    """Run an immediate update, then start the daily scheduler thread."""
    # Immediate update on startup
    run_update()

    # Schedule daily updates
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()
    print("[updater] Background scheduler started (daily at 10:30 PM ET)")
