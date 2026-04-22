#!/usr/bin/env python3
"""Daily update script: fetches latest stock prices, updates forecast tracking,
and refreshes all dashboard data files.

Usage:
    python scripts/daily_update.py              # Update with latest available data
    python scripts/daily_update.py --date 2026-04-21  # Update specific date
"""

import json
import sys
import os
from datetime import datetime, timedelta

# Add project root to path
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

TICKERS = {"WPP": "WPP", "Publicis": "PUB.PA", "Omnicom": "OMC", "SP500": "^GSPC"}
STOCK_DATA_JSON = os.path.join(ROOT, "stock_data.json")
STOCK_DATA_JS = os.path.join(ROOT, "app", "static", "stock_data.js")
FORECAST_TRACKING_JS = os.path.join(ROOT, "app", "static", "forecast_tracking.js")


def fetch_prices(target_date=None):
    """Fetch latest stock prices via yfinance. Returns dict of {name: {date: close}}."""
    import yfinance as yf

    end = datetime.now() + timedelta(days=1)
    start = end - timedelta(days=14)
    prices = {}

    for name, ticker in TICKERS.items():
        df = yf.download(ticker, start=start.strftime("%Y-%m-%d"),
                         end=end.strftime("%Y-%m-%d"), progress=False)
        day_prices = {}
        for d in df.index:
            close = float(df.loc[d, "Close"].iloc[0]) if hasattr(df.loc[d, "Close"], "iloc") else float(df.loc[d, "Close"])
            day_prices[d.strftime("%Y-%m-%d")] = round(close, 2)
        prices[name] = day_prices

    return prices


def update_stock_data_json(prices):
    """Append new dates to stock_data.json (10-year history)."""
    with open(STOCK_DATA_JSON) as f:
        data = json.load(f)

    updated = 0
    for name in TICKERS:
        if name not in data or name not in prices:
            continue
        existing_dates = set(data[name]["dates"])
        for date_str in sorted(prices[name].keys()):
            if date_str not in existing_dates:
                data[name]["dates"].append(date_str)
                data[name]["closes"].append(prices[name][date_str])
                updated += 1

    data["_last_updated"] = datetime.now(tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with open(STOCK_DATA_JSON, "w") as f:
        json.dump(data, f)

    print(f"  stock_data.json: {updated} new data points added")
    return data


def update_stock_data_js(stock_data):
    """Regenerate app/static/stock_data.js (full history for dashboard)."""
    output = {}
    for name in ["WPP", "Publicis", "Omnicom", "SP500"]:
        if name not in stock_data:
            continue
        output[name] = {"dates": stock_data[name]["dates"], "closes": stock_data[name]["closes"]}

    with open(STOCK_DATA_JS, "w") as f:
        f.write(f"const STOCK_DATA = {json.dumps(output)};")

    print(f"  stock_data.js: regenerated (5-year window)")


def update_forecast_tracking(prices):
    """Update forecast_tracking.js with actual prices for dates that were pending."""
    with open(FORECAST_TRACKING_JS) as f:
        content = f.read()

    # Parse the JS constant
    json_str = content.strip()
    if json_str.startswith("const FORECAST_TRACKING = "):
        json_str = json_str[len("const FORECAST_TRACKING = "):]
    if json_str.endswith(";"):
        json_str = json_str[:-1]
    tracking = json.loads(json_str)

    updates = 0
    for name in ["WPP", "Publicis", "Omnicom"]:
        if name not in tracking or name not in prices:
            continue

        actual_prices = prices[name]
        prev_actual = None
        prev_forecast = None
        correct_directions = 0
        total_directions = 0
        errors = []
        n_within = 0

        for entry in tracking[name]["daily"]:
            date = entry["date"]

            # Fill in actual if we have it and it's pending
            if entry["status"] == "pending" and date in actual_prices:
                actual = actual_prices[date]
                forecast = entry["forecast"]
                error = round(actual - forecast, 2)
                error_pct = round((error / actual) * 100, 2)
                within = entry["lower"] <= actual <= entry["upper"]

                entry["actual"] = actual
                entry["error"] = error
                entry["error_pct"] = error_pct
                entry["within_ci"] = within
                entry["status"] = "actual"
                updates += 1
                print(f"  {name} {date}: forecast=${forecast}, actual=${actual}, error={error_pct:+.2f}%, CI={'HIT' if within else 'MISS'}")

            # Accumulate stats for summary
            if entry["status"] == "actual":
                errors.append(abs(entry["error_pct"]))
                if entry["within_ci"]:
                    n_within += 1

                # Direction accuracy
                if prev_actual is not None:
                    actual_dir = entry["actual"] - prev_actual
                    forecast_dir = entry["forecast"] - prev_actual  # forecast vs prev actual
                    if (actual_dir >= 0) == (forecast_dir >= 0):
                        correct_directions += 1
                    total_directions += 1

                prev_actual = entry["actual"]
                prev_forecast = entry["forecast"]

        # Recompute summary
        n_actual = len(errors)
        summary = tracking[name]["summary"]
        summary["days_with_actuals"] = n_actual
        summary["days_pending"] = len(tracking[name]["daily"]) - n_actual
        summary["days_within_ci"] = n_within
        summary["ci_hit_rate"] = round(n_within / n_actual * 100, 1) if n_actual > 0 else None
        summary["mape"] = round(sum(errors) / len(errors), 2) if errors else None
        summary["direction_accuracy"] = round(correct_directions / total_directions * 100, 1) if total_directions > 0 else None

        # Update last actual
        actuals = [e for e in tracking[name]["daily"] if e["status"] == "actual"]
        if actuals:
            summary["last_actual_date"] = actuals[-1]["date"]
            summary["last_actual_price"] = actuals[-1]["actual"]

    with open(FORECAST_TRACKING_JS, "w") as f:
        f.write(f"const FORECAST_TRACKING = {json.dumps(tracking)};")

    print(f"  forecast_tracking.js: {updates} entries updated")
    return tracking


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Daily stock data update")
    parser.add_argument("--date", help="Specific date to update (YYYY-MM-DD)")
    args = parser.parse_args()

    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Starting daily update...")

    # 1. Fetch prices
    print("\n1. Fetching stock prices...")
    prices = fetch_prices(args.date)
    for name, day_prices in prices.items():
        latest = max(day_prices.keys()) if day_prices else "N/A"
        print(f"  {name}: latest={latest}, price=${day_prices.get(latest, 'N/A')}")

    # 2. Update stock_data.json
    print("\n2. Updating stock_data.json...")
    stock_data = update_stock_data_json(prices)

    # 3. Regenerate stock_data.js
    print("\n3. Updating stock_data.js...")
    update_stock_data_js(stock_data)

    # 4. Update forecast tracking
    print("\n4. Updating forecast tracking...")
    tracking = update_forecast_tracking(prices)

    # 5. Summary
    print("\n--- Summary ---")
    for name in ["WPP", "Publicis", "Omnicom"]:
        s = tracking[name]["summary"]
        print(f"  {name}: {s['days_with_actuals']} days tracked, MAPE={s['mape']}%, CI={s['ci_hit_rate']}%, Dir={s['direction_accuracy']}%")

    print(f"\nDone. Last updated: {stock_data['_last_updated']}")


if __name__ == "__main__":
    main()
