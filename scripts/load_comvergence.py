#!/usr/bin/env python3
"""Load COMvergence c-dash CARD Excel data into BigQuery.

Usage:
    python scripts/load_comvergence.py                          # Full load (replace)
    python scripts/load_comvergence.py --mode incremental       # Upsert by card_id
    python scripts/load_comvergence.py --file data/custom.xlsx  # Custom file path
    python scripts/load_comvergence.py --dry-run                # Validate only, no BQ upload
"""

import argparse
import os
import sys
import re
from datetime import datetime

import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_FILE = os.path.join(ROOT, "data", "COMvergence_c-dash_CARD_20260422_1626.xlsx")
PROJECT = "na-analytics"
DATASET = "media_stocks"
TABLE_RAW = "comvergence_raw"


# Column name normalization: multi-line Excel headers → clean snake_case
COLUMN_MAP = {
    "Zone": "zone",
    "Country": "country",
    "ParentCo": "parent_co",
    "Advertiser": "advertiser",
    "Top Brands": "top_brands",
    "Category GAMA": "category_gama",
    "Category": "category",
    "Client Footprint": "client_footprint",
    "Total Net\nMedia Spend\n2025 $M": "total_spend_2025_m",
    "Offline\nNet Media Spend\n2025 $M": "offline_spend_2025_m",
    "Digital Net\nMedia Spend\n2025 $M": "digital_spend_2025_m",
    "Digital Share\n2025 %": "digital_share_2025_pct",
    "Total Net\nMedia Spend\n2024 $M": "total_spend_2024_m",
    "Offline\nNet Media Spend\n2024 $M": "offline_spend_2024_m",
    "Digital Net\nMedia Spend\n2024 $M": "digital_spend_2024_m",
    "Digital Share\n2024 %": "digital_share_2024_pct",
    "Total Net\nMedia Spend\n2023 $M": "total_spend_2023_m",
    "Offline\nNet Media Spend\n2023 $M": "offline_spend_2023_m",
    "Digital Net\nMedia Spend\n2023 $M": "digital_spend_2023_m",
    "Digital Share\n2023 %": "digital_share_2023_pct",
    "Holding": "holding",
    "Group": "group_name",
    "Agency Network": "agency_network",
    "Agency": "agency",
    "Agency City": "agency_city",
    "Bespoke Unit": "bespoke_unit",
    "Assignments": "assignments",
    "Media": "media",
    "Last Announcement\nQuarter": "last_announcement_quarter",
    "Last Announcement\nDate": "last_announcement_date",
    "Effective Move\nDate": "effective_move_date",
    "First Win Date": "first_win_date",
    "Move Type": "move_type",
    "Last\xa0Incumbent\nHolding": "last_incumbent_holding",
    "Last Incumbent\nGroup": "last_incumbent_group",
    "Last Incumbent\nAgency Network": "last_incumbent_agency_network",
    "Last Incumbent\nAgency": "last_incumbent_agency",
    "Pitch Coverage": "pitch_coverage",
    "Winner Coverage details": "winner_coverage_details",
    "Consultants": "consultants",
    "Comments": "comments",
    "Last update": "last_update",
    "Card id": "card_id",
}

# Holding → stock ticker mapping
HOLDING_TO_TICKER = {
    "WPP": "WPP",
    "Publicis Groupe": "PUB.PA",
    "Omnicom": "OMC",
}


def load_excel(filepath):
    """Read COMvergence Excel and normalize columns."""
    print(f"Reading {filepath}...")
    df = pd.read_excel(filepath, sheet_name="Data", engine="openpyxl")
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

    # Rename columns
    df = df.rename(columns=COLUMN_MAP)

    # Any columns not in the map — normalize them too
    for col in df.columns:
        if col not in COLUMN_MAP.values():
            clean = re.sub(r'[^a-z0-9]+', '_', col.lower().replace('\n', ' ')).strip('_')
            df = df.rename(columns={col: clean})

    # Type coercions
    date_cols = ["last_announcement_date", "effective_move_date", "first_win_date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # last_update is a timestamp string
    if "last_update" in df.columns:
        df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")

    # Ensure card_id is string
    if "card_id" in df.columns:
        df["card_id"] = df["card_id"].astype(str)

    # Spend columns: ensure numeric
    spend_cols = [c for c in df.columns if "spend" in c or "share" in c]
    for col in spend_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def aggregate_global_events(df, window_days=28, min_spend_m=50):
    """Aggregate local market-level moves into global events.

    COMvergence tracks at the local level (e.g., Coca-Cola moving in China, France,
    Brazil are separate rows). This function clusters moves for the same advertiser,
    from/to the same holdings, within a window_days period into a single global event.

    Only includes events with total spend >= min_spend_m.

    Returns a DataFrame of global move events.
    """
    moves = df[df["move_type"].isin(["Agency", "New-assignment"])].copy()
    moves["spend"] = moves["total_spend_2025_m"].fillna(
        moves["total_spend_2024_m"]).fillna(
        moves["total_spend_2023_m"]).fillna(0)
    moves["announce_date"] = pd.to_datetime(moves["last_announcement_date"], errors="coerce")
    moves = moves.dropna(subset=["announce_date"])
    moves = moves[moves["holding"] != moves["last_incumbent_holding"]]

    global_events = []
    grouped = moves.groupby(["advertiser", "holding", "last_incumbent_holding"])

    for (adv, to_h, from_h), group_df in grouped:
        sorted_df = group_df.sort_values("announce_date")
        cluster = []
        cluster_start = None

        for _, row in sorted_df.iterrows():
            if cluster_start is None or (row["announce_date"] - cluster_start).days <= window_days:
                cluster.append(row)
                if cluster_start is None:
                    cluster_start = row["announce_date"]
            else:
                # Emit cluster
                total_spend = sum(r["spend"] for r in cluster)
                if total_spend >= min_spend_m:
                    markets = list(set(r["country"] for r in cluster if pd.notna(r.get("country"))))
                    global_events.append({
                        "advertiser": adv,
                        "from_holding": from_h,
                        "to_holding": to_h,
                        "date": cluster[0]["announce_date"],
                        "quarter": str(cluster[0].get("last_announcement_quarter", "")),
                        "spend_m": round(total_spend, 1),
                        "n_markets": len(cluster),
                        "markets": markets[:5],
                        "category": str(cluster[0].get("category_gama", "")),
                        "to_network": str(cluster[0].get("agency_network", "")),
                        "from_network": str(cluster[0].get("last_incumbent_agency_network", "")),
                    })
                cluster = [row]
                cluster_start = row["announce_date"]

        # Final cluster
        if cluster:
            total_spend = sum(r["spend"] for r in cluster)
            if total_spend >= min_spend_m:
                markets = list(set(r["country"] for r in cluster if pd.notna(r.get("country"))))
                global_events.append({
                    "advertiser": adv,
                    "from_holding": from_h,
                    "to_holding": to_h,
                    "date": cluster[0]["announce_date"],
                    "quarter": str(cluster[0].get("last_announcement_quarter", "")),
                    "spend_m": round(total_spend, 1),
                    "n_markets": len(cluster),
                    "markets": markets[:5],
                    "category": str(cluster[0].get("category_gama", "")),
                    "to_network": str(cluster[0].get("agency_network", "")),
                    "from_network": str(cluster[0].get("last_incumbent_agency_network", "")),
                })

    result = pd.DataFrame(global_events).sort_values("date", ascending=False) if global_events else pd.DataFrame()
    print(f"\n  Global event aggregation: {len(moves)} local moves → {len(result)} global events (window={window_days}d, min=${min_spend_m}M)")
    return result


def validate(df):
    """Run validation checks on loaded data."""
    print("\nValidation:")
    print(f"  Rows: {len(df)}")
    print(f"  Columns: {list(df.columns)}")

    # Check expected row count range
    if len(df) < 30000:
        print(f"  WARNING: Expected ~40K rows, got {len(df)}")

    # Spend totals by holding
    if "holding" in df.columns and "total_spend_2025_m" in df.columns:
        spend = df.groupby("holding")["total_spend_2025_m"].sum().sort_values(ascending=False)
        print("\n  2025 Spend by Holding ($M):")
        for h, s in spend.head(8).items():
            print(f"    {h}: ${s:,.0f}M")

    # Holdings distribution
    if "holding" in df.columns:
        counts = df["holding"].value_counts()
        print("\n  Assignments by Holding:")
        for h, c in counts.head(8).items():
            print(f"    {h}: {c}")

    # Move types
    if "move_type" in df.columns:
        moves = df["move_type"].value_counts()
        print("\n  Move Types:")
        for m, c in moves.head(8).items():
            print(f"    {m}: {c}")

    # Null rates for key columns
    key_cols = ["holding", "total_spend_2025_m", "last_announcement_date", "move_type", "card_id"]
    print("\n  Null rates (key columns):")
    for col in key_cols:
        if col in df.columns:
            null_pct = df[col].isnull().mean() * 100
            print(f"    {col}: {null_pct:.1f}%")

    print("\n  Validation complete.")
    return True


def upload_to_bigquery(df, mode="replace"):
    """Upload DataFrame to BigQuery."""
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT)
    table_id = f"{PROJECT}.{DATASET}.{TABLE_RAW}"

    if mode == "replace":
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
            schema=_get_schema(),
        )
        print(f"\nUploading {len(df)} rows to {table_id} (REPLACE)...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"  Loaded {job.output_rows} rows.")

    elif mode == "incremental":
        # Load to temp table, then MERGE
        tmp_table = f"{PROJECT}.{DATASET}._comvergence_staging"
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
            schema=_get_schema(),
        )
        print(f"\nStaging {len(df)} rows to {tmp_table}...")
        job = client.load_table_from_dataframe(df, tmp_table, job_config=job_config)
        job.result()

        # MERGE on card_id
        merge_sql = f"""
        MERGE `{table_id}` T
        USING `{tmp_table}` S
        ON T.card_id = S.card_id
        WHEN MATCHED THEN
            UPDATE SET
                zone = S.zone, country = S.country, parent_co = S.parent_co,
                advertiser = S.advertiser, top_brands = S.top_brands,
                category_gama = S.category_gama, category = S.category,
                client_footprint = S.client_footprint,
                total_spend_2025_m = S.total_spend_2025_m,
                offline_spend_2025_m = S.offline_spend_2025_m,
                digital_spend_2025_m = S.digital_spend_2025_m,
                digital_share_2025_pct = S.digital_share_2025_pct,
                total_spend_2024_m = S.total_spend_2024_m,
                offline_spend_2024_m = S.offline_spend_2024_m,
                digital_spend_2024_m = S.digital_spend_2024_m,
                digital_share_2024_pct = S.digital_share_2024_pct,
                total_spend_2023_m = S.total_spend_2023_m,
                offline_spend_2023_m = S.offline_spend_2023_m,
                digital_spend_2023_m = S.digital_spend_2023_m,
                digital_share_2023_pct = S.digital_share_2023_pct,
                holding = S.holding, group_name = S.group_name,
                agency_network = S.agency_network, agency = S.agency,
                agency_city = S.agency_city, bespoke_unit = S.bespoke_unit,
                assignments = S.assignments, media = S.media,
                last_announcement_quarter = S.last_announcement_quarter,
                last_announcement_date = S.last_announcement_date,
                effective_move_date = S.effective_move_date,
                first_win_date = S.first_win_date,
                move_type = S.move_type,
                last_incumbent_holding = S.last_incumbent_holding,
                last_incumbent_group = S.last_incumbent_group,
                last_incumbent_agency_network = S.last_incumbent_agency_network,
                last_incumbent_agency = S.last_incumbent_agency,
                pitch_coverage = S.pitch_coverage,
                winner_coverage_details = S.winner_coverage_details,
                consultants = S.consultants, comments = S.comments,
                last_update = S.last_update
        WHEN NOT MATCHED THEN
            INSERT ROW
        """
        print(f"  Running MERGE on card_id...")
        result = client.query(merge_sql).result()
        print(f"  MERGE complete.")

        # Clean up staging
        client.delete_table(tmp_table, not_found_ok=True)

    # Create the aggregation view
    _create_aggregation_view(client)

    print(f"\nDone. Table: {table_id}")


def _get_schema():
    """BigQuery schema for comvergence_raw."""
    from google.cloud import bigquery

    return [
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("parent_co", "STRING"),
        bigquery.SchemaField("advertiser", "STRING"),
        bigquery.SchemaField("top_brands", "STRING"),
        bigquery.SchemaField("category_gama", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("client_footprint", "STRING"),
        bigquery.SchemaField("total_spend_2025_m", "FLOAT64"),
        bigquery.SchemaField("offline_spend_2025_m", "FLOAT64"),
        bigquery.SchemaField("digital_spend_2025_m", "FLOAT64"),
        bigquery.SchemaField("digital_share_2025_pct", "FLOAT64"),
        bigquery.SchemaField("total_spend_2024_m", "FLOAT64"),
        bigquery.SchemaField("offline_spend_2024_m", "FLOAT64"),
        bigquery.SchemaField("digital_spend_2024_m", "FLOAT64"),
        bigquery.SchemaField("digital_share_2024_pct", "FLOAT64"),
        bigquery.SchemaField("total_spend_2023_m", "FLOAT64"),
        bigquery.SchemaField("offline_spend_2023_m", "FLOAT64"),
        bigquery.SchemaField("digital_spend_2023_m", "FLOAT64"),
        bigquery.SchemaField("digital_share_2023_pct", "FLOAT64"),
        bigquery.SchemaField("holding", "STRING"),
        bigquery.SchemaField("group_name", "STRING"),
        bigquery.SchemaField("agency_network", "STRING"),
        bigquery.SchemaField("agency", "STRING"),
        bigquery.SchemaField("agency_city", "STRING"),
        bigquery.SchemaField("bespoke_unit", "STRING"),
        bigquery.SchemaField("assignments", "STRING"),
        bigquery.SchemaField("media", "STRING"),
        bigquery.SchemaField("last_announcement_quarter", "STRING"),
        bigquery.SchemaField("last_announcement_date", "DATE"),
        bigquery.SchemaField("effective_move_date", "DATE"),
        bigquery.SchemaField("first_win_date", "DATE"),
        bigquery.SchemaField("move_type", "STRING"),
        bigquery.SchemaField("last_incumbent_holding", "STRING"),
        bigquery.SchemaField("last_incumbent_group", "STRING"),
        bigquery.SchemaField("last_incumbent_agency_network", "STRING"),
        bigquery.SchemaField("last_incumbent_agency", "STRING"),
        bigquery.SchemaField("pitch_coverage", "STRING"),
        bigquery.SchemaField("winner_coverage_details", "STRING"),
        bigquery.SchemaField("consultants", "STRING"),
        bigquery.SchemaField("comments", "STRING"),
        bigquery.SchemaField("last_update", "TIMESTAMP"),
        bigquery.SchemaField("card_id", "STRING"),
    ]


def _create_aggregation_view(client):
    """Create/update the comvergence_daily_features aggregation view.

    Produces daily rows per holding (WPP, Publicis Groupe, Omnicom) with
    rolling competition metrics. Uses last_announcement_date to avoid
    look-ahead bias.
    """
    view_id = f"{PROJECT}.{DATASET}.comvergence_daily_features"
    view_sql = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH
    dates AS (
        SELECT DISTINCT date FROM `{PROJECT}.{DATASET}.daily_features`
    ),
    holdings AS (
        SELECT 'WPP' AS holding, 'WPP' AS ticker UNION ALL
        SELECT 'Publicis Groupe', 'PUB.PA' UNION ALL
        SELECT 'Omnicom', 'OMC'
    ),
    date_holding AS (
        SELECT d.date, h.holding, h.ticker FROM dates d CROSS JOIN holdings h
    ),
    wins AS (
        SELECT holding AS winner, last_announcement_date AS announce_date,
            COALESCE(total_spend_2025_m, total_spend_2024_m, total_spend_2023_m, 0) AS spend_m,
            last_incumbent_holding AS loser
        FROM `{PROJECT}.{DATASET}.{TABLE_RAW}`
        WHERE move_type IN ('Agency', 'New-assignment')
            AND holding != last_incumbent_holding AND last_announcement_date IS NOT NULL
    ),
    losses AS (
        SELECT last_incumbent_holding AS loser, last_announcement_date AS announce_date,
            COALESCE(total_spend_2025_m, total_spend_2024_m, total_spend_2023_m, 0) AS spend_m,
            holding AS winner
        FROM `{PROJECT}.{DATASET}.{TABLE_RAW}`
        WHERE move_type IN ('Agency', 'New-assignment')
            AND holding != last_incumbent_holding AND last_announcement_date IS NOT NULL
    ),
    portfolio AS (
        SELECT holding, COUNT(*) AS total_assignments,
            SUM(COALESCE(total_spend_2025_m, 0)) AS total_spend_m,
            SAFE_DIVIDE(SUM(COALESCE(digital_spend_2025_m, 0)),
                NULLIF(SUM(COALESCE(total_spend_2025_m, 0)), 0)) * 100 AS digital_share_pct
        FROM `{PROJECT}.{DATASET}.{TABLE_RAW}` GROUP BY holding
    ),
    category_hhi AS (
        SELECT holding, SUM(share_sq) AS concentration_hhi FROM (
            SELECT holding, POW(SAFE_DIVIDE(cat_spend, NULLIF(total_holding_spend, 0)), 2) AS share_sq
            FROM (
                SELECT holding, category_gama,
                    SUM(COALESCE(total_spend_2025_m, 0)) AS cat_spend,
                    SUM(SUM(COALESCE(total_spend_2025_m, 0))) OVER (PARTITION BY holding) AS total_holding_spend
                FROM `{PROJECT}.{DATASET}.{TABLE_RAW}` GROUP BY holding, category_gama
            )
        ) GROUP BY holding
    ),
    total_market AS (
        SELECT SUM(COALESCE(total_spend_2025_m, 0)) AS total_market_spend_m
        FROM `{PROJECT}.{DATASET}.{TABLE_RAW}`
    ),
    rolling AS (
        SELECT dh.date, dh.holding, dh.ticker,
            (SELECT COUNT(*) FROM wins w WHERE w.winner = dh.holding
                AND w.announce_date BETWEEN DATE_SUB(dh.date, INTERVAL 90 DAY) AND dh.date) AS wins_90d,
            (SELECT COUNT(*) FROM losses l WHERE l.loser = dh.holding
                AND l.announce_date BETWEEN DATE_SUB(dh.date, INTERVAL 90 DAY) AND dh.date) AS losses_90d,
            (SELECT COALESCE(SUM(spend_m), 0) FROM wins w WHERE w.winner = dh.holding
                AND w.announce_date BETWEEN DATE_SUB(dh.date, INTERVAL 90 DAY) AND dh.date) AS win_spend_90d_m,
            (SELECT COALESCE(SUM(spend_m), 0) FROM losses l WHERE l.loser = dh.holding
                AND l.announce_date BETWEEN DATE_SUB(dh.date, INTERVAL 90 DAY) AND dh.date) AS loss_spend_90d_m,
            (SELECT COALESCE(SUM(spend_m), 0) FROM losses l WHERE l.loser = dh.holding
                AND l.winner IN ('WPP', 'Publicis Groupe', 'Omnicom')
                AND l.announce_date BETWEEN DATE_SUB(dh.date, INTERVAL 180 DAY) AND dh.date) AS competitive_pressure_180d_m
        FROM date_holding dh
    )
    SELECT r.date, r.ticker,
        (r.wins_90d - r.losses_90d) AS comv_net_wins_90d,
        ROUND((r.win_spend_90d_m - r.loss_spend_90d_m) / 1000, 3) AS comv_net_spend_90d_bn,
        ROUND(SAFE_DIVIDE(p.total_spend_m, tm.total_market_spend_m) * 100, 2) AS comv_market_share_pct,
        ROUND(p.digital_share_pct, 1) AS comv_digital_share_pct,
        ROUND(r.competitive_pressure_180d_m / 1000, 3) AS comv_competitive_pressure,
        ROUND(ch.concentration_hhi, 4) AS comv_concentration_hhi
    FROM rolling r
    LEFT JOIN portfolio p ON p.holding = r.holding
    LEFT JOIN category_hhi ch ON ch.holding = r.holding
    CROSS JOIN total_market tm
    ORDER BY r.date, r.ticker
    """
    print("\nCreating aggregation view...")
    client.query(view_sql).result()
    print(f"  View created: {view_id}")


def generate_comvergence_js(df, output_path=None, global_events=None):
    """Generate comvergence_data.js for the dashboard from the raw DataFrame."""
    if output_path is None:
        output_path = os.path.join(ROOT, "app", "static", "comvergence_data.js")

    big6 = ["WPP", "Publicis Groupe", "Omnicom", "dentsu", "Havas", "Independents"]

    def safe_spend(row):
        """Get spend from row, handling NaN. Prefers 2025, falls back to 2024, then 2023."""
        for col in ["total_spend_2025_m", "total_spend_2024_m", "total_spend_2023_m"]:
            v = row.get(col)
            if pd.notna(v):
                return float(v)
        return 0.0

    # 1. Portfolio summary per holding
    portfolio = {}
    for h in big6:
        hdf = df[df["holding"] == h]
        portfolio[h] = {
            "assignments": int(len(hdf)),
            "spend_2025_m": round(float(hdf["total_spend_2025_m"].sum()), 1),
            "spend_2024_m": round(float(hdf["total_spend_2024_m"].sum()), 1),
            "spend_2023_m": round(float(hdf["total_spend_2023_m"].sum()), 1),
            "digital_spend_2025_m": round(float(hdf["digital_spend_2025_m"].sum()), 1),
            "digital_share_pct": round(
                float(hdf["digital_spend_2025_m"].sum() / max(hdf["total_spend_2025_m"].sum(), 1) * 100), 1
            ),
            "spend_yoy_pct": round(
                float((hdf["total_spend_2025_m"].sum() - hdf["total_spend_2024_m"].sum())
                      / max(hdf["total_spend_2024_m"].sum(), 1) * 100), 1
            ),
        }

    # 1b. Agency hierarchy — Holding → Group → Agency Network → Agency (with spend)
    hierarchy = {}
    for h in big6:
        hdf = df[df["holding"] == h]
        networks = {}
        for network, ndf in hdf.groupby("agency_network"):
            net_spend = float(ndf["total_spend_2025_m"].sum())
            if net_spend < 100:
                continue
            agencies = {}
            for agency, adf in ndf.groupby("agency"):
                a_spend = float(adf["total_spend_2025_m"].sum())
                if a_spend < 100:
                    continue
                agencies[agency] = {
                    "assignments": int(len(adf)),
                    "spend_m": round(a_spend, 1),
                }
            networks[network] = {
                "assignments": int(len(ndf)),
                "spend_m": round(net_spend, 1),
                "agencies": agencies,
            }
        hierarchy[h] = {
            "total_assignments": int(len(hdf)),
            "total_spend_m": round(float(hdf["total_spend_2025_m"].sum()), 1),
            "networks": dict(sorted(networks.items(), key=lambda x: -x[1]["spend_m"])),
        }

    # 2. Competitive flow matrix — from global events ($50M+ aggregated moves)
    flows = {}
    if global_events is not None and len(global_events) > 0:
        ge_flows = global_events[
            global_events["from_holding"].isin(big6) & global_events["to_holding"].isin(big6)
        ]
        for _, row in ge_flows.iterrows():
            key = f"{row['from_holding']}|{row['to_holding']}"
            if key not in flows:
                flows[key] = {"from": row["from_holding"], "to": row["to_holding"], "count": 0, "spend_m": 0}
            flows[key]["count"] += 1
            flows[key]["spend_m"] += row["spend_m"]
    flow_list = sorted(flows.values(), key=lambda x: -x["spend_m"])
    for f in flow_list:
        f["spend_m"] = round(f["spend_m"], 1)

    # 3. Movement timeline by year — from global events
    timeline = {}
    for h in ["WPP", "Publicis Groupe", "Omnicom"]:
        yearly = {}
        if global_events is not None and len(global_events) > 0:
            h_events = global_events[
                (global_events["to_holding"] == h) | (global_events["from_holding"] == h)
            ]
            for _, row in h_events.iterrows():
                year = row["date"].year if pd.notna(row.get("date")) else None
                if not year or year < 2015:
                    continue
                if year not in yearly:
                    yearly[year] = {"wins": 0, "losses": 0, "win_spend_m": 0, "loss_spend_m": 0}
                if row["to_holding"] == h:
                    yearly[year]["wins"] += 1
                    yearly[year]["win_spend_m"] += row["spend_m"]
                if row["from_holding"] == h:
                    yearly[year]["losses"] += 1
                    yearly[year]["loss_spend_m"] += row["spend_m"]
        for y in yearly:
            yearly[y]["win_spend_m"] = round(yearly[y]["win_spend_m"], 1)
            yearly[y]["loss_spend_m"] = round(yearly[y]["loss_spend_m"], 1)
        timeline[h] = dict(sorted(yearly.items()))

    # 3b. Global moves ledger for the dashboard (top 300 by spend)
    global_moves_js = []
    if global_events is not None and len(global_events) > 0:
        for _, row in global_events.head(300).iterrows():
            global_moves_js.append({
                "advertiser": row["advertiser"],
                "from_h": row["from_holding"],
                "to_h": row["to_holding"],
                "date": str(row["quarter"]),
                "spend_m": round(row["spend_m"], 1),
                "n_markets": int(row["n_markets"]),
                "category": row.get("category", ""),
                "to_net": row.get("to_network", ""),
                "from_net": row.get("from_network", ""),
            })

    # 4. Category breakdown for WPP
    wpp_cats = {}
    wpp_df = df[df["holding"] == "WPP"]
    cat_agg = wpp_df.groupby("category_gama").agg(
        count=("card_id", "count"),
        spend_m=("total_spend_2025_m", "sum"),
    ).sort_values("spend_m", ascending=False)
    for cat, row in cat_agg.head(15).iterrows():
        wpp_cats[cat] = {"count": int(row["count"]), "spend_m": round(float(row["spend_m"]), 1)}

    # 5. Top WPP accounts by spend (at-risk: long tenure or recent pitch activity)
    top_accounts = []
    wpp_sorted = wpp_df.sort_values("total_spend_2025_m", ascending=False).head(30)
    for _, row in wpp_sorted.iterrows():
        acct = {
            "advertiser": str(row.get("advertiser", "")),
            "parent_co": str(row.get("parent_co", "")),
            "category": str(row.get("category_gama", "")),
            "spend_2025_m": round(safe_spend(row), 1),
            "agency_network": str(row.get("agency_network", "")),
            "agency": str(row.get("agency", "")),
            "group_name": str(row.get("group_name", "")),
            "first_win": str(row.get("first_win_date", ""))[:10] if pd.notna(row.get("first_win_date")) else None,
            "move_type": str(row.get("move_type", "")),
            "zone": str(row.get("zone", "")),
            "country": str(row.get("country", "")),
        }
        top_accounts.append(acct)

    # 6. Geographic breakdown
    geo = {}
    for zone in ["EMEA", "APAC", "N.A.", "LATAM"]:
        zdf = wpp_df[wpp_df["zone"] == zone]
        geo[zone] = {
            "assignments": int(len(zdf)),
            "spend_m": round(float(zdf["total_spend_2025_m"].sum()), 1),
        }

    # 7. Advertiser intelligence lookup — per-advertiser detail for click-through modal
    #    Keyed by advertiser name (lowercased), includes agency history + moves + spend
    advertiser_intel = {}
    for adv_name, adf in df.groupby("advertiser"):
        # Get all assignments for this advertiser
        assignments = []
        for _, row in adf.sort_values("total_spend_2025_m", ascending=False).iterrows():
            assignments.append({
                "holding": str(row.get("holding", "")),
                "agency": str(row.get("agency", "")),
                "zone": str(row.get("zone", "")),
                "country": str(row.get("country", "")),
                "category": str(row.get("category_gama", "")),
                "spend_2025_m": round(safe_spend(row), 1),
                "spend_2024_m": round(float(row["total_spend_2024_m"]) if pd.notna(row.get("total_spend_2024_m")) else 0, 1),
                "digital_share_pct": round(float(row["digital_share_2025_pct"]) if pd.notna(row.get("digital_share_2025_pct")) else 0, 1),
                "move_type": str(row.get("move_type", "")),
                "last_announcement": str(row.get("last_announcement_quarter", "")),
                "first_win": str(row.get("first_win_date", ""))[:10] if pd.notna(row.get("first_win_date")) else None,
                "last_incumbent_holding": str(row.get("last_incumbent_holding", "")),
                "last_incumbent_agency": str(row.get("last_incumbent_agency", "")),
            })

        total_spend = sum(a["spend_2025_m"] for a in assignments)
        if total_spend < 500:  # skip small advertisers to keep file size under 500KB
            continue

        parent_co = str(adf.iloc[0].get("parent_co", ""))

        # Build concise move history (only actual moves, not retained)
        moves = []
        for a in assignments:
            if a["move_type"] in ("Agency", "New-assignment", "Transfer") and a["last_incumbent_holding"]:
                moves.append({
                    "date": a["last_announcement"],
                    "from_h": a["last_incumbent_holding"],
                    "from_a": a["last_incumbent_agency"],
                    "to_h": a["holding"],
                    "to_a": a["agency"],
                    "zone": a["zone"],
                    "spend_m": a["spend_2025_m"],
                })

        # Build holding → agency network breakdown
        holding_detail = []
        for holding_name, hgrp in adf.groupby("holding"):
            h_spend = float(hgrp["total_spend_2025_m"].sum())
            networks = {}
            for net, ngrp in hgrp.groupby("agency_network"):
                n_spend = float(ngrp["total_spend_2025_m"].sum())
                if n_spend > 0:
                    networks[net] = round(n_spend, 1)
            holding_detail.append({
                "holding": holding_name,
                "spend_m": round(h_spend, 1),
                "count": int(len(hgrp)),
                "networks": dict(sorted(networks.items(), key=lambda x: -x[1])[:4]),
            })
        holding_detail.sort(key=lambda x: -x["spend_m"])

        advertiser_intel[adv_name] = {
            "parent_co": parent_co,
            "total_spend_2025_m": round(total_spend, 1),
            "n_markets": len(assignments),
            "holdings": holding_detail[:6],
            "top_brands": str(adf.iloc[0].get("top_brands", ""))[:80],
            "category": str(adf.iloc[0].get("category_gama", "")),
            "moves": moves[:6],
        }

    import json
    data = {
        "portfolio": portfolio,
        "hierarchy": hierarchy,
        "flows": flow_list,
        "timeline": timeline,
        "wpp_categories": wpp_cats,
        "wpp_top_accounts": top_accounts,
        "wpp_geo": geo,
        "global_moves": global_moves_js,
        "advertiser_intel": advertiser_intel,
        "_generated": datetime.now(tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    with open(output_path, "w") as f:
        f.write(f"const COMVERGENCE = {json.dumps(data, separators=(',', ':'))};")

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nGenerated {output_path} ({size_kb:.0f} KB)")
    return data


def generate_advertisers_js(df, output_path=None):
    """Generate advertisers_data.js from COMvergence — top 100 advertisers by global spend."""
    if output_path is None:
        output_path = os.path.join(ROOT, "app", "static", "advertisers_data.js")

    import json

    # Map COMvergence categories to pitch prediction sectors
    SECTOR_MAP = {
        "FMCG (Food & Soft Drinks)": "FMCG/Beverages",
        "FMCG (Care)": "FMCG",
        "Automotive": "Auto",
        "Technology & IT": "Tech",
        "Pharmaceutical": "Pharma",
        "Financial": "Finance",
        "Travel, Tourism & Leisure": "Travel",
        "Entertainment & Media": "Media/Entertainment",
        "Retail": "Retail",
        "Sports & Clothing": "Apparel/Sportswear",
        "Home Goods": "FMCG",
        "Insurance": "Finance",
        "Alcohol": "FMCG/Beverages",
        "Luxury": "Luxury",
        "Games, Toys, Gambling": "Media/Entertainment",
        "Government": "Other",
        "Restaurants": "QSR",
        "Other Key Categories": "Other",
        "Telecom": "Telecom",
        "Energy": "Energy",
    }

    # Aggregate by advertiser across all markets
    agg = df.groupby("advertiser").agg(
        total_spend_2025=("total_spend_2025_m", "sum"),
        total_spend_2024=("total_spend_2024_m", "sum"),
        n_markets=("card_id", "count"),
        category=("category_gama", "first"),
        parent_co=("parent_co", "first"),
    ).sort_values("total_spend_2025", ascending=False)

    advertisers = []
    for adv_name, row in agg.head(100).iterrows():
        adf = df[df["advertiser"] == adv_name]

        # Determine primary holding (by spend)
        holding_spend = adf.groupby("holding")["total_spend_2025_m"].sum().sort_values(ascending=False)
        top_holdings = holding_spend.head(3)

        if len(top_holdings) >= 2 and top_holdings.iloc[1] > top_holdings.iloc[0] * 0.3:
            group = "Mixed"
        else:
            group = top_holdings.index[0] if len(top_holdings) > 0 else "Unknown"
            # Normalize group names to match pitch prediction expectations
            if group == "Publicis Groupe":
                group = "Publicis"

        # Primary agency description
        primary_agencies = []
        for h_name, h_spend in top_holdings.items():
            h_df = adf[adf["holding"] == h_name]
            top_net = h_df.groupby("agency_network")["total_spend_2025_m"].sum().sort_values(ascending=False)
            net_name = top_net.index[0] if len(top_net) > 0 else h_name
            short_h = h_name.replace("Publicis Groupe", "Publicis")
            primary_agencies.append(f"{net_name} ({short_h})")
        agency_str = "; ".join(primary_agencies[:3])

        # Last review date — most recent move announcement
        moves = adf[adf["move_type"].isin(["Agency", "New-assignment", "Transfer"])]
        last_review_dates = moves["last_announcement_date"].dropna()
        if len(last_review_dates) > 0:
            last_review_year = int(last_review_dates.max().year)
        else:
            # Use first_win_date as fallback
            fw = adf["first_win_date"].dropna()
            last_review_year = int(fw.max().year) if len(fw) > 0 else 2020

        sector = SECTOR_MAP.get(row["category"], "Other")
        spend = round(float(row["total_spend_2025"]), 0) if pd.notna(row["total_spend_2025"]) else 0

        advertisers.append({
            "name": adv_name,
            "spend": int(spend),
            "group": group,
            "agency": agency_str,
            "sector": sector,
            "last_review": last_review_year,
        })

    with open(output_path, "w") as f:
        f.write(f"const ADVERTISERS = {json.dumps(advertisers, separators=(',', ':'))};")

    print(f"Generated {output_path} ({len(advertisers)} advertisers)")


def generate_pitch_prediction_params(df, output_path=None, global_events=None):
    """Compute COMvergence-derived parameters for the pitch prediction model.

    Updates SECTOR_FREQ (sector churn rates), GROUP_VULN (holding vulnerability),
    and FLOW (transition matrix) based on actual COMvergence move data.
    Writes updated values to a comment block in pitch_prediction.js header.
    """
    # Compute actual sector churn rates from COMvergence
    moves = df[df["move_type"].isin(["Agency", "New-assignment"])].copy()
    total_by_cat = df.groupby("category_gama")["card_id"].count()
    moves_by_cat = moves.groupby("category_gama")["card_id"].count()

    sector_churn = {}
    for cat in total_by_cat.index:
        total = total_by_cat.get(cat, 0)
        moved = moves_by_cat.get(cat, 0)
        if total > 50:  # only categories with enough data
            rate = round(moved / total, 2)
            sector_churn[cat] = rate

    # Compute holding vulnerability (loss rate)
    big_holdings = ["WPP", "Publicis Groupe", "Omnicom", "dentsu", "Havas"]
    group_vuln = {}
    for h in big_holdings:
        total = len(df[df["holding"] == h])
        lost = len(moves[moves["last_incumbent_holding"] == h])
        if total > 0:
            group_vuln[h] = round(lost / total, 2)

    # Compute flow transition matrix
    flows = {}
    for src in big_holdings:
        src_moves = moves[moves["last_incumbent_holding"] == src]
        total_lost = len(src_moves)
        if total_lost == 0:
            continue
        dests = {}
        for dst in big_holdings:
            if dst == src:
                continue
            count = len(src_moves[src_moves["holding"] == dst])
            if count > 0:
                dests[dst] = round(count / total_lost, 2)
        flows[src] = dests

    # Print the computed values (for manual verification)
    print("\nCOMvergence-derived pitch prediction parameters:")
    print("  Sector churn rates (top 10):")
    for cat, rate in sorted(sector_churn.items(), key=lambda x: -x[1])[:10]:
        print(f"    {cat}: {rate}")
    print("  Holding vulnerability:")
    for h, v in sorted(group_vuln.items(), key=lambda x: -x[1]):
        print(f"    {h}: {v}")
    print("  Flow matrix (where lost accounts go):")
    for src, dests in flows.items():
        print(f"    {src} → {dests}")


def main():
    parser = argparse.ArgumentParser(description="Load COMvergence data to BigQuery")
    parser.add_argument("--file", default=DEFAULT_FILE, help="Path to Excel file")
    parser.add_argument("--mode", choices=["replace", "incremental"], default="replace",
                        help="Load mode: replace (full) or incremental (upsert by card_id)")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, no BQ upload")
    parser.add_argument("--skip-js", action="store_true", help="Skip dashboard JS generation")
    args = parser.parse_args()

    df = load_excel(args.file)
    validate(df)

    if not args.skip_js:
        global_events = aggregate_global_events(df)
        generate_comvergence_js(df, global_events=global_events)
        generate_advertisers_js(df)
        generate_pitch_prediction_params(df, global_events=global_events)

    if args.dry_run:
        print("\n--dry-run: Skipping BigQuery upload.")
        return

    upload_to_bigquery(df, mode=args.mode)


if __name__ == "__main__":
    main()
