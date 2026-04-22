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


def generate_comvergence_js(df, output_path=None):
    """Generate comvergence_data.js for the dashboard from the raw DataFrame."""
    if output_path is None:
        output_path = os.path.join(ROOT, "app", "static", "comvergence_data.js")

    big6 = ["WPP", "Publicis Groupe", "Omnicom", "dentsu", "Havas", "Independents"]

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

    # 2. Competitive flow matrix (spend flows between holdings)
    flows = {}
    agency_moves = df[
        (df["move_type"].isin(["Agency", "New-assignment"]))
        & (df["holding"].isin(big6))
        & (df["last_incumbent_holding"].isin(big6))
        & (df["holding"] != df["last_incumbent_holding"])
    ]
    for _, row in agency_moves.iterrows():
        key = f"{row['last_incumbent_holding']}|{row['holding']}"
        spend = float(row.get("total_spend_2025_m") or row.get("total_spend_2024_m") or 0)
        if key not in flows:
            flows[key] = {"from": row["last_incumbent_holding"], "to": row["holding"], "count": 0, "spend_m": 0}
        flows[key]["count"] += 1
        flows[key]["spend_m"] += spend
    flow_list = sorted(flows.values(), key=lambda x: -x["spend_m"])
    for f in flow_list:
        f["spend_m"] = round(f["spend_m"], 1)

    # 3. WPP movement timeline by year
    timeline = {}
    for h in ["WPP", "Publicis Groupe", "Omnicom"]:
        moves_df = df[
            ((df["holding"] == h) | (df["last_incumbent_holding"] == h))
            & (df["move_type"].isin(["Agency", "New-assignment"]))
            & (df["last_announcement_quarter"].notna())
        ].copy()
        yearly = {}
        for _, row in moves_df.iterrows():
            q = str(row["last_announcement_quarter"])
            year = q[:4] if len(q) >= 4 else None
            if not year or not year.isdigit():
                continue
            year = int(year)
            if year < 2015:
                continue
            if year not in yearly:
                yearly[year] = {"wins": 0, "losses": 0, "win_spend_m": 0, "loss_spend_m": 0}
            spend = float(row.get("total_spend_2025_m") or row.get("total_spend_2024_m") or 0)
            if row["holding"] == h and row["last_incumbent_holding"] != h:
                yearly[year]["wins"] += 1
                yearly[year]["win_spend_m"] += spend
            elif row["last_incumbent_holding"] == h and row["holding"] != h:
                yearly[year]["losses"] += 1
                yearly[year]["loss_spend_m"] += spend
        for y in yearly:
            yearly[y]["win_spend_m"] = round(yearly[y]["win_spend_m"], 1)
            yearly[y]["loss_spend_m"] = round(yearly[y]["loss_spend_m"], 1)
        timeline[h] = dict(sorted(yearly.items()))

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
            "spend_2025_m": round(float(row.get("total_spend_2025_m") or 0), 1),
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
                "spend_2025_m": round(float(row.get("total_spend_2025_m") or 0), 1),
                "spend_2024_m": round(float(row.get("total_spend_2024_m") or 0), 1),
                "digital_share_pct": round(float(row.get("digital_share_2025_pct") or 0), 1),
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
        "advertiser_intel": advertiser_intel,
        "_generated": datetime.now(tz=__import__('datetime').timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }

    with open(output_path, "w") as f:
        f.write(f"const COMVERGENCE = {json.dumps(data, separators=(',', ':'))};")

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nGenerated {output_path} ({size_kb:.0f} KB)")
    return data


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
        generate_comvergence_js(df)

    if args.dry_run:
        print("\n--dry-run: Skipping BigQuery upload.")
        return

    upload_to_bigquery(df, mode=args.mode)


if __name__ == "__main__":
    main()
