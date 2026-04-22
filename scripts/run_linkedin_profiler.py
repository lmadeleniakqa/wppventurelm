#!/usr/bin/env python3
"""Run LinkedIn profiler locally — scrapes agency + client profiles, stores in BigQuery,
and computes relationship scores (school overlap, past employers, WPP alumni).

Usage:
    python scripts/run_linkedin_profiler.py                              # All companies
    python scripts/run_linkedin_profiler.py --pool agency                # WPP + competitor agencies only
    python scripts/run_linkedin_profiler.py --pool client                # Advertisers only
    python scripts/run_linkedin_profiler.py --companies "Mindshare,Ford" # Specific companies
    python scripts/run_linkedin_profiler.py --dry-run                    # Test without scraping
    python scripts/run_linkedin_profiler.py --relationships-only         # Just recompute scores
    python scripts/run_linkedin_profiler.py --max-profiles 20            # Limit per company
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from google.cloud import bigquery, secretmanager
from apify_client import ApifyClient

PROJECT = "na-analytics"
DATASET = "media_stocks"

# Import from the function
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "functions", "linkedin_profiler"))
from main import (COMPANIES, AGENCY_COMPANIES, CLIENT_COMPANIES,
                  LINKEDIN_COMPANY_URLS, parse_profile, store_profiles,
                  update_company_summary, compute_relationships)


def get_apify_key():
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(
        name=f"projects/{PROJECT}/secrets/apify-api-key/versions/latest"
    )
    return response.payload.data.decode("UTF-8")


def run(companies, max_profiles=15, dry_run=False):
    apify_key = get_apify_key()
    apify = ApifyClient(apify_key)
    bq = bigquery.Client(project=PROJECT)

    agency_count = sum(1 for c in companies if c.get("pool") == "agency")
    client_count = sum(1 for c in companies if c.get("pool") == "client")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing {len(companies)} companies "
          f"({agency_count} agencies, {client_count} clients), max {max_profiles} profiles each\n")

    total_stored = 0
    total_alumni = 0
    for i, company in enumerate(companies):
        cname = company["name"]
        pool = company.get("pool", "client")
        holding = company.get("holding", "")
        print(f"[{i+1}/{len(companies)}] {cname} ({holding}, {pool})")

        if dry_run:
            print(f"  → DRY RUN, skipping\n")
            continue

        try:
            company_url = LINKEDIN_COMPANY_URLS.get(company["name"])

            if pool == "agency":
                run_input = {
                    "searchQuery": company.get("keywords", "media planning client strategy"),
                    "profileScraperMode": "Full",
                    "maxItems": max_profiles,
                    "seniorityLevelIds": ["120", "220", "300", "310"],
                }
            else:
                run_input = {
                    "searchQuery": "marketing media advertising",
                    "profileScraperMode": "Full",
                    "maxItems": max_profiles,
                    "functionIds": ["15", "16"],
                    "seniorityLevelIds": ["120", "220", "300", "310"],
                }

            if company_url:
                run_input["currentCompanies"] = [company_url]
            else:
                run_input["searchQuery"] = f"{company['search_term']} {company.get('keywords', 'marketing media')}"

            print(f"  → Searching LinkedIn...")
            run_result = apify.actor("harvestapi/linkedin-profile-search").call(run_input=run_input)

            raw_profiles = list(apify.dataset(run_result["defaultDatasetId"]).iterate_items())
            print(f"  → Found {len(raw_profiles)} profiles")

            parsed = [parse_profile(r, company) for r in raw_profiles]
            parsed = [p for p in parsed if p["full_name"]]

            if parsed:
                stored = store_profiles(bq, parsed)
                update_company_summary(bq, company, stored)
                total_stored += stored

                alumni = sum(1 for p in parsed if p.get("wpp_alumni"))
                total_alumni += alumni

                for p in parsed[:3]:
                    school_short = p["school"][:40] if p["school"] else "—"
                    alumni_tag = " [WPP ALUM]" if p.get("wpp_alumni") else ""
                    print(f"     {p['full_name'][:28]:28s}  {p['current_role'][:35]:35s}  {school_short}{alumni_tag}")
                if len(parsed) > 3:
                    print(f"     ... and {len(parsed) - 3} more")
                if alumni:
                    print(f"     → {alumni} WPP alumni found!")
            else:
                print(f"  → No valid profiles found")

            print()
            time.sleep(3)

        except Exception as e:
            print(f"  → ERROR: {e}\n")

    # Compute relationship scores
    if not dry_run and total_stored > 0:
        print("\nComputing relationship scores...")
        compute_relationships(bq)

    print(f"\n{'='*60}")
    print(f"Done. {total_stored} profiles stored, {total_alumni} WPP alumni found.")
    print(f"Tables: {PROJECT}.{DATASET}.linkedin_profiles")
    print(f"        {PROJECT}.{DATASET}.company_profiles")
    print(f"        {PROJECT}.{DATASET}.relationship_scores")
    print(f"        {PROJECT}.{DATASET}.wpp_alumni_client_side")
    print(f"        {PROJECT}.{DATASET}.school_overlaps")
    print(f"        {PROJECT}.{DATASET}.employer_overlaps")


def main():
    parser = argparse.ArgumentParser(description="LinkedIn profiler — agencies + clients")
    parser.add_argument("--pool", choices=["agency", "client"], help="Scrape only agencies or clients")
    parser.add_argument("--companies", help="Comma-separated company names to process")
    parser.add_argument("--max-profiles", type=int, default=15, help="Max profiles per company")
    parser.add_argument("--dry-run", action="store_true", help="Test without scraping")
    parser.add_argument("--relationships-only", action="store_true",
                        help="Skip scraping, just recompute relationship scores from existing data")
    args = parser.parse_args()

    if args.relationships_only:
        bq = bigquery.Client(project=PROJECT)
        compute_relationships(bq)
        print("Relationship scores recomputed.")
        return

    companies = COMPANIES
    if args.pool:
        companies = [c for c in companies if c.get("pool") == args.pool]
    if args.companies:
        names = [n.strip() for n in args.companies.split(",")]
        companies = [c for c in companies if any(n.lower() in c["name"].lower() for n in names)]
        if not companies:
            print(f"No companies matched: {args.companies}")
            print(f"Available: {', '.join(c['name'] for c in COMPANIES)}")
            sys.exit(1)

    run(companies, max_profiles=args.max_profiles, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
