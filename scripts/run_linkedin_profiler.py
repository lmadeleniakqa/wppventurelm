#!/usr/bin/env python3
"""Run LinkedIn profiler locally — scrapes profiles and stores in BigQuery.

Usage:
    python scripts/run_linkedin_profiler.py                          # All 30 companies
    python scripts/run_linkedin_profiler.py --companies "Ford,Apple"  # Specific companies
    python scripts/run_linkedin_profiler.py --dry-run                 # Test without scraping
    python scripts/run_linkedin_profiler.py --max-profiles 10         # Limit per company
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

# Import company list from the function
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                "functions", "linkedin_profiler"))
from main import COMPANIES, LINKEDIN_COMPANY_URLS, parse_profile, store_profiles, update_company_summary


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

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Processing {len(companies)} companies, "
          f"max {max_profiles} profiles each\n")

    total_stored = 0
    for i, company in enumerate(companies):
        cname = company["name"]
        print(f"[{i+1}/{len(companies)}] {cname} ({company['group']}, {company['sector']})")

        if dry_run:
            print(f"  → DRY RUN, skipping\n")
            continue

        try:
            # Run Apify actor with correct schema
            company_url = LINKEDIN_COMPANY_URLS.get(company["name"])
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
                run_input["searchQuery"] = f"{company['search_term']} marketing media"

            print(f"  → Searching LinkedIn...")
            run = apify.actor("harvestapi/linkedin-profile-search").call(run_input=run_input)

            raw_profiles = list(apify.dataset(run["defaultDatasetId"]).iterate_items())
            print(f"  → Found {len(raw_profiles)} profiles")

            # Parse and store
            parsed = [parse_profile(r, company) for r in raw_profiles]
            parsed = [p for p in parsed if p["full_name"]]

            if parsed:
                stored = store_profiles(bq, parsed)
                update_company_summary(bq, company, stored)
                total_stored += stored

                # Show sample
                for p in parsed[:3]:
                    school_short = p["school"][:50] if p["school"] else "—"
                    print(f"     • {p['full_name'][:30]:30s}  {p['current_role'][:40]:40s}  🎓 {school_short}")
                if len(parsed) > 3:
                    print(f"     ... and {len(parsed) - 3} more")
            else:
                print(f"  → No valid profiles found")

            print()
            time.sleep(3)  # Rate limit

        except Exception as e:
            print(f"  → ERROR: {e}\n")

    print(f"\n{'='*60}")
    print(f"Done. {total_stored} profiles stored in BigQuery.")
    print(f"Tables: {PROJECT}.{DATASET}.linkedin_profiles")
    print(f"        {PROJECT}.{DATASET}.company_profiles")


def main():
    parser = argparse.ArgumentParser(description="LinkedIn company profiler")
    parser.add_argument("--companies", help="Comma-separated company names to process")
    parser.add_argument("--max-profiles", type=int, default=15, help="Max profiles per company")
    parser.add_argument("--dry-run", action="store_true", help="Test without scraping")
    args = parser.parse_args()

    companies = COMPANIES
    if args.companies:
        names = [n.strip() for n in args.companies.split(",")]
        companies = [c for c in COMPANIES if any(n.lower() in c["name"].lower() for n in names)]
        if not companies:
            print(f"No companies matched: {args.companies}")
            print(f"Available: {', '.join(c['name'] for c in COMPANIES)}")
            sys.exit(1)

    run(companies, max_profiles=args.max_profiles, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
