"""LinkedIn Company Profiler — scrapes employee profiles for companies in
the pitch prediction pipeline, stores results in BigQuery.

Uses Apify actor harvestapi/linkedin-profile-search to find employees,
then enriches company profiles with education/experience breakdowns.

Trigger: HTTP (manual or Cloud Scheduler weekly)
"""

import functions_framework
import json
import os
import time
from datetime import datetime, timezone
from google.cloud import bigquery, secretmanager


PROJECT = "na-analytics"
DATASET = "media_stocks"
SECRET_NAME = f"projects/{PROJECT}/secrets/apify-api-key/versions/latest"

# Companies to profile — loaded from pitch_prediction data
# Focus on high-score accounts (55+) to avoid wasting API calls
COMPANIES = [
    # WPP (defend)
    {"name": "Ford Motor Company", "group": "WPP", "sector": "Auto",
     "search_term": "Ford Motor Company", "keywords": "marketing media advertising"},
    {"name": "Colgate-Palmolive", "group": "WPP", "sector": "FMCG",
     "search_term": "Colgate-Palmolive", "keywords": "marketing media CMO"},
    {"name": "Danone", "group": "WPP", "sector": "FMCG",
     "search_term": "Danone", "keywords": "marketing media advertising"},
    {"name": "Adidas", "group": "WPP", "sector": "Retail",
     "search_term": "Adidas", "keywords": "marketing media global"},
    {"name": "Mastercard", "group": "WPP", "sector": "Finance",
     "search_term": "Mastercard", "keywords": "marketing media advertising"},
    {"name": "HP Inc.", "group": "WPP", "sector": "Tech",
     "search_term": "HP Inc.", "keywords": "marketing media advertising"},
    {"name": "Shell", "group": "WPP", "sector": "Energy",
     "search_term": "Shell", "keywords": "marketing communications brand"},
    {"name": "Target", "group": "WPP", "sector": "Retail",
     "search_term": "Target Corporation", "keywords": "marketing media advertising"},
    {"name": "T-Mobile", "group": "WPP", "sector": "Telecom",
     "search_term": "T-Mobile", "keywords": "marketing media advertising CMO"},
    # Publicis (attack)
    {"name": "Samsung Electronics", "group": "Publicis", "sector": "Tech",
     "search_term": "Samsung Electronics", "keywords": "marketing media advertising"},
    {"name": "Microsoft", "group": "Publicis", "sector": "Tech",
     "search_term": "Microsoft", "keywords": "marketing media advertising CMO"},
    {"name": "AB InBev", "group": "Publicis", "sector": "FMCG",
     "search_term": "AB InBev", "keywords": "marketing media global"},
    {"name": "Stellantis", "group": "Publicis", "sector": "Auto",
     "search_term": "Stellantis", "keywords": "marketing media advertising"},
    {"name": "Pfizer", "group": "Publicis", "sector": "Pharma",
     "search_term": "Pfizer", "keywords": "marketing communications brand"},
    {"name": "Walmart", "group": "Publicis", "sector": "Retail",
     "search_term": "Walmart", "keywords": "marketing media advertising"},
    {"name": "Starbucks", "group": "Publicis", "sector": "QSR",
     "search_term": "Starbucks", "keywords": "marketing media CMO brand"},
    {"name": "Uber", "group": "Publicis", "sector": "Tech",
     "search_term": "Uber", "keywords": "marketing media advertising"},
    # Omnicom (attack)
    {"name": "Apple", "group": "Omnicom", "sector": "Tech",
     "search_term": "Apple", "keywords": "marketing media advertising"},
    {"name": "Volkswagen Group", "group": "Omnicom", "sector": "Auto",
     "search_term": "Volkswagen Group", "keywords": "marketing media advertising"},
    {"name": "PepsiCo", "group": "Omnicom", "sector": "FMCG",
     "search_term": "PepsiCo", "keywords": "marketing media CMO"},
    {"name": "Mercedes-Benz", "group": "Omnicom", "sector": "Auto",
     "search_term": "Mercedes-Benz", "keywords": "marketing media advertising"},
    {"name": "AT&T", "group": "Omnicom", "sector": "Telecom",
     "search_term": "AT&T", "keywords": "marketing media advertising"},
    # Dentsu (attack)
    {"name": "Toyota Motor", "group": "Dentsu", "sector": "Auto",
     "search_term": "Toyota Motor", "keywords": "marketing media advertising"},
    {"name": "General Motors", "group": "Dentsu", "sector": "Auto",
     "search_term": "General Motors", "keywords": "marketing media advertising CMO"},
    {"name": "Meta", "group": "Dentsu", "sector": "Tech",
     "search_term": "Meta", "keywords": "marketing media advertising"},
    # Mixed (opportunistic)
    {"name": "Procter & Gamble", "group": "Mixed", "sector": "FMCG",
     "search_term": "Procter & Gamble", "keywords": "marketing media CMO brand"},
    {"name": "Unilever", "group": "Mixed", "sector": "FMCG",
     "search_term": "Unilever", "keywords": "marketing media CMO"},
    {"name": "L'Oréal", "group": "Mixed", "sector": "FMCG",
     "search_term": "L'Oréal", "keywords": "marketing media advertising"},
    {"name": "Coca-Cola", "group": "Mixed", "sector": "FMCG",
     "search_term": "The Coca-Cola Company", "keywords": "marketing media CMO"},
    {"name": "Nestlé", "group": "Mixed", "sector": "FMCG",
     "search_term": "Nestlé", "keywords": "marketing media advertising"},
]


def get_apify_key():
    """Retrieve Apify API key from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=SECRET_NAME)
    return response.payload.data.decode("UTF-8")


LINKEDIN_COMPANY_URLS = {
    "Ford Motor Company": "https://www.linkedin.com/company/ford-motor-company/",
    "Colgate-Palmolive": "https://www.linkedin.com/company/colgate-palmolive/",
    "Danone": "https://www.linkedin.com/company/danone/",
    "Adidas": "https://www.linkedin.com/company/adidas/",
    "Mastercard": "https://www.linkedin.com/company/mastercard/",
    "HP Inc.": "https://www.linkedin.com/company/hp/",
    "Shell": "https://www.linkedin.com/company/shell/",
    "Target": "https://www.linkedin.com/company/target/",
    "T-Mobile": "https://www.linkedin.com/company/t-mobile/",
    "Samsung Electronics": "https://www.linkedin.com/company/samsung-electronics/",
    "Microsoft": "https://www.linkedin.com/company/microsoft/",
    "AB InBev": "https://www.linkedin.com/company/ab-inbev/",
    "Stellantis": "https://www.linkedin.com/company/stellantis/",
    "Pfizer": "https://www.linkedin.com/company/pfizer/",
    "Walmart": "https://www.linkedin.com/company/walmart/",
    "Starbucks": "https://www.linkedin.com/company/starbucks/",
    "Uber": "https://www.linkedin.com/company/uber-com/",
    "Apple": "https://www.linkedin.com/company/apple/",
    "Volkswagen Group": "https://www.linkedin.com/company/volkswagen-ag/",
    "PepsiCo": "https://www.linkedin.com/company/pepsico/",
    "Mercedes-Benz": "https://www.linkedin.com/company/mercedes-benz/",
    "AT&T": "https://www.linkedin.com/company/att/",
    "Toyota Motor": "https://www.linkedin.com/company/toyota-motor-corporation/",
    "General Motors": "https://www.linkedin.com/company/general-motors/",
    "Meta": "https://www.linkedin.com/company/meta/",
    "Procter & Gamble": "https://www.linkedin.com/company/procter-and-gamble/",
    "Unilever": "https://www.linkedin.com/company/unilever/",
    "L'Oréal": "https://www.linkedin.com/company/loreal/",
    "Coca-Cola": "https://www.linkedin.com/company/the-coca-cola-company/",
    "Nestlé": "https://www.linkedin.com/company/nestle-s-a-/",
}


def search_linkedin_profiles(apify_key, company, max_results=15):
    """Use Apify actor to search LinkedIn for marketing/media leaders at a company."""
    from apify_client import ApifyClient

    client = ApifyClient(apify_key)

    company_url = LINKEDIN_COMPANY_URLS.get(company["name"])
    run_input = {
        "searchQuery": "marketing media advertising",
        "profileScraperMode": "Full",
        "maxItems": max_results,
        "functionIds": ["15", "16"],  # Marketing, Media and Communication
        "seniorityLevelIds": ["120", "220", "300", "310"],  # Senior, Director, VP, CXO
    }
    if company_url:
        run_input["currentCompanies"] = [company_url]
    else:
        # Fallback: use company name in search query
        run_input["searchQuery"] = f"{company['search_term']} marketing media"

    run = client.actor("harvestapi/linkedin-profile-search").call(run_input=run_input)

    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    return results


def parse_profile(raw, company):
    """Parse raw Apify result into a clean profile dict for BigQuery."""
    now = datetime.now(timezone.utc).isoformat()

    # Extract education from profileTopEducation or educations
    educations = raw.get("profileTopEducation", raw.get("educations", []))
    schools = []
    degrees = []
    if isinstance(educations, list):
        for ed in educations:
            if isinstance(ed, dict):
                schools.append(ed.get("schoolName", ed.get("school", "")))
                degrees.append(ed.get("degreeName", ed.get("degree", "")))

    # Extract current position
    current_positions = raw.get("currentPosition", [])
    current_company = ""
    current_role = ""
    if isinstance(current_positions, list) and current_positions:
        pos = current_positions[0]
        current_company = pos.get("companyName", "")
        current_role = pos.get("position", "")

    # Extract past experience
    past_positions = raw.get("pastPosition", raw.get("experiences", []))
    prev_companies = []
    prev_roles = []
    if isinstance(past_positions, list):
        for exp in past_positions:
            if isinstance(exp, dict):
                co = exp.get("companyName", exp.get("company", ""))
                role = exp.get("position", exp.get("title", ""))
                if co:
                    prev_companies.append(co)
                if role:
                    prev_roles.append(role)

    # Location
    loc = raw.get("location", {})
    location_str = ""
    if isinstance(loc, dict):
        location_str = loc.get("linkedinText", "")
        parsed = loc.get("parsed", {})
        if parsed:
            location_str = parsed.get("text", location_str)
    elif isinstance(loc, str):
        location_str = loc

    # Skills
    skills_list = raw.get("topSkills", raw.get("skills", []))
    skills_str = ""
    if isinstance(skills_list, list):
        skills_str = " | ".join(str(s) for s in skills_list[:20])

    full_name = f"{raw.get('firstName', '')} {raw.get('lastName', '')}".strip()
    if not full_name:
        full_name = raw.get("fullName", raw.get("name", ""))

    return {
        "company_name": company["name"],
        "holding_group": company["group"],
        "full_name": full_name,
        "title": raw.get("headline", ""),
        "linkedin_url": raw.get("linkedinUrl", raw.get("profileUrl", "")),
        "location": location_str,
        "current_company": current_company or company["search_term"],
        "current_role": current_role or raw.get("headline", ""),
        "education": " | ".join(filter(None, [f"{s} ({d})" if d else s for s, d in zip(schools, degrees + [""] * len(schools))])),
        "school": " | ".join(filter(None, schools)),
        "degree": " | ".join(filter(None, degrees)),
        "previous_companies": " | ".join(filter(None, prev_companies[:10])),
        "previous_roles": " | ".join(filter(None, prev_roles[:10])),
        "skills": skills_str,
        "connections": int(raw.get("connectionsCount", 0) or 0),
        "summary": (raw.get("about", "") or "")[:2000],
        "scraped_at": now,
    }


def store_profiles(bq, profiles):
    """Insert parsed profiles into BigQuery."""
    if not profiles:
        return 0
    table_ref = f"{PROJECT}.{DATASET}.linkedin_profiles"
    errors = bq.insert_rows_json(table_ref, profiles)
    if errors:
        print(f"BQ insert errors: {errors[:3]}")
    return len(profiles)


def update_company_summary(bq, company, profile_count):
    """Upsert company_profiles table with latest scrape metadata."""
    table_ref = f"{PROJECT}.{DATASET}.company_profiles"
    now = datetime.now(timezone.utc).isoformat()
    row = {
        "company_name": company["name"],
        "holding_group": company["group"],
        "sector": company["sector"],
        "annual_spend_m": 0,  # Will be enriched from pitch data
        "composite_score": 0,
        "risk_bracket": "",
        "peak_quarter": "",
        "linkedin_url": "",
        "employee_count": profile_count,
        "headquarters": "",
        "profile_updated_at": now,
    }
    # Use merge to upsert
    merge_query = f"""
    MERGE `{table_ref}` T
    USING (SELECT @company_name AS company_name) S
    ON T.company_name = S.company_name
    WHEN MATCHED THEN
      UPDATE SET employee_count = @count, profile_updated_at = @updated, holding_group = @group, sector = @sector
    WHEN NOT MATCHED THEN
      INSERT (company_name, holding_group, sector, employee_count, profile_updated_at)
      VALUES (@company_name, @group, @sector, @count, @updated)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_name", "STRING", company["name"]),
            bigquery.ScalarQueryParameter("group", "STRING", company["group"]),
            bigquery.ScalarQueryParameter("sector", "STRING", company["sector"]),
            bigquery.ScalarQueryParameter("count", "INT64", profile_count),
            bigquery.ScalarQueryParameter("updated", "TIMESTAMP", now),
        ]
    )
    bq.query(merge_query, job_config=job_config).result()


@functions_framework.http
def linkedin_profiler(request):
    """Main entry point. Scrapes LinkedIn profiles for companies in the pipeline."""
    results = {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    # Parse request for optional filters
    req_data = request.get_json(silent=True) or {}
    company_filter = req_data.get("companies")  # List of company names to process
    max_per_company = req_data.get("max_profiles", 15)
    dry_run = req_data.get("dry_run", False)

    # Get API key
    apify_key = get_apify_key()

    # Filter companies if specified
    companies = COMPANIES
    if company_filter:
        companies = [c for c in COMPANIES if c["name"] in company_filter]

    bq = bigquery.Client(project=PROJECT)
    results["companies"] = {}

    for company in companies:
        cname = company["name"]
        print(f"Processing {cname} ({company['group']})...")

        if dry_run:
            results["companies"][cname] = {"status": "dry_run", "group": company["group"]}
            continue

        try:
            # Search LinkedIn
            raw_profiles = search_linkedin_profiles(apify_key, company, max_results=max_per_company)
            print(f"  Found {len(raw_profiles)} profiles")

            # Parse
            parsed = [parse_profile(r, company) for r in raw_profiles]
            parsed = [p for p in parsed if p["full_name"]]  # Skip empty

            # Store in BQ
            stored = store_profiles(bq, parsed)
            update_company_summary(bq, company, stored)

            results["companies"][cname] = {
                "status": "ok",
                "profiles_found": len(raw_profiles),
                "profiles_stored": stored,
                "group": company["group"],
            }

            # Rate limit between companies
            time.sleep(3)

        except Exception as e:
            print(f"  ERROR: {e}")
            results["companies"][cname] = {"status": "error", "error": str(e)}

    results["total_companies"] = len(companies)
    results["total_processed"] = sum(1 for v in results["companies"].values() if v.get("status") == "ok")

    return json.dumps(results, indent=2), 200, {"Content-Type": "application/json"}
