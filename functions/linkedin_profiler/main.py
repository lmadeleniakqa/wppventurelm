"""LinkedIn Company Profiler — scrapes employee profiles for agencies and
advertisers, stores in BigQuery, and computes relationship scores.

Two pools:
  1. AGENCY companies — WPP agencies (EssenceMediacom, Mindshare, Wavemaker, etc.)
     and competitor agencies (Starcom, OMD, Carat, etc.)
  2. CLIENT companies — top advertisers from COMvergence (by spend)

Relationship analysis finds:
  - School overlaps between agency and client-side people
  - Past working relationships (shared previous employers)
  - WPP alumni now client-side (warm leads)

Uses Apify actor harvestapi/linkedin-profile-search.
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

# =====================================================================
# AGENCY COMPANIES — from COMvergence hierarchy
# =====================================================================
AGENCY_COMPANIES = [
    # WPP agencies (our side — scrape deeply)
    {"name": "EssenceMediacom", "holding": "WPP", "pool": "agency",
     "search_term": "EssenceMediacom", "keywords": "media planning buying client"},
    {"name": "Mindshare", "holding": "WPP", "pool": "agency",
     "search_term": "Mindshare", "keywords": "media planning buying strategy"},
    {"name": "Wavemaker", "holding": "WPP", "pool": "agency",
     "search_term": "Wavemaker", "keywords": "media planning buying growth"},
    {"name": "GroupM", "holding": "WPP", "pool": "agency",
     "search_term": "GroupM", "keywords": "media investment strategy"},
    {"name": "T&Pm", "holding": "WPP", "pool": "agency",
     "search_term": "The&Partnership", "keywords": "media advertising"},
    {"name": "Ogilvy", "holding": "WPP", "pool": "agency",
     "search_term": "Ogilvy", "keywords": "advertising creative strategy"},
    {"name": "AKQA", "holding": "WPP", "pool": "agency",
     "search_term": "AKQA", "keywords": "digital experience strategy"},
    # Competitor agencies (for relationship mapping)
    {"name": "Starcom", "holding": "Publicis Groupe", "pool": "agency",
     "search_term": "Starcom", "keywords": "media planning buying"},
    {"name": "Zenith", "holding": "Publicis Groupe", "pool": "agency",
     "search_term": "Zenith Media", "keywords": "media planning ROI"},
    {"name": "Spark Foundry", "holding": "Publicis Groupe", "pool": "agency",
     "search_term": "Spark Foundry", "keywords": "media planning"},
    {"name": "OMD", "holding": "Omnicom", "pool": "agency",
     "search_term": "OMD Worldwide", "keywords": "media planning buying"},
    {"name": "PHD", "holding": "Omnicom", "pool": "agency",
     "search_term": "PHD Media", "keywords": "media strategy planning"},
    {"name": "Hearts & Science", "holding": "Omnicom", "pool": "agency",
     "search_term": "Hearts & Science", "keywords": "media data marketing"},
    {"name": "Carat", "holding": "dentsu", "pool": "agency",
     "search_term": "Carat", "keywords": "media planning buying"},
    {"name": "iProspect", "holding": "dentsu", "pool": "agency",
     "search_term": "iProspect", "keywords": "performance media digital"},
]

# =====================================================================
# CLIENT COMPANIES — COMvergence top advertisers (aligned with pitch prediction)
# =====================================================================
CLIENT_COMPANIES = [
    # WPP clients (defend)
    {"name": "Unilever", "holding": "WPP", "pool": "client", "sector": "FMCG",
     "search_term": "Unilever", "keywords": "marketing media CMO"},
    {"name": "Coca-Cola", "holding": "WPP", "pool": "client", "sector": "FMCG/Beverages",
     "search_term": "The Coca-Cola Company", "keywords": "marketing media CMO"},
    {"name": "Ford Motor Company", "holding": "WPP", "pool": "client", "sector": "Auto",
     "search_term": "Ford Motor Company", "keywords": "marketing media advertising"},
    {"name": "Colgate-Palmolive", "holding": "WPP", "pool": "client", "sector": "FMCG",
     "search_term": "Colgate-Palmolive", "keywords": "marketing media CMO"},
    {"name": "Danone", "holding": "WPP", "pool": "client", "sector": "FMCG",
     "search_term": "Danone", "keywords": "marketing media advertising"},
    {"name": "Adidas", "holding": "WPP", "pool": "client", "sector": "Apparel",
     "search_term": "Adidas", "keywords": "marketing media global"},
    {"name": "Mastercard", "holding": "WPP", "pool": "client", "sector": "Finance",
     "search_term": "Mastercard", "keywords": "marketing media advertising"},
    {"name": "HP Inc.", "holding": "WPP", "pool": "client", "sector": "Tech",
     "search_term": "HP Inc.", "keywords": "marketing media advertising"},
    {"name": "Shell", "holding": "WPP", "pool": "client", "sector": "Energy",
     "search_term": "Shell", "keywords": "marketing communications brand"},
    {"name": "Target", "holding": "WPP", "pool": "client", "sector": "Retail",
     "search_term": "Target Corporation", "keywords": "marketing media advertising"},
    {"name": "Estée Lauder", "holding": "WPP", "pool": "client", "sector": "Beauty",
     "search_term": "Estée Lauder Companies", "keywords": "marketing media advertising"},
    # Publicis clients (attack opportunities)
    {"name": "Samsung Electronics", "holding": "Publicis Groupe", "pool": "client", "sector": "Tech",
     "search_term": "Samsung Electronics", "keywords": "marketing media advertising"},
    {"name": "Mars", "holding": "Publicis Groupe", "pool": "client", "sector": "FMCG",
     "search_term": "Mars Incorporated", "keywords": "marketing media advertising"},
    {"name": "Stellantis", "holding": "Publicis Groupe", "pool": "client", "sector": "Auto",
     "search_term": "Stellantis", "keywords": "marketing media advertising"},
    {"name": "Pfizer", "holding": "Publicis Groupe", "pool": "client", "sector": "Pharma",
     "search_term": "Pfizer", "keywords": "marketing communications brand"},
    {"name": "Walmart", "holding": "Publicis Groupe", "pool": "client", "sector": "Retail",
     "search_term": "Walmart", "keywords": "marketing media advertising"},
    {"name": "Starbucks", "holding": "Publicis Groupe", "pool": "client", "sector": "QSR",
     "search_term": "Starbucks", "keywords": "marketing media CMO brand"},
    # Omnicom clients (attack opportunities)
    {"name": "Apple", "holding": "Omnicom", "pool": "client", "sector": "Tech",
     "search_term": "Apple", "keywords": "marketing media advertising"},
    {"name": "PepsiCo", "holding": "Omnicom", "pool": "client", "sector": "FMCG",
     "search_term": "PepsiCo", "keywords": "marketing media CMO"},
    {"name": "Mercedes-Benz", "holding": "Omnicom", "pool": "client", "sector": "Auto",
     "search_term": "Mercedes-Benz", "keywords": "marketing media advertising"},
    {"name": "AT&T", "holding": "Omnicom", "pool": "client", "sector": "Telecom",
     "search_term": "AT&T", "keywords": "marketing media advertising"},
    # Dentsu clients (attack opportunities)
    {"name": "Toyota Motor", "holding": "dentsu", "pool": "client", "sector": "Auto",
     "search_term": "Toyota Motor", "keywords": "marketing media advertising"},
    {"name": "General Motors", "holding": "dentsu", "pool": "client", "sector": "Auto",
     "search_term": "General Motors", "keywords": "marketing media advertising CMO"},
    # Mixed (contested)
    {"name": "Procter & Gamble", "holding": "Mixed", "pool": "client", "sector": "FMCG",
     "search_term": "Procter & Gamble", "keywords": "marketing media CMO brand"},
    {"name": "L'Oréal", "holding": "Mixed", "pool": "client", "sector": "FMCG",
     "search_term": "L'Oréal", "keywords": "marketing media advertising"},
    {"name": "Nestlé", "holding": "Mixed", "pool": "client", "sector": "FMCG",
     "search_term": "Nestlé", "keywords": "marketing media advertising"},
    {"name": "Amazon", "holding": "Mixed", "pool": "client", "sector": "Tech/Retail",
     "search_term": "Amazon", "keywords": "marketing media advertising"},
]

# Combined list
COMPANIES = AGENCY_COMPANIES + CLIENT_COMPANIES

# All WPP agency names (for detecting WPP alumni in career history)
WPP_AGENCY_NAMES = [
    "essencemediacom", "mindshare", "wavemaker", "groupm", "mediacom",
    "mec", "maxus", "ogilvy", "jwt", "j. walter thompson", "grey",
    "young & rubicam", "y&r", "burson", "hill+knowlton", "wunderman",
    "akqa", "vmly&r", "vml", "bossanova", "choreograph", "finecast",
    "hogarth", "landor", "superunion", "design bridge", "wppmedia",
    "wpp", "the&partnership", "t&pm",
]

LINKEDIN_COMPANY_URLS = {
    # WPP agencies
    "EssenceMediacom": "https://www.linkedin.com/company/essencemediacom/",
    "Mindshare": "https://www.linkedin.com/company/mindshare/",
    "Wavemaker": "https://www.linkedin.com/company/wavemakerglobal/",
    "GroupM": "https://www.linkedin.com/company/groupm/",
    "Ogilvy": "https://www.linkedin.com/company/ogilvy/",
    "AKQA": "https://www.linkedin.com/company/akqa/",
    # Competitor agencies
    "Starcom": "https://www.linkedin.com/company/starcom/",
    "Zenith": "https://www.linkedin.com/company/zenithmedia/",
    "Spark Foundry": "https://www.linkedin.com/company/spark-foundry/",
    "OMD": "https://www.linkedin.com/company/omd/",
    "PHD": "https://www.linkedin.com/company/phd-media/",
    "Hearts & Science": "https://www.linkedin.com/company/hearts-science/",
    "Carat": "https://www.linkedin.com/company/carat/",
    "iProspect": "https://www.linkedin.com/company/iprospect/",
    # Client companies
    "Unilever": "https://www.linkedin.com/company/unilever/",
    "Coca-Cola": "https://www.linkedin.com/company/the-coca-cola-company/",
    "Ford Motor Company": "https://www.linkedin.com/company/ford-motor-company/",
    "Colgate-Palmolive": "https://www.linkedin.com/company/colgate-palmolive/",
    "Danone": "https://www.linkedin.com/company/danone/",
    "Adidas": "https://www.linkedin.com/company/adidas/",
    "Mastercard": "https://www.linkedin.com/company/mastercard/",
    "HP Inc.": "https://www.linkedin.com/company/hp/",
    "Shell": "https://www.linkedin.com/company/shell/",
    "Target": "https://www.linkedin.com/company/target/",
    "Estée Lauder": "https://www.linkedin.com/company/the-estee-lauder-companies/",
    "Samsung Electronics": "https://www.linkedin.com/company/samsung-electronics/",
    "Mars": "https://www.linkedin.com/company/mars/",
    "Stellantis": "https://www.linkedin.com/company/stellantis/",
    "Pfizer": "https://www.linkedin.com/company/pfizer/",
    "Walmart": "https://www.linkedin.com/company/walmart/",
    "Starbucks": "https://www.linkedin.com/company/starbucks/",
    "Apple": "https://www.linkedin.com/company/apple/",
    "PepsiCo": "https://www.linkedin.com/company/pepsico/",
    "Mercedes-Benz": "https://www.linkedin.com/company/mercedes-benz/",
    "AT&T": "https://www.linkedin.com/company/att/",
    "Toyota Motor": "https://www.linkedin.com/company/toyota-motor-corporation/",
    "General Motors": "https://www.linkedin.com/company/general-motors/",
    "Procter & Gamble": "https://www.linkedin.com/company/procter-and-gamble/",
    "L'Oréal": "https://www.linkedin.com/company/loreal/",
    "Nestlé": "https://www.linkedin.com/company/nestle-s-a-/",
    "Amazon": "https://www.linkedin.com/company/amazon/",
}


def get_apify_key():
    """Retrieve Apify API key from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=SECRET_NAME)
    return response.payload.data.decode("UTF-8")


def search_linkedin_profiles(apify_key, company, max_results=15):
    """Use Apify actor to search LinkedIn for marketing/media leaders at a company."""
    from apify_client import ApifyClient

    client = ApifyClient(apify_key)

    company_url = LINKEDIN_COMPANY_URLS.get(company["name"])

    # Agency searches: broader role scope (account directors, strategy, client leads)
    if company.get("pool") == "agency":
        run_input = {
            "searchQuery": company.get("keywords", "media planning client strategy"),
            "profileScraperMode": "Full",
            "maxItems": max_results,
            "seniorityLevelIds": ["120", "220", "300", "310"],  # Senior+
        }
    else:
        # Client searches: marketing/media decision-makers
        run_input = {
            "searchQuery": "marketing media advertising",
            "profileScraperMode": "Full",
            "maxItems": max_results,
            "functionIds": ["15", "16"],  # Marketing, Media and Communication
            "seniorityLevelIds": ["120", "220", "300", "310"],
        }

    if company_url:
        run_input["currentCompanies"] = [company_url]
    else:
        run_input["searchQuery"] = f"{company['search_term']} {company.get('keywords', 'marketing media')}"

    run = client.actor("harvestapi/linkedin-profile-search").call(run_input=run_input)

    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)

    return results


def parse_profile(raw, company):
    """Parse raw Apify result into a clean profile dict for BigQuery."""
    now = datetime.now(timezone.utc).isoformat()

    # Extract education
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

    # Detect WPP agency experience in career history
    all_companies = [current_company] + prev_companies
    wpp_alumni = False
    wpp_agencies_worked = []
    for co in all_companies:
        co_lower = (co or "").lower()
        for wpp_name in WPP_AGENCY_NAMES:
            if wpp_name in co_lower:
                wpp_alumni = True
                if co not in wpp_agencies_worked:
                    wpp_agencies_worked.append(co)
                break

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
        "holding_group": company.get("holding", company.get("group", "")),
        "pool": company.get("pool", "client"),
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
        "wpp_alumni": wpp_alumni,
        "wpp_agencies_worked": " | ".join(wpp_agencies_worked),
        "skills": skills_str,
        "connections": int(raw.get("connectionsCount", 0) or 0),
        "summary": (raw.get("about", "") or "")[:2000],
        "scraped_at": now,
    }


def store_profiles(bq, profiles):
    """Insert parsed profiles into BigQuery using streaming insert with retry.

    Streams each batch immediately so data is persisted even if later
    companies fail. Retries once on transient errors.
    """
    if not profiles:
        return 0
    table_ref = f"{PROJECT}.{DATASET}.linkedin_profiles"
    stored = 0
    # Insert in batches of 50 to avoid payload limits
    for i in range(0, len(profiles), 50):
        batch = profiles[i:i+50]
        for attempt in range(2):
            errors = bq.insert_rows_json(table_ref, batch)
            if not errors:
                stored += len(batch)
                break
            elif attempt == 0:
                print(f"    BQ insert retry ({len(errors)} errors)...")
                time.sleep(2)
            else:
                print(f"    BQ insert FAILED: {errors[:2]}")
                # Store failed batch to local file as backup
                backup_path = f"/tmp/linkedin_profiles_failed_{int(time.time())}.json"
                with open(backup_path, "w") as f:
                    json.dump(batch, f)
                print(f"    Saved failed batch to {backup_path}")
    return stored


def update_company_summary(bq, company, profile_count):
    """Upsert company_profiles table with latest scrape metadata."""
    table_ref = f"{PROJECT}.{DATASET}.company_profiles"
    now = datetime.now(timezone.utc).isoformat()
    merge_query = f"""
    MERGE `{table_ref}` T
    USING (SELECT @company_name AS company_name) S
    ON T.company_name = S.company_name
    WHEN MATCHED THEN
      UPDATE SET employee_count = @count, profile_updated_at = @updated,
                 holding_group = @holding, sector = @sector, pool = @pool
    WHEN NOT MATCHED THEN
      INSERT (company_name, holding_group, sector, pool, employee_count, profile_updated_at)
      VALUES (@company_name, @holding, @sector, @pool, @count, @updated)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("company_name", "STRING", company["name"]),
            bigquery.ScalarQueryParameter("holding", "STRING", company.get("holding", "")),
            bigquery.ScalarQueryParameter("sector", "STRING", company.get("sector", "")),
            bigquery.ScalarQueryParameter("pool", "STRING", company.get("pool", "client")),
            bigquery.ScalarQueryParameter("count", "INT64", profile_count),
            bigquery.ScalarQueryParameter("updated", "TIMESTAMP", now),
        ]
    )
    bq.query(merge_query, job_config=job_config).result()


def compute_relationships(bq):
    """Compute relationship scores between agency and client-side people.

    Finds:
    1. School overlaps — agency person and client person attended same school
    2. Past employer overlaps — agency person and client person worked at same company
    3. WPP alumni — client-side people who previously worked at a WPP agency

    Results stored in `relationship_scores` table.
    """
    print("Computing relationship scores...")

    # 1. WPP alumni now client-side
    alumni_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.wpp_alumni_client_side` AS
    SELECT
        full_name, company_name, holding_group, current_role, school,
        wpp_agencies_worked, linkedin_url, location
    FROM `{PROJECT}.{DATASET}.linkedin_profiles`
    WHERE pool = 'client'
      AND wpp_alumni = TRUE
    ORDER BY company_name
    """
    bq.query(alumni_query).result()

    # 2. School overlap between ALL agency people and client-side people
    school_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.school_overlaps` AS
    SELECT
        a.agency_person, a.agency_name, a.agency_holding,
        c.client_person, c.client_company, c.client_holding,
        a_school AS shared_school
    FROM (
        SELECT full_name AS agency_person, company_name AS agency_name,
               holding_group AS agency_holding, a_school
        FROM `{PROJECT}.{DATASET}.linkedin_profiles`,
             UNNEST(SPLIT(school, ' | ')) AS a_school
        WHERE pool = 'agency' AND school IS NOT NULL AND school != ''
    ) a
    JOIN (
        SELECT full_name AS client_person, company_name AS client_company,
               holding_group AS client_holding, c_school
        FROM `{PROJECT}.{DATASET}.linkedin_profiles`,
             UNNEST(SPLIT(school, ' | ')) AS c_school
        WHERE pool = 'client' AND school IS NOT NULL AND school != ''
    ) c
    ON LOWER(TRIM(a.a_school)) = LOWER(TRIM(c.c_school))
    WHERE TRIM(a.a_school) != ''
    """
    bq.query(school_query).result()

    # 3. Past employer overlap between ALL agency people and client-side people
    employer_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.employer_overlaps` AS
    SELECT
        a.agency_person, a.agency_name, a.agency_holding,
        c.client_person, c.client_company, c.client_holding,
        a_co AS shared_employer
    FROM (
        SELECT full_name AS agency_person, company_name AS agency_name,
               holding_group AS agency_holding, a_co
        FROM `{PROJECT}.{DATASET}.linkedin_profiles`,
             UNNEST(SPLIT(previous_companies, ' | ')) AS a_co
        WHERE pool = 'agency' AND previous_companies IS NOT NULL AND previous_companies != ''
    ) a
    JOIN (
        SELECT full_name AS client_person, company_name AS client_company,
               holding_group AS client_holding, c_co
        FROM `{PROJECT}.{DATASET}.linkedin_profiles`,
             UNNEST(SPLIT(previous_companies, ' | ')) AS c_co
        WHERE pool = 'client' AND previous_companies IS NOT NULL AND previous_companies != ''
    ) c
    ON LOWER(TRIM(a.a_co)) = LOWER(TRIM(c.c_co))
    WHERE TRIM(a.a_co) != ''
      AND LOWER(TRIM(a.a_co)) NOT IN ('freelance', 'self-employed', 'consultant')
    """
    bq.query(employer_query).result()

    # 4. Relationship score per client company — separate WPP vs competitor connections
    score_query = f"""
    CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.relationship_scores` AS
    SELECT
        client_company,
        client_holding,
        -- WPP connections (our advantage)
        COUNTIF(source = 'alumni') AS wpp_alumni_count,
        COUNTIF(source = 'school' AND agency_holding = 'WPP') AS wpp_school_overlap,
        COUNTIF(source = 'employer' AND agency_holding = 'WPP') AS wpp_employer_overlap,
        -- Competitor connections (their advantage)
        COUNTIF(source = 'school' AND agency_holding != 'WPP') AS competitor_school_overlap,
        COUNTIF(source = 'employer' AND agency_holding != 'WPP') AS competitor_employer_overlap,
        -- Scores
        COUNTIF(source = 'alumni') * 3
            + COUNTIF(source = 'school' AND agency_holding = 'WPP') * 2
            + COUNTIF(source = 'employer' AND agency_holding = 'WPP') * 1 AS wpp_relationship_score,
        COUNTIF(source = 'school' AND agency_holding != 'WPP') * 2
            + COUNTIF(source = 'employer' AND agency_holding != 'WPP') * 1 AS competitor_relationship_score
    FROM (
        SELECT company_name AS client_company, holding_group AS client_holding,
               'alumni' AS source, 'WPP' AS agency_holding
        FROM `{PROJECT}.{DATASET}.wpp_alumni_client_side`
        UNION ALL
        SELECT client_company, client_holding, 'school', agency_holding
        FROM `{PROJECT}.{DATASET}.school_overlaps`
        UNION ALL
        SELECT client_company, client_holding, 'employer', agency_holding
        FROM `{PROJECT}.{DATASET}.employer_overlaps`
    )
    GROUP BY client_company, client_holding
    ORDER BY wpp_relationship_score DESC
    """
    bq.query(score_query).result()
    print("  Relationship scores computed (WPP + competitor).")


@functions_framework.http
def linkedin_profiler(request):
    """Main entry point. Scrapes LinkedIn profiles for companies in the pipeline."""
    results = {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}

    req_data = request.get_json(silent=True) or {}
    company_filter = req_data.get("companies")
    pool_filter = req_data.get("pool")  # "agency", "client", or None for both
    max_per_company = req_data.get("max_profiles", 15)
    dry_run = req_data.get("dry_run", False)
    compute_rels = req_data.get("compute_relationships", True)

    apify_key = get_apify_key()

    companies = COMPANIES
    if pool_filter:
        companies = [c for c in companies if c.get("pool") == pool_filter]
    if company_filter:
        companies = [c for c in companies if c["name"] in company_filter]

    bq = bigquery.Client(project=PROJECT)
    results["companies"] = {}

    for company in companies:
        cname = company["name"]
        print(f"Processing {cname} ({company.get('holding', '')}, {company.get('pool', '')})...")

        if dry_run:
            results["companies"][cname] = {"status": "dry_run", "pool": company.get("pool")}
            continue

        try:
            raw_profiles = search_linkedin_profiles(apify_key, company, max_results=max_per_company)
            print(f"  Found {len(raw_profiles)} profiles")

            parsed = [parse_profile(r, company) for r in raw_profiles]
            parsed = [p for p in parsed if p["full_name"]]

            stored = store_profiles(bq, parsed)
            update_company_summary(bq, company, stored)

            # Count WPP alumni found
            alumni_count = sum(1 for p in parsed if p.get("wpp_alumni"))

            results["companies"][cname] = {
                "status": "ok",
                "pool": company.get("pool"),
                "profiles_found": len(raw_profiles),
                "profiles_stored": stored,
                "wpp_alumni_found": alumni_count,
                "holding": company.get("holding", ""),
            }

            time.sleep(3)

        except Exception as e:
            print(f"  ERROR: {e}")
            results["companies"][cname] = {"status": "error", "error": str(e)}

    # Compute relationship scores after scraping
    if compute_rels and not dry_run:
        try:
            compute_relationships(bq)
            results["relationships_computed"] = True
        except Exception as e:
            results["relationships_error"] = str(e)

    results["total_companies"] = len(companies)
    results["agency_companies"] = sum(1 for c in companies if c.get("pool") == "agency")
    results["client_companies"] = sum(1 for c in companies if c.get("pool") == "client")
    results["total_processed"] = sum(1 for v in results["companies"].values() if v.get("status") == "ok")

    return json.dumps(results, indent=2), 200, {"Content-Type": "application/json"}
