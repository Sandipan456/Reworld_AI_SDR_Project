import os
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from typing import Optional
from google import genai
import re

# Load .env
load_dotenv()
gemini_api_key = os.environ.get("Gemini_api")
APOLLO_API_URL = "https://api.apollo.io/api/v1/organizations/enrich"
knowldge_graph_key = os.environ.get("google_cloud") 
apollo_key = os.environ.get("Apollo_api")

headers = {
    "accept": "application/json",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "x-api-key": apollo_key
}


def extract_domain(url):
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return match.group(1) if match else None

domain_df = pd.read_csv("Company_name_enriched.csv")

# Proper column initialization
for col in ["Revenue", "Revenue_printed", "address", "city", "state", "postal_code", "country", "ownedby_org", "employee_count"]:
    domain_df[col] = pd.NA

for i, row in domain_df.iterrows():
    URL = row["Official Website"]
    Name = row["Company name"]
    if not isinstance(URL, str) or URL.strip().upper() == "N/A":
        print(f"URL doesn't exist for company: {Name}, skipping...")
        continue
    print(URL)
    print(type(URL))
    domain = extract_domain(URL)
    if not domain:
        print(f"Invalid domain for {Name}, skipping...")
        continue

    payload = {"domain": domain}
    try:
        response = requests.post(APOLLO_API_URL, headers=headers, json=payload)
        data = response.json()

        org = data.get("organization", {})
        if not org:
            print(f"No organization data for: {Name}, skipping...")
            continue

        domain_df.at[i, "Revenue"] = org.get("annual_revenue")
        domain_df.at[i, "Revenue_printed"] = org.get("annual_revenue_printed")
        domain_df.at[i, "address"] = org.get("raw_address")
        domain_df.at[i, "city"] = org.get("city")
        domain_df.at[i, "state"] = org.get("state")
        domain_df.at[i, "postal_code"] = org.get("postal_code")
        domain_df.at[i, "country"] = org.get("country")
        owned = org.get("owned_by_organization")
        domain_df.at[i, "ownedby_org"] = {
            "name": owned.get("name"),
            "website_url": owned.get("website_url")
        } if owned else None
        domain_df.at[i, "employee_count"] = org.get("estimated_num_employees")

        print(f"Written Info for {Name}")
        time.sleep(0.5)  # Be nice to the API
    except Exception as e:
        print(f"Failed to fetch data for {Name}: {e}")
        continue

domain_df.to_csv("Final_leads_Updated.csv", index=False)


    