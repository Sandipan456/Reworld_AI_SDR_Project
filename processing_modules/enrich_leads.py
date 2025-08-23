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



# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# df = pd.read_csv("osm_industry_results_chunked.csv")

# list_f = df['Factory Name'].to_list()
# print(list_f)

# prompt = \
# f"""
# You are an industrial data assistant. Given the name of a facility or factory, try to guess the name of its parent company. If you are unsure, return "N/A".
# Return the output as a list.
# example output: [company 1, conpany 2, company3]

# Here is the list of factories below:
# {list_f}
# """

# print(df.shape[0] == len(list_f))

# client = genai.Client(api_key=gemini_api_key)

# response = client.models.generate_content(
#     model="gemini-2.5-flash",
#     contents=prompt
# )

# match = re.search(r"\[.*?\]", response.text, re.DOTALL)
# if match:
#     list_string = match.group(0)
#     import ast
#     company_list = ast.literal_eval(list_string)
#     print(company_list)
# else:
#     print("No list found.")

# df["Company name"] = company_list

# print(df)

# df.to_csv("Company_name_updated.csv")




# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


# INPUT_CSV = "Company_name_updated.csv"
# OUTPUT_CSV = "Company_name_enriched.csv"
# RATE_LIMIT_DELAY = 0.5  # Seconds between requests to avoid throttling

# # === Load input file ===
# df = pd.read_csv(INPUT_CSV)

# # Ensure the column exists
# if 'Company name' not in df.columns:
#     raise ValueError("Column 'Company name' not found in input CSV.")

# # Prepare output columns
# df["Enhanced Company Name"] = ""
# df["Official Website"] = ""

# # === Query function ===
# def query_kg(company_name, api_key):
#     url = "https://kgsearch.googleapis.com/v1/entities:search"
#     params = {
#         "query": company_name,
#         "key": api_key,
#         "limit": 1,
#         "indent": True,
#     }
#     try:
#         response = requests.get(url, params=params)
#         if response.status_code != 200:
#             return None, None
#         data = response.json()
#         if not data.get("itemListElement"):
#             return None, None
#         result = data["itemListElement"][0]["result"]
#         name = result.get("name")
#         website = result.get("url")
#         return name, website
#     except Exception as e:
#         print(f"Error querying {company_name}: {e}")
#         return None, None

# # === Enrichment loop ===
# for i, row in df.iterrows():
#     company = row['Company name']
#     if pd.isna(company) or company.strip().lower() == "N/A":
#         continue

#     print(f"[{i+1}/{len(df)}] Querying KG for: {company}")
#     enhanced_name, website = query_kg(company, knowldge_graph_key)

#     df.at[i, "Enhanced Company Name"] = enhanced_name or "N/A"
#     df.at[i, "Official Website"] = website or "N/A"
    
#     time.sleep(RATE_LIMIT_DELAY)

# # === Save result ===
# df.to_csv(OUTPUT_CSV, index=False)
# print(f"\n✅ Done! Enriched data saved to: {OUTPUT_CSV}")


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


    