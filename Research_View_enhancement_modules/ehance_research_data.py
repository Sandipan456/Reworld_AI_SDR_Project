import pandas as pd
from database.db_utils.connection import get_sqlalchemy_engine
from dotenv import load_dotenv
import os
import requests
import time
import google.generativeai as genai
import re
import ast
import json


load_dotenv()
gemini_api_key = os.environ.get("Gemini_api")
kg_url = "https://kgsearch.googleapis.com/v1/entities:search"
APOLLO_API_URL = "https://api.apollo.io/api/v1/organizations/enrich"
knowldge_graph_key = os.environ.get("google_cloud") 
apollo_key = os.environ.get("Apollo_api")



headers = {
    "accept": "application/json",
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "x-api-key": apollo_key
}


genai.configure(api_key=gemini_api_key)
gemini_model = genai.GenerativeModel(model_name="models/gemini-1.5-flash-latest")



conn = get_sqlalchemy_engine()


def load_facilites_in_chunks(conn, chunk_size = 100):
    """
    Generator to load facility data in chunks from frs_with_landfills table.
    Only loads registry_id and fac_name.
    """

    query = "SELECT * FROM frs_master_data"
    offset = 0


    while True:
        chunk_query = f"{query} OFFSET {offset} LIMIT {chunk_size}"
        df = pd.read_sql(chunk_query, conn)

        if df.empty:
            break
        
        yield df
        offset += chunk_size





def get_company_names(df: pd.DataFrame) -> pd.DataFrame:
    list_f = df["fac_name"].tolist()

    prompt = f"""
You are an industrial data assistant. Given the name of a facility or factory, try to guess the name of its parent company.
If you are unsure, return "N/A".

Return the output as a Python list. Example:
["Shell", "N/A", "ExxonMobil", ...]

Here is the list of factories below:
{list_f}
    """
    print(f"Sending batch of {len(list_f)} facilities to Gemini...")

    try:
        response = gemini_model.generate_content(prompt)
        match = re.search(r"\[.*?\]", response.text, re.DOTALL)

        if not match:
            print("[Gemini] No list found in response.")
            return pd.DataFrame()
        
        company_list = ast.literal_eval(match.group(0))

        if len(company_list) != len(df):
            print("[Gemini] Mismatched response length.")
            return pd.DataFrame()

        df["inferred_parent"] = company_list

        # Drop rows where LLM said "N/A"
        df = df[df["inferred_parent"].str.upper() != "N/A"].reset_index(drop=True)

        print("Company name lists successfully fetched from gemini...")
        return df
    
    except Exception as e:
        print(f"[Gemini Error] {e}")
        pd.DataFrame()





def query_google_kg(company_name: str) -> dict:
    """
    Calls Google Knowledge Graph Search API for a given company name.
    Returns name, URL, and description (or None if not found).
    """

    params = {
        "query": company_name,
        "key": knowldge_graph_key,
        "limit": 1,
        "indent": True
    }


    try:
        response = requests.get(kg_url, params=params)
        response.raise_for_status()
        data = response.json()

        if not data.get("itemListElement"):
            return {"kg_name": None, "kg_url": None, "kg_description": None}

        result = data["itemListElement"][0]["result"]

        return {
            "kg_name": result.get("name"),
            "kg_url": result.get("url"),
            "kg_description": result.get("description")
        }

    except Exception as e:
        print(f"[KG Error] {company_name}: {e}")
        return {"kg_name": None, "kg_url": None, "kg_description": None}



def enrich_with_google_kg(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies Google KG API on 'inferred_parent' column and adds new columns.
    Logs progress and results for transparency.
    """
    kg_results = {
        "kg_name": [],
        "kg_url": [],
        "kg_description": []
    }

    total = len(df)
    print(f"🔍 Starting KG enrichment for {total} rows...")

    for idx, parent_name in enumerate(df["inferred_parent"], start=1):
        print(f"→ [{idx}/{total}] Querying KG for: '{parent_name}'")

        result = query_google_kg(parent_name)
        
        kg_results["kg_name"].append(result["kg_name"])
        kg_results["kg_url"].append(result["kg_url"])
        kg_results["kg_description"].append(result["kg_description"])

        # print(f"   ✔ Result: Name = {result['kg_name']}, URL = {result['kg_url']}")
        time.sleep(1.2)  # Respect API rate limit
        

    # Append to DataFrame
    df["parent_company_name_kg"] = kg_results["kg_name"]
    df["domain"] = kg_results["kg_url"]
    df["kg_description"] = kg_results["kg_description"]

    print("KG enrichment complete for this chunk.")
    return df

## Need to refactor in OSM_ENrichment.py as well ---------------------------------------------------------------

def extract_domain(url: str) -> str:
    if not url or not isinstance(url, str):
        return None

    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    if match:
        return match.group(1)

    # If input is already a domain without scheme
    if "." in url and " " not in url:
        return url.strip().lower()

    return None
# --------------------------------------------------------------------------------------------

def get_info_from_apollo(df: pd.DataFrame) -> pd.DataFrame:
    apollo_results = {
        "revenue": [],
        "revenue_printed": [],
        "HQ_address": [],
        "HQ_city": [],
        "HQ_State": [],
        "HQ_Country": [],
        "HQ_postal_code": [],
        "owned_by_org": [],
        "employee_count": []
    }

    for idx, row in df.iterrows():
        url = row.get("domain")
        domain = extract_domain(url)

        if not domain:
            print("❌ Domain missing for:", row.get("parent_company_name_kg"))
            for key in apollo_results:
                apollo_results[key].append(None if key != "owned_by_org" else {})
            continue

        try:
            payload = {"domain": domain}
            response = requests.post(APOLLO_API_URL, headers=headers, json=payload)
            data = response.json()
            org = data.get("organization", {})

            apollo_results["revenue"].append(org.get("annual_revenue"))
            apollo_results["revenue_printed"].append(org.get("annual_revenue_printed"))
            apollo_results["HQ_address"].append(org.get("raw_address"))
            apollo_results["HQ_city"].append(org.get("city"))
            apollo_results["HQ_State"].append(org.get("state"))
            apollo_results["HQ_postal_code"].append(org.get("postal_code"))   # ✅ Corrected
            apollo_results["HQ_Country"].append(org.get("country"))           # ✅ Corrected
            apollo_results["employee_count"].append(org.get("estimated_num_employees"))

            owned = org.get("owned_by_organization") or {}
            apollo_results["owned_by_org"].append({
                "name": owned.get("name"),
                "website_url": owned.get("website_url")
            })

            time.sleep(1.2)
        except Exception as e:
            print(f"⚠️ Apollo error {e} for domain {domain}")
            for key in apollo_results:
                apollo_results[key].append(None if key != "owned_by_org" else {})
            time.sleep(1.2)

    for col in apollo_results:
        df[col] = apollo_results[col]

    return df



def serialize_json_columns(df: pd.DataFrame, json_columns: list[str]) -> pd.DataFrame:
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df


def push_to_postgres(df: pd.DataFrame, conn, table_name: str = "enhanced_data_2"):
    """
    Pushes the enriched DataFrame to a new or existing table in Postgres.
    Overwrites if table already exists.
    """
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="append",  # use 'append' if you prefer appending
        index=False,
        method="multi"
    )
    print(f"Pushed {len(df)} rows to table '{table_name}'")



def run_facility_enrichment_pipeline_ed2(chunk_size=100):
    all_enriched = []

    for chunk_df in load_facilites_in_chunks(conn, chunk_size=chunk_size):
        print(f"\n Processing chunk of {len(chunk_df)} rows...")

        # Step 1: Gemini inference
        enriched_with_parents = get_company_names(chunk_df.copy())
        if enriched_with_parents.empty:
            print("Skipping chunk — no valid parent companies.")
            continue

        # Step 2: Google KG enrichment
        enriched_chunk = enrich_with_google_kg(enriched_with_parents)
        all_enriched.append(enriched_chunk)
        break

    # Combine all valid enriched chunks
    if not all_enriched:
        print("No enriched data to push.")
        return

    kg_df = pd.concat(all_enriched, ignore_index=True)
    final_df = get_info_from_apollo(kg_df)
    # final_df = serialize_json_columns(final_df, json_columns=)
    final_df = serialize_json_columns(final_df, json_columns=["nearby_landfills_json", "owned_by_org"])
    print(f"Final enriched DataFrame shape: {final_df.shape}")

    # Push to Postgres
    push_to_postgres(final_df, conn)