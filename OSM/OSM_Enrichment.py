import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
from typing import Optional
import re
from database.db_utils.connection import get_connection
from psycopg2.extras import execute_values
import json
import google.generativeai as genai

conn = get_connection()




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


genai.configure(api_key=gemini_api_key)
gemini_model = genai.GenerativeModel(model_name="models/gemini-2.5-pro")



# Overpass API endpoint
over_pass_url = "https://overpass-api.de/api/interpreter"

# Define target tags
industry_tag_dict = {
    "Chemical": {
        "man_made": "works",
        "industrial": ["chemical", "refinery", "petroleum_terminal", "gas_plant", "gas_storage", "oil_mill"],
        "product": ["chemicals", "petrochemicals"],
        "landuse": "industrial"
    },
    "F&B_Tobacco": {
        "man_made": "works",
        "landuse": "industrial",
        "product": ["food", "beverage", "tobacco"],
        "industrial": ["food_processing", "bakery", "brewery", "winery", "rice_mill"]
    },
    "CPG":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["packaging", "consumer_goods"],
        "industrial": ["logistics", "storage"]
    },
    "Pharma":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["pharmaceuticals"],
        "industrial": ["chemical"]
    },
    "Automotive":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["auto_parts"],
        "industrial": ["automotive_parts", "machine_shop"]
    },
    "Plastic_rubber":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["rubber"],
        "industrial": ["plastic_processing"]
    },
    # "paper":{
    #     "man_made": "works",
    #     "landuse": "industrial",
    #     "product": ["rubber"],
    #     "industrial": ["paper_mill", "paper"]
    # },
    "equipment":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["machinery", "electronics"],
        "industrial": ["warehouse"]
    }
    # "Machinery"
}

def get_OSM_data(bbox, Reworld_fac_name):
    results = []

    for key, values in industry_tag_dict.items():
        main_industry = key
        industries = values["industrial"]
        products = values["product"]
        man_made = values["man_made"]
        landuse = values["landuse"]

        print(f"Searching for mainindustry:{main_industry}")
        for industry in industries:
            for product in products:
                tag_combo = f"{industry}|{product}"
                tag_combo_save =  f"industrial: {industry}|product: {product}|manmade: {man_made}|landuse: {landuse}"
                query = f"""
                [out:json][timeout:25]; 
                    ( node["industrial"="{industry}"]({bbox["ymin"]},{bbox["xmin"]},{bbox["ymax"]},
                    {bbox["xmax"]}); node["product"="{product}"]({bbox["ymin"]},{bbox["xmin"]},
                    {bbox["ymax"]},{bbox["xmax"]}); node["landuse"="{landuse}"]({bbox["ymin"]},
                    {bbox["xmin"]},{bbox["ymax"]},{bbox["xmax"]}); 
                    node["man_made"="{man_made}"]({bbox["ymin"]},{bbox["xmin"]},{bbox["ymax"]},{bbox["xmax"]}); 
                    way["industrial"="{industry}"]({bbox["ymin"]},{bbox["xmin"]},{bbox["ymax"]},{bbox["xmax"]}); 
                    relation["industrial"="{industry}"]({bbox["ymin"]},{bbox["xmin"]},{bbox["ymax"]},{bbox["xmax"]}); ); 
                out center tags; """


                response = requests.post(over_pass_url, data=query)
                if response.status_code != 200:
                    print(f"Request failed for {tag_combo} with code {response.status_code}")
                    continue
                print(f"Request Fetched for {tag_combo} with code {response.status_code}")
                data = response.json()

                # print("Fetched results data", data.get("elements", []))

                for element in data.get("elements", []):
                    tags = element.get("tags", {})
                    name = tags.get("name")
                    if not name:
                        continue  # Skip entries without factory name

                    lat = element.get("lat") or element.get("center", {}).get("lat")
                    lon = element.get("lon") or element.get("center", {}).get("lon")

                    results.append({
                        "Facility": Reworld_fac_name,
                        "Industry": main_industry,
                        "Factory Name": name,
                        "Address": tags.get("addr:street", ""),
                        "City": tags.get("addr:city", ""),
                        "Zipcode": tags.get("addr:postcode", ""),
                        "Latitude": lat,
                        "Longitude": lon,
                        "Tags Used": tag_combo_save
                    })

                time.sleep(2)  # Prevent API throttling

    # Save to CSV
    df = pd.DataFrame(results)
    # output_path = os.path.join(os.getcwd(), "Reworld_OSM_Chemical_Facilities.csv")
    # df.to_csv(output_path, index=False)
    # print(f"✅ Saved {len(df)} facilities to: {output_path}")
    print(df.head())
    return df




def get_company_names(df: pd.DataFrame):

    list_f = df['Factory Name'].to_list()

    
    prompt = \
    f"""
    You are an industrial data assistant. Given the name of a facility or factory, try to guess the name of its parent company. If you are unsure, return "N/A".
    Return the output as a list.
    example output: [company 1, conpany 2, company3]

    Here is the list of factories below:
    {list_f}
    """

    print(df.shape[0] == len(list_f))

    # client = genai.Client(api_key=gemini_api_key)
    try:
        response = gemini_model.generate_content(prompt)

        match = re.search(r"\[.*?\]", response.text, re.DOTALL)
        if match:
            list_string = match.group(0)
            import ast
            company_list = ast.literal_eval(list_string)
            print("company_list Found")
        else:
            print("No list found.")

        if (df.shape[0] != len(company_list)):
            return pd.DataFrame()
    

        df["Company name"] = company_list

        # df.to_csv("Reworld_OSM_Chemical_FacilitiesV1.csv", index=False)
        return df
    except Exception as e:
        print("Failed to fetch data from Gemini", e)
        return pd.DataFrame()




def enrich_company_names_via_kg(
    df: pd.DataFrame,
    # output_csv_path: str,
    company_column: str = "Company name",
    rate_limit_delay: float = 0.5
):
    """
    Enriches a DataFrame of company names using Google Knowledge Graph API.

    Parameters:
        df (pd.DataFrame): Input DataFrame with a company name column.
        output_csv_path (str): File path to save the enriched CSV.
        company_column (str): Column in df containing company names.
        rate_limit_delay (float): Delay between API calls (in seconds).
    """

    # Validate input
    if company_column not in df.columns:
        raise ValueError(f"Column '{company_column}' not found in input DataFrame.")

    # Prepare output columns
    df["Enhanced Company Name"] = ""
    df["Official Website"] = ""

    # Query function
    def query_kg(company_name):
        url = "https://kgsearch.googleapis.com/v1/entities:search"
        params = {
            "query": company_name,
            "key": knowldge_graph_key,
            "limit": 1,
            "indent": True,
        }
        try:
            response = requests.get(url, params=params)
            if response.status_code != 200:
                return None, None
            data = response.json()
            if not data.get("itemListElement"):
                return None, None
            result = data["itemListElement"][0]["result"]
            name = result.get("name")
            website = result.get("url")
            return name, website
        except Exception as e:
            print(f"Error querying {company_name}: {e}")
            return None, None

    # Enrichment loop
    for i, row in df.iterrows():
        company = row[company_column]
        if pd.isna(company) or company.strip().lower() == "n/a":
            continue

        print(f"[{i + 1}/{len(df)}] Querying KG for: {company}")
        enhanced_name, website = query_kg(company)

        df.at[i, "Enhanced Company Name"] = enhanced_name or "N/A"
        df.at[i, "Official Website"] = website or "N/A"

        time.sleep(rate_limit_delay)

    # df.to_csv(output_csv_path, index=False)
    return df
    # print(f"\n✅ Done! Enriched data saved to: {output_csv_path}")



def extract_domain(url: str) -> str:
    match = re.search(r"https?://(?:www\.)?([^/]+)", url)
    return match.group(1) if match else None

def enrich_domains_via_apollo(
    df: pd.DataFrame,
    # output_csv_path: str,
    website_column: str = "Official Website",
    name_column: str = "Company name",
    rate_limit_delay: float = 0.5
):
    """
    Enriches company data using Apollo API and saves a cleaned CSV.
    """

    # Drop unwanted/unstructured columns like 'Unnamed: 0'
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')].copy()

    # Initialize enrichment columns
    enrichment_cols = [
        "Revenue", "Revenue_printed", "address", "city", "state",
        "postal_code", "country", "ownedby_org", "employee_count"
    ]
    for col in enrichment_cols:
        if col not in df.columns:
            df[col] = None

    for i, row in df.iterrows():
        url = row.get(website_column)
        name = row.get(name_column)

        if not isinstance(url, str) or url.strip().upper() == "N/A":
            print(f"URL doesn't exist for company: {name}, skipping...")
            continue

        domain = extract_domain(url)
        if not domain:
            print(f"Invalid domain for {name}, skipping...")
            continue

        print(f"[{i+1}/{len(df)}] Querying domain: {domain}")

        try:
            payload = {"domain": domain}
            response = requests.post(APOLLO_API_URL, headers=headers, json=payload)
            data = response.json()

            org = data.get("organization", {})
            if not org:
                print(f"No organization data for: {name}, skipping...")
                continue

            df.at[i, "Revenue"] = org.get("annual_revenue")
            df.at[i, "Revenue_printed"] = org.get("annual_revenue_printed")
            df.at[i, "address"] = org.get("raw_address")
            df.at[i, "city"] = org.get("city")
            df.at[i, "state"] = org.get("state")
            df.at[i, "postal_code"] = org.get("postal_code")
            df.at[i, "country"] = org.get("country")

            owned = org.get("owned_by_organization")
            df.at[i, "ownedby_org"] = {
                "name": owned.get("name"),
                "website_url": owned.get("website_url")
            } if owned else None

            df.at[i, "employee_count"] = org.get("estimated_num_employees")

            print(f"✅ Written info for {name}")
            time.sleep(rate_limit_delay)

        except Exception as e:
            print(f"❌ Failed to fetch data for {name}: {e}")
            continue

    # Keep only the required columns in final output
    final_columns = [
        "Facility", "Industry", "Factory Name", "Address", "City", "Zipcode",
        "Latitude", "Longitude", "Tags Used", "Company name", "Enhanced Company Name",
        "Official Website", "Revenue", "Revenue_printed", "address", "city", "state",
        "postal_code", "country", "ownedby_org", "employee_count"
    ]

    available_cols = [col for col in final_columns if col in df.columns]
    return df[available_cols]
    # df[available_cols].to_csv(output_csv_path, index=False)
    # print(f"\n✅ Final enriched data saved to: {output_csv_path}")





def push_df_to_db(df: pd.DataFrame, table_name: str = "OSM_enhanced_data"):
    # Rename for DB compatibility
    df = df.rename(columns={
        "Address": "address_osm",
        "City": "city_osm",
        "Zipcode": "zipcode_osm",
        "Tags Used": "tags_used",
        "Company name": "company_name",
        "Enhanced Company Name": "enhanced_company_name",
        "Official Website": "official_website",
        "Revenue": "revenue",
        "Revenue_printed": "revenue_printed",
        "address": "hq_address",
        "city": "hq_city",
        "state": "hq_state",
        "postal_code": "hq_postal_code",
        "country": "hq_country",
        "ownedby_org": "owned_by_org",
        "employee_count": "employee_count",
        "Facility": "facility",
        "Industry": "industry",
        "Factory Name": "factory_name",
        "Latitude": "latitude",
        "Longitude": "longitude"
    })

    # Clean JSON fields (e.g. owned_by_org)
    if "owned_by_org" in df.columns:
        df["owned_by_org"] = df["owned_by_org"].apply(
            lambda x: json.dumps(x) if isinstance(x, dict) else None
        )

    # Define the columns to insert
    columns = [
        "facility", "industry", "factory_name", "address_osm", "city_osm", "zipcode_osm",
        "latitude", "longitude", "tags_used", "company_name", "enhanced_company_name",
        "official_website", "revenue", "revenue_printed", "hq_address", "hq_city",
        "hq_state", "hq_postal_code", "hq_country", "owned_by_org", "employee_count"
    ]

    values = df[columns].values.tolist()

    insert_query = f"""
    INSERT INTO {table_name} ({', '.join(columns)})
    VALUES %s
    ON CONFLICT DO NOTHING;
    """

    with conn.cursor() as cur:
        execute_values(cur, insert_query, values)
        conn.commit()

    print(f"✅ Successfully pushed {len(df)} rows to `{table_name}`.")


















# Example bounding box (Houston area)
bbox = {
    "xmin": -96.90,
    "ymin": 29.70,
    "xmax": -95.00,
    "ymax": 30.70
}




def Enhance_OSM_Data(bbox, Reworld_fac_name):
    df = get_OSM_data(bbox, Reworld_fac_name)

    if df.empty:
        print("No data found")
        return 
    print("--------------------- VALUE OF DF -------------------\n")
    print(df.columns)
    print("------------------------------------------------------\n")



    df1 = get_company_names(df)

    if df1.empty:
        return

    print("--------------------- VALUE OF DF1 -------------------\n")
    print(df1.columns)
    print("------------------------------------------------------\n")

    df2 = enrich_company_names_via_kg(df1)

    print("--------------------- VALUE OF DF2 -------------------\n")
    print(df2.columns)
    print("------------------------------------------------------\n")

    df3 = enrich_domains_via_apollo(df2)


    print("--------------------- VALUE OF DF3 -------------------\n")
    print(df3.columns)
    print("------------------------------------------------------\n")

    df3.to_csv("TEST_OSM_ALL_INUSTRY.csv", index=False)
    # push_df_to_db(df3)




# get_OSM_data(bbox)

# dfx = pd.read_csv("Reworld_OSM_Chemical_FacilitiesV2.csv")
# # enhance_Factory_names(dfx)
# enrich_domains_via_apollo(dfx, output_csv_path="Reworld_OSM_Chemical_FacilitiesV3.csv")
