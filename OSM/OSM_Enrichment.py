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
from logger import logger
from google.cloud import secretmanager
from google.cloud import logging as cloud_logging
import os
import logging
import math


load_dotenv()

if os.getenv("ENVIRONMENT") == "cloud":
    client = cloud_logging.Client()
    client.setup_logging()

logger = logging.getLogger(__name__)

conn = get_connection()





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
        "industrial": ["chemical", "refinery", "petroleum_terminal", "gas_storage", "oil_mill"],
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
        "industrial": ["logistics", "storage", "warehouse", "refrigerated_warehouse"]
    },
    "Pharma":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["pharmaceuticals", "biologics"],
        "industrial": ["chemical"]
    },
    "Automotive":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["auto_parts", "vehicles"],
        "industrial": ["automotive_parts", "machine_shop"]
    },
    "Plastic_rubber":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["rubber"],
        "industrial": ["plastic_processing"]
    },
    "paper":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["rubber"],
        "industrial": ["paper_mill", "paper"]
    },
    "equipment":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["machinery", "electronics"],
        "industrial": ["warehouse"]
    },
    "Machinery":{
        "man_made": "works",
        "landuse": "industrial",
        "product": ["machinery"],
        "industrial": ["machine_shop"]
    }
}

def get_OSM_data(bbox, Reworld_fac_name):
    results = []

    for key, values in industry_tag_dict.items():
        main_industry = key
        industries = values["industrial"]
        products = values["product"]
        man_made = values["man_made"]
        landuse = values["landuse"]

        logger.info(f"Searching for mainindustry:{main_industry}")
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
                    logger.error(f"Request failed for {tag_combo} with code {response.status_code}")
                    logger.info(f"Request failed for {tag_combo} with code {response.status_code}")
                    continue
                logger.info(f"Request Fetched for {tag_combo} with code {response.status_code}")
                logger.info(f"Request Fetched for {tag_combo} with code {response.status_code}")
                data = response.json()

                # logger.info("Fetched results data", data.get("elements", []))

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
    # logger.info(f"✅ Saved {len(df)} facilities to: {output_path}")
    logger.info(df.head())
    return df




# def get_company_names(df: pd.DataFrame):

#     list_f = df['Factory Name'].to_list()

    
#     prompt =f"""
#         You are an industrial data assistant. You will be given a list of factory or facility names. The list contains **{len(list_f)} items**.

#         Your task is to infer the most likely **parent company name** for each factory.

#         Rules:
#         1. Your response **must be a Python list** with the **same number of items** as the input.
#         2. Each output item must correspond **exactly in order** to the factory name at the same index.
#         3. If you are not sure of the parent company, return "N/A" for that item.
#         4. Do not add explanations, newlines, or anything outside the list.

#         Factory names:
#         {list_f}

#         Output:
#     """

#     logger.info(df.shape[0] == len(list_f))

#     # client = genai.Client(api_key=gemini_api_key)
#     try:
#         response = gemini_model.generate_content(prompt)

#         match = re.search(r"\[.*?\]", response.text, re.DOTALL)
#         if match:
#             list_string = match.group(0)
#             import ast
#             company_list = ast.literal_eval(list_string)
#             logger.info("company_list Found")
#         else:
#             logger.info("No list found.")

#         if (df.shape[0] != len(company_list)):
#             logger.error(f"Company List length {len(company_list)} not same as Dataframe size {df.shape[0]}")
#             return pd.DataFrame()
    
#         logger.info(f"Company List length {len(company_list)} same as Dataframe size {df.shape[0]}")
#         df["Company name"] = company_list

#         # df.to_csv("Reworld_OSM_Chemical_FacilitiesV1.csv", index=False)
#         return df
#     except Exception as e:
#         logger.error("Failed to fetch data from Gemini", e)
#         logger.info("Failed to fetch data from Gemini", e)
#         return pd.DataFrame()


def get_company_names(df: pd.DataFrame):
    import math
    import pandas as pd
    
    # Input validation
    if df is None or df.empty:
        logger.warning("Empty or None dataframe provided")
        return pd.DataFrame()
    
    if 'Factory Name' not in df.columns:
        logger.error("'Factory Name' column not found in dataframe")
        return df  # Return original df without modification
    
    chunk_size = 100
    
    # If dataframe is small enough, process normally
    if len(df) <= chunk_size:
        return process_single_chunk(df)
    
    # Process in chunks
    try:
        chunks = math.ceil(len(df) / chunk_size)
        result_df = pd.DataFrame()
        failed_chunks = []
        
        logger.info(f"Processing {len(df)} rows in {chunks} chunks of {chunk_size}")
        
        for i in range(chunks):
            try:
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(df))
                
                # Get chunk with error handling
                chunk_df = df.iloc[start_idx:end_idx].copy()
                logger.info(f"Processing chunk {i+1}/{chunks}: rows {start_idx}-{end_idx-1}")
                
                # Process the chunk
                processed_chunk = process_single_chunk(chunk_df)
                
                # Check if processing was successful
                if processed_chunk is not None and not processed_chunk.empty:
                    # Ensure result_df has same structure as processed_chunk
                    if result_df.empty:
                        result_df = processed_chunk
                    else:
                        result_df = pd.concat([result_df, processed_chunk], ignore_index=True)
                    logger.info(f"Successfully processed chunk {i+1}")
                else:
                    logger.warning(f"Chunk {i+1} returned empty result")
                    failed_chunks.append(i+1)
                    
            except Exception as chunk_error:
                logger.error(f"Error processing chunk {i+1}: {str(chunk_error)}")
                failed_chunks.append(i+1)
                continue
        
        # Log summary
        if failed_chunks:
            logger.warning(f"Failed to process {len(failed_chunks)} chunks: {failed_chunks}")
        
        processed_rows = len(result_df) if not result_df.empty else 0
        logger.info(f"Successfully processed {processed_rows} out of {len(df)} total rows")
        
        # Return results even if some chunks failed
        return result_df if not result_df.empty else pd.DataFrame()
        
    except Exception as main_error:
        logger.error(f"Critical error in chunk processing: {str(main_error)}")
        return pd.DataFrame()  # Return original dataframe if everything fails


def process_single_chunk(df: pd.DataFrame):
    """Crash-proof single chunk processing"""
    try:
        # Input validation
        if df is None or df.empty:
            logger.warning("Empty chunk provided to process_single_chunk")
            return pd.DataFrame()
        
        if 'Factory Name' not in df.columns:
            logger.error("'Factory Name' column missing in chunk")
            return pd.DataFrame()
        
        list_f = df['Factory Name'].dropna().to_list()  # Remove NaN values
        
        if not list_f:
            logger.warning("No valid factory names found in chunk")
            return pd.DataFrame()
        
        prompt = f"""
            You are an industrial data assistant. You will be given a list of factory or facility names. The list contains **{len(list_f)} items**.

            Your task is to infer the most likely **parent company name** for each factory.

            Rules:
            1. Your response **must be a Python list** with the **same number of items** as the input.
            2. Each output item must correspond **exactly in order** to the factory name at the same index.
            3. If you are not sure of the parent company, return "N/A" for that item.
            4. Do not add explanations, newlines, or anything outside the list.

            Factory names:
            {list_f}

            Output:
        """

        logger.info(f"Processing {len(list_f)} factory names")

        # API call with error handling
        try:
            response = gemini_model.generate_content(prompt)
            
            if not hasattr(response, 'text') or not response.text:
                logger.error("No response text received from Gemini")
                return pd.DataFrame()
            
        except Exception as api_error:
            logger.error(f"API call failed: {str(api_error)}")
            return pd.DataFrame()

        # Parse response with error handling
        try:
            import re
            match = re.search(r"\[.*?\]", response.text, re.DOTALL)
            
            if match:
                list_string = match.group(0)
                import ast
                company_list = ast.literal_eval(list_string)
                logger.info("Company list successfully parsed")
            else:
                logger.warning("No valid list found in response")
                return pd.DataFrame()
                
        except Exception as parse_error:
            logger.error(f"Failed to parse response: {str(parse_error)}")
            return pd.DataFrame()

        # Validate response length
        if len(company_list) != df.shape[0]:
            logger.error(f"Company list length {len(company_list)} doesn't match dataframe size {df.shape[0]}")
            return pd.DataFrame()
    
        # Add company names to dataframe
        try:
            df_copy = df.copy()
            df_copy["Company name"] = company_list
            logger.info(f"Successfully added company names to {len(df_copy)} rows")
            return df_copy
            
        except Exception as assignment_error:
            logger.error(f"Failed to assign company names: {str(assignment_error)}")
            return pd.DataFrame()
        
    except Exception as general_error:
        logger.error(f"Unexpected error in process_single_chunk: {str(general_error)}")
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
            logger.error(f"Error querying {company_name}: {e}")
            return None, None

    # Enrichment loop
    for i, row in df.iterrows():
        company = row[company_column]
        if pd.isna(company) or company.strip().lower() == "n/a":
            continue

        logger.info(f"[{i + 1}/{len(df)}] Querying KG for: {company}")
        enhanced_name, website = query_kg(company)

        df.at[i, "Enhanced Company Name"] = enhanced_name or "N/A"
        df.at[i, "Official Website"] = website or "N/A"

        time.sleep(rate_limit_delay)

    # df.to_csv(output_csv_path, index=False)
    return df
    # logger.info(f"\n✅ Done! Enriched data saved to: {output_csv_path}")



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
            logger.info(f"URL doesn't exist for company: {name}, skipping...")
            continue

        domain = extract_domain(url)
        if not domain:
            logger.info(f"Invalid domain for {name}, skipping...")
            continue

        logger.info(f"[{i+1}/{len(df)}] Querying domain: {domain}")

        try:
            payload = {"domain": domain}
            response = requests.post(APOLLO_API_URL, headers=headers, json=payload)
            data = response.json()

            org = data.get("organization", {})
            if not org:
                logger.info(f"No organization data for: {name}, skipping...")
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

            logger.info(f"✅ Written info for {name}")
            time.sleep(rate_limit_delay)

        except Exception as e:
            logger.error(f"❌ Failed to fetch data for {name}: {e}")
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
    # logger.info(f"\n✅ Final enriched data saved to: {output_csv_path}")





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
    
    # Replace NaNs with None
    df = df.where(pd.notnull(df), None)

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

    logger.info(f"✅ Successfully pushed {len(df)} rows to `{table_name}`.")







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
        logger.info("No data found")
        return 
    # logger.info("--------------------- VALUE OF DF -------------------\n")
    # logger.info(df.columns)
    # logger.info("------------------------------------------------------\n")
    logger.info("OSM Data Found")


    df1 = get_company_names(df)

    if df1.empty:
        return

    logger.info("--------------------- VALUE OF DF1 -------------------\n")
    logger.info(f"VALUE OF DF1 {df1.columns}")
    logger.info("------------------------------------------------------\n")

    df2 = enrich_company_names_via_kg(df1)

    logger.info("--------------------- VALUE OF DF2 -------------------\n")
    logger.info(f"VALUE OF DF2 {df2.columns}")
    logger.info("------------------------------------------------------\n")

    df3 = enrich_domains_via_apollo(df2)


    # logger.info("--------------------- VALUE OF DF3 -------------------\n")
    logger.info(f"VALUE OF DF3 {df3.columns}")
    # logger.info("------------------------------------------------------\n")

    # df3.to_csv("TEST_OSM_ALL_INUSTRY.csv", index=False)
    push_df_to_db(df3)




# get_OSM_data(bbox)

# dfx = pd.read_csv("Reworld_OSM_Chemical_FacilitiesV2.csv")
# # enhance_Factory_names(dfx)
# enrich_domains_via_apollo(dfx, output_csv_path="Reworld_OSM_Chemical_FacilitiesV3.csv")
