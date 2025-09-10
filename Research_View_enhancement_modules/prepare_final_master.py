import pandas as pd
from database.db_utils.connection import get_engine
from dotenv import load_dotenv
import os
import time
import google.generativeai as genai
import re
import json
from geopy.distance import geodesic
from logger import logger

load_dotenv()
gemini_api_key = os.environ.get("Gemini_api")

genai.configure(api_key=gemini_api_key)
gemini_model = genai.GenerativeModel(model_name="models/gemini-2.5-pro")

conn = get_engine()




# Load Reworld facilities with lat/lon
reworld_df = pd.read_csv("Reworld_facilities__US_and_CA.csv")
print("Corrected")
reworld_df = reworld_df.dropna(subset=["Latitude", "Longitude"])

# Ensure coordinates are float
reworld_df["Latitude"] = reworld_df["Latitude"].astype(float)
reworld_df["Longitude"] = reworld_df["Longitude"].astype(float)


def find_closest_reworld_facility(row, reworld_df):
    fac_coords = (row["fac_lat"], row["fac_long"])

    # Compute distances to all Reworld facilities
    distances = reworld_df.apply(
        lambda x: geodesic(fac_coords, (x["Latitude"], x["Longitude"])).miles,
        axis=1
    )

    # Find index of the closest
    min_idx = distances.idxmin()
    closest_name = reworld_df.loc[min_idx, "Facility Name"]
    closest_distance = distances[min_idx]

    return pd.Series([closest_name, closest_distance])


def load_facilities_in_chunks(conn, chunk_size=100):
    """
    Generator to load facility data in chunks from enhanced_data_2,
    excluding rows that already exist in final_master_data (if that table exists).
    """

    # Check if final_master_data exists
    check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'final_master_data'
        );
    """
    table_exists = pd.read_sql(check_table_query, conn).iloc[0, 0]

    if table_exists:
        base_query = """
            SELECT *
            FROM enhanced_data_2
            WHERE registry_id NOT IN (
                SELECT DISTINCT registry_id FROM final_master_data
            )
        """
    else:
        # If final_master_data doesn't exist, just take all enhanced_data_2 rows
        base_query = """
            SELECT *
            FROM enhanced_data_2
        """

    offset = 0
    while True:
        chunk_query = f"{base_query} OFFSET {offset} LIMIT {chunk_size}"
        df = pd.read_sql(chunk_query, conn)

        if df.empty:
            logger.info("✅ All chunks processed.")
            break  # <-- important to stop iteration

        yield df
        offset += chunk_size

def enrich_facility_row_via_llm(row):
    inferred_parent = row.get("inferred_parent", "N/A") or "N/A"
    kg_parent = row.get("parent_company_name_kg", "N/A") or "N/A"
    domain = row.get("domain", "N/A") or "N/A"
    facility_desc = f"{row.get('fac_name', 'N/A')}, {row.get('fac_street', '')}, {row.get('fac_city', '')}, {row.get('fac_state', '')}"
    prompt = f"""
    You are a research assistant extracting structured information about industrial companies and their facilities from noisy or partial data.

    Use the information below to generate a clean and complete JSON response. Try to avoid using "N/A" unless absolutely necessary. If exact values are unavailable, make reasonable inferences or use approximations with proper units (e.g., “approx. 200,000 sq ft”, “over 1 million lbs”, etc.). Use industry norms, clues from context, or public references to fill in gaps.

    Company Information:
    - Inferred Parent Company: {inferred_parent}
    - Parent Company from Knowledge Graph: {kg_parent}
    - Website Domain: {domain}

    Facility Information:
    - Location Description: {facility_desc}

    Return a JSON object with the following structure:
    - "company_overview": A concise summary of what the company does. Use the parent company name or domain to search public knowledge or infer from facility description.
    - "sustainability_goals": A bullet-point list of known or typical sustainability goals or initiatives by the company. If specific goals are not known, infer likely ones based on industry and company size (e.g., “reducing hazardous waste”, “increasing recycling rate”, etc.). Also mention if you have made any assumptions.
    - "facility_square_footage": Facility size in square feet. If exact number is missing, estimate based on company type, domain, or facility description. If nothing can be inferred, return "N/A". Also mention the level of confidence
    - "waste_metrics": A dictionary with these keys:
        - "PW_solids": Amount of process waste solids (e.g., “120 tons”, “~350,000 lbs”) or best approximation.
        - "WWT_drums": Number of wastewater treatment drums. Use approximate count or range if specific value is unavailable.
        - "total_waste": Total waste generated (lbs or tons). Use estimates if specific numbers aren’t available.
        If no information is available even after reasonable inference, return "N/A".

    Return **only** the JSON. Do not add explanations or extra text.
    """
    


    try:
        response = gemini_model.generate_content(prompt)
        response_text = response.text.strip()
        logger.info(f"Prompt sent for: {inferred_parent or kg_parent}")
        print(f"Raw response: {response_text[:300]}...\n")  # Preview

        json_match = re.search(r"\{.*\}", response_text, re.DOTALL)
        if not json_match:
            logger.info(f"[WARNING] No JSON match found for: {inferred_parent} / {kg_parent}")
            return _na_payload()

        parsed = json.loads(json_match.group(0))
        return {
            "company_overview": parsed.get("company_overview", "N/A"),
            "sustainability_goals": parsed.get("sustainability_goals", "N/A"),
            "facility_square_footage": parsed.get("facility_square_footage", "N/A"),
            "waste_metrics": parsed.get("waste_metrics", {
                "PW_solids": "N/A",
                "WWT_drums": "N/A",
                "total_waste": "N/A"
            })
        }

    except Exception as e:
        logger.error(f"[❌ LLM ERROR] Failed for: {inferred_parent} / {kg_parent} → {e}")
        return _na_payload()


def _na_payload():
    return {
        "company_overview": "N/A",
        "sustainability_goals": "N/A",
        "facility_square_footage": "N/A",
        "waste_metrics": {
            "PW_solids": "N/A",
            "WWT_drums": "N/A",
            "total_waste": "N/A"
        }
    }


def enrich_chunk_with_llm(chunk_df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Enriching {len(chunk_df)} rows...")

    results = {
        "company_overview": [],
        "sustainability_goals": [],
        "facility_square_footage": [],
        "PW_solids": [],
        "WWT_drums": [],
        "total_waste": [],
    }

    for i, (_, row) in enumerate(chunk_df.iterrows()):
        logger.info(f"\n📄 Row {i + 1}/{len(chunk_df)}: {row.get('fac_name')} ({row.get('inferred_parent') or row.get('parent_company_name_kg')})")
        enriched = enrich_facility_row_via_llm(row)

        results["company_overview"].append(enriched["company_overview"])
        results["sustainability_goals"].append(enriched["sustainability_goals"])
        results["facility_square_footage"].append(enriched["facility_square_footage"])

        waste = enriched.get("waste_metrics", {})
        if not isinstance(waste, dict):
            waste = {
                "PW_solids": "N/A",
                "WWT_drums": "N/A",
                "total_waste": "N/A"
            }          
        results["PW_solids"].append(waste.get("PW_solids", "N/A"))
        results["WWT_drums"].append(waste.get("WWT_drums", "N/A"))
        results["total_waste"].append(waste.get("total_waste", "N/A"))

        time.sleep(1.1)  # Respect API limits

    for key, values in results.items():
        chunk_df[key] = values

    logger.info("✅ Enrichment complete for this chunk.\n")
    return chunk_df


def serialize_json_columns(df: pd.DataFrame, json_columns: list[str]) -> pd.DataFrame:
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
    return df


def push_to_postgres(df: pd.DataFrame, conn, table_name: str = "final_master_data"):
    df.to_sql(
        name=table_name,
        con=conn,
        if_exists="append",
        index=False,
        method="multi"
    )
    logger.info(f"Pushed {len(df)} rows to table '{table_name}'\n")


def run_facility_llm_enrichment_pipeline(chunk_size=100):
    logger.info("Starting full enrichment pipeline...\n")

    for i, chunk_df in enumerate(load_facilities_in_chunks(conn, chunk_size=chunk_size)):
        logger.info(f"\n============================")
        logger.info(f"🔄 Processing Chunk {i + 1}")
        logger.info(f"============================")

        enriched_chunk = enrich_chunk_with_llm(chunk_df.copy())

        enriched_chunk = serialize_json_columns(
            enriched_chunk,
            json_columns=["sustainability_goals", "owned_by_org", "facility_square_footage"]
        )

        enriched_chunk[["closest_reworld_facility_name", "distance_to_closest_reworld_miles"]] = enriched_chunk.apply(
                                                                                                find_closest_reworld_facility, axis=1, reworld_df=reworld_df
                                                                                            )

        push_to_postgres(enriched_chunk, conn)

        logger.info(f"✅ Chunk {i + 1} processed and saved.\n")
        logger.info(f"============================\n")
        # break
        
    logger.info("Pipeline complete!")
