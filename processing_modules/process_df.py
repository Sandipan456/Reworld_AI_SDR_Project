import pandas as pd
# from pathlib import Path
import os
import math
# from enrichment_engine.constants import industry_tag_dict
import time
import requests


# file_path = Path("data") / "Reworld_facilities_US_and_CA.csv"


industry_tag_dict = {
    "Chemical": [
        "industrial=chemical", "industrial=refinery", "product=cleaning_supplies",
        "product=chemical", "man_made=works", "landuse=industrial",
        "building=factory", "shop=cleaning_supplies", "office=chemical",
        "utility=gas", "power=generator", "plant=chemical", "product=disinfectants"
    ],
    "F&B_Tobacco": [
        "industrial=bakery", "industrial=brewery", "industrial=rice mill",
        "industrial=food processing", "man_made=works", "product=food",
        "product=meat", "product=beverages", "product=snacks", "product=alcohol",
        "building=factory", "shop=beverages", "shop=wine", "craft=winery",
        "landuse=industrial"
    ],
    "CPG": [
        "industrial=factory", "industrial=warehouse", "industrial=logistics",
        "man_made=works", "product=consumer_goods", "product=household_items",
        "building=warehouse", "landuse=industrial", "shop=supermarket",
        "shop=department_store", "shop=general"
    ],
    "Pharma": [
        "product=pharmaceuticals", "industrial=chemical", "man_made=works",
        "shop=chemist", "building=factory", "office=pharmaceutical",
        "craft=pharmaceutical", "product=medicines", "landuse=industrial"
    ],
    "Automotive": [
        "industrial=automotive parts", "shop=car_parts", "shop=car",
        "shop=tyres", "craft=car_repair", "product=vehicles",
        "man_made=works", "landuse=industrial", "building=garage"
    ],
    "Plastics_Rubber": [
        "industrial=plastic processing", "industrial=factory", "product=plastics",
        "product=rubber", "shop=plastic", "man_made=works", "landuse=industrial"
    ],
    "Paper": [
        "industrial=paper", "industrial=paper mill", "product=paper",
        "shop=stationery", "man_made=works", "landuse=industrial", "building=factory"
    ],
    "Equipment": [
        "industrial=equipment", "industrial=machine shop", "product=industrial_equipment",
        "shop=hardware", "shop=industrial", "shop=tools", "man_made=works",
        "building=factory", "landuse=industrial"
    ],
    "Machinery": [
        "industrial=machine shop", "product=machinery", "shop=machinery",
        "man_made=works", "building=factory", "landuse=industrial",
        "plant=machinery", "craft=metal_construction"
    ]
}




def getBoundingBox(lat: float, long: float, radiusInMiles: float):
    milesPerDegreeLat = 69
    milesPerDegreeLon = 69.0 * math.cos(lat * math.pi / 180)

    deltaLat = radiusInMiles / milesPerDegreeLat
    deltaLong = radiusInMiles / milesPerDegreeLon

    result = {
        "minLat" : lat - deltaLat,
        "maxLat" : lat + deltaLat,
        "minLong" : long - deltaLong,
        "maxLong" : long + deltaLong
    }

    return result


def haversine(lat1: float, lon1: float, lat2: float, lon2: float):

    R = 3960
    toRad = lambda x : x * math.PI / 180

    dLat = toRad(lat2 - lat1)
    dlon = toRad(lon2 - lon1)

    a = math.sin(dLat / 2) ** 2 + \
        math.cos(toRad(lat1)) * math.cos(toRad(lat2)) *\
        math.sin(dlon / 2) ** 2
    
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1- a))

    return R * c

def get_chunks(center_lat, center_lon, radius_miles, chunk_deg):
    print("📍 Generating geographic chunks...")
    miles_per_deg_lat = 69.0
    miles_per_deg_lon = 69.0 * math.cos(math.radians(center_lat))
    delta_lat = radius_miles / miles_per_deg_lat
    delta_lon = radius_miles / miles_per_deg_lon

    min_lat = center_lat - delta_lat
    max_lat = center_lat + delta_lat
    min_lon = center_lon - delta_lon
    max_lon = center_lon + delta_lon

    lat_steps = int((max_lat - min_lat) / chunk_deg) + 1
    lon_steps = int((max_lon - min_lon) / chunk_deg) + 1

    chunks = []
    for i in range(lat_steps):
        for j in range(lon_steps):
            south = min_lat + i * chunk_deg
            north = min(south + chunk_deg, max_lat)
            west = min_lon + j * chunk_deg
            east = min(west + chunk_deg, max_lon)
            chunks.append((south, west, north, east))
    print(f"✅ Generated {len(chunks)} chunks.")
    return chunks



def query_osm(tag, bbox, overpass_url="https://overpass.kumi.systems/api/interpreter", retries=3):
    key, value = tag.split('=')
    query = f"""
    [out:json][timeout:60];
    (
      node["{key}"="{value}"]({bbox});
      way["{key}"="{value}"]({bbox});
      relation["{key}"="{value}"]({bbox});
    );
    out center tags;
    """
    for attempt in range(1, retries + 1):
        try:
            print(f"🔍 Querying {tag} in {bbox} | Attempt {attempt}")
            response = requests.post(overpass_url, data={"data": query})
            if response.status_code == 429:
                wait = 5 * attempt
                print(f"⏳ Rate limit hit. Waiting {wait}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json().get("elements", [])
        except requests.exceptions.HTTPError as http_err:
            print(f"❌ HTTP error for {tag}: {http_err}")
            if response.status_code in [429, 504]:
                time.sleep(5 * attempt)
                continue
            return []
        except Exception as e:
            print(f" Error for {tag}: {e}")
            time.sleep(2 * attempt)
    print(f"All attempts failed for {tag}")
    return []

def parse_results(facility_name, elements, industry):
    parsed = []
    for el in elements:
        tags = el.get("tags", {})
        name = tags.get("name")
        if not name:
            continue
        lat = el.get("lat") or el.get("center", {}).get("lat")
        lon = el.get("lon") or el.get("center", {}).get("lon")
        parsed.append({
            "Facility": facility_name,
            "Industry": industry,
            "Factory Name": tags.get("name", "Unnamed"),
            "Address": tags.get("addr:street", ""),
            "City": tags.get("addr:city", ""),
            "Zipcode": tags.get("addr:postcode", ""),
            "Latitude": lat,
            "Longitude": lon
        })
    return parsed


def run_queries_chunked(facility_name, center_lat, center_lon, output_file, radius=300, chunk_deg=0.145):
    print("Starting chunked queries")
    chunks = get_chunks(center_lat, center_lon, radius, chunk_deg)
    total_written = 0

    for idx, (south, west, north, east) in enumerate(chunks):
        bbox = f"{south},{west},{north},{east}"
        print(f"\nProcessing chunk {idx + 1}/{len(chunks)}: {bbox}")

        for industry, tags in industry_tag_dict.items():
            print(f"Industry: {industry}")
            for tag in tags:
                try:
                    elements = query_osm(tag, bbox)
                    parsed = parse_results(facility_name, elements, industry)
                    if parsed:
                        chunk_df = pd.DataFrame(parsed)
                        # Remove duplicates within the chunk itself
                        chunk_df.drop_duplicates(subset=["Factory Name", "Latitude", "Longitude"], inplace=True)

                        file_exists = os.path.exists(output_file)
                        if file_exists:
                            existing_df = pd.read_csv(output_file, usecols=["Factory Name", "Latitude", "Longitude"])
                            merged_df = pd.merge(chunk_df, existing_df, on=["Factory Name", "Latitude", "Longitude"], how="left", indicator=True)
                            new_entries = merged_df[merged_df["_merge"] == "left_only"].drop(columns=["_merge"])
                        else:
                            new_entries = chunk_df

                        if not new_entries.empty:
                            new_entries.to_csv(output_file, mode='a', index=False, header=not file_exists)
                            total_written += len(new_entries)
                            print(f"Wrote {len(new_entries)} new rows from {tag} in chunk {idx+1}")
                        else:
                            print(f"All rows for {tag} already exist. Skipping write.")

                    else:
                        print(f"No data for {tag}")
                    time.sleep(1)
                except Exception as e:
                    print(f"Error writing {tag} in chunk {idx+1}: {e}")

    print(f"\n All done. Total rows written: {total_written}")

def get_leads(df: pd.DataFrame, output_file_path, chunk_deg, limit = 2):
    # counter = 0
    for i, row in df.iterrows():
        facillity_name = row["Facility Name"]
        center_lat = row["Latitude"]
        center_lon = row["Longitude"]
        
        if i == limit:
            break
        
        run_queries_chunked(facillity_name, center_lat, center_lon, output_file_path, chunk_deg)


if __name__ == "__main__":
    output_file = "osm_industry_results_chunked.csv"
    file_path = os.path.join("data", "Reworld_facilities__US_and_CA.csv")
    df = pd.read_csv(file_path)
    chunk_deg = 50 / 69.0 


    get_leads(df, output_file, chunk_deg)
