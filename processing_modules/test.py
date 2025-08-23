import requests
import math
import time
import pandas as pd
import os

# -------- CONFIGURATION --------
center_lat = 40.7128  # New York City (example)
center_lon = -74.0060
radius_miles = 300
chunk_deg = 100 / 69.0 

output_file = "osm_industry_results_chunked.csv"
facility_name = "Reworld MacArthur"

# --------- TAGS DICTIONARY ---------
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

# -------- BOUNDING BOX CHUNKS --------
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

# -------- QUERY --------
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

# -------- PARSE --------
def parse_results(elements, industry):
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

# -------- MAIN FUNCTION --------
def run_queries_chunked(center_lat, center_lon, radius=300, chunk_deg=0.145):
    print("🚀 Starting chunked queries")
    chunks = get_chunks(center_lat, center_lon, radius, chunk_deg)
    total_written = 0

    for idx, (south, west, north, east) in enumerate(chunks):
        bbox = f"{south},{west},{north},{east}"
        print(f"\n🧭 Processing chunk {idx + 1}/{len(chunks)}: {bbox}")

        for industry, tags in industry_tag_dict.items():
            print(f"🔧 Industry: {industry}")
            for tag in tags:
                try:
                    elements = query_osm(tag, bbox)
                    parsed = parse_results(elements, industry)
                    if parsed:
                        chunk_df = pd.DataFrame(parsed)
                        file_exists = os.path.exists(output_file)
                        chunk_df.to_csv(output_file, mode='a', index=False, header=not file_exists)
                        total_written += len(chunk_df)
                        print(f"💾 Wrote {len(chunk_df)} rows from {tag} in chunk {idx+1}")
                    else:
                        print(f"📭 No data for {tag}")
                    time.sleep(1)
                except Exception as e:
                    print(f"⚠️ Error writing {tag} in chunk {idx+1}: {e}")

    print(f"\n✅ All done. Total rows written: {total_written}")

# -------- RUN --------
if __name__ == "__main__":
    run_queries_chunked(center_lat, center_lon, radius_miles, chunk_deg)