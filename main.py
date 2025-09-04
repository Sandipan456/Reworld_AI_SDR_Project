from database.db_utils.connection import get_connection
from database.db_utils.schema import create_tables
import pandas as pd
from processing_modules.Get_EPA_Data import get_TRI_Data, echo_all_facilities_in_25_miles_radius, get_HIFLD_Data_within_25_mile_radius, echo_rcra_facilities_in_25_miles_radius
from utils.bbounding_boxes import create_bounding_box_300_miles, generate_25_mile_bounding_boxes, generate_bounding_boxes
from OSM.OSM_Enrichment import Enhance_OSM_Data
from Research_View_enhancement_modules.ehance_research_data import run_facility_enrichment_pipeline_ed2
from Research_View_enhancement_modules.prepare_final_master import run_facility_llm_enrichment_pipeline
from SQL_RUN.run_frs_pipeline import run_frs_SQL_pipeline
from logger import logger

# Setup DB
conn = get_connection()
create_tables()



Reworld_df = pd.read_csv("data/Reworld_facilities__US_and_CA.csv")


get_TRI_Data("data/US_GOV_Waste_Sites/Toxic_Release_Inventory.csv")
for idx, reworld_row in Reworld_df.iterrows():
    Reworld_facility_name = reworld_row["Facility Name"]
    Reworld_facility_lat = reworld_row["Latitude"]
    Reworld_facility_lon = reworld_row["Longitude"]

    bbox = create_bounding_box_300_miles(lat=Reworld_facility_lat, lon=Reworld_facility_lon)

    bboxes_25 = generate_25_mile_bounding_boxes(bbox)

    for i, bounding_box in enumerate(bboxes_25):
        print(f"Fetching data for bounding box {i  + 1} / {len(bboxes_25)}")
        echo_all_facilities_in_25_miles_radius(bounding_box)
        get_HIFLD_Data_within_25_mile_radius(bounding_box)
        echo_rcra_facilities_in_25_miles_radius(bounding_box)

    run_frs_SQL_pipeline()

    # break
    bboxes = generate_bounding_boxes(bbox=bbox, mile_radius=75)
    for bounding_box in bboxes:
        renamed_bbox = {
            "ymin": bounding_box["minLat"],
            "xmin": bounding_box["minLon"],
            "ymax": bounding_box["maxLat"],
            "xmax": bounding_box["maxLon"]
        }
        Enhance_OSM_Data(renamed_bbox, Reworld_facility_name)


    run_facility_enrichment_pipeline_ed2(chunk_size=12) # -----> Remove break
    run_facility_llm_enrichment_pipeline(chunk_size=12)

conn.close()