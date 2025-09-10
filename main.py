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
import asyncio

# Setup DB
conn = get_connection()
create_tables()


Reworld_df = pd.read_csv("Reworld_facilities__US_and_CA.csv")
# run_frs_SQL_pipeline()

# # get_TRI_Data("Toxic_Release_Inventory.csv")
for idx, reworld_row in Reworld_df.iterrows():

    if idx < 36:
        continue

    logger.info(f"Facility: {idx}/{len(Reworld_df)}")
    Reworld_facility_name = reworld_row["Facility Name"]
    Reworld_facility_lat = reworld_row["Latitude"]
    Reworld_facility_lon = reworld_row["Longitude"]

    bbox = create_bounding_box_300_miles(lat=Reworld_facility_lat, lon=Reworld_facility_lon)

    bboxes_25 = generate_25_mile_bounding_boxes(bbox)

    for i, bounding_box in enumerate(bboxes_25):
        logger.info(f"Fetching data for bounding box {i  + 1} / {len(bboxes_25)}")
        try:
            echo_all_facilities_in_25_miles_radius(bounding_box)
        except:
            logger.error(f"Failed to fetch all facilities data for bounding box {i} for facility {Reworld_facility_name}")
        try:
            get_HIFLD_Data_within_25_mile_radius(bounding_box)
        except:
            logger.error(f"Failed to fetch HIFLD data for bounding box {i} for facility {Reworld_facility_name}")
        try:
            echo_rcra_facilities_in_25_miles_radius(bounding_box)
        except:
            logger.error(f"Failed to RCRA facilities data for bounding box {i} for facility {Reworld_facility_name}")

    try:
        run_frs_SQL_pipeline()
    except:
        logger.error(f"Failed to run SQL Queries for: {Reworld_facility_name}")

    # bboxes = generate_bounding_boxes(bbox=bbox, mile_radius=25)
    # for i, bounding_box in enumerate(bboxes):
    #     renamed_bbox = {
    #         "ymin": bounding_box["minLat"],
    #         "xmin": bounding_box["minLon"],
    #         "ymax": bounding_box["maxLat"],
    #         "xmax": bounding_box["maxLon"]
    #     }
    #     Enhance_OSM_Data(renamed_bbox, Reworld_facility_name)
        
async def get_OSM_Data():
    
    for idx, reworld_row in Reworld_df.iterrows():

        logger.info(f"OSM Data for Facility: {idx}/{len(Reworld_df)}")
        
        Reworld_facility_name = reworld_row["Facility Name"]
        Reworld_facility_lat = reworld_row["Latitude"]
        Reworld_facility_lon = reworld_row["Longitude"]

        bbox = create_bounding_box_300_miles(lat=Reworld_facility_lat, lon=Reworld_facility_lon)

        bboxes = generate_bounding_boxes(bbox=bbox, mile_radius=25)
        
        for i, bounding_box in enumerate(bboxes):
            logger.info(f"Fetching OSM data for bounding box {i  + 1} / {len(bboxes)}")
            renamed_bbox = {
                "ymin": bounding_box["minLat"],
                "xmin": bounding_box["minLon"],
                "ymax": bounding_box["maxLat"],
                "xmax": bounding_box["maxLon"]
            }
            Enhance_OSM_Data(renamed_bbox, Reworld_facility_name)

async def run_epa_enrichment_pipeLine():
    run_facility_enrichment_pipeline_ed2(chunk_size=100) # -----> Remove break
    run_facility_llm_enrichment_pipeline(chunk_size=100) # ---> Remove break

async def main():
    await asyncio.gather(
        get_OSM_Data(),
        run_epa_enrichment_pipeLine()
    )

if __name__ == "__main__":
    asyncio.run(main())
    conn.close()
    logger.info("Executed Successfully")