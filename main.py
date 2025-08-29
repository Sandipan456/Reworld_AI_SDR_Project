from database.db_utils.connection import get_connection
from database.db_utils.schema import create_tables
import pandas as pd
from psycopg2.extras import execute_values
from processing_modules.Get_EPA_Data import get_TRI_Data, push_rcra_data, echo_all_facilities_in_25_miles_radius, get_HIFLD_Data_within_25_mile_radius, echo_rcra_facilities_in_25_miles_radius
from utils.bbounding_boxes import create_bounding_box_300_miles, generate_25_mile_bounding_boxes
import time
from OSM.OSM_Enrichment import Enhance_OSM_Data

# Setup DB
conn = get_connection()
create_tables()

lat = 40.85081
lon = -73.3309


# file_path = "data/US_GOV_Waste_Sites/Toxic_Release_Inventory.csv"

# get_TRI_Data(file_path)
# rcra_datas = ["data/US_GOV_Waste_Sites/RCRA_DATA_PART1.csv", "data/US_GOV_Waste_Sites/RCRA_DATA_PART2.csv",
#               "data/US_GOV_Waste_Sites/RCRA_DATA_PART3.csv"]

# for rcra_data in rcra_datas:
#     push_rcra_data(rcra_data)

bbox = create_bounding_box_300_miles(lat, lon)

bboxes = generate_25_mile_bounding_boxes(bbox)

for i, bounding_box in enumerate(bboxes):
    echo_rcra_facilities_in_25_miles_radius(bbox)
    # break
    print(f"Processing {i + 1} Bounding box out of {len(bboxes)}")
#     if i < 93:
#         continue
#     # Rename keys for downstream usage (without changing actual bbox values)
#     renamed_bbox = {
#         "ymin": bounding_box["minLat"],
#         "xmin": bounding_box["minLon"],
#         "ymax": bounding_box["maxLat"],
#         "xmax": bounding_box["maxLon"]
#     }

#     # print(renamed_bbox)

#     Enhance_OSM_Data(renamed_bbox)
#     # if (i == 19):
#     #     break

# # renamed_bbox = {
# #         "ymin": bbox["minLat"],
# #         "xmin": bbox["minLon"],
# #         "ymax": bbox["maxLat"],
# #         "xmax": bbox["maxLon"]
# #     }
# # print(renamed_bbox)
# # Enhance_OSM_Data(renamed_bbox)

# # # Finish
# # conn.commit()
# # # cur.close()
# # conn.close()
