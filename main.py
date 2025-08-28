from database.db_utils.connection import get_connection
from database.db_utils.schema import create_tables
import pandas as pd
from psycopg2.extras import execute_values
from processing_modules.Get_EPA_Data import get_TRI_Data, push_rcra_data, echo_all_facilities_in_25_miles_radius, get_HIFLD_Data_within_25_mile_radius
from utils.bbounding_boxes import create_bounding_box_300_miles, generate_25_mile_bounding_boxes
import time

# Setup DB
conn = get_connection()

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
    echo_all_facilities_in_25_miles_radius(bounding_box)
    time.sleep(0.5)
    get_HIFLD_Data_within_25_mile_radius(bounding_box)
    time.sleep(0.5)
    print(f"Processed {i + 1} Bounding boxes out of {len(bboxes)}")


# Finish
conn.commit()
# cur.close()
conn.close()
