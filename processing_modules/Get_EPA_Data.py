import requests
import json
import pandas as pd
from database.db_utils.connection import get_connection
from psycopg2.extras import execute_values
from logger import logger

conn = get_connection()



def echo_all_facilities_in_25_miles_radius(bbox):
    bounding_box = {
        "xmin": bbox["minLon"],
        "ymin": bbox["minLat"],
        "xmax": bbox["maxLon"],
        "ymax": bbox["maxLat"],
        "spatialReference": {"wkid": 4326}
    }

    cols_to_keep = [
        "REGISTRY_ID","FAC_NAME", "FAC_STREET", "FAC_CITY", "FAC_STATE", "FAC_ZIP",
        "FAC_COUNTY", "FAC_FIPS_CODE", "FAC_LAT", "FAC_LONG",
        "FAC_NAICS_CODES", "FAC_SIC_CODES",
        "AIR_FLAG", "RCRA_FLAG", "TRI_FLAG", "SDWIS_FLAG", "GHG_FLAG",
        "AIR_IDS", "NPDES_IDS", "RCRA_IDS", "TRI_IDS", "SDWA_IDS", "GHG_IDS"
    ]

    # API endpoint
    url = "https://echogeo.epa.gov/arcgis/rest/services/ECHO/Facilities/MapServer/0/query"

    # Query Parameters
    params = {
        "f": "geojson",
        "where": "1=1",  # No attribute filter
        "outFields": "*",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "geometry": json.dumps(bounding_box),
        "spatialRel": "esriSpatialRelIntersects",
        "outSR": "4326"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
    except Exception as e:
        logger.info(f"Failed to fetch ECHO failities data for {bbox}")
        print(f"Failed to fetch data with request failed")
        return

    if response.status_code == 200:
        logger.info(f"Echo Facility_Data fetched Successfully for {bbox}")
        print("Facility_Data fetched Successfully")
        try:
            data = response.json()

            features = data.get("features", [])
            records = [feature["properties"] for feature in features]


            df = pd.DataFrame(records)
            df = df[[col for col in cols_to_keep if col in df.columns]]

            for col in cols_to_keep:
                if col not in df.columns:
                    df[col] = None
            df = df[cols_to_keep]
            # print("Total columns:", len(df.columns))
            # print(df.head(10))

            record_tuples = df.values.tolist()

            try:
                cur = conn.cursor()
                execute_values(cur, """
                    INSERT INTO facilities (
                        REGISTRY_ID, FAC_NAME, FAC_STREET, FAC_CITY, FAC_STATE, FAC_ZIP,
                        FAC_COUNTY, FAC_FIPS_CODE, FAC_LAT, FAC_LONG,
                        FAC_NAICS_CODES, FAC_SIC_CODES,
                        AIR_FLAG, RCRA_FLAG, TRI_FLAG, SDWIS_FLAG, GHG_FLAG,
                        AIR_IDS, NPDES_IDS, RCRA_IDS, TRI_IDS, SDWA_IDS, GHG_IDS
                    ) VALUES %s
                    ON CONFLICT (REGISTRY_ID) DO NOTHING
                """, record_tuples)

                conn.commit()
                logger.info("Successfully pushed ECHO Data to database.")
                print("Successfully pushed to database.")

            except Exception as e:
                logger.error(f"Failed to push EPA data to Data base error: {e}")
                conn.rollback()
                print("Failed to push to database")
                print("Error:\n", e)

            # limit_records = records[:10]
            # df = pd.DataFrame(limit_records)
            # final_cols = [col for col in cols_to_keep if col in df.columns]

            # df[final_cols].to_csv("US_GOV_Waste_Sites/part_1_all_facilities.csv", index=False)
            # print(json.dumps(data, indent=2))  # Pretty-print JSON
            # return df
        except Exception as e:
            print(f"Failed to process the fetched data with exception:\n {e}")
            return None
    else:
        print("Failed to load. Status code:", response.status_code)
        print("Response:", response.text)
        return None





def get_HIFLD_Data_within_25_mile_radius(bbox):

    cur = conn.cursor()

    columns_to_keep = [
        "swid",         # Unique facility ID
        "name",         # Facility name
        "address",      # Street address
        "city",         # City
        "state",        # State code
        "zip",          # ZIP code
        "telephone",    # Contact number
        "country",      # Country
        "latitude",     # Latitude coordinate
        "longitude",    # Longitude coordinate
        "naics_code",   # NAICS industry code
        "naics_desc",   # Description of NAICS code
        "owner",        # Facility owner
        "permit_no",    # Regulatory permit number
        "type"          # Facility type (e.g., landfill, transfer)
    ]

    bounding_box = {
        "xmin": bbox["minLon"],
        "ymin": bbox["minLat"],
        "xmax": bbox["maxLon"],
        "ymax": bbox["maxLat"],
        "spatialReference": {"wkid": 4326}
    }

    url ="https://maps.nccs.nasa.gov/mapping/rest/services/hifld_open/chemicals/MapServer/6/query"

    params = {
        "f": "geojson",
        "where": "1=1",  # No attribute filter
        "outFields": "*",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "geometry": json.dumps(bounding_box),
        "spatialRel": "esriSpatialRelIntersects",
        "outSR": "4326"
    }

    try:
        response = requests.get(url, params=params, timeout=10)
    except Exception as e:
        print(f"Failed to fetch data with request failed")
        logger.error(f"Failed to fetch HIFLD data with request for: {bbox}")
        return

    if response.status_code == 200:
        try:
            data = response.json()
            features = data.get("features", [])
            records = [feature["properties"] for feature in features]

            df = pd.DataFrame(records)
            for col in columns_to_keep:
                if col not in df.columns:
                    df[col] = None
            df = df[columns_to_keep]

            # Convert to list of tuples
            record_tuples = df.values.tolist()

            # Insert into database
            try:
                execute_values(cur, """
                    INSERT INTO hifld_landfills (
                        swid, name, address, city, state, zip,
                        telephone, country, latitude, longitude,
                        naics_code, naics_desc, owner, permit_no, type
                    ) VALUES %s
                    ON CONFLICT (swid) DO NOTHING;
                """, record_tuples)
                conn.commit()
                logger.info("Successfully pushed HIFLD data to the database.")
                print("Successfully pushed HIFLD data to the database.")

            except Exception as e:
                conn.rollback()
                logger.error(f"Failed to push HIFLD data to database. {e}")
                print("Failed to push HIFLD data to database.")
                print("Error:\n", e)

            # Save preview
            # df.head(10).to_csv("US_GOV_Waste_Sites/part_3_HIFLD_landfills.csv", index=False)
            return None
        except Exception as e:
            logger.error("HIFLD Failed to process response.\n{e}")
            print(f"Failed to process response.\n{e}")
            return None
    else:
        print(f"Failed to fetch data staus_code{response.status_code}")




def get_TRI_Data(file_path):
    column_rename_map = {
    '1. YEAR': 'Year',
    '2. TRIFD': 'Trifd',
    '3. FRS ID': 'Frs Id',
    '4. FACILITY NAME': 'Facility Name',
    '5. STREET ADDRESS': 'Street Address',
    '6. CITY': 'City',
    '7. COUNTY': 'County',
    '8. ST': 'St',
    '9. ZIP': 'Zip',
    '10. BIA': 'Bia',
    '11. TRIBE': 'Tribe',
    '12. LATITUDE': 'Latitude',
    '13. LONGITUDE': 'Longitude',
    '14. HORIZONTAL DATUM': 'Horizontal Datum',
    '15. PARENT CO NAME': 'Parent Co Name',
    '16. PARENT CO DB NUM': 'Parent Co Db Num',
    '17. STANDARD PARENT CO NAME': 'Standard Parent Co Name',
    '18. FOREIGN PARENT CO NAME': 'Foreign Parent Co Name',
    '19. FOREIGN PARENT CO DB NUM': 'Foreign Parent Co Db Num',
    '20. STANDARD FOREIGN PARENT CO NAME': 'Standard Foreign Parent Co Name',
    '21. FEDERAL FACILITY': 'Federal Facility',
    '22. INDUSTRY SECTOR CODE': 'Industry Sector Code',
    '23. INDUSTRY SECTOR': 'Industry Sector',
    '24. PRIMARY SIC': 'Primary Sic',
    '25. SIC 2': 'Sic 2',
    '26. SIC 3': 'Sic 3',
    '27. SIC 4': 'Sic 4',
    '28. SIC 5': 'Sic 5',
    '29. SIC 6': 'Sic 6',
    '30. PRIMARY NAICS': 'Primary Naics',
    '31. NAICS 2': 'Naics 2',
    '32. NAICS 3': 'Naics 3',
    '33. NAICS 4': 'Naics 4',
    '34. NAICS 5': 'Naics 5',
    '35. NAICS 6': 'Naics 6',
    '36. DOC_CTRL_NUM': 'Doc_Ctrl_Num',
    '37. CHEMICAL': 'Chemical',
    '38. ELEMENTAL METAL INCLUDED': 'Elemental Metal Included',
    '39. TRI CHEMICAL/COMPOUND ID': 'Tri Chemical/Compound Id',
    '40. CAS#': 'Cas#',
    '41. SRS ID': 'Srs Id',
    '42. CLEAN AIR ACT CHEMICAL': 'Clean Air Act Chemical',
    '43. CLASSIFICATION': 'Classification',
    '44. METAL': 'Metal',
    '45. METAL CATEGORY': 'Metal Category',
    '46. CARCINOGEN': 'Carcinogen',
    '47. PBT': 'Pbt',
    '48. PFAS': 'Pfas',
    '49. FORM TYPE': 'Form Type',
    '50. UNIT OF MEASURE': 'Unit Of Measure',
    '51. 5.1 - FUGITIVE AIR': 'Fugitive Air',
    '52. 5.2 - STACK AIR': 'Stack Air',
    '53. 5.3 - WATER': 'Water',
    '54. 5.4 - UNDERGROUND': 'Underground',
    '55. 5.4.1 - UNDERGROUND CL I': 'Underground Cl I',
    '56. 5.4.2 - UNDERGROUND C II-V': 'Underground C Ii-V',
    '57. 5.5.1 - LANDFILLS': 'Landfills',
    '58. 5.5.1A - RCRA C LANDFILL': 'Rcra C Landfill',
    '59. 5.5.1B - OTHER LANDFILLS': 'Other Landfills',
    '60. 5.5.2 - LAND TREATMENT': 'Land Treatment',
    '61. 5.5.3 - SURFACE IMPNDMNT': 'Surface Impndmnt',
    '62. 5.5.3A - RCRA SURFACE IM': 'Rcra Surface Im',
    '63. 5.5.3B - OTHER SURFACE I': 'Other Surface I',
    '64. 5.5.4 - OTHER DISPOSAL': 'Other Disposal',
    '65. ON-SITE RELEASE TOTAL': 'On-Site Release Total',
    '66. 6.1 - POTW - TRNS RLSE': 'Potw - Trns Rlse',
    '67. 6.1 - POTW - TRNS TRT': 'Potw - Trns Trt',
    '68. POTW - TOTAL TRANSFERS': 'Potw - Total Transfers',
    '69. 6.2 - M10': 'M10',
    '70. 6.2 - M41': 'M41',
    '71. 6.2 - M62': 'M62',
    '72. 6.2 - M40 METAL': 'M40 Metal',
    '73. 6.2 - M61 METAL': 'M61 Metal',
    '74. 6.2 - M71': 'M71',
    '75. 6.2 - M81': 'M81',
    '76. 6.2 - M82': 'M82',
    '77. 6.2 - M72': 'M72',
    '78. 6.2 - M63': 'M63',
    '79. 6.2 - M66': 'M66',
    '80. 6.2 - M67': 'M67',
    '81. 6.2 - M64': 'M64',
    '82. 6.2 - M65': 'M65',
    '83. 6.2 - M73': 'M73',
    '84. 6.2 - M79': 'M79',
    '85. 6.2 - M90': 'M90',
    '86. 6.2 - M94': 'M94',
    '87. 6.2 - M99': 'M99',
    '88. OFF-SITE RELEASE TOTAL': 'Off-Site Release Total',
    '89. 6.2 - M20': 'M20',
    '90. 6.2 - M24': 'M24',
    '91. 6.2 - M26': 'M26',
    '92. 6.2 - M28': 'M28',
    '93. 6.2 - M93': 'M93',
    '94. OFF-SITE RECYCLED TOTAL': 'Off-Site Recycled Total',
    '95. 6.2 - M56': 'M56',
    '96. 6.2 - M92': 'M92',
    '97. OFF-SITE ENERGY RECOVERY T': 'Off-Site Energy Recovery T',
    '98. 6.2 - M40 NON-METAL': 'M40 Non-Metal',
    '99. 6.2 - M50': 'M50',
    '100. 6.2 - M54': 'M54',
    '101. 6.2 - M61 NON-METAL': 'M61 Non-Metal',
    '102. 6.2 - M69': 'M69',
    '103. 6.2 - M95': 'M95',
    '104. OFF-SITE TREATED TOTAL': 'Off-Site Treated Total',
    '105. 6.2 - UNCLASSIFIED': 'Unclassified',
    '106. 6.2 - TOTAL TRANSFER': 'Total Transfer',
    '107. TOTAL RELEASES': 'Total Releases',
    '108. 8.1 - RELEASES': 'Releases',
    '109. 8.1A - ON-SITE CONTAINED': 'On-Site Contained',
    '110. 8.1B - ON-SITE OTHER': 'On-Site Other',
    '111. 8.1C - OFF-SITE CONTAIN': 'Off-Site Contain',
    '112. 8.1D - OFF-SITE OTHER R': 'Off-Site Other R',
    '113. 8.2 - ENERGY RECOVER ON': 'Energy Recover On',
    '114. 8.3 - ENERGY RECOVER OF': 'Energy Recover Of',
    '115. 8.4 - RECYCLING ON SITE': 'Recycling On Site',
    '116. 8.5 - RECYCLING OFF SIT': 'Recycling Off Sit',
    '117. 8.6 - TREATMENT ON SITE': 'Treatment On Site',
    '118. 8.7 - TREATMENT OFF SITE': 'Treatment Off Site',
    '119. PRODUCTION WSTE (8.1-8.7)': 'Production Wste (8.1-8.7)',
    '120. 8.8 - ONE-TIME RELEASE': 'One-Time Release',
    '121. PROD_RATIO_OR_ ACTIVITY': 'Prod_Ratio_Or_ Activity',
    '122. 8.9 - PRODUCTION RATIO': 'Production Ratio'
}


    
    df = pd.read_csv(file_path, low_memory=False)

    df.columns = [column_rename_map[f"{col}"] for col in df.columns]
    
    columns_to_keep = [
        "Year", "Trifd", "Frs Id", "Facility Name", "Street Address",
        "City", "St", "Zip", "County", "Latitude", "Longitude",
        "Parent Co Name", "Standard Parent Co Name", "Federal Facility",
        "Industry Sector Code", "Industry Sector", "Primary Sic", "Primary Naics",
        "Chemical", "Cas#", "Carcinogen", "Pbt", "Pfas", "Unit Of Measure",
        "Fugitive Air", "Stack Air", "Water", "Landfills",
        "On-Site Release Total", "Off-Site Release Total", "Off-Site Recycled Total",
        "Off-Site Energy Recovery T", "Off-Site Treated Total", "Total Transfer",
        "Total Releases", "Production Wste (8.1-8.7)", "Naics 6", "Naics 4"
    ]



    existing_columns = [col for col in columns_to_keep if col in df.columns]
    print(df.head())
    print(df[existing_columns].shape[0])
    # df[existing_columns].to_csv("US_GOV_Waste_Sites/part_4_TRI_Data.csv", index=False)


    records = df[existing_columns].to_records(index=False).tolist()

    cur = conn.cursor()

    insert_query = """
        INSERT INTO tri_facilities (
            year,
            trifd,
            frs_id,
            facility_name,
            street_address,
            city,
            state,
            zip,
            county,
            latitude,
            longitude,
            parent_co_name,
            standard_parent_co_name,
            federal_facility,
            industry_sector_code,
            industry_sector,
            primary_sic,
            primary_naics,
            chemical,
            cas_number,
            carcinogen,
            pbt,
            pfas,
            unit_of_measure,
            fugitive_air,
            stack_air,
            water,
            landfills,
            on_site_release_total,
            off_site_release_total,
            off_site_recycled_total,
            off_site_energy_recovery,
            off_site_treated_total,
            total_transfer,
            total_releases,
            production_waste,
            naics_6,
            naics_4
        ) VALUES %s
        ON CONFLICT (trifd) do NOTHING;
    """
    try:
        execute_values(cur, insert_query, records)
        conn.commit()
        cur.close()
        print("Succesfully pushed TRI datato database")
        logger.info("Succesfully pushed TRI data to database")
    except Exception as e:
        logger.error(f"Failed to push TRI Data to database error:{e}")
        print("Failed to push TRI Data to database")
        print("Error \n", e)



def push_rcra_data(file_path):
    df_rcra = pd.read_csv(file_path)

    cur = conn.cursor()
    print(df_rcra.columns)
    print(df_rcra.head(2))

    i = 0
    for col in df_rcra.columns:
        print(f"{col}: Dtype {type(df_rcra.iloc[0][i])}")
        i+=1
    # Convert DataFrame to list of tuples
    records = df_rcra.to_records(index=False).tolist()

    try:
        execute_values(cur, """
            INSERT INTO rcra_compliance (
                rcra_name, source_id, rcra_street, rcra_city, rcra_state, registry_id,
                rcra_snc, rcra_qtrs_with_nc, rcra_insp_cnt, rcra_fea_cnt
            )
            VALUES %s
            ON CONFLICT (registry_id) DO NOTHING
        """, records)
        print("Succesfully pushed to database")
    except Exception as e:
        conn.rollback() # --> Incase One fails others should work
        print("Failed to push to database")
        print("Error \n", e)



def echo_rcra_facilities_in_25_miles_radius(bbox):
    bounding_box = {
        "xmin": bbox["minLon"],
        "ymin": bbox["minLat"],
        "xmax": bbox["maxLon"],
        "ymax": bbox["maxLat"],
        "spatialReference": {"wkid": 4326}
    }

    url = "https://echogeo.epa.gov/arcgis/rest/services/ECHO/Facilities/MapServer/3/query"

    params = {
        "f": "geojson",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "geometryType": "esriGeometryEnvelope",
        "geometry": json.dumps(bounding_box),
        "spatialRel": "esriSpatialRelIntersects",
        "outSR": "4326"
    }

    try:
        response = requests.get(url, params=params, timeout=15)
    except Exception as e:
        print(f"Failed to fetch data with request failed: {e}")
        logger.error(f"Failed to fetch data with request failed: {e}")
        return None

    if response.status_code == 200:

        cur = conn.cursor()
        columns = [
            'OBJECTID', 'SOURCE_ID', 'EPA_SYSTEM', 'REGISTRY_ID', 'STATUTE', 'RCR_NAME', 'RCR_STREET',
            'RCR_CITY', 'RCR_STATE', 'RCR_STATE_DISTRICT', 'RCR_ZIP', 'RCR_COUNTY', 'RCR_EPA_REGION',
            'RCR_STATUS', 'RCR_INDIAN_CNTRY_FLG', 'RCR_TRIBAL_LAND_CODE', 'FAC_FIPS_CODE', 'FAC_LAT', 'FAC_LONG',
            'RCRA_UNIVERSE', 'RCRA_NAICS', 'FAC_SIC_CODES', 'FAC_PERCENT_MINORITY', 'FAC_POPULATION_DENSITY',
            'AIR_IDS', 'CWA_IDS', 'RCRA_IDS', 'TRI_IDS', 'SDWA_IDS', 'RCRA_CASE_IDS', 'RCRA_CURR_SNC',
            'RCRA_CURR_COMPL_STATUS', 'RCRA_QTRS_IN_SNC', 'RCRA_QTRS_IN_NC', 'RCRA_CURR_VIOLATION_TYPES',
            'RCRA_IEA_CNT', 'RCRA_FEA_CNT', 'RCRA_PENALTIES', 'FAC_TRI_REPORTER', 'FAC_TRI_ON_SITE_RELEASES',
            'RCR_FIPS_CODE', 'RCR_LAND_TYPE_CODE', 'FAC_TRI_LAND_RELEASES'
        ]

        print("RCRA Facility Data fetched successfully")
        logger.info("RCRA Facility Data fetched successfully")
        try:
            data = response.json()
            features = data.get("features", [])
            records = [feature["properties"] for feature in features]

            if not records:
                print("No facilities found in the given bounding box.")
                return None

            df = pd.DataFrame(records)

            df_rcra_clean  = df[columns].where(pd.notnull(df), None)
            # Normalize column names to lowercase with underscores
            df_rcra_clean .columns = [col.lower() for col in df_rcra_clean .columns]

            records = [tuple(row) for row in df_rcra_clean.to_numpy()]
            

            records = df_rcra_clean.to_records(index=False).tolist()

            placeholders = ', '.join(col.lower() for col in columns)

            query = f"""
                INSERT INTO rcra_facilities ({placeholders})
                VALUES %s;
                
            """


            execute_values(cur, query, records)
            conn.commit()
            cur.close()

            print(f"✅ Inserted {len(df)} rows into rcra_facilities.")
            logger.info(f"✅ Inserted {len(df)} rows into rcra_facilities.")
            return df

        except Exception as e:
            conn.rollback()
            # print(f"❌ Failed to process or insert data: {e}")
            logger.error(f"❌ Failed to process or insert data: {e}")
            return None
    else:
        print("❌ Failed to load. Status code:", response.status_code)
        print("Response:", response.text)
        logger.error("❌ Failed to load. Status code:", response.status_code)
        logger.error("Response:", response)
        return None