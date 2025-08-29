from database.db_utils.connection import get_connection

facility_table = \
"""
CREATE TABLE IF NOT EXISTS facilities (
    REGISTRY_ID BIGINT PRIMARY KEY,
    FAC_NAME TEXT,
    FAC_STREET TEXT,
    FAC_CITY TEXT,
    FAC_STATE VARCHAR(2),
    FAC_ZIP TEXT,
    FAC_COUNTY TEXT,
    FAC_FIPS_CODE TEXT,
    FAC_LAT DOUBLE PRECISION,
    FAC_LONG DOUBLE PRECISION,
    FAC_NAICS_CODES TEXT,
    FAC_SIC_CODES TEXT,
    AIR_FLAG CHAR(1),
    RCRA_FLAG CHAR(1),
    TRI_FLAG CHAR(1),
    SDWIS_FLAG CHAR(1),
    GHG_FLAG CHAR(1),
    AIR_IDS TEXT,
    NPDES_IDS TEXT,
    RCRA_IDS TEXT,
    TRI_IDS TEXT,
    SDWA_IDS TEXT,
    GHG_IDS TEXT
);
"""


HIFLD_landfills = \
"""
CREATE TABLE IF NOT EXISTS HIFLD_landfills (
    swid TEXT PRIMARY KEY,
    name TEXT,
    address TEXT,
    city TEXT,
    state VARCHAR(2),
    zip TEXT,
    telephone TEXT,
    country TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    naics_code TEXT,
    naics_desc TEXT,
    owner TEXT,
    permit_no TEXT,
    type TEXT
);
"""

# RCRA_DATA = \
# """
# CREATE TABLE IF NOT EXISTS rcra_compliance (
#     rcra_name TEXT,
#     source_id TEXT,
#     rcra_street TEXT,
#     rcra_city TEXT,
#     rcra_state VARCHAR(2),
#     registry_id TEXT PRIMARY KEY,
#     rcra_snc TEXT,  
#     rcra_qtrs_with_nc INTEGER,  -- Quarters with Noncompliance
#     rcra_insp_cnt INTEGER,      -- Number of inspections
#     rcra_fea_cnt INTEGER        -- Number of formal enforcement actions
# );
# """

TRI_DATA = \
"""
CREATE TABLE IF NOT EXISTS tri_facilities (
    year INTEGER,
    trifd TEXT,
    frs_id TEXT,
    facility_name TEXT,
    street_address TEXT,
    city TEXT,
    state VARCHAR(2),
    zip TEXT,
    county TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    parent_co_name TEXT,
    standard_parent_co_name TEXT,
    federal_facility TEXT,  -- Y/N
    industry_sector_code TEXT,
    industry_sector TEXT,
    primary_sic TEXT,
    primary_naics TEXT,
    chemical TEXT,
    cas_number TEXT,
    carcinogen TEXT,  -- Y/N
    pbt TEXT,         -- Y/N
    pfas TEXT,        -- Y/N
    unit_of_measure TEXT,
    fugitive_air DOUBLE PRECISION,
    stack_air DOUBLE PRECISION,
    water DOUBLE PRECISION,
    landfills DOUBLE PRECISION,
    on_site_release_total DOUBLE PRECISION,
    off_site_release_total DOUBLE PRECISION,
    off_site_recycled_total DOUBLE PRECISION,
    off_site_energy_recovery DOUBLE PRECISION,
    off_site_treated_total DOUBLE PRECISION,
    total_transfer DOUBLE PRECISION,
    total_releases DOUBLE PRECISION,
    production_waste DOUBLE PRECISION,
    naics_6 TEXT,
    naics_4 TEXT
);

"""
OSM_Enhanced_Data = \
"""
CREATE TABLE IF NOT EXISTS OSM_enhanced_data (
    facility TEXT,
    industry TEXT,
    factory_name TEXT PRIMARY KEY,
    address_osm TEXT,
    city_osm TEXT,
    zipcode_osm TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    tags_used TEXT,
    company_name TEXT,
    enhanced_company_name TEXT,
    official_website TEXT,
    revenue BIGINT,
    revenue_printed TEXT,
    hq_address TEXT,
    hq_city TEXT,
    hq_state TEXT,
    hq_postal_code TEXT,
    hq_country TEXT,
    owned_by_org JSONB,
    employee_count INTEGER
);

"""


rcra_table = """
CREATE TABLE IF NOT EXISTS rcra_facilities (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    
    
    OBJECTID BIGINT,
    SOURCE_ID TEXT,
    EPA_SYSTEM VARCHAR(10),
    REGISTRY_ID TEXT,
    STATUTE VARCHAR(50),
    RCR_NAME VARCHAR(200),
    RCR_STREET VARCHAR(200),
    RCR_CITY VARCHAR(100),
    RCR_STATE CHAR(2),
    RCR_STATE_DISTRICT VARCHAR(40),
    RCR_ZIP VARCHAR(10),
    RCR_COUNTY VARCHAR(100),
    RCR_EPA_REGION CHAR(2),
    RCR_STATUS VARCHAR(100),
    RCR_INDIAN_CNTRY_FLG CHAR(1),
    RCR_TRIBAL_LAND_CODE VARCHAR(80),

    FAC_FIPS_CODE VARCHAR(15),
    FAC_LAT DOUBLE PRECISION,
    FAC_LONG DOUBLE PRECISION,


    RCRA_UNIVERSE VARCHAR(4000),
    RCRA_NAICS TEXT,
    FAC_SIC_CODES TEXT,

    FAC_PERCENT_MINORITY DOUBLE PRECISION,
    FAC_POPULATION_DENSITY DOUBLE PRECISION,


    AIR_IDS TEXT,
    CWA_IDS TEXT,
    RCRA_IDS TEXT,
    TRI_IDS TEXT,
    SDWA_IDS TEXT,
    RCRA_CASE_IDS TEXT,

    RCRA_CURR_SNC CHAR(3),
    RCRA_CURR_COMPL_STATUS VARCHAR(50),
    RCRA_QTRS_IN_SNC SMALLINT,
    RCRA_QTRS_IN_NC SMALLINT,
    RCRA_CURR_VIOLATION_TYPES TEXT,


    RCRA_IEA_CNT INTEGER,
    RCRA_FEA_CNT INTEGER,
    RCRA_PENALTIES DOUBLE PRECISION,

    FAC_TRI_REPORTER CHAR(1),
    FAC_TRI_ON_SITE_RELEASES DOUBLE PRECISION,




    RCR_FIPS_CODE VARCHAR(5),
    RCR_LAND_TYPE_CODE VARCHAR(30),
    FAC_TRI_LAND_RELEASES DOUBLE PRECISION
    
);

"""

def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(facility_table)
    cur.execute(HIFLD_landfills)
    # cur.execute(RCRA_DATA)
    cur.execute(rcra_table)
    cur.execute(TRI_DATA)
    cur.execute(OSM_Enhanced_Data)

    conn.commit()
    cur.close()
    conn.close()