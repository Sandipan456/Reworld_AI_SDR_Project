

INSERT INTO frs_master_data (
    fac_long, fac_lat, registry_id, on_site_release_total, off_site_release_total,
    off_site_treated_total, rcra_fac_lat, rcra_fac_long, production_waste, total_releases,
    tri_flag, sdwis_flag, ghg_flag, air_ids, npdes_ids, rcra_ids, tri_ids, sdwa_ids, ghg_ids, 
    trifd, primary_naics, chemical, parent_co_name, standard_parent_co_name,
    unit_of_measure, rcra_registry_id, rcr_status, rcr_name, rcr_city, rcr_state, rcr_zip,
    rcra_universe, rcra_naics, industry_sector, fac_name, fac_street, fac_city, fac_state,
    fac_zip, fac_county, fac_fips_code, fac_naics_codes, fac_sic_codes, air_flag, rcra_flag,
    landfill_count_within_20km, nearby_landfills_json
)
SELECT 
    fac_long, fac_lat, registry_id, on_site_release_total, off_site_release_total,
    off_site_treated_total, rcra_fac_lat, rcra_fac_long, production_waste, total_releases,
    tri_flag, sdwis_flag, ghg_flag, air_ids, npdes_ids, rcra_ids, tri_ids, sdwa_ids, ghg_ids,
    trifd, primary_naics, chemical, parent_co_name, standard_parent_co_name,
    unit_of_measure, rcra_registry_id, rcr_status, rcr_name, rcr_city, rcr_state, rcr_zip,
    rcra_universe, rcra_naics, industry_sector, fac_name, fac_street, fac_city, fac_state,
    fac_zip, fac_county, fac_fips_code, fac_naics_codes, fac_sic_codes, air_flag, rcra_flag,
    NULL::INTEGER AS landfill_count_within_20km,  -- Set to NULL initially
    NULL::JSONB AS nearby_landfills_json          -- Set to NULL initially
FROM frs_master
ON CONFLICT (registry_id) DO UPDATE SET
    fac_long = EXCLUDED.fac_long,
    fac_lat = EXCLUDED.fac_lat,
    on_site_release_total = EXCLUDED.on_site_release_total,
    off_site_release_total = EXCLUDED.off_site_release_total,
    off_site_treated_total = EXCLUDED.off_site_treated_total,
    rcra_fac_lat = EXCLUDED.rcra_fac_lat,
    rcra_fac_long = EXCLUDED.rcra_fac_long,
    production_waste = EXCLUDED.production_waste,
    total_releases = EXCLUDED.total_releases,
    tri_flag = EXCLUDED.tri_flag,
    sdwis_flag = EXCLUDED.sdwis_flag,
    ghg_flag = EXCLUDED.ghg_flag,
    air_ids = EXCLUDED.air_ids,
    npdes_ids = EXCLUDED.npdes_ids,
    rcra_ids = EXCLUDED.rcra_ids,
    tri_ids = EXCLUDED.tri_ids,
    sdwa_ids = EXCLUDED.sdwa_ids,
    ghg_ids = EXCLUDED.ghg_ids,
    trifd = EXCLUDED.trifd,
    primary_naics = EXCLUDED.primary_naics,
    chemical = EXCLUDED.chemical,
    parent_co_name = EXCLUDED.parent_co_name,
    standard_parent_co_name = EXCLUDED.standard_parent_co_name,
    unit_of_measure = EXCLUDED.unit_of_measure,
    rcra_registry_id = EXCLUDED.rcra_registry_id,
    rcr_status = EXCLUDED.rcr_status,
    rcr_name = EXCLUDED.rcr_name,
    rcr_city = EXCLUDED.rcr_city,
    rcr_state = EXCLUDED.rcr_state,
    rcr_zip = EXCLUDED.rcr_zip,
    rcra_universe = EXCLUDED.rcra_universe,
    rcra_naics = EXCLUDED.rcra_naics,
    industry_sector = EXCLUDED.industry_sector,
    fac_name = EXCLUDED.fac_name,
    fac_street = EXCLUDED.fac_street,
    fac_city = EXCLUDED.fac_city,
    fac_state = EXCLUDED.fac_state,
    fac_zip = EXCLUDED.fac_zip,
    fac_county = EXCLUDED.fac_county,
    fac_fips_code = EXCLUDED.fac_fips_code,
    fac_naics_codes = EXCLUDED.fac_naics_codes,
    fac_sic_codes = EXCLUDED.fac_sic_codes,
    air_flag = EXCLUDED.air_flag,
    rcra_flag = EXCLUDED.rcra_flag;



