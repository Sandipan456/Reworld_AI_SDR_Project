CREATE TABLE IF NOT EXISTS frs_master_data AS
SELECT 
    f.*, 
    NULL::INTEGER AS landfill_count_within_20km,
    NULL::JSONB   AS nearby_landfills_json
FROM frs_master f
WHERE false;

ALTER TABLE frs_master_data ADD PRIMARY KEY (registry_id);
