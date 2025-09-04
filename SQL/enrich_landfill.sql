UPDATE frs_master_data d
SET 
    landfill_count_within_20km = l.landfill_count,
    nearby_landfills_json = l.nearby_json
FROM (
    SELECT 
        f.registry_id,
        COUNT(h.swid) AS landfill_count,
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'swid', h.swid,
                'name', h.name,
                'address', h.address,
                'city', h.city,
                'state', h.state,
                'zip', h.zip,
                'telephone', h.telephone,
                'country', h.country,
                'latitude', h.latitude,
                'longitude', h.longitude,
                'naics_code', h.naics_code,
                'naics_desc', h.naics_desc,
                'owner', h.owner,
                'permit_no', h.permit_no,
                'type', h.type
            )
        ) FILTER (WHERE h.swid IS NOT NULL) AS nearby_json
    FROM frs_master_data f
    LEFT JOIN HIFLD_landfills h
      ON (
          6371 * acos(
              cos(radians(f.fac_lat)) * cos(radians(h.latitude)) *
              cos(radians(h.longitude) - radians(f.fac_long)) +
              sin(radians(f.fac_lat)) * sin(radians(h.latitude))
          )
      ) <= 20
    GROUP BY f.registry_id
) l
WHERE d.registry_id = l.registry_id;
