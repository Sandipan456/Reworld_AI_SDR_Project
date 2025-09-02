DROP TABLE IF EXISTS frs_master;

CREATE TABLE frs_master AS
SELECT DISTINCT ON (f.registry_id) 
    f.*, 
    tf.trifd, tf.primary_naics, tf.chemical, tf.on_site_release_total, tf.parent_co_name,
    tf.standard_parent_co_name, tf.industry_sector, tf.unit_of_measure, tf.off_site_release_total, 
    tf.off_site_treated_total, tf.production_waste, tf.total_releases,
    rf.registry_id AS rcra_registry_id, rf.rcr_status, rf.rcr_name,
    rf.rcr_city, rf.rcr_state, rf.rcr_zip, rf.fac_lat AS rcra_fac_lat,
    rf.fac_long AS rcra_fac_long, rf.rcra_universe, rf.rcra_naics
FROM facilities f
LEFT JOIN tri_facilities tf ON f.tri_ids = tf.trifd
LEFT JOIN rcra_facilities rf ON f.registry_id::text = rf.registry_id::text
ORDER BY f.registry_id, tf.trifd, rf.registry_id;
