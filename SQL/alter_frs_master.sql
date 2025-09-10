-- Now add the PRIMARY KEY constraint on registry_id
ALTER TABLE frs_master
ADD CONSTRAINT frs_master_pk PRIMARY KEY (registry_id);