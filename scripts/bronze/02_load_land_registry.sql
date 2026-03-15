COPY bronze.land_registry_raw
FROM '/data/land_registry/pp-complete.csv'
DELIMITER ','
CSV
QUOTE '"';