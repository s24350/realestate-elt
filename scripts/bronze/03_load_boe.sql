-- scripts/bronze/03_load_boe.sql
TRUNCATE bronze.boe_raw;

COPY bronze.boe_raw
FROM '/data/boe/Bank of England  Database.csv'
WITH (
  FORMAT csv,
  HEADER true,
  DELIMITER ',',
  QUOTE '"'
);