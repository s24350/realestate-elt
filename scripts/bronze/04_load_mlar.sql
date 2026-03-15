-- scripts/bronze/04_load_mlar.sql
TRUNCATE bronze.mlar_1_21_raw;
COPY bronze.mlar_1_21_raw
FROM '/data/mlar/mlar_1_21.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

TRUNCATE bronze.mlar_1_32_raw;
COPY bronze.mlar_1_32_raw
FROM '/data/mlar/mlar_1_32.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

TRUNCATE bronze.mlar_1_33_raw;
COPY bronze.mlar_1_33_raw
FROM '/data/mlar/mlar_1_33.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');