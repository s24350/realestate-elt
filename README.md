# ELT Project: Mortgage Lending vs. Property Prices in the UK

## Table of Contents
- [1. Analytical Goal](#1-analytical-goal)
- [2. Data Source Descriptions](#2-data-source-descriptions)
  - [2.1. Land Registry – Price Paid Data](#21-land-registry--price-paid-data)
  - [2.2. Bank of England – Lending to Individuals (Monthly)](#22-bank-of-england--lending-to-individuals-monthly)
  - [2.3. FCA / Bank of England – Mortgage Lenders & Administrators Return (MLAR)](#23-fca--bank-of-england--mortgage-lenders--administrators-return-mlar)
- [3. ETL Architecture (Medallion)](#3-etl-architecture-medallion)
- [4. Prerequisites and Setup](#4-prerequisites-and-setup)
  - [4.1. Clone the Repository](#41-clone-the-repository)
  - [4.2. Start the PostgreSQL Container](#42-start-the-postgresql-container)
  - [4.3. Install Python Dependencies (for preprocessing)](#43-install-python-dependencies-for-preprocessing)
- [5. Script Execution Order](#5-script-execution-order)
  - [5.1. Bronze Layer](#51-bronze-layer-create-tables-and-load-raw-data)
  - [5.2. Silver Layer](#52-silver-layer-cleaning-and-transformation)
  - [5.3. Gold Layer](#53-gold-layer-aggregations-and-joins)
- [6. Data Quality Risks](#6-data-quality-risks)
  - [Risk 1: Unstructured MLAR Format](#risk-1-unstructured-mlar-format-excel-with-multiple-sheets)
  - [Risk 2: Inconsistent Missing‑Value Encodings](#risk-2-inconsistent-missingvalue-encodings)
  - [Risk 3: Temporal Misalignment](#risk-3-temporal-misalignment-monthly-vs-quarterly-data)
  - [Risk 4: Different Units of Measurement](#risk-4-different-units-of-measurement)
- [7. Data Schema (Silver and Gold)](#7-data-schema-silver-and-gold)
  - [7.1. silver.boe_monthly_clean](#71-silverboe_monthly_clean)
  - [7.2. silver.land_registry_quarterly](#72-silverland_registry_quarterly)
  - [7.3. silver.land_registry_by_type](#73-silverland_registry_by_type)
  - [7.4. silver.mlar_long](#74-silvermlar_long)
  - [7.5. gold.housing_credit_summary](#75-goldhousing_credit_summary)
- [8. Summary and Future Directions](#8-summary-and-future-directions)
## 1. Analytical Goal (Problem Statement)
The aim of this project is to investigate the relationship between mortgage lending (approvals, values, purpose) and actual property transaction prices in the United Kingdom. The analysis covers the period from 1995 to early 2026 and combines three data sources:

- Actual property sales transactions (Land Registry – Price Paid Data).
- Monthly statistics on mortgage approvals (Bank of England).
- Detailed quarterly data on mortgage lenders’ portfolios, including new commitments, repayments, and impaired loans (MLAR).

The final `gold` table enables questions like: *“How did the number of transactions and average property price change compared to the number of mortgage approvals in the same quarter?”* or *“What percentage of new loans were advanced to borrowers with impaired credit, and how does this correlate with median house prices?”*

## 2. Data Source Descriptions

### 2.1. Land Registry – Price Paid Data
**Full name:** HM Land Registry Price Paid Data  
**Publisher:** HM Land Registry (UK Government)  
**Data link:** [https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads)  
**Acquisition:** On the webpage select the “Single file” download option – after extraction you obtain a file named `pp-complete.csv` (~5.3 GB). It contains all residential property sales in England and Wales from January 1995 to the present (updated monthly). Each row is a single transaction with a unique identifier, date, price, full address (including postcode), property type, and category flags. The data are published under the Open Government Licence.

### 2.2. Bank of England – Lending to Individuals (Monthly)
**Full name:** Bank of England statistical database – Lending to individuals series  
**Publisher:** Bank of England  
**Data link:** [https://www.bankofengland.co.uk/boeapps/database/tables.asp](https://www.bankofengland.co.uk/boeapps/database/tables.asp) – from the list of tables, select **A5.4** (Lending to individuals). A [direct export link](https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?Travel=NIxSUx&FromSeries=1&ToSeries=50&DAT=ALL&FNY=&CSVF=TT&html.x=98&html.y=41&C=OO4&C=OO2&C=ZZ&C=OOC&C=OO6&C=KOA&C=100&C=101&C=112&C=1CM&C=OO7&C=OO5&Filter=N) for the pre‑selected series is also available on the same page.  
**Acquisition:** Click the “Export source data to csv” button on the page. The downloaded file is named `Bank of England  Database.csv` (~42 KB). It contains monthly data from 1993 onwards (with minor gaps in the earliest years) on the number of mortgage approvals, split by institution type (monetary financial institutions – MFI and other specialist lenders) and loan purpose (house purchase, remortgage, other secured lending). All figures are counts of approved applications, seasonally adjusted. Column names include series codes (e.g. LPMB3C8, LPMB4B3), which have been preserved in the final table column names.

### 2.3. FCA / Bank of England – Mortgage Lenders & Administrators Return (MLAR)
**Full name:** Mortgage Lenders and Administrators Return – Statistics  
**Publisher:** Bank of England & Financial Conduct Authority (FCA)  
**Data link:** [https://www.bankofengland.co.uk/statistics/details/further-details-about-mortgage-lenders-and-administrators-statistics-data](https://www.bankofengland.co.uk/statistics/details/further-details-about-mortgage-lenders-and-administrators-statistics-data)  
**Acquisition:** On the page, look for the label **'view the data'** which redirects to the quarterly data. Select a quarter of interest (e.g. Q3 2025). The downloaded file is `mlar-longrun-detailed.XLSX` – an Excel workbook containing multiple worksheets, each presenting data in pivot‑table format with multi‑row headers. For this project three worksheets were used:
- **1.21** – Residential loans to individuals: Business flows (gross advances, net advances, new commitments).
- **1.32** – Residential loans to individuals: Nature of loan (impaired credit history, repayment type, drawing facility).
- **1.33** – Residential loans to individuals: Purpose of loan (house purchase, remortgage, other).

Data are quarterly, expressed in millions of pounds (for monetary amounts) or percentages (for structure).

## 3. ETL Architecture (Medallion)
The project follows the medallion pattern (bronze, silver, gold) in a pure ELT (Extract, Load, Transform) fashion. The process begins with **preprocessing** – converting the complex MLAR Excel files to plain CSV (script `mlar_parser.py`). All data (CSV) are then **loaded** into the **bronze** layer in PostgreSQL. In the **silver** layer data are cleaned: duplicates are removed, invalid placeholders (e.g. `n/a`, `-`) are turned into NULL, and the wide MLAR quarter columns are “unpivoted” into a long format, enabling easy joins. Finally, in the **gold** layer the data are aggregated to a common time key (quarter) and joined into a single fact table `housing_credit_summary`, ready for analysis. Additionally a dictionary table `gold.column_dictionary` was created containing metadata for every column (source, original label, unit, transformation), greatly improving understandability.

## 4. Prerequisites and Setup

### 4.1. Clone the Repository
```bash
git clone https://github.com/AnsealArt/realestate-elt.git
cd realestate-elt
```
### 4.2. Start the PostgreSQL Container
The project uses Docker to run the database. The root directory contains a `docker-compose.yml` file that defines a PostgreSQL 16 container and mounts the local `data/` and `scripts/` folders inside the container.

```bash
# Start the container in the background
docker compose up -d

# Verify the container is running
docker ps
```

### 4.3. Install Python Dependencies (for preprocessing)
Processing the MLAR `.xlsx` files to `.csv` requires `pandas` and `openpyxl`. Install them from `requirements.txt`:

```bash
pip install -r requirements.txt
```

Run the preprocessing script (the resulting CSV files are already included in the repository under `data/mlar/`):

```bash
python scripts/preprocessing/mlar_parser.py
```
MLAR data preprocessing is mandatory as it provide change from semi-structured `.xlsx` report to structured `.csv` file.

## 5. Script Execution Order
The following commands should be run in a console (the double slash `//` was used to avoid path conversion in **Git Bash** during project development).

### 5.1. Bronze Layer (create tables and load raw data)
```bash
# 1. Create schema and tables
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/bronze/01_create_tables.sql

# 2. Load Land Registry data (5.3 GB file – may take tens of minutes)
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/bronze/02_load_land_registry.sql

# 3. Load Bank of England data
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/bronze/03_load_boe.sql

# 4. Load MLAR data (three tables)
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/bronze/04_load_mlar.sql

# 5. Sanity checks – row counts
docker exec -it elt_postgres psql -U postgres -d elt_db -c "SELECT COUNT(*) FROM bronze.land_registry_raw;"
docker exec -it elt_postgres psql -U postgres -d elt_db -c "SELECT COUNT(*) FROM bronze.boe_raw;"
docker exec -it elt_postgres psql -U postgres -d elt_db -c "SELECT COUNT(*) FROM bronze.mlar_1_21_raw;"
```

### 5.2. Silver Layer (cleaning and transformation)
```bash
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/silver/01_clean_land_registry.sql
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/silver/02_clean_boe.sql
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/silver/03_clean_mlar.sql
```

### 5.3. Gold Layer (aggregations and joins)
```bash
# Main fact table
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/gold/01_aggregations.sql

# Dictionary table (metadata)
docker exec -it elt_postgres psql -U postgres -d elt_db -f //scripts/gold/02_create_metadata.sql
```

## 6. Data Quality Risks
Four major data quality risks were identified and addressed in the project.

### Risk 1: Unstructured MLAR Format (Excel with multiple sheets)
**Description:** The file `mlar-longrun-detailed.XLSX` contains data in pivot‑table format with multi‑row headers, merged cells, and descriptive text. Direct loading into a database is impossible.  
**Solution:** A Python preprocessing script (`mlar_parser.py`) was written to extract the three key worksheets (1.21, 1.32, 1.33), skip irrelevant rows, create sensible column names (quarters), and save the data as CSV. Additionally a mapping step (using the files in `mappings/`) preserves the full category hierarchy.

### Risk 2: Inconsistent Missing‑Value Encodings
**Description:** Missing data are represented in various ways: `n/a` (BoE), `-` (MLAR), empty strings `""` (MLAR), and even whole rows consisting entirely of `n/a` (BoE in the 1990s). Direct casting to numeric types would fail.  
**Solution:** In the bronze layer all numeric‑intended columns were defined as `TEXT`, allowing any value to be loaded. In the silver layer a `CASE WHEN value ~ '^[-]?[0-9.]+$' THEN value::numeric ELSE NULL END` construction safely converts only proper numbers, turning everything else (including `n/a`, `-`, `""`) into SQL NULL. For BoE, `NULLIF(column, 'n/a')` was used before the regex check.

The following bronze‑layer query confirms the presence of `n/a`:
```sql
SELECT DISTINCT other_spec_remortgage
FROM bronze.boe_raw
WHERE other_spec_remortgage !~ '^[-]?[0-9]+(\.[0-9]+)?$';
-- Result: one row with n/a
```
The following bronze‑layer query confirms the presence of `-`:
```sql
SELECT category, "2007Q1"
FROM bronze.mlar_1_21_raw
WHERE "2007Q1" !~ '^[-]?[0-9]+(\.[0-9]+)?$';
-- Result: three rows with -
```

### Risk 3: Temporal Misalignment (Monthly vs. Quarterly Data)
**Description:** Land Registry stores transaction dates (day precision), BOE provides monthly data, and MLAR is quarterly. To join these datasets a common time interval was needed.  
**Solution:** In the silver layer, for BOE we added `month_start` and `quarter_start` columns using `date_trunc('quarter', date)` which rounds the given date down to the first day of the quarter in which that date occurs. Land Registry transactions were aggregated to quarters (tables `land_registry_quarterly` and `by_type`). In MLAR the `quarter_start` column was generated as a date (first day of the quarter). All silver tables now share a `quarter_start` key, used in the gold layer for a `FULL OUTER JOIN`.

### Risk 4: Different Units of Measurement
**Description:** The three sources use different units:
- Land Registry: prices in pounds (£) – full transaction amount.
- BOE: approval counts (units).
- MLAR: monetary amounts in millions of pounds (£m) and percentages (%).  
**Solution:** In the gold layer, during the creation of the `housing_credit_summary` table, all MLAR money columns were multiplied by 1,000,000 to obtain consistent full pounds. Column names include suffixes `__gbp`, `__pct`, or `__count` to clearly indicate the unit. Additionally a dictionary table `gold.column_dictionary` stores metadata about original unit, transformation, and source for every column.

## 7. Data Schema (Silver and Gold)
Below are the structures of the main silver and gold tables with example values.

### 7.1. `silver.boe_monthly_clean`
| column                       | type                      | key              | example               |
|------------------------------|---------------------------|------------------|-----------------------|
| date                         | date                      | primary time key | 2026-01-31            |
| other_spec_remortgage        | numeric                   |                  | 2461                  |
| other_spec_other_lending     | numeric                   |                  | 687                   |
| total_secured_lending        | numeric                   |                  | 111963                |
| mfi_remortgage               | numeric                   |                  | 37091                 |
| mfi_other_lending            | numeric                   |                  | 13452                 |
| mfi_house_purchase           | numeric                   |                  | 60859                 |
| total_remortgage             | numeric                   |                  | 38103                 |
| total_other_lending          | numeric                   |                  | 13860                 |
| total_house_purchase         | numeric                   |                  | 59999                 |
| other_spec_house_purchase    | numeric                   |                  | 2049                  |
| mfi_total_approvals          | numeric                   |                  | 108292                |
| other_spec_total_approvals   | numeric                   |                  | 4752                  |
| month_start                  | timestamp with time zone  |                  | 2026-01-01 00:00:00+00 |
| quarter_start                | timestamp with time zone  | join key         | 2026-01-01 00:00:00+00 |
| year                         | integer                   |                  | 2026                  |
| month                        | integer                   |                  | 1                     |

### 7.2. `silver.land_registry_quarterly`
| column               | type                      | key        | example         |
|----------------------|---------------------------|------------|-----------------|
| quarter_start        | timestamp                 | join key   | 1995-01-01      |
| quarter_label        | text                      |            | 1995Q1          |
| total_transactions   | bigint                    |            | 172668          |
| avg_price            | numeric(18,2)             |            | 66496.64        |
| median_price         | double precision          |            | 53500           |
| min_price            | numeric                   |            | 1               |
| max_price            | numeric                   |            | 5610000         |

### 7.3. `silver.land_registry_by_type`
| column         | type                      | key                        | example        |
|----------------|---------------------------|----------------------------|----------------|
| quarter_start  | timestamp                 | join key                   | 1995-01-01     |
| quarter_label  | text                      |                            | 1995Q1         |
| property_type  | char(1)                   | segmentation key           | D              |
| cnt            | bigint                    |                            | 38991          |
| avg_price      | numeric(18,2)             |                            | 103130.15      |
| min_price      | numeric                   |                            | 175            |
| max_price      | numeric                   |                            | 2000000        |

### 7.4. `silver.mlar_long`
| column         | type      | key                              | example                                           |
|----------------|-----------|----------------------------------|---------------------------------------------------|
| src            | text      | dataset key                      | 1.21                                              |
| category       | text      | business dimension               | Regulated - Business flows - Gross advances       |
| quarter        | text      | original quarter label           | 2007Q1                                            |
| value          | numeric   |                                  | 73139.02                                          |
| year           | integer   | derived                          | 2007                                              |
| quarter_num    | integer   | derived                          | 1                                                 |
| quarter_start  | date      | join key                         | 2007-01-01                                        |

### 7.5. `gold.housing_credit_summary`
| column                                                   | type          | key         | example               |
|----------------------------------------------------------|---------------|-------------|-----------------------|
| quarter_start__date                                      | date          | pk / join   | 2007-01-01            |
| transactions_total__LR__count                            | bigint        |             | 172668                |
| price_avg__LR__gbp                                       | numeric(18,2) |             | 66496.64              |
| price_median__LR__gbp                                    | numeric(18,2) |             | 53500.00              |
| boe_total_secured_lending__LPMB3C8__count                | numeric       |             | 111963                |
| boe_total_remortgage__LPMB4B3__count                     | numeric       |             | 38103                 |
| boe_total_other_lending__LPMB4B4__count                  | numeric       |             | 13860                 |
| boe_house_purchase__LPMVTVX__count                       | numeric       |             | 59999                 |
| boe_mfi_total_approvals__LPMZ3UP__count                  | numeric       |             | 108292                |
| mlar_gross_advances__MLAR_1_21_C_1__gbp                   | numeric       |             | 73139020000.00        |
| mlar_net_advances__MLAR_1_21_C_2__gbp                     | numeric       |             | 61301074000.00        |
| mlar_new_commitments__MLAR_1_21_C_3__gbp                  | numeric       |             | 81996015000.00        |
| mlar_imp_repayment__MLAR_1_32_C_3__pct                    | numeric(6,3)  |             | 3.745                 |
| mlar_imp_interest_only__MLAR_1_32_C_4__pct                | numeric(6,3)  |             | 3.786                 |
| mlar_imp_combined__MLAR_1_32_C_5__pct                     | numeric(6,3)  |             | 4.016                 |
| mlar_imp_other__MLAR_1_32_C_6__pct                        | numeric(6,3)  |             | 3.599                 |
| mlar_new_house_purchase__MLAR_1_33_C_29__gbp              | numeric       |             | 40000000000.00        |
| mlar_new_remortgage__MLAR_1_33_C_30__gbp                  | numeric       |             | 30000000000.00        |
| mlar_new_other__MLAR_1_33_C_31__gbp                       | numeric       |             | 10000000000.00        |
| dq_boe_nulls_count__meta                                  | integer       |             | 2                     |
| dq_mlar_nulls_count__meta                                 | integer       |             | 1                     |
| source_available_lr__flag                                 | boolean       |             | true                  |
| source_available_boe__flag                                | boolean       |             | true                  |
| source_available_mlar__flag                               | boolean       |             | true                  |

In addition, the schema `gold` contains the table `column_dictionary` with full documentation for every column (source, original label, unit, transformation, description, and example value).

## 8. Summary and Future Directions
The project implements the medallion architecture for three diverse data sources covering the UK housing and mortgage market. Through cleaning and transformation we obtained a consistent fact table `gold.housing_credit_summary` spanning 132 quarters (from Q2 1993 to Q1 2026). Data quality indicators show that the main sources (Land Registry and BOE) are available for the vast majority of periods, while MLAR data start only in 2007. The average number of missing values in selected MLAR metrics is about 4.3 per quarter, which is acceptable. The project provides a solid foundation for further econometric analyses, such as investigating the impact of interest rates on transaction volumes or forecasting house prices based on mortgage approval data.

--- 
*Last updated: March 2026*