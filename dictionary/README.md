# ETL Governance Pipeline — Technical Test

## Overview
Multi-agent ETL validation pipeline that uses OpenAI Assistants (GPT-4o + Code Interpreter) to validate incoming data files against a governance dictionary, then routes files through a SQL ETL pipeline.

## Architecture (2 Diagrams)

### Diagram 1: ETL Governance Pipeline
Orchestrated by `gov.sp_orchestrate_co_stg_transactions`

```
Landing (input file)
     |
raw.sp_import - load RAW
     |
RAW: raw.co_stg_transactions
     |
stg.sp_load - validate & stage
     |
STG: stg.co_stg_transactions
     |
dbo.sp_upsert - upsert final
     |
Final: dbo.transactions
     |
File move decision
   |-- ACCEPT -> Archive
   +-- REJECT -> Quarantine
```

### Diagram 2: Agentic Validation Chain
Dictionary-driven validation with 11 sequential checks:

| # | Check | Details |
|---|-------|---------|
| 1 | Read inputs | file + dictionary |
| 2 | Extract governance | Object & Fields sheets |
| 3 | Validate headers | Match canonical_field names from dictionary |
| 4 | Validate column count | Expected 17 |
| 5 | Validate file format | CSV / TSV |
| 6 | Check duplicates | Business key: sales_id |
| 7 | Validate dates | dictionary.date_format |
| 8 | Check types & lengths | Per fields definition |
| 9 | Check decimals spec | Precision & scale |
| 10 | Check mandatory fields | is_required = YES |
| 11 | Check catalog values | allowed_values |

Result: **ACCEPT -> Run ETL** or **REJECT -> Quarantine**

## Data Files

### `data/raw_pos_2.csv` — "Good" file (matches expected schema)
- 1000 rows, 17 columns
- `product_id`: integer (101-110)
- `sales_date`: format `YYYY-MM-DD`
- Columns: sales_id, product_id, store_id, product_name, brand, category, volume_ml, alcohol_percentage, store_name, city, state, region, store_type, sales_date, quantity_sold, unit_price, total_sales

### `data/raw_pos_1.csv` — "Bad" file (different schema — needs AI correction)
- 100 rows, **20 columns** (3 extra: email, transaction_id, payment_method)
- `product_id`: **alphanumeric** (e.g., VQ189IVC) — should be integer
- `sales_date`: format **YYYY/MM/DD** — should be YYYY-MM-DD
- sales_id range: 1001-1100

### `data/co_raw_transactions_20251023_2025.tsv.txt` — Original test file (TSV)
- 1000 rows, 17 columns, tab-separated
- `sales_date`: format **M/D/YYYY** — also non-standard

### Key Differences Between Files
| Aspect | raw_pos_2 | raw_pos_1 | old TSV |
|--------|-----------|-----------|---------|
| Column count | 17 | 20 (+3 extra) | 17 |
| product_id | integer | alphanumeric | integer |
| sales_date | YYYY-MM-DD | YYYY/MM/DD | M/D/YYYY |
| Delimiter | comma | comma | tab |

## Dictionary Files

### `Dicionary_format.xlsx` — Governance dictionary template (root)
Sheets: Lists, Object, Fields
- **Object**: input_name, destination_table, file_delimiter, encoding, has_header, expected_column_count, date_format, decimal_sep, etc.
- **Fields**: legacy_field, canonical_field, sql_data_type, length, precision, scale, is_pk, is_required, date_format, allowed_values, etc.
- Currently has sample data (3 fields). **Needs updating for the 17 POS columns.**

### `data/Dictionary_co_stg_transactions.xlsx` — Working copy in data/

## Project Structure

```
dictionary/
├── README.md                  <- This file
├── .env / .env.example        <- OpenAI API key config
├── config.py                  <- Loads .env
├── requirements.txt           <- Dependencies
├── SETUP.txt                  <- Setup instructions
├── Dicionary_format.xlsx      <- Governance dictionary (NEEDS UPDATE for POS data)
├── etl_agents.py              <- Legacy agents code (Date Agent + Column Lister)
├── run_pipeline.py            <- Legacy pipeline runner
├── data/                      <- ALL data files
|   ├── raw_pos_1.csv          <- Test file 1 (bad schema - 20 cols)
|   ├── raw_pos_2.csv          <- Test file 2 (good schema - 17 cols)
|   ├── co_raw_transactions_20251023_2025.tsv.txt  <- Original TSV
|   └── Dictionary_co_stg_transactions.xlsx
├── docs/                      <- Architecture diagrams
|   ├── etl_diagram.png
|   └── agentic_diagram.png
├── sql/                       <- SQL scripts (TO BUILD)
├── landing/                   <- File drop zone
├── work/                      <- Working directory
├── archive/                   <- Accepted files
└── quarantine/                <- Rejected files
```

## Implementation Status

### Agentic Validation Chain
- [x] [1] Read inputs — file upload + dictionary load
- [x] [7] Validate dates — Date Governance Agent
- [ ] [2] Extract governance — Object & Fields parsing
- [ ] [3] Validate headers
- [ ] [4] Validate column count (17)
- [ ] [5] Validate file format (CSV/TSV)
- [ ] [6] Check duplicates (sales_id)
- [ ] [8] Check types & lengths
- [ ] [9] Check decimals spec
- [ ] [10] Check mandatory fields
- [ ] [11] Check catalog values

### SQL ETL Pipeline
- [ ] `raw.sp_import` — Load file into raw table
- [ ] `raw.co_stg_transactions` — Raw table DDL
- [ ] `stg.sp_load` — Validate & stage
- [ ] `stg.co_stg_transactions` — Staging table DDL
- [ ] `dbo.sp_upsert` — Upsert to final
- [ ] `dbo.transactions` — Final table DDL
- [ ] `gov.sp_orchestrate_co_stg_transactions` — Orchestrator SP

## Tech Stack
- Python 3.10+
- OpenAI Assistants API (GPT-4o + Code Interpreter)
- pandas, openpyxl
- SQL Server (SSMS 18.0)
- Docker (optional)
