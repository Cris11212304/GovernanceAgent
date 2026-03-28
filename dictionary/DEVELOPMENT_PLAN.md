# DEVELOPMENT_PLAN.md — Instructions for the Agentic Model
> **WARNING: TEMPORARY FILE** — Delete after development is complete.

---

## Objective
Build an **ETL Governance Pipeline** in Python that:
1. Validates and cleans data files against a governance dictionary
2. Uses **artificial intelligence** (OpenAI) to automatically reconcile data with schemas different from expected
3. Prepares data for SQL Server ingestion

---

## Project Structure

```
C:\Users\USER\OneDrive\Documentos\GovernanceAgent\dictionary\
|
├── DEVELOPMENT_PLAN.md        <- THIS FILE (instructions)
├── README.md                  <- Project documentation
├── .env                       <- OpenAI API Key (already configured)
├── config.py                  <- Loads environment variables
├── requirements.txt           <- Dependencies (update as needed)
├── Dicionary_format.xlsx      <- Governance dictionary template (3 sample fields)
|
├── etl_agents.py              <- OLD CODE — do not reuse directly
├── run_pipeline.py            <- OLD CODE — do not reuse directly
├── SETUP.txt                  <- OLD FILE — replace at the end
|
├── data/                      <- INPUT FILES (files arrive here)
|   ├── raw_pos_1.csv          <- BAD test file (different schema)
|   ├── raw_pos_2.csv          <- GOOD test file (correct schema)
|   ├── co_raw_transactions_20251023_2025.tsv.txt  <- Original TSV
|   └── Dictionary_co_stg_transactions.xlsx         <- Dictionary with 17 columns
|
├── docs/                      <- Architecture diagrams (visual reference)
|   ├── etl_diagram.png        <- ETL Pipeline (Landing -> Raw -> Stg -> Final)
|   └── agentic_diagram.png    <- 11-check validation chain
|
├── processing/                <- CREATE — Files being cleaned/transformed
├── ready/                     <- CREATE — Clean files ready for SQL ingestion
├── archive/                   <- Processed files (success)
├── quarantine/                <- Rejected files (could not be corrected)
├── logs/                      <- CREATE — Validation reports
└── sql/                       <- SQL scripts (low priority, created later)
```

---

## The Governance Dictionary

**Key file:** `data/Dictionary_co_stg_transactions.xlsx`

It has 3 sheets:

### Sheet `Object` (object/table metadata)
| Field | Value |
|-------|-------|
| input_name | co_stg_transactions |
| destination_table | dbo.transactions |
| file_delimiter | , (comma) |
| encoding | UTF-8 |
| has_header | YES |
| expected_column_count | **17** |
| date_format | **yyyy-MM-dd** |
| decimal_sep | . |
| thousand_sep | , |
| null_token | NULL |
| format | tsv |
| time_zone | America/Bogota |
| invalid_rows_tolerance_pct | 10 |
| business_key | **sales_id** |

### Sheet `Fields` (definition of 17 columns)
| # | legacy_field | canonical_field | sql_data_type | length | is_pk | is_required | allowed_values |
|---|---|---|---|---|---|---|---|
| 1 | sales_id | sales_id | INT | 4 | YES | YES | — |
| 2 | product_id | product_id | INT | 3 | NO | YES | — |
| 3 | store_id | store_id | INT | 3 | NO | YES | — |
| 4 | product_name | product_name | STRING | 29 | NO | NO | — |
| 5 | brand | brand_type | STRING | 9 | NO | NO | Baileys, Captain, Ciroc, Don, Guinness, Johnnie, Smirnoff, Tanqueray |
| 6 | category | category_type | STRING | 7 | NO | NO | Beer, Gin, Liqueur, Rum, Tequila, Vodka, Whisky |
| 7 | volume_ml | volume_ml_number | INT | 4 | NO | NO | — |
| 8 | alcohol_percentage | alcohol_percentage_number | DECIMAL | 5 (2 dec, 2 int) | NO | NO | — |
| 9 | store_name | store_name | STRING | 34 | NO | NO | — |
| 10 | city | city_name | STRING | 24 | NO | NO | — |
| 11 | state | state_name | STRING | 2 | NO | NO | — |
| 12 | region | region_name | STRING | 9 | NO | NO | — |
| 13 | store_type | store_type | STRING | 9 | NO | NO | Wholesale, Retail |
| 14 | sales_date | sales_date | DATE | 10 | NO | YES | format: yyyy-MM-dd |
| 15 | quantity_sold | quantity_sold_number | DECIMAL | 2 (2 dec, 0 int) | NO | YES | — |
| 16 | unit_price | unit_price_number | DECIMAL | 5 (2 dec, 2 int) | NO | YES | — |
| 17 | total_sales | total_sales_number | DECIMAL | 7 (4 dec, 2 int) | NO | YES | — |

### Sheet `Lists` (valid values for dictionary dropdowns)
Contains valid options for delimiters, encodings, data types, file formats, etc.

---

## Execution Flow (what needs to be built)

### Entry point: `run.py` (CREATE)
```
python run.py data/raw_pos_2.csv
```
or
```
python run.py data/raw_pos_1.csv
```

The script must:
1. Receive the file path as an argument
2. Load the dictionary (`data/Dictionary_co_stg_transactions.xlsx`)
3. Execute the validation pipeline
4. Generate a report in `logs/`
5. Move the file to `ready/`, `archive/` or `quarantine/` based on result

### Orchestrator logical flow:

```
File arrives at data/
        |
        v
[PHASE 1] Validation with fixed rules (no AI)
        |
        |-- Check 1: Read file + dictionary
        |-- Check 2: Extract rules from Object & Fields
        |-- Check 3: Validate headers (do they match legacy_field from dictionary?)
        |-- Check 4: Validate column count (are there 17?)
        |-- Check 5: Validate file format (CSV/TSV)
        |-- Check 6: Find duplicates by business_key (sales_id)
        |-- Check 7: Validate date format (yyyy-MM-dd)
        |-- Check 8: Validate data types and lengths
        |-- Check 9: Validate decimal specification
        |-- Check 10: Validate mandatory fields (is_required = YES)
        |-- Check 11: Validate catalog values (allowed_values)
        |
        v
[DECISION] Does the schema match the dictionary?
        |
        |-- YES -> Apply minor corrections (dates, types)
        |         -> Move to ready/ -> Ready for SQL ingestion
        |
        +-- NO -> Schema is DIFFERENT (extra columns, incompatible types, etc.)
                  |
                  v
          [PHASE 2] AI Agent (OpenAI)
                  |
                  |-- Send to model: dictionary + incoming data
                  |-- Model analyzes differences
                  |-- Generates dynamic transformations:
                  |     - Remove extra columns (email, transaction_id, payment_method)
                  |     - Convert alphanumeric product_id to numeric (mapping or regeneration)
                  |     - Normalize date format (YYYY/MM/DD -> yyyy-MM-dd)
                  |     - Any other necessary adjustments
                  |-- Applies transformations
                  |-- Re-validates with fixed rules
                  |
                  v
          [DECISION] Could it be corrected?
                  |
                  |-- YES -> Move to ready/
                  +-- NO -> Move to quarantine/
```

---

## Validation Report (logs/)

Generate a JSON file per execution in `logs/`. Example:

```json
{
  "file": "raw_pos_2.csv",
  "timestamp": "2026-03-28T10:30:00",
  "dictionary": "Dictionary_co_stg_transactions.xlsx",
  "checks": {
    "read_inputs": {"status": "PASS", "detail": "File loaded: 1000 rows, 17 columns"},
    "extract_governance": {"status": "PASS", "detail": "Object + Fields parsed"},
    "validate_headers": {"status": "PASS", "detail": "All 17 headers match"},
    "validate_column_count": {"status": "PASS", "detail": "Expected 17, got 17"},
    "validate_file_format": {"status": "PASS", "detail": "CSV format detected"},
    "check_duplicates": {"status": "PASS", "detail": "0 duplicates on sales_id"},
    "validate_dates": {"status": "FAIL", "detail": "sales_date: 23 rows with format M/D/YYYY instead of yyyy-MM-dd"},
    "check_types_lengths": {"status": "PASS", "detail": "All types match"},
    "check_decimals": {"status": "PASS", "detail": "Precision OK"},
    "check_mandatory": {"status": "PASS", "detail": "No nulls in required fields"},
    "check_catalog_values": {"status": "PASS", "detail": "All values in allowed lists"}
  },
  "schema_match": true,
  "ai_agent_used": false,
  "result": "ACCEPT",
  "output_file": "ready/raw_pos_2_cleaned.csv",
  "corrections_applied": ["date format normalized: M/D/YYYY -> yyyy-MM-dd"]
}
```

---

## The Two Test Files — What to Expect

### `raw_pos_2.csv` — "Good" file (correct schema)
- **1000 rows**, 17 columns
- `sales_id`: 1-1000 (INT, no duplicates)
- `product_id`: numeric (101-110)
- `sales_date`: format `YYYY-MM-DD` (matches dictionary)
- Delimiter: comma
- **Expected:** Passes all 11 validations -> ACCEPT -> `ready/`

### `raw_pos_1.csv` — "Bad" file (different schema)
- **100 rows**, **20 columns** (3 extra)
- `sales_id`: 1001-1100
- `product_id`: **ALPHANUMERIC** (e.g.: `VQ189IVC`) — should be INT
- `sales_date`: format **YYYY/MM/DD** — should be yyyy-MM-dd
- **3 extra columns**: `email`, `transaction_id`, `payment_method` — not in dictionary
- Delimiter: comma
- **Expected:** Fails validation -> schema mismatch -> **activates AI Agent** -> AI adjusts -> re-validates -> ACCEPT -> `ready/`

### `co_raw_transactions_20251023_2025.tsv.txt` — Original TSV file
- 1000 rows, 17 columns, tab-separated
- `sales_date`: format **M/D/YYYY**
- Can be used as a third test case

---

## AI Agent — Specification

### When it activates
The AI agent activates when **Phase 1** detects schema incompatibilities that cannot be resolved with simple fixed rules:
- Column count different from expected
- Extra or missing columns
- Incompatible data types (e.g.: alphanumeric where INT is expected)
- Any structural discrepancy

**Does NOT activate** for simple corrections like:
- Normalizing date format (fixed rules handle this)
- Trimming whitespace
- Null values in non-required fields

### What the agent does
1. Receives as context: the **dictionary** (Object + Fields) and a **sample of incoming data**
2. Analyzes differences between expected schema and actual schema
3. Generates a **transformation plan** (which columns to drop, which types to convert, which mappings to apply)
4. Executes transformations on the data
5. Returns transformed data for re-validation

### Technology
- **OpenAI API** (already configured in `.env`)
- Model: `gpt-4o` with Code Interpreter (OpenAI Assistants API)
- The key is already in `.env` — use `config.py` to load it

### Example prompt for the agent
```
You are a data governance agent. Your task is to transform a dataset
to match a target schema defined in a dictionary.

EXPECTED SCHEMA (from dictionary):
- 17 columns: [list of columns with types]
- Date format: yyyy-MM-dd
- product_id must be INT
- business_key: sales_id

INCOMING DATA:
- 20 columns detected
- Extra columns: email, transaction_id, payment_method
- product_id is alphanumeric
- Date format: YYYY/MM/DD

GENERATE a Python script that transforms the data to match the schema.
```

---

## Suggested Code Structure

```
dictionary/
├── run.py                     <- Main entry point
├── config.py                  <- Configuration (already exists)
├── pipeline/                  <- CREATE — Pipeline module
|   ├── __init__.py
|   ├── orchestrator.py        <- Main orchestrator
|   ├── validator.py           <- 11 fixed-rule validations
|   ├── ai_agent.py            <- AI agent for schema reconciliation
|   ├── cleaner.py             <- Cleaning and transformations
|   ├── dictionary_loader.py   <- Dictionary Excel loading and parsing
|   └── reporter.py            <- Report generation in logs/
└── requirements.txt           <- Update with necessary dependencies
```

### Required dependencies (update `requirements.txt`):
```
openai>=1.0.0
python-dotenv>=1.0.0
pandas>=2.0.0
openpyxl>=3.0.0
```

---

## Important Notes

1. **Do not reuse `etl_agents.py` or `run_pipeline.py` directly** — they are earlier prototypes. Take ideas but rewrite cleanly.

2. **The master dictionary is `data/Dictionary_co_stg_transactions.xlsx`** — it has 17 columns with all their attributes. The `Dicionary_format.xlsx` at root is an example template with only 3 fields.

3. **The OpenAI API Key is already configured** in `.env` and loaded via `config.py`.

4. **The virtual environment exists** at `../myenv/` but you may need to install additional dependencies.

5. **Delivery priority:**
   - HIGH: Python validation + cleaning pipeline (fixed rules)
   - HIGH: AI agent for schema reconciliation
   - MEDIUM: Orchestrator integrating both
   - LOW: Validation report in logs/
   - DEFERRED: SQL scripts (will be done separately later)

6. **Test with both files:**
   - `python run.py data/raw_pos_2.csv` -> should pass without AI
   - `python run.py data/raw_pos_1.csv` -> should activate the AI agent

---

## Success Criteria

- [ ] `raw_pos_2.csv` passes all 11 validations and ends up in `ready/`
- [ ] `raw_pos_1.csv` fails validation, AI agent corrects it, and it ends up in `ready/`
- [ ] A JSON report is generated in `logs/` for each execution
- [ ] Code is clean, modular, and explainable in 15 minutes
- [ ] Processed files are moved correctly between folders
