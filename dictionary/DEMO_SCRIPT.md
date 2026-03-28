# Demo Script — ETL Governance Pipeline

> This is your recording script. Text in **[BRACKETS]** are notes for you — don't read them aloud.
> Text in normal format is what you say. Adapt it to your natural speaking style.

---

## 1. Introduction

Hello, my name is Christian Alejandro Romero Ruiz, and this is my demonstration of the data ingestion assessment that I've built.

Right now we're looking at the repository. You can see the typical artifacts of any standard repository — like requirements.txt for dependencies, .env.example for configuration, and a README with documentation.

Additionally, we have several folders that serve specific functions within the pipeline. Let me walk you through each one.

---

## 2. Project Structure

### data/
First, the **data** folder. This is the entry point — where data files are initially placed before processing. It also contains the governance dictionary, which is the core of this entire solution.

### The Governance Dictionary

**[Open Dictionary_co_stg_transactions.xlsx in the data/ folder and show the sheets]**

As you can see, this is an Excel document I defined as a governance layer for the data. It has three sheets:

- The **Object** sheet defines the metadata of the expected file: what delimiter it uses, the encoding, the expected date format (yyyy-MM-dd), the number of expected columns (17), and the business key (sales_id).

- The **Fields** sheet is the column-by-column definition: each field's name, its expected data type (INT, STRING, DECIMAL, DATE), maximum length, whether it's required, and if it has a list of allowed values — for example, the `brand` column only accepts values like Baileys, Smirnoff, Tanqueray, and so on.

- The **Lists** sheet contains the valid dropdown options that the dictionary itself uses.

The key idea is: if an incoming file doesn't comply with what this dictionary says, the AI will attempt to reconcile it.

### docs/

**[Open docs/ and show agentic_diagram.png]**

Inside docs, I've stored architecture diagrams that I created to explain the system. This one in particular shows the complete validation flow — the 11 sequential checks that every file goes through, and the decision at the end: ACCEPT or REJECT.

**[You can also briefly show etl_diagram.png if you want]**

### pipeline/

This is where all the logic lives. It's a Python module with six scripts, each with a clear responsibility:

- **dictionary_loader.py** — Reads and parses the Excel governance dictionary. It extracts the Object sheet into a metadata dictionary and the Fields sheet into a list of column definitions with their types, lengths, and constraints. It's the first thing that runs — everything depends on this parsed dictionary.

- **validator.py** — Runs the 11 fixed-rule validation checks against the data. These are deterministic checks — no AI involved. It validates: that headers match the dictionary, that the column count is 17, the file format, duplicates on the business key, date formats, data types and lengths, decimal precision, mandatory fields (no nulls where is_required = YES), and catalog values (for example, that `brand` only contains values from the allowed list). Each check returns PASS or FAIL with a detail message.

- **cleaner.py** — Applies minor, automatic corrections that don't require AI. Specifically, it: trims whitespace from all string columns, normalizes date columns to the expected yyyy-MM-dd format (using pandas date parsing), and casts integer columns like sales_id, product_id, store_id, and volume_ml to their proper integer type. These are safe, deterministic fixes.

- **ai_agent.py** — This is the AI reconciliation module. It only activates when the validator detects a structural schema mismatch — like extra columns, missing columns, or incompatible data types. It uses **Claude Sonnet 4** from Anthropic. What it does is: it sends the governance dictionary and a sample of the incoming data to the model, asks it to generate a Python transformation script, and then executes that script locally to produce a corrected file. The AI decides what to do — drop extra columns, convert types, remap values — whatever is needed to make the data match the dictionary.

- **reporter.py** — Generates a JSON validation report for every execution and saves it in the logs/ folder. The report includes: the file name, timestamp, which of the 11 checks passed or failed, whether the AI agent was used, what corrections were applied, and the final result (ACCEPT or REJECT).

- **orchestrator.py** — This is the conductor that ties everything together. It runs the full pipeline in two phases. **Phase 1**: load the dictionary, load the data file, run all 11 validation checks, and if the schema matches, apply minor corrections via the cleaner. **Phase 2** (only if the schema doesn't match): call the AI agent, get a corrected file, re-run all 11 validations on the corrected data, and then decide: ACCEPT to ready/ or REJECT to quarantine/.

### processing/

This is an intermediate folder. When a file fails the schema validation and the AI agent needs to intervene, the corrected version that the AI produces is temporarily saved here. If the AI's correction passes re-validation, the final clean file moves to ready/. If not, the original goes to quarantine/.

### quarantine/

This is where files go when they definitively cannot be ingested — neither the fixed rules nor the AI could fix them.

### ready/

This is the final destination for clean files. Whether the file passed directly through fixed rules or was corrected by the AI, the final cleaned version ends up here. This is the folder that SQL Server reads from for ingestion.

### logs/

Stores a JSON report for every pipeline execution. You can see the full audit trail — which checks passed, which failed, what was corrected, and the final verdict.

---

## 3. The Five-Step Process

**[Show the steps on screen — you can have a terminal or a text file open]**

As you can see, I've defined a very simple five-step process to ingest data:

**Step 1:** Place the data file in the `data/` folder.

**Step 2:** Run the Python pipeline:
```
python run.py data/my_file.csv
```
This runs the orchestrator, which validates the file against the dictionary, applies corrections (either rule-based or AI-driven), and moves the file to ready/ or quarantine/.

**Step 3:** In SSMS, execute the stored procedure to load the cleaned file from ready/ into the raw SQL table:
```sql
EXEC raw.sp_load_raw @file_path = 'C:\...\ready\my_file_cleaned.csv';
```

**Step 4:** Execute the stored procedure to upsert from raw into the master table:
```sql
EXEC dbo.sp_upsert_transactions;
```

**Step 5:** Query the tables to confirm the data was loaded successfully.

For the AI model, specifically we are using **Claude Sonnet 4** (claude-sonnet-4-20250514) from Anthropic, via their API.

---

## 4. SQL Tables Explanation

**[Show SSMS with the two tables]**

We have two SQL tables, and they serve different purposes:

- **raw.co_stg_transactions** — This is the raw landing table. Every column is VARCHAR. No type validation happens here — it's just a direct dump of the CSV. This exists so we have a copy of exactly what arrived, before any SQL-level transformation.

- **dbo.transactions** — This is the master table with proper data types: INT for IDs, DECIMAL for prices, DATE for dates. The upsert stored procedure reads from the raw table, casts every column to its correct type, and does a MERGE: if the sales_id already exists, it updates the row; if it's new, it inserts it. This way, we can load multiple files and the master table stays deduplicated by business key.

**[Run SELECT * FROM dbo.transactions to show it's empty]**

As you can see, both tables are currently empty. Let's run the demo.

---

## 5. Demo — Good File (raw_pos_2.csv)

**[CONTEXT FOR YOU: raw_pos_2.csv has 1000 rows, 17 columns, correct schema. It should pass all 11 checks without AI intervention.]**

Now let's test with the first file, which is the "good" file — raw_pos_2.csv. This file has 1000 rows and 17 columns, all matching the expected schema. Product IDs are integers, dates are in YYYY-MM-DD format, no extra columns.

**[Run: python run.py data/raw_pos_2.csv]**

As you can see, all 11 validation checks passed. The pipeline applied some minor corrections — date normalization and integer casting — but no AI was needed. The file went directly to ready/.

**[NOTE: The file does NOT pass through processing/ in this case. It goes straight from data/ → validation → ready/]**

**[Now run the SQL load in SSMS]**

Now let me load it into SQL.

**[Run the SP, then SELECT * FROM dbo.transactions]**

And there we go — 1000 rows loaded into the master table. We can see the sales data: product names, brands like Smirnoff and Tanqueray, categories like Rum and Vodka, alcohol percentages, volumes, store information, cities, and sales amounts.

---

## 6. Demo — Bad File (raw_pos_1.csv)

**[CONTEXT FOR YOU: raw_pos_1.csv has 100 rows, 20 columns. It has 4 problems:
1. Three EXTRA columns that don't belong: email, transaction_id, payment_method
2. product_id is ALPHANUMERIC (e.g. "VQ189IVC") instead of integer
3. sales_date format is YYYY/MM/DD (slashes) instead of YYYY-MM-DD (dashes)
4. Column count is 20 instead of 17

The AI will: drop the 3 extra columns, convert product_id from alphanumeric to sequential integers (1, 2, 3...), and normalize dates from slashes to dashes.]**

Now let's test with the "bad" file — raw_pos_1.csv. This file has 100 rows but 20 columns instead of 17. It has three extra columns that don't belong in our schema: email, transaction_id, and payment_method. Additionally, the product_id field contains alphanumeric codes like "VQ189IVC" instead of integers, and the dates use slashes (2025/04/25) instead of dashes (2025-04-25).

**[Run: python run.py data/raw_pos_1.csv]**

As you can see in the output, Phase 1 detected four failures:
- validate_headers: FAIL — three extra columns found
- validate_column_count: FAIL — expected 17, got 20
- validate_dates: FAIL — 100 rows with invalid date format
- check_types_lengths: FAIL — product_id has 100 non-integer values

Since this is a structural schema mismatch, the AI agent was activated. It generated a Python transformation script and executed it locally. The corrected file was saved in processing/.

Then the pipeline re-ran all 11 checks on the AI's output — and this time, all passed. The final file moved to ready/.

What did the AI do specifically?
1. **Dropped the 3 extra columns** (email, transaction_id, payment_method) — they're not in the dictionary
2. **Converted product_id** from alphanumeric codes to sequential integers (1, 2, 3, ...) — because the dictionary says product_id must be INT
3. **Normalized dates** from YYYY/MM/DD to YYYY-MM-DD — matching the dictionary's expected format

**[Now run the SQL load in SSMS]**

Let me load this into SQL as well.

**[Run the SP, then SELECT * FROM dbo.transactions]**

And now we have 1100 rows total — the original 1000 from the good file plus 100 from the bad file that the AI corrected. The master table used MERGE with sales_id as the business key, so if there had been any duplicates, they would have been updated instead of duplicated.

---

## 7. Closing

So to summarize: this pipeline provides a governed data ingestion process where a dictionary defines the rules, 11 fixed checks validate every file, and when the schema doesn't match, an AI agent — Claude Sonnet 4 — automatically generates and executes the transformations needed to reconcile the data. Everything is auditable through JSON logs, and the final clean data is ready for SQL ingestion.

The repository is available at the GitHub link I've shared, and that concludes my demonstration. Thank you.

---

## Notes for You (Don't Read Aloud)

### Data context
This is a **liquor/beverage sales dataset**. Each row is a transaction with:
- Product info: name, brand (Smirnoff, Tanqueray, etc.), category (Vodka, Rum, Whisky...), volume in ml, alcohol percentage
- Store info: store name, city, state, region, store type (Wholesale/Retail)
- Sales info: date, quantity sold, unit price, total sales amount

### Why raw vs master?
- **Raw** = safety net. Everything as VARCHAR. If something goes wrong with type casting, you still have the original data.
- **Master** = typed, deduplicated, production-ready. The MERGE ensures idempotency — you can re-run the same file and it won't create duplicates.

### Model used
Claude Sonnet 4 (claude-sonnet-4-20250514) via the Anthropic API. It generates a pandas transformation script that gets executed locally — the model doesn't access your files directly, it generates code that your machine runs.
