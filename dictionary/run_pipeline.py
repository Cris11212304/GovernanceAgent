"""
Simple runner script for the ETL multi-agent pipeline
"""
from pathlib import Path
from etl_agents import run_etl_pipeline

# Paths
WORK_DIR = Path(__file__).parent / "work"
DICTIONARY_PATH = Path(__file__).parent / "Dictionary_co_stg_transactions.xlsx"

# Find first data file in work/
data_files = list(WORK_DIR.glob("*.tsv")) + list(WORK_DIR.glob("*.csv"))

if not data_files:
    print("❌ No data files found in work/ directory")
    print(f"   Please place a TSV or CSV file in: {WORK_DIR}")
    exit(1)

DATA_FILE = data_files[0]

print(f"📖 Dictionary: {DICTIONARY_PATH.name}")
print(f"📄 Data file: {DATA_FILE.name}")
print()

# Run the pipeline
success = run_etl_pipeline(
    dictionary_path=str(DICTIONARY_PATH),
    data_file_path=str(DATA_FILE),
    work_dir=str(WORK_DIR)
)

if success:
    print("\n✅ Pipeline completed successfully!")
else:
    print("\n❌ Pipeline failed - check logs above")
