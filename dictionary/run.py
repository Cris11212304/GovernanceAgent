"""
Entry point for the ETL Governance Pipeline.

Usage:
    python run.py data/raw_pos_2.csv
    python run.py data/raw_pos_1.csv
"""
import sys
from pathlib import Path

# Add project root to path
PROJECT_DIR = Path(__file__).parent
sys.path.insert(0, str(PROJECT_DIR))

from pipeline.orchestrator import run_pipeline
import config


def main():
    if len(sys.argv) < 2:
        print("Usage: python run.py <data_file>")
        print("Example: python run.py data/raw_pos_2.csv")
        sys.exit(1)

    data_file = Path(sys.argv[1])

    # Resolve relative paths from project dir
    if not data_file.is_absolute():
        data_file = PROJECT_DIR / data_file

    if not data_file.exists():
        print(f"Error: File not found: {data_file}")
        sys.exit(1)

    # Default dictionary path
    dictionary_file = PROJECT_DIR / "data" / "Dictionary_co_stg_transactions.xlsx"
    if not dictionary_file.exists():
        print(f"Error: Dictionary not found: {dictionary_file}")
        sys.exit(1)

    # Resolve API key based on provider
    api_key = config.API_KEY

    result = run_pipeline(
        data_file=str(data_file),
        dictionary_file=str(dictionary_file),
        api_key=api_key,
        ai_provider=config.AI_PROVIDER,
        base_dir=str(PROJECT_DIR),
    )

    sys.exit(0 if result["result"] == "ACCEPT" else 1)


if __name__ == "__main__":
    main()
