"""
Orchestrator — Ties the full pipeline together.

Phase 1: Fixed-rule validation + minor cleaning
Phase 2: AI agent for schema reconciliation (only if needed)
"""
import shutil
from pathlib import Path

import pandas as pd

from pipeline.dictionary_loader import load_dictionary
from pipeline.validator import run_all_checks, has_schema_mismatch, all_passed
from pipeline.cleaner import apply_corrections
from pipeline.ai_agent import reconcile_schema
from pipeline.reporter import generate_report


def run_pipeline(
    data_file: str,
    dictionary_file: str,
    api_key: str,
    ai_provider: str,
    base_dir: str,
) -> dict:
    """
    Execute the full ETL governance pipeline.

    Args:
        data_file: Path to the incoming data file
        dictionary_file: Path to the governance dictionary Excel
        api_key: OpenAI API key
        base_dir: Project base directory (dictionary/)

    Returns:
        dict with pipeline result summary
    """
    base = Path(base_dir)
    ready_dir = base / "ready"
    quarantine_dir = base / "quarantine"
    processing_dir = base / "processing"
    logs_dir = base / "logs"

    # Ensure directories exist
    for d in [ready_dir, quarantine_dir, processing_dir, logs_dir]:
        d.mkdir(exist_ok=True)

    file_name = Path(data_file).name
    dict_name = Path(dictionary_file).name

    print("=" * 60)
    print("ETL GOVERNANCE PIPELINE")
    print("=" * 60)
    print(f"  File:       {file_name}")
    print(f"  Dictionary: {dict_name}")

    # --- Step 1: Load dictionary ---
    print("\n[1/6] Loading governance dictionary...")
    governance = load_dictionary(dictionary_file)
    obj = governance["object"]
    fields = governance["fields"]
    print(f"  Object: {obj.get('input_name')} -> {obj.get('destination_table')}")
    print(f"  Fields: {len(fields)} columns defined")

    # --- Step 2: Load data file ---
    print("\n[2/6] Loading data file...")
    delimiter = obj.get("file_delimiter", ",")
    df = pd.read_csv(data_file, delimiter=delimiter, dtype=str, encoding=obj.get("encoding", "utf-8"))
    print(f"  Loaded: {len(df)} rows, {len(df.columns)} columns")

    # --- Step 3: Run Phase 1 validation ---
    print("\n[3/6] Running Phase 1 — Fixed-rule validation (11 checks)...")
    results = run_all_checks(df, governance)

    for check_name, result in results.items():
        status_icon = "PASS" if result["status"] == "PASS" else "FAIL"
        print(f"  [{status_icon}] {check_name}: {result['detail']}")

    corrections = []
    ai_used = False
    ai_summary = None
    schema_ok = not has_schema_mismatch(results)

    if schema_ok:
        # --- Step 4a: Minor corrections only ---
        print("\n[4/6] Schema matches — applying minor corrections...")
        df, corrections = apply_corrections(df, governance)
        if corrections:
            for c in corrections:
                print(f"  - {c}")
        else:
            print("  No corrections needed")

        # Re-validate after corrections
        results = run_all_checks(df, governance)
        final_pass = all_passed(results)
    else:
        # --- Step 4b: Schema mismatch — call AI agent ---
        print("\n[4/6] Schema mismatch detected — activating AI Agent...")
        ai_used = True

        corrected_path, ai_summary = reconcile_schema(
            data_file_path=data_file,
            dictionary_path=dictionary_file,
            governance=governance,
            validation_results=results,
            output_dir=str(processing_dir),
            api_key=api_key,
            provider=ai_provider,
        )

        print(f"  AI Agent response: {ai_summary[:200]}..." if ai_summary and len(ai_summary) > 200 else f"  AI Agent response: {ai_summary}")

        if corrected_path:
            print(f"  Corrected file: {corrected_path}")
            # Load corrected file and re-validate
            df = pd.read_csv(corrected_path, dtype=str)
            print(f"  Re-loaded: {len(df)} rows, {len(df.columns)} columns")

            # Apply minor corrections on AI output too
            df, corrections = apply_corrections(df, governance)

            print("\n[5/6] Re-validating AI-corrected file...")
            results = run_all_checks(df, governance)
            for check_name, result in results.items():
                status_icon = "PASS" if result["status"] == "PASS" else "FAIL"
                print(f"  [{status_icon}] {check_name}: {result['detail']}")

            final_pass = all_passed(results)
        else:
            print("  AI Agent could not produce a corrected file")
            final_pass = False

    # --- Step 5: Route file ---
    print("\n[5/6] Routing file...")
    if final_pass:
        result_status = "ACCEPT"
        output_name = Path(file_name).stem + "_cleaned.csv"
        output_path = ready_dir / output_name
        df.to_csv(output_path, index=False)
        print(f"  ACCEPT -> {output_path}")
    else:
        result_status = "REJECT"
        output_path = quarantine_dir / file_name
        shutil.copy2(data_file, output_path)
        print(f"  REJECT -> {output_path}")

    # --- Step 6: Generate report ---
    print("\n[6/6] Generating validation report...")
    report_path = generate_report(
        file_name=file_name,
        dictionary_name=dict_name,
        checks=results,
        schema_match=schema_ok,
        ai_agent_used=ai_used,
        result=result_status,
        output_file=str(output_path) if final_pass else None,
        corrections=corrections,
        ai_summary=ai_summary,
        logs_dir=str(logs_dir),
    )
    print(f"  Report: {report_path}")

    print("\n" + "=" * 60)
    print(f"PIPELINE RESULT: {result_status}")
    print("=" * 60)

    return {
        "result": result_status,
        "file": file_name,
        "output": str(output_path) if final_pass else None,
        "report": report_path,
        "ai_used": ai_used,
    }
