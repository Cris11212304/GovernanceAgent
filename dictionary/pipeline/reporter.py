"""
Reporter — Generates JSON validation reports in logs/.
"""
import json
from datetime import datetime
from pathlib import Path


def generate_report(
    file_name: str,
    dictionary_name: str,
    checks: dict,
    schema_match: bool,
    ai_agent_used: bool,
    result: str,
    output_file: str | None,
    corrections: list[str],
    ai_summary: str | None,
    logs_dir: str,
) -> str:
    """Generate a JSON report and save to logs/. Returns the report path."""
    timestamp = datetime.now().isoformat(timespec="seconds")

    report = {
        "file": file_name,
        "timestamp": timestamp,
        "dictionary": dictionary_name,
        "checks": checks,
        "schema_match": schema_match,
        "ai_agent_used": ai_agent_used,
        "result": result,
        "output_file": output_file,
        "corrections_applied": corrections,
    }

    if ai_summary:
        report["ai_agent_summary"] = ai_summary

    # Save
    logs_path = Path(logs_dir)
    logs_path.mkdir(exist_ok=True)

    safe_name = Path(file_name).stem
    report_name = f"{safe_name}_{timestamp.replace(':', '-')}.json"
    report_path = logs_path / report_name

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    return str(report_path)
