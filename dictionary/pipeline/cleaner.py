"""
Cleaner — Applies minor, rule-based corrections to data.
Handles date normalization, whitespace trimming, and type casting.
Does NOT handle schema mismatches (that's the AI agent's job).
"""
import pandas as pd
from datetime import datetime


def apply_corrections(df: pd.DataFrame, governance: dict) -> tuple[pd.DataFrame, list[str]]:
    """Apply minor corrections and return (cleaned_df, list_of_corrections)."""
    obj = governance["object"]
    fields = governance["fields"]
    corrections = []

    df = df.copy()

    # Trim whitespace on all string columns
    for col in df.select_dtypes(include="object").columns:
        before = df[col].copy()
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", pd.NA)
        if not before.equals(df[col]):
            corrections.append(f"trimmed whitespace in {col}")

    # Normalize dates
    date_corrections = _fix_dates(df, obj, fields)
    corrections.extend(date_corrections)

    # Cast INT columns
    for f in fields:
        col = f["legacy_field"]
        if col not in df.columns:
            continue
        if f.get("sql_data_type") == "INT":
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
                corrections.append(f"cast {col} to integer")
            except (ValueError, TypeError):
                pass

    return df, corrections


def _fix_dates(df: pd.DataFrame, obj: dict, fields: list[dict]) -> list[str]:
    """Attempt to normalize date columns to the expected format."""
    target_java = obj.get("date_format", "yyyy-MM-dd")
    target_py = _java_to_python(target_java)
    corrections = []

    date_fields = [f["legacy_field"] for f in fields
                   if f.get("sql_data_type") in ("DATE", "TIMESTAMP")
                   and f["legacy_field"] in df.columns]

    for col in date_fields:
        converted = pd.to_datetime(df[col], errors="coerce", format="mixed")
        if converted.isna().all():
            continue
        df[col] = converted.dt.strftime(target_py)
        df[col] = df[col].replace("NaT", pd.NA)
        corrections.append(f"date format normalized in {col} -> {target_java}")

    return corrections


def _java_to_python(java_fmt: str) -> str:
    mapping = {
        "yyyy": "%Y",
        "MM": "%m",
        "dd": "%d",
        "HH": "%H",
        "mm": "%M",
        "ss": "%S",
    }
    result = java_fmt
    for java, py in mapping.items():
        result = result.replace(java, py)
    return result
