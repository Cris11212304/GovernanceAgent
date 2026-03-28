"""
Validator — 11 fixed-rule validation checks against the governance dictionary.
Each check returns a dict with {"status": "PASS"|"FAIL", "detail": "..."}.
"""
import pandas as pd
import re
from datetime import datetime


def run_all_checks(df: pd.DataFrame, governance: dict) -> dict:
    """Run all 11 validation checks and return results dict."""
    obj = governance["object"]
    fields = governance["fields"]

    results = {}
    results["read_inputs"] = check_read_inputs(df, fields)
    results["extract_governance"] = check_extract_governance(obj, fields)
    results["validate_headers"] = check_headers(df, fields)
    results["validate_column_count"] = check_column_count(df, obj)
    results["validate_file_format"] = check_file_format(obj)
    results["check_duplicates"] = check_duplicates(df, obj)
    results["validate_dates"] = check_dates(df, obj, fields)
    results["check_types_lengths"] = check_types_lengths(df, fields)
    results["check_decimals"] = check_decimals(df, fields)
    results["check_mandatory"] = check_mandatory(df, fields)
    results["check_catalog_values"] = check_catalog_values(df, fields)

    return results


def has_schema_mismatch(results: dict) -> bool:
    """Determine if failures indicate a structural schema mismatch (needs AI)
    vs minor fixable issues (dates, trimming)."""
    schema_checks = [
        "validate_headers",
        "validate_column_count",
        "check_types_lengths",
    ]
    for key in schema_checks:
        if results.get(key, {}).get("status") == "FAIL":
            return True
    return False


def all_passed(results: dict) -> bool:
    """Return True if every check passed."""
    return all(r["status"] == "PASS" for r in results.values())


# --- Check 1: Read inputs ---
def check_read_inputs(df: pd.DataFrame, fields: list[dict]) -> dict:
    rows, cols = df.shape
    return {
        "status": "PASS",
        "detail": f"File loaded: {rows} rows, {cols} columns",
    }


# --- Check 2: Extract governance ---
def check_extract_governance(obj: dict, fields: list[dict]) -> dict:
    if not obj or not fields:
        return {"status": "FAIL", "detail": "Could not parse Object or Fields"}
    return {"status": "PASS", "detail": "Object + Fields parsed"}


# --- Check 3: Validate headers ---
def check_headers(df: pd.DataFrame, fields: list[dict]) -> dict:
    expected = [f["legacy_field"] for f in fields]
    actual = list(df.columns)

    missing = [h for h in expected if h not in actual]
    extra = [h for h in actual if h not in expected]

    if not missing and not extra:
        return {
            "status": "PASS",
            "detail": f"All {len(expected)} headers match",
        }

    parts = []
    if missing:
        parts.append(f"missing: {missing}")
    if extra:
        parts.append(f"extra: {extra}")
    return {"status": "FAIL", "detail": "; ".join(parts)}


# --- Check 4: Validate column count ---
def check_column_count(df: pd.DataFrame, obj: dict) -> dict:
    expected = obj.get("expected_column_count", 0)
    actual = len(df.columns)
    if actual == expected:
        return {"status": "PASS", "detail": f"Expected {expected}, got {actual}"}
    return {
        "status": "FAIL",
        "detail": f"Expected {expected}, got {actual}",
    }


# --- Check 5: Validate file format ---
def check_file_format(obj: dict) -> dict:
    delimiter = obj.get("file_delimiter", ",")
    fmt = "CSV" if delimiter == "," else "TSV" if delimiter == "\t" else f"delim='{delimiter}'"
    return {"status": "PASS", "detail": f"{fmt} format detected (delimiter: {repr(delimiter)})"}


# --- Check 6: Check duplicates ---
def check_duplicates(df: pd.DataFrame, obj: dict) -> dict:
    bk = obj.get("business_key")
    if not bk or bk not in df.columns:
        return {"status": "PASS", "detail": f"Business key '{bk}' not in columns, skipped"}

    dupes = df[bk].duplicated().sum()
    if dupes == 0:
        return {"status": "PASS", "detail": f"0 duplicates on {bk}"}
    return {
        "status": "FAIL",
        "detail": f"{dupes} duplicates on {bk}",
    }


# --- Check 7: Validate dates ---
def check_dates(df: pd.DataFrame, obj: dict, fields: list[dict]) -> dict:
    date_format_java = obj.get("date_format", "yyyy-MM-dd")
    py_format = _java_to_python_date_format(date_format_java)

    date_fields = [f["legacy_field"] for f in fields
                   if f.get("sql_data_type") in ("DATE", "TIMESTAMP")
                   and f["legacy_field"] in df.columns]

    if not date_fields:
        return {"status": "PASS", "detail": "No date columns found"}

    bad_cols = {}
    for col in date_fields:
        invalid_count = 0
        for val in df[col].dropna():
            try:
                datetime.strptime(str(val).strip(), py_format)
            except ValueError:
                invalid_count += 1
        if invalid_count > 0:
            bad_cols[col] = invalid_count

    if not bad_cols:
        return {"status": "PASS", "detail": f"All dates match {date_format_java}"}

    details = "; ".join(f"{col}: {n} invalid rows" for col, n in bad_cols.items())
    return {"status": "FAIL", "detail": details}


# --- Check 8: Check types & lengths ---
def check_types_lengths(df: pd.DataFrame, fields: list[dict]) -> dict:
    issues = []
    for f in fields:
        col = f["legacy_field"]
        if col not in df.columns:
            continue

        dtype = f.get("sql_data_type", "").upper()
        max_len = f.get("length")
        series = df[col].dropna()

        if dtype == "INT":
            non_int = 0
            for val in series:
                try:
                    int(val)
                except (ValueError, TypeError):
                    non_int += 1
            if non_int > 0:
                issues.append(f"{col}: {non_int} non-integer values")

        if max_len and dtype in ("STRING", "VARCHAR(n)"):
            over = (series.astype(str).str.len() > max_len).sum()
            if over > 0:
                issues.append(f"{col}: {over} values exceed max length {max_len}")

    if not issues:
        return {"status": "PASS", "detail": "All types match"}
    return {"status": "FAIL", "detail": "; ".join(issues)}


# --- Check 9: Check decimals ---
def check_decimals(df: pd.DataFrame, fields: list[dict]) -> dict:
    issues = []
    decimal_fields = [f for f in fields if f.get("sql_data_type") == "DECIMAL"]

    for f in decimal_fields:
        col = f["legacy_field"]
        if col not in df.columns:
            continue

        max_decimal = f.get("decimal")
        series = df[col].dropna()

        if max_decimal is None:
            continue

        for val in series:
            s = str(val)
            if "." in s:
                _, dec_part = s.split(".", 1)
                if len(dec_part) > max_decimal:
                    issues.append(f"{col}: decimal part exceeds {max_decimal} digits")
                    break

    if not issues:
        return {"status": "PASS", "detail": "Precision OK"}
    return {"status": "FAIL", "detail": "; ".join(issues)}


# --- Check 10: Check mandatory fields ---
def check_mandatory(df: pd.DataFrame, fields: list[dict]) -> dict:
    issues = []
    required = [f["legacy_field"] for f in fields if f.get("is_required") == "YES"]

    for col in required:
        if col not in df.columns:
            issues.append(f"{col}: column missing")
            continue
        nulls = df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum()
        if nulls > 0:
            issues.append(f"{col}: {nulls} null/empty values")

    if not issues:
        return {"status": "PASS", "detail": "No nulls in required fields"}
    return {"status": "FAIL", "detail": "; ".join(issues)}


# --- Check 11: Check catalog values ---
def check_catalog_values(df: pd.DataFrame, fields: list[dict]) -> dict:
    issues = []
    catalog_fields = [f for f in fields if f.get("allowed_values")]

    for f in catalog_fields:
        col = f["legacy_field"]
        if col not in df.columns:
            continue
        allowed = f["allowed_values"]
        series = df[col].dropna().astype(str).str.strip()
        invalid = series[~series.isin(allowed)]
        if len(invalid) > 0:
            bad_vals = invalid.unique()[:5]
            issues.append(f"{col}: {len(invalid)} invalid values (e.g. {list(bad_vals)})")

    if not issues:
        return {"status": "PASS", "detail": "All values in allowed lists"}
    return {"status": "FAIL", "detail": "; ".join(issues)}


# --- Helpers ---

def _java_to_python_date_format(java_fmt: str) -> str:
    """Convert Java-style date format to Python strftime format."""
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
