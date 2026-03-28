"""
AI Agent — Uses an LLM (Anthropic Claude, Google Gemini, or OpenAI) to reconcile
schema mismatches that fixed rules cannot handle.

The agent receives governance context + data sample, generates a Python
transformation script, which is then executed locally to produce a corrected file.
"""
import re
import traceback
from pathlib import Path

import pandas as pd


def reconcile_schema(
    data_file_path: str,
    dictionary_path: str,
    governance: dict,
    validation_results: dict,
    output_dir: str,
    api_key: str,
    provider: str = "anthropic",
) -> tuple[str | None, str]:
    """
    Use AI to fix schema mismatches.

    Returns:
        (corrected_file_path, agent_summary) — path is None if reconciliation failed.
    """
    # Read a sample of the data for context
    df_sample = pd.read_csv(data_file_path, nrows=5, dtype=str)
    sample_csv = df_sample.to_csv(index=False)

    # Build the prompt
    prompt = _build_prompt(governance, validation_results, sample_csv, data_file_path)

    # Get transformation code from AI
    try:
        if provider == "anthropic":
            code = _call_anthropic(api_key, prompt)
        elif provider == "gemini":
            code = _call_gemini(api_key, prompt)
        else:
            code = _call_openai(api_key, prompt)
    except Exception as e:
        return None, f"AI API call failed: {type(e).__name__}: {e}"

    if not code:
        return None, "AI agent did not return any transformation code"

    print(f"  AI generated transformation code ({len(code)} chars)")

    # Execute the transformation
    original_name = Path(data_file_path).stem
    output_path = str(Path(output_dir) / f"{original_name}_corrected.csv")

    success, message = _execute_transformation(code, data_file_path, output_path)

    if success and Path(output_path).exists():
        return output_path, f"Transformation applied successfully. {message}"
    else:
        return None, f"Transformation failed: {message}\n\nGenerated code:\n{code}"


def _call_anthropic(api_key: str, prompt: str) -> str | None:
    """Call Anthropic Claude to generate transformation code."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    message = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        system="You are a data governance agent. Respond ONLY with a single Python code block. No explanations before or after the code.",
    )

    text = message.content[0].text
    return _extract_code(text)


def _call_gemini(api_key: str, prompt: str) -> str | None:
    """Call Google Gemini to generate transformation code."""
    import google.generativeai as genai

    genai.configure(api_key=api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    response = model.generate_content(prompt)
    text = response.text
    return _extract_code(text)


def _call_openai(api_key: str, prompt: str) -> str | None:
    """Call OpenAI to generate transformation code."""
    from openai import OpenAI

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a data governance agent. Respond ONLY with Python code."},
            {"role": "user", "content": prompt},
        ],
        temperature=0,
    )
    text = response.choices[0].message.content
    return _extract_code(text)


def _extract_code(text: str) -> str | None:
    """Extract Python code block from AI response."""
    # Try ```python ... ``` blocks
    pattern = r"```python\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    if matches:
        return matches[0].strip()

    # Try generic code blocks
    pattern = r"```\s*\n(.*?)```"
    matches = re.findall(pattern, text, re.DOTALL)
    if matches:
        return matches[0].strip()

    # If no code blocks, check if the response looks like code
    if "import " in text or "pd.read_csv" in text or "df[" in text:
        return text.strip()

    return None


def _execute_transformation(code: str, input_path: str, output_path: str) -> tuple[bool, str]:
    """Execute the AI-generated transformation code safely."""
    # Inject input/output paths into the code
    code = code.replace("INPUT_FILE_PATH", f"r'{input_path}'")
    code = code.replace("OUTPUT_FILE_PATH", f"r'{output_path}'")

    # If the code doesn't reference the actual paths, prepend variable definitions
    if input_path not in code and "INPUT_FILE_PATH" not in code:
        prefix = f"input_path = r'{input_path}'\noutput_path = r'{output_path}'\n"
        code = prefix + code

    # Execute in a controlled namespace
    namespace = {
        "pd": pd,
        "Path": Path,
        "input_path": input_path,
        "output_path": output_path,
    }

    try:
        exec(code, namespace)
        if Path(output_path).exists():
            return True, "Code executed and output file created"
        else:
            return False, "Code executed but no output file was created"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}\n{traceback.format_exc()}"


def _build_prompt(governance: dict, validation_results: dict, sample_csv: str, data_file_path: str) -> str:
    """Build a detailed prompt for the AI agent."""
    obj = governance["object"]
    fields = governance["fields"]

    # Summarize expected schema
    field_lines = []
    for f in fields:
        line = f"  - {f['legacy_field']} ({f.get('sql_data_type', '?')})"
        if f.get("allowed_values"):
            line += f" allowed={f['allowed_values']}"
        if f.get("is_required") == "YES":
            line += " [REQUIRED]"
        field_lines.append(line)

    # Summarize failures
    failures = []
    for check_name, result in validation_results.items():
        if result["status"] == "FAIL":
            failures.append(f"  - {check_name}: {result['detail']}")

    return f"""You are a data governance agent. Generate a Python script using pandas that transforms
a data file to match a target schema.

IMPORTANT RULES:
- The script will be executed with `exec()`. Variables `input_path` and `output_path` are pre-defined as strings.
- `pd` (pandas) is already imported and available. Do NOT import pandas.
- Read from `input_path`, write the corrected CSV to `output_path`.
- Do NOT use `print()`, `input()`, or any interactive I/O.
- The output must be a valid CSV with exactly the columns listed below, in order.

EXPECTED SCHEMA (from governance dictionary):
- Table: {obj.get('destination_table')}
- Expected column count: {obj.get('expected_column_count')}
- Date format: {obj.get('date_format')} (Python strftime: %Y-%m-%d)
- Business key: {obj.get('business_key')}
- Columns (in order):
{chr(10).join(field_lines)}

VALIDATION FAILURES DETECTED:
{chr(10).join(failures) if failures else '  None'}

SAMPLE OF INCOMING DATA (first 5 rows):
{sample_csv}

INPUT FILE: The full data is at `input_path` (CSV, comma-delimited).
OUTPUT FILE: Save corrected data to `output_path` (CSV, comma-delimited, with header).

Generate ONLY a Python code block. The code must:
1. Read the full input file from `input_path` using `pd.read_csv(input_path, dtype=str)`
2. Drop any columns NOT in the expected schema
3. Keep only the 17 expected columns in this exact order: {', '.join(f['legacy_field'] for f in fields)}
4. Convert data types as needed (e.g. alphanumeric product_id -> sequential integer IDs starting from 1)
5. Normalize dates to %Y-%m-%d format
6. Save the result to `output_path` using `df.to_csv(output_path, index=False)`
"""
