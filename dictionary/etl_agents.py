"""
Multi-Agent ETL System with OpenAI Assistants
Agent 1: Date Governance Agent
Agent 2: Column Lister Agent
"""
import os
import time
from pathlib import Path
from openai import OpenAI
import config

client = OpenAI(api_key=config.OPENAI_API_KEY)

# ========== AGENT DEFINITIONS ==========

def create_date_agent():
    """Create Date Governance Agent with Code Interpreter"""
    agent = client.beta.assistants.create(
        name="Date Governance Agent",
        instructions="""You are a Date Governance Agent with an active Code Interpreter.

You will recieve TWO files as attachments in this run:
1) A governance dictionary in Excel with two sheets exactly named:
   - "object"
   - "fields"
2) A data file (TSV or CSV or Fixed,) containing the dataset to check.

Your tasks:
1) Read the dictionary:
   - From sheet "object", read the column "date_format" (row 1) → this is the only valid format for all date fields. Example: "yyyy-MM-dd".
   - From sheet "fields", identify every row in the columns "sql_data_type" and "canonical_field", the first one indicates the data type of every field, focus in the "DATE" or "Timestamps" data types then collect their "canonical_field" names. All those columns in the data must follow the same expected format.
2) Load the data file robustly
3) Validate:
   - For each identified date column, check every value strictly matches the expected "date_format" from "object".
   - If ALL values in ALL date columns match → verdict = OK.
   - Otherwise → verdict = NOTOK and count how many rows do not match. If multiple formats are present, list the distinct patterns you detect.
4) If verdict = NOTOK, FIX in place using Python:
   - Normalize every detectable variant into the expected format.
   - Leave truly unparseable values as-is (do not invent dates).
   - Save a corrected copy next to the working file as "<original_basename>_fixed.tsv" (tab-separated).
   - Attach the corrected file in the response.
5) Output ONLY a compact line with:
   - "OK, <short note>"  OR  "NOTOK, <short note>"
   Where the short note includes the affected columns and a brief summary. Do not print code, do not print markdown.

If you find blank or null dates o many corrupted values, just not try to fix the file and just do generate "NOTOK, Imposible to fix"
""",
        model="gpt-4o",
        tools=[{"type": "code_interpreter"}]
    )
    return agent.id

def create_column_lister_agent():
    """Create Column Lister Agent with Code Interpreter"""
    agent = client.beta.assistants.create(
        name="Column Lister Agent",
        instructions="""You are a Column Lister Agent with Code Interpreter.

You will receive ONE data file (TSV, CSV, or Fixed format).

Your task:
1) Load the data file robustly
2) Extract and list all column names
3) Output ONLY the column names, one per line, no additional text or markdown.
""",
        model="gpt-4o",
        tools=[{"type": "code_interpreter"}]
    )
    return agent.id


# ========== AGENT EXECUTION ==========

def upload_file(file_path):
    """Upload file to OpenAI and return file ID"""
    with open(file_path, "rb") as f:
        file = client.files.create(file=f, purpose="assistants")
    return file.id

def run_agent(agent_id, file_ids, message="Process these files"):
    """Run an agent and wait for completion"""
    # Create thread
    thread = client.beta.threads.create()

    # Create message with file attachments
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content=message,
        attachments=[{"file_id": fid, "tools": [{"type": "code_interpreter"}]} for fid in file_ids]
    )

    # Run assistant
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=agent_id
    )

    # Wait for completion
    while run.status in ["queued", "in_progress"]:
        time.sleep(2)
        run = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)

    if run.status != "completed":
        raise Exception(f"Run failed with status: {run.status}")

    # Get messages
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    assistant_message = messages.data[0]

    # Extract text response
    text_content = ""
    for content in assistant_message.content:
        if content.type == "text":
            text_content += content.text.value

    # Extract file attachments if any
    output_files = []
    for content in assistant_message.content:
        if hasattr(content, "text") and hasattr(content.text, "annotations"):
            for annotation in content.text.annotations:
                if hasattr(annotation, "file_path"):
                    output_files.append(annotation.file_path.file_id)

    return text_content.strip(), output_files, thread.id


def download_file(file_id, output_path):
    """Download file from OpenAI"""
    content = client.files.content(file_id)
    with open(output_path, "wb") as f:
        f.write(content.read())


# ========== ORCHESTRATOR ==========

def run_etl_pipeline(dictionary_path, data_file_path, work_dir):
    """
    Orchestrate the multi-agent ETL pipeline

    Args:
        dictionary_path: Path to Excel dictionary
        data_file_path: Path to data file in work/ directory
        work_dir: Path to work/ directory
    """
    print("=" * 60)
    print("ETL MULTI-AGENT PIPELINE")
    print("=" * 60)

    # Create agents
    print("\n[1/5] Creating agents...")
    date_agent_id = create_date_agent()
    lister_agent_id = create_column_lister_agent()
    print(f"  ✓ Date Agent: {date_agent_id}")
    print(f"  ✓ Lister Agent: {lister_agent_id}")

    # Upload files
    print("\n[2/5] Uploading files...")
    dict_file_id = upload_file(dictionary_path)
    data_file_id = upload_file(data_file_path)
    print(f"  ✓ Dictionary: {dict_file_id}")
    print(f"  ✓ Data file: {data_file_id}")

    # Run Agent 1: Date Governance
    print("\n[3/5] Running Date Governance Agent...")
    verdict, output_files, thread_id = run_agent(
        date_agent_id,
        [dict_file_id, data_file_id],
        "Validate and fix date formats"
    )
    print(f"  Verdict: {verdict}")

    # Parse verdict
    is_ok = verdict.startswith("OK")
    is_fixed = "NOTOK" in verdict and "Imposible to fix" not in verdict
    is_unfixable = "Imposible to fix" in verdict

    # Handle file replacement
    current_file_path = data_file_path

    if is_fixed and output_files:
        print("\n[4/5] Agent fixed the file, downloading and replacing...")
        # Download fixed file
        original_name = Path(data_file_path).stem
        fixed_path = Path(work_dir) / f"{original_name}_fixed.tsv"
        download_file(output_files[0], fixed_path)

        # Replace original with fixed
        os.remove(data_file_path)
        os.rename(fixed_path, data_file_path)
        print(f"  ✓ Replaced {data_file_path} with fixed version")
        current_file_path = data_file_path
    elif is_ok:
        print("\n[4/5] File passed validation, no changes needed")
    elif is_unfixable:
        print("\n[4/5] File is unfixable, STOPPING PIPELINE")
        print("=" * 60)
        print("PIPELINE RESULT: FAILED (unfixable data)")
        print("=" * 60)
        return False

    # Run Agent 2: Column Lister
    print("\n[5/5] Running Column Lister Agent...")
    new_data_file_id = upload_file(current_file_path)
    columns, _, _ = run_agent(
        lister_agent_id,
        [new_data_file_id],
        "List all columns in this file"
    )
    print(f"  Columns found:\n{columns}")

    # Success
    print("\n" + "=" * 60)
    print("PIPELINE RESULT: SUCCESS")
    print("=" * 60)
    return True


# ========== MAIN ==========

if __name__ == "__main__":
    # Configuration
    WORK_DIR = Path(__file__).parent / "work"
    DICTIONARY_PATH = Path(__file__).parent / "Dictionary_co_stg_transactions.xlsx"

    # Find first file in work/ directory
    work_files = list(WORK_DIR.glob("*.tsv")) + list(WORK_DIR.glob("*.csv"))

    if not work_files:
        print("ERROR: No TSV/CSV files found in work/ directory")
        exit(1)

    DATA_FILE_PATH = work_files[0]

    print(f"Dictionary: {DICTIONARY_PATH}")
    print(f"Data file: {DATA_FILE_PATH}")

    # Run pipeline
    success = run_etl_pipeline(
        dictionary_path=str(DICTIONARY_PATH),
        data_file_path=str(DATA_FILE_PATH),
        work_dir=str(WORK_DIR)
    )

    if success:
        print("\n✓ All agents completed successfully")
    else:
        print("\n✗ Pipeline stopped due to unfixable errors")
