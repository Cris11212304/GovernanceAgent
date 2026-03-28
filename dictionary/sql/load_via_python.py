"""
Load cleaned CSV files into SQL Server via pyodbc.
Handles CSV fields with commas in quoted strings correctly.
"""
import csv
import sys
import os

try:
    import pyodbc
except ImportError:
    print("Installing pyodbc...")
    os.system("pip install pyodbc")
    import pyodbc

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=GovernanceDB;"
    "Trusted_Connection=yes;"
)

def load_file(file_path):
    print(f"\n{'='*60}")
    print(f"Loading: {file_path}")
    print(f"{'='*60}")
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Step 1: Truncate raw table
    print("[1/3] Truncating raw.co_stg_transactions...")
    cursor.execute("TRUNCATE TABLE raw.co_stg_transactions")
    conn.commit()
    
    # Step 2: Insert rows from CSV
    print("[2/3] Inserting rows...")
    insert_sql = """
        INSERT INTO raw.co_stg_transactions 
        (sales_id, product_id, store_id, product_name, brand, category,
         volume_ml, alcohol_percentage, store_name, city, state, region,
         store_type, sales_date, quantity_sold, unit_price, total_sales,
         source_file)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    
    row_count = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)  # skip header
        
        for row in reader:
            if len(row) != 17:
                print(f"  WARN: Skipping row with {len(row)} columns: {row[:3]}...")
                continue
            cursor.execute(insert_sql, row + [os.path.basename(file_path)])
            row_count += 1
            
            if row_count % 200 == 0:
                conn.commit()
                print(f"  ... {row_count} rows inserted")
    
    conn.commit()
    print(f"  Total: {row_count} rows into raw.co_stg_transactions")
    
    # Step 3: Upsert to master
    print("[3/3] Upserting to dbo.transactions...")
    cursor.execute("EXEC dbo.sp_upsert_transactions")
    conn.commit()
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM raw.co_stg_transactions")
    raw_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dbo.transactions")
    master_count = cursor.fetchone()[0]
    
    print(f"\n  raw.co_stg_transactions: {raw_count} rows")
    print(f"  dbo.transactions:        {master_count} rows")
    print(f"\n{'='*60}")
    print("DONE")
    print(f"{'='*60}")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    ready_dir = os.path.join(os.path.dirname(__file__), '..', 'ready')
    
    if len(sys.argv) > 1:
        load_file(sys.argv[1])
    else:
        # Load all cleaned files in ready/
        for fname in sorted(os.listdir(ready_dir)):
            if fname.endswith('_cleaned.csv'):
                load_file(os.path.join(ready_dir, fname))
