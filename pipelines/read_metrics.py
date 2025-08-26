import duckdb

# Paths
DB_PATH = "warehouse/aoe.duckdb"
SQL_PATH = "sql/metrics.sql"

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

# Read and execute metrics SQL
with open(SQL_PATH, "r", encoding="utf-8") as f:
    sql_script = f.read()
con.execute(sql_script)

# Helper function to preview a table
def preview(table_name, limit=5):
    print(f"\n=== {table_name} ===")
    try:
        df = con.execute(f"SELECT * FROM {table_name} LIMIT {limit}").df()
        print(df)
    except Exception as e:
        print(f"Error reading {table_name}: {e}")

# Preview each gold table
preview("gold.apm")
preview("gold.age_timings")
preview("gold.openings")
preview("gold.winrate_civ")

con.close()
