import duckdb
import os

WAREHOUSE = "warehouse/aoe.duckdb"
RAW_EVENTS = "warehouse/events_raw.parquet"

def main():
    os.makedirs("warehouse", exist_ok=True)
    con = duckdb.connect(WAREHOUSE)

    # 1. Bronze = raw parquet
    con.execute("DROP TABLE IF EXISTS bronze")
    con.execute(f"CREATE TABLE bronze AS SELECT * FROM '{RAW_EVENTS}'")

    # 2. Silver = cleaned events
    con.execute("DROP TABLE IF EXISTS events_clean")
    con.execute("""
        CREATE TABLE events_clean AS
        SELECT
            event_id,
            match_id,
            player_id,
            map_type,
            civilization,
            civilization_category,
            strategy,

            -- Elo (rank) as float
            TRY_CAST(elo AS DOUBLE) AS elo,

            -- Win flag normalized
            CASE
                WHEN LOWER(CAST(win AS VARCHAR)) IN ('1','true') THEN 1
                WHEN LOWER(CAST(win AS VARCHAR)) IN ('0','false') THEN 0
                ELSE NULL
            END AS win,

            -- Event properties
            activity,
            TRY_CAST(amount AS INTEGER) AS amount,

            -- Timestamp handling
            ts AS event_time,
            EXTRACT(EPOCH FROM (ts - MIN(ts) OVER (PARTITION BY case_id))) AS seconds_since_start,

            TRY_CAST("@@index" AS INTEGER) AS event_index,
            TRY_CAST("@@case_index" AS INTEGER) AS case_index

        FROM bronze
        WHERE activity IS NOT NULL
        AND match_id IS NOT NULL
        AND player_id IS NOT NULL
    """)

    # 3. Index for faster analysis
    con.execute("CREATE INDEX IF NOT EXISTS idx_events_player ON events_clean(player_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_events_match ON events_clean(match_id)")

    # Debug
    print(con.execute("SELECT * FROM events_clean LIMIT 10").fetchdf())
    print(f"✅ Wrote Silver tables into {WAREHOUSE}")

if __name__ == "__main__":
    main()
