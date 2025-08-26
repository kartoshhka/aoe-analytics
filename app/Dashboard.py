import duckdb
import streamlit as st
import pandas as pd

# Connect to DuckDB database (adjust path if needed)
con = duckdb.connect(database="warehouse/aoe.duckdb", read_only=True)

st.title("🛡️ Age of Empires II – Player Strategy Dashboard")

# Sidebar filters
st.sidebar.header("Filters")
elo_threshold = st.sidebar.slider("Minimum Elo", min_value=800, max_value=2500, value=1200, step=50)
max_opening_actions = st.sidebar.slider("Opening length (actions)", 5, 20, 10)

# === APM (first 10 minutes) ===
st.subheader("📊 Actions per Minute (APM) – First 10 Minutes")

apm_query = f"""
    SELECT player_id, match_id,
           COUNT(*) * 6.0 / 600.0 AS apm  -- actions / minutes (10 min = 600s)
    FROM events_clean
    WHERE seconds_since_start <= 600
    GROUP BY player_id, match_id
"""
apm_df = con.execute(apm_query).fetchdf()
st.dataframe(apm_df.head(20))


# === Age Timings ===
st.subheader("🏰 Average Age Timings (seconds since start)")

age_query = """
    SELECT civilization, activity, AVG(seconds_since_start) AS avg_time
    FROM events_clean
    WHERE LOWER(activity) LIKE '%age%'
    GROUP BY civilization, activity
    ORDER BY avg_time
"""
age_df = con.execute(age_query).fetchdf()
st.dataframe(age_df)


# === Opening Build Orders ===
st.subheader(f"⚔️ Opening Build Orders (first {max_opening_actions} actions)")

opening_query = f"""
    WITH ranked_events AS (
        SELECT player_id, match_id, activity,
               ROW_NUMBER() OVER (PARTITION BY player_id, match_id ORDER BY seconds_since_start) AS rn,
               elo
        FROM events_clean
    )
    SELECT player_id, match_id, activity, rn
    FROM ranked_events
    WHERE rn <= {max_opening_actions}
      AND elo >= {elo_threshold}
    ORDER BY player_id, match_id, rn
"""
opening_df = con.execute(opening_query).fetchdf()
st.dataframe(opening_df)


# === Winrate by Civilization ===
st.subheader("🏆 Winrate by Civilization")

winrate_query = """
    SELECT civilization,
           COUNT(*) AS games,
           AVG(win) * 100 AS winrate
    FROM events_clean
    WHERE win IS NOT NULL
    GROUP BY civilization
    ORDER BY winrate DESC
"""
winrate_df = con.execute(winrate_query).fetchdf()
st.dataframe(winrate_df)
