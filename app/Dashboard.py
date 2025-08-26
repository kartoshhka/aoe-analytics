# app/Dashboard.py
import streamlit as st
import duckdb
import pandas as pd

def short_id(id_str, length=8):
    if len(id_str) > length:
        return f"{id_str[:length]}…"
    return id_str

# ----------------------------------------
# 1️⃣ Connect to DuckDB
# ----------------------------------------
con = duckdb.connect(database="warehouse/aoe.duckdb", read_only=True)

# ----------------------------------------
# 2️⃣ Sidebar filters
# ----------------------------------------
st.sidebar.header("Filters")

# Elo slider
min_elo, max_elo = st.sidebar.slider("Elo range", 800, 3100, (2500, 3100))

# Civilization select
civilizations = con.execute("SELECT DISTINCT civilization FROM events_clean").fetchdf()
civilizations_list = civilizations["civilization"].dropna().tolist()
selected_civ = st.sidebar.multiselect("Civilization", civilizations_list, default=civilizations_list)

# Opening Build Order length
top_n_actions = st.sidebar.slider("Opening Build Order Length (top N actions)", 10, 20, 50)

# ----------------------------------------
# 3️⃣ Load Gold metrics with filters
# ----------------------------------------

# Player summary query
player_summary_query = f"""
SELECT *
FROM gold.player_summary
WHERE max_elo BETWEEN {min_elo} AND {max_elo}
"""

player_summary_df = con.execute(player_summary_query).fetchdf()
player_summary_df['player_id'] = player_summary_df['player_id'].apply(lambda x: short_id(x, 8))

# EPM per player-match
apm_query = f"""
    SELECT DISTINCT e.elo, a.player_id, a.match_id, a.apm, e.civilization
    FROM gold.apm a
    JOIN (
        SELECT DISTINCT elo, player_id, match_id, civilization
        FROM events_clean
    ) e
    USING(player_id, match_id)
    WHERE e.elo BETWEEN {min_elo} AND {max_elo}
      AND e.civilization IN ({','.join([f"'{c}'" for c in selected_civ])})
"""
apm_df = con.execute(apm_query).fetchdf()
apm_df['player_id'] = apm_df['player_id'].apply(lambda x: short_id(x, 8))
apm_df['match_id'] = apm_df['match_id'].apply(lambda x: short_id(x, 10))

# Age timings
age_query = f"""
    SELECT civilization, activity, avg_time_min
    FROM gold.age_timings
    WHERE civilization IN ({','.join([f"'{c}'" for c in selected_civ])})
"""
age_df = con.execute(age_query).fetchdf()

# Opening build orders
opening_query = f"""
    SELECT elo, player_id, match_id, activity, action_rank
    FROM gold.openings
    WHERE action_rank <= {top_n_actions}
      AND elo BETWEEN {min_elo} AND {max_elo}
      AND civilization IN ({','.join([f"'{c}'" for c in selected_civ])})
    ORDER BY player_id, match_id, action_rank
"""
opening_df = con.execute(opening_query).fetchdf()
opening_df['player_id'] = opening_df['player_id'].apply(lambda x: short_id(x, 8))
opening_df['match_id'] = opening_df['match_id'].apply(lambda x: short_id(x, 10))

# Winrate by civilization
winrate_query = f"""
    SELECT civilization, total_games, winrate
    FROM gold.winrate_civ
    WHERE civilization IN ({','.join([f"'{c}'" for c in selected_civ])})
"""
winrate_df = con.execute(winrate_query).fetchdf()

# ----------------------------------------
# 4️⃣ Display metrics in Streamlit
# ----------------------------------------
st.title("🛡️ Age of Empires Analytics Dashboard")

st.header("👾 All Players Summary")
st.dataframe(player_summary_df)

st.header("📊 EPM (Events per Minute) - First 10 minutes")
st.dataframe(apm_df)

st.header("🏰 Average Age Timings")
st.dataframe(age_df)

st.header(f"⚔️ Opening Build Orders (Top {top_n_actions} actions)")
st.dataframe(opening_df)

st.header("🏆 Winrate by Civilization")
st.dataframe(winrate_df)
