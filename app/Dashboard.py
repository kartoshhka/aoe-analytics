# app/Dashboard.py
import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def create_actions_heatmap(df):
    # Prepare data for heatmap: rows=actions, columns=step (action_rank)
    heatmap_df = (
        df.groupby(["activity", "action_rank"])
        .size()
        .unstack(fill_value=0)
        .sort_index(axis=1)
    )

    plt.figure(figsize=(top_n_actions * 0.5, 16))  # Increase width based on number of actions
    sns.heatmap(heatmap_df, cmap="Blues", linewidths=0.5, annot=True, fmt="d", annot_kws={"size":8})
    plt.xlabel("Build Order Step (action_rank)")
    plt.ylabel("Action")
    plt.title("Most Common Actions at Each Build Order Step")
    plt.xticks(rotation=45, ha='right', fontsize=10)  # Rotate and enlarge x labels
    plt.yticks(fontsize=10)  # Enlarge y labels
    st.pyplot(plt)
    plt.close()

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
    SELECT civilization, activity, avg_time_mins
    FROM gold.age_timings
    WHERE civilization IN ({','.join([f"'{c}'" for c in selected_civ])})
"""
age_df = con.execute(age_query).fetchdf()

# Opening build orders
opening_query = f"""
    SELECT elo, player_id, match_id, win, activity, action_rank,
    civilization, civilization_category, map_type, strategy
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

# Winrate by strategy
civ_conditions = " OR ".join([f"civilizations LIKE '%{c}%'" for c in selected_civ])
winrate_civ_query = f"""
    SELECT strategy, total_games, winrate, civilizations
    FROM gold.winrate_strat
    WHERE {civ_conditions}
"""
winrate_civ_df = con.execute(winrate_civ_query).fetchdf()

# ----------------------------------------
# 4️⃣ Display metrics in Streamlit
# ----------------------------------------
st.title("🛡️ Age of Empires Analytics Dashboard")

st.header("👾 All Players Summary")
st.dataframe(player_summary_df)

st.header("📊 EPM (Events per Minute) - First 10 minutes")
st.dataframe(apm_df)

st.header("🏰 Average Age Timings")
#st.dataframe(age_df)
# Pivot so each civilization is a row, activities are columns
pivot_df = age_df.pivot(index="civilization", columns="activity", values="avg_time_mins")
# Sort columns by the mean age timing across civilizations (ascending)
sorted_columns = pivot_df.mean(axis=0).sort_values().index
pivot_df = pivot_df[sorted_columns]
st.dataframe(pivot_df)

st.header(f"⚔️ Opening Build Orders (Top {top_n_actions} actions)")

# Group actions for each player/match into a single row
opening_grouped = (
    opening_df
    .groupby(["elo", "player_id", "match_id", "win",
    "civilization", "civilization_category", "map_type", "strategy"
    ])
    .agg({
        "activity": lambda acts: " → ".join(acts),  # Use arrows or commas to separate actions
        "action_rank": lambda ranks: ", ".join(str(r) for r in ranks)
    })
    .reset_index()
)
opening_grouped["win"] = opening_grouped["win"].apply(lambda x: True if x == 1 else False)
opening_grouped = opening_grouped.sort_values("elo", ascending=False)
opening_grouped = opening_grouped.drop(columns=["action_rank"])
st.dataframe(opening_grouped)
with st.expander("Show table (better actions visibility, but no filters)"):
    st.write("This static table shows all build order actions for each player-match. You can see the full build order, but sorting and filtering are disabled.")
    st.table(opening_grouped)

st.header("🔥 Build Order Step-Action Heatmap")
create_actions_heatmap(opening_df)

st.header("🏆 Winrate by Civilization")
st.dataframe(winrate_df)

st.header("♟️ Winrate by Strategy")
st.dataframe(winrate_civ_df)
