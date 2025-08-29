# Age of Empires Analytics Pipeline

**Demo Dashboard:** https://kartoshhka-aoe-analytics-appdashboard-ag4rkn.streamlit.app/

<img width="1704" height="872" alt="image" src="https://github.com/user-attachments/assets/6b347fb8-e336-4244-a0f3-b239a36fb036" />


## Summary

This pipeline enables scalable analysis of Age of Empires player ratings, opening build orders, and performance metrics.

This project extracts player event logs from Age of Empires `.xes` files, transforms them into a DuckDB analytics database, and provides a Streamlit dashboard for game strategy insights.

One of the key features of this project is the analysis of unknown player strategies using match event logs. The goal is to identify repeating build patterns that may represent previously undiscovered strategies. By examining clusters of similar sequences and their associated win rates, this analysis can reveal effective strategies and potential imbalances in the game.

<img width="1325" height="800" alt="image" src="https://github.com/user-attachments/assets/bc4c5ee8-bdff-4a79-845f-b8467d29bc2e" />


---

## Workflow Overview

1. **Extract Events from `.xes` Files**
2. **Transform and Clean Data into DuckDB (Silver Layer)**
3. **Generate Gold Metrics Tables**
4. **Visualize with Streamlit Dashboard**

---
## 0️⃣ Python environment & install deps

**Run:**
```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## 1️⃣ Extract Events from `.xes` Files

- Download AoE events `.xes` files into the `data/` directory ([Here's open Zenodo database](https://zenodo.org/records/11060884))
- Each file is processed to extract traces and events, including player id, match id, activity, timestamps, and game context.
- The combined event data is saved as a Parquet file:
  `warehouse/events_raw.parquet`

**Run:**
```bash
python pipelines/extract_xes.py
```

---

## 2️⃣ Transform and Clean Data into DuckDB

- The raw Parquet file is loaded into DuckDB as the `bronze` table.
- Data is cleaned and normalized into the `events_clean` table (Silver Layer), with proper types, normalized win flags, and calculated elapsed times.
- Indexes are created for fast querying.

**Run:**
```bash
python pipelines/transform_events.py
```

---

## 3️⃣ Generate Gold Metrics Tables

- Executes SQL scripts from `sql/metrics.sql` to create advanced analytics tables in the `gold` schema:
  - Player summary
  - Events per minute (APM)
  - Age timings
  - Opening build orders
  - Winrate by civilization
  - Winrate by strategy

**Run:**
```bash
python pipelines/read_metrics.py
```
---
## 4️⃣ Analyze uknown strategies

- (Optional) Known strategies are analyzed to determine similarity levels across different metrics: n-gram cosine similarity, Jaccard similarity, and Levenshtein similarity.
- Player action sequences with unknown strategies are converted into n-grams (ngram_n = 3) to capture recurring motifs.
- Similar sequences are grouped using MinHash + LSH for efficient candidate bucketing.
- Clusters are refined with DBSCAN, keeping only clusters where at least one player repeats the same build order, highlighting likely real strategies rather than random matches.
- Cluster coherence is evaluated using n-gram cosine similarity, Jaccard similarity, and Levenshtein similarity.
- Cluster stats are recorded: number of matches, number of players, and win rate.
- High win rates for repeating patterns may indicate novel or imbalanced strategies.

**Run:**
```bash
python pipelines/discover_strategies.py
```
---

## 5️⃣ Visualize with Streamlit Dashboard

- The dashboard (`app/Dashboard.py`) connects to the DuckDB database.
- Interactive filters for Elo, civilization, and build order length.
- Displays player summaries, APM, age timings, opening strategies, winrates, and unknown strategies analysis.

**Run:**
```bash
streamlit run app/Dashboard.py
```

---

## Directory Structure

```
data/                # Place your .xes files here
warehouse/           # Output Parquet and DuckDB files
pipelines/           # ETL scripts
sql/                 # SQL scripts for metrics
app/                 # Streamlit dashboard
tests/               # Unit tests
```

---


## CI Testing

- Minimal test data can be symlinked/copied from `tests/` to `data/` for CI runs.
- Tests validate the presence and schema of gold tables.
