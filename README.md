# Age of Empires Analytics Pipeline
<img width="1704" height="872" alt="image" src="https://github.com/user-attachments/assets/6b347fb8-e336-4244-a0f3-b239a36fb036" />


## Summary

This pipeline enables scalable analysis of Age of Empires player ratings, opening build orders, and performance metrics.

This project extracts player event logs from Age of Empires `.xes` files, transforms them into a DuckDB analytics database, and provides a Streamlit dashboard for game strategy insights.

---

## Workflow Overview

1. **Extract Events from `.xes` Files**
2. **Transform and Clean Data into DuckDB (Silver Layer)**
3. **Generate Gold Metrics Tables**
4. **Visualize with Streamlit Dashboard**

---
## 0️⃣ Python environment & install deps
### Requirements

- Python 3.10+
- DuckDB
- Pandas
- lxml
- Streamlit

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

## 3️⃣ (Optional) Generate Gold Metrics Tables

- Executes SQL scripts from `sql/metrics.sql` to create advanced analytics tables in the `gold` schema:
  - Player summary
  - Events per minute (APM)
  - Age timings
  - Opening build orders
  - Winrate by civilization

**Run:**
```bash
python pipelines/read_metrics.py
```

---

## 4️⃣ Visualize with Streamlit Dashboard

- The dashboard (`app/Dashboard.py`) connects to the DuckDB database.
- Interactive filters for Elo, civilization, and build order length.
- Displays player summaries, APM, age timings, opening strategies, and winrates.

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
