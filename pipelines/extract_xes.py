import os
import pandas as pd
from lxml import etree
from dateutil import parser as dtparse
from pathlib import Path

DATA_DIR = Path("data")
OUT_DIR = Path("warehouse")
OUT_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_PATH = OUT_DIR / "events_raw.parquet"

CHUNK_SIZE = 100_000  # Number of events per chunk

def parse_xes_file_chunked(xes_path, chunk_callback):
    """
    Efficiently parse a large XES file and process events in chunks.
    Calls chunk_callback(rows) every CHUNK_SIZE events.
    """
    ns = "{http://www.xes-standard.org/}" # XES namespace

    rows = []
    trace_attrs = {}
    case_id = None

    # Use lxml's iterparse for low-memory XML parsing
    context = etree.iterparse(xes_path, events=("start", "end"))

    # XES: <log><trace>...<event/>...</trace>...</log>
    for event, elem in context:
        if event == "start" and elem.tag == f"{ns}trace":
            trace_attrs = {}
            case_id = None

        elif event == "end":
            # Case level attributes
            if elem.tag == f"{ns}string" and elem.getparent().tag == f"{ns}trace":
                trace_attrs[elem.attrib["key"]] = elem.attrib.get("value")
                if elem.attrib["key"] == "concept:name":
                    case_id = elem.attrib.get("value") # unique ID for this trace
            elif elem.tag == f"{ns}float" and elem.getparent().tag == f"{ns}trace":
                trace_attrs[elem.attrib["key"]] = elem.attrib.get("value")
            elif elem.tag == f"{ns}int" and elem.getparent().tag == f"{ns}trace":
                trace_attrs[elem.attrib["key"]] = elem.attrib.get("value")

            # Event-level attributes
            elif elem.tag == f"{ns}event":
                ev = {"case_id": case_id}
                for child in elem:
                    if child.tag == f"{ns}string":
                        k, v = child.attrib.get("key"), child.attrib.get("value")
                        if k == "concept:name": # <- THIS is the event activity
                            ev["activity"] = v
                        else:
                            ev[k] = v
                    elif child.tag == f"{ns}float":
                        ev[child.attrib["key"]] = child.attrib.get("value")
                    elif child.tag == f"{ns}int":
                        ev[child.attrib["key"]] = child.attrib.get("value")
                    elif child.tag == f"{ns}date":
                        ev["ts"] = child.attrib.get("value") # Timestamp

                # stable event_id: prefer @@index if present
                idx = ev.get("@@index")
                if idx is None:
                    idx = str(len(rows))
                ev["event_id"] = f"{case_id}-{idx}"

                # merge case attributes so each row has full context
                ev.update(trace_attrs)
                rows.append(ev)

                # if enough events, process chunk and clear memory
                if len(rows) >= CHUNK_SIZE:
                    chunk_callback(rows)
                    rows.clear()
                elem.clear()  # Free memory

            # Free memory after processing trace
            elif elem.tag == f"{ns}trace":
                elem.clear()  # Free memory

    # Process any remaining events after parsing file
    if rows:
        chunk_callback(rows)

def main():
    xes_files = list(DATA_DIR.glob("*.xes"))
    if not xes_files:
        raise FileNotFoundError(f"No .xes files found in {DATA_DIR}")

    # Remove old parquet if exists
    if BRONZE_PATH.exists():
        BRONZE_PATH.unlink()

    all_rows = []
    total_events = 0

    def write_chunk(chunk_rows):
        nonlocal all_rows, total_events
        all_rows.extend(chunk_rows)
        total_events += len(chunk_rows)

    for xes_file in xes_files:
        print(f"Processing {xes_file} ...")
        parse_xes_file_chunked(xes_file, write_chunk)

    # Write all events to Parquet in one go
    if all_rows:
        df = pd.DataFrame(all_rows)
        # Preferred column order for downstream processing
        preferred = [
            "event_id", "ts", "activity",
            "match_id", "player_id", "map_type", "civilization", "civilization_category", "elo",
            "case_id", "strategy", "win", "amount", "@@index", "@@case_index"
        ]
        cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
        df = df[cols]
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
        # Write to Parquet file
        df.to_parquet(BRONZE_PATH, index=False, engine="pyarrow")

    print(f"✅ Parsed {len(xes_files)} files with {total_events:,} events")

if __name__ == "__main__":
    main()
