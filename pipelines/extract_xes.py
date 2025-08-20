import os
import pandas as pd
from lxml import etree
from dateutil import parser as dtparse
from pathlib import Path

DATA_FILE = "data/aoe_interaction_log_1_of_10.xes"
OUT_DIR = Path("warehouse")
OUT_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_PATH = OUT_DIR / "events_raw.parquet"

def main():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(f"Missing {DATA_FILE}. Put your XES file there.")

    # Parse XML with lxml
    with open(DATA_FILE, "rb") as f:
        tree = etree.parse(f)
    root = tree.getroot()

    # XES namespace
    ns = {"xes": "http://www.xes-standard.org/"}

    rows = []
    # XES: <log><trace>...<event/>...</trace>...</log>
    debug_trace_counter = 0

    # Case level attributes
    for trace in root.findall("xes:trace", ns):
        trace_attrs = {}
        for string in trace.findall("xes:string", ns):
            trace_attrs[string.attrib["key"]] = string.attrib.get("value")
        for fl in trace.findall("xes:float", ns):
            trace_attrs[fl.attrib["key"]] = fl.attrib.get("value")
        for it in trace.findall("xes:int", ns):
            trace_attrs[it.attrib["key"]] = it.attrib.get("value")
        debug_trace_counter += 1
        case_id = trace_attrs.get("concept:name")  # unique ID for this trace


        # Event-level attributes
        for event in trace.findall("xes:event", ns):
            ev = {"case_id": case_id}
            for string in event.findall("xes:string", ns):
                k, v = string.attrib.get("key"), string.attrib.get("value")
                if k == "concept:name":
                    ev["activity"] = v  # <- THIS is the event activity
                else:
                    ev[k] = v
                #ev[string.attrib["key"]] = string.attrib.get("value")
            for fl in event.findall("xes:float", ns):
                ev[fl.attrib["key"]] = fl.attrib.get("value")
            for it in event.findall("xes:int", ns):
                ev[it.attrib["key"]] = it.attrib.get("value")
            for date in event.findall("xes:date", ns):
                ev["ts"] = date.attrib.get("value")

            # stable event_id: prefer @@index if present
            idx = ev.get("@@index")
            if idx is None:
                idx = str(len(rows))
            ev["event_id"] = f"{case_id}-{idx}"

            # merge case attributes so each row has full context
            ev.update(trace_attrs)
            rows.append(ev)


    df = pd.DataFrame(rows)

    # Basic column order
    preferred = [
        "event_id", "ts", "activity",
        "match_id", "player_id", "map_type", "civilization", "civilization_category", "elo",
        "case_id", "strategy", "win", "amount", "@@index", "@@case_index"
    ]
    # Reorder when present
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]

    # Coerce ts to datetime
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")

    # Save Bronze
    df.to_parquet(BRONZE_PATH, index=False)
    print(f"✅ Parsed {debug_trace_counter} traces with {len(df):,} events")


if __name__ == "__main__":
    main()
