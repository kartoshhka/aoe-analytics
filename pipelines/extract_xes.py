import os
import pandas as pd
from lxml import etree
from dateutil import parser as dtparse
from pathlib import Path

DATA_DIR = Path("data")
OUT_DIR = Path("warehouse")
OUT_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_PATH = OUT_DIR / "events_raw.parquet"

def parse_xes_file(xes_path):
    ns = {"xes": "http://www.xes-standard.org/"}
    rows = []
    with open(xes_path, "rb") as f:
        tree = etree.parse(f)
    root = tree.getroot()
    for trace in root.findall("xes:trace", ns):
        trace_attrs = {}
        for string in trace.findall("xes:string", ns):
            trace_attrs[string.attrib["key"]] = string.attrib.get("value")
        for fl in trace.findall("xes:float", ns):
            trace_attrs[fl.attrib["key"]] = fl.attrib.get("value")
        for it in trace.findall("xes:int", ns):
            trace_attrs[it.attrib["key"]] = it.attrib.get("value")
        case_id = trace_attrs.get("concept:name")
        for event in trace.findall("xes:event", ns):
            ev = {"case_id": case_id}
            for string in event.findall("xes:string", ns):
                k, v = string.attrib.get("key"), string.attrib.get("value")
                if k == "concept:name":
                    ev["activity"] = v
                else:
                    ev[k] = v
            for fl in event.findall("xes:float", ns):
                ev[fl.attrib["key"]] = fl.attrib.get("value")
            for it in event.findall("xes:int", ns):
                ev[it.attrib["key"]] = it.attrib.get("value")
            for date in event.findall("xes:date", ns):
                ev["ts"] = date.attrib.get("value")
            idx = ev.get("@@index")
            if idx is None:
                idx = str(len(rows))
            ev["event_id"] = f"{case_id}-{idx}"
            ev.update(trace_attrs)
            rows.append(ev)
    return rows

def main():
    xes_files = list(DATA_DIR.glob("*.xes"))
    if not xes_files:
        raise FileNotFoundError(f"No .xes files found in {DATA_DIR}")
    all_rows = []
    for xes_file in xes_files:
        print(f"Processing {xes_file} ...")
        rows = parse_xes_file(xes_file)
        all_rows.extend(rows)
    df = pd.DataFrame(all_rows)
    preferred = [
        "event_id", "ts", "activity",
        "match_id", "player_id", "map_type", "civilization", "civilization_category", "elo",
        "case_id", "strategy", "win", "amount", "@@index", "@@case_index"
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    if "ts" in df.columns:
        df["ts"] = pd.to_datetime(df["ts"], errors="coerce")
    df.to_parquet(BRONZE_PATH, index=False)
    print(f"✅ Parsed {len(xes_files)} files with {len(df):,} events")

if __name__ == "__main__":
    main()
