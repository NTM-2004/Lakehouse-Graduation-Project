#!/usr/bin/env python3
"""build_lead_json.py - Olist marketing funnel CSVs -> nested JSONL batches (stdlib only, no pip).

Input : olist_marketing_qualified_leads_dataset.csv, olist_closed_deals_dataset.csv
Output: <output-root>/<batch>/marketing_leads.jsonl + _SUCCESS   (same batches as BATCH_SPLIT_GUIDE.md)

One line = one lead. If the lead has a closed deal, it is nested under "deal".
A missing value (no deal, empty origin, empty field) = the KEY IS ABSENT, not null.

Batch rules (same boundaries as the other two sources):
  - a lead is written in the batch of its first_contact_date (here: all in bulk),
    carrying its deal only if the deal was won in that same batch;
  - a deal won in a LATER batch re-emits its lead (with the deal) in that batch
    -> the same mql_id can appear in several batches; Silver keeps the latest.
  - deals won after 2018-08-31 are dropped (50) unless --include-post-window.

    python json_pipeline/build_lead_json.py --src raw/json/archive --output-root raw/marketing_funnel
"""
import argparse
import csv
import json
import os
from datetime import datetime

BULK = "bulk_2016-09_2018-05"
BATCHES = [  # (name, start, end) inclusive - identical to BATCH_BOUNDARIES in the other scripts
    (BULK, None, datetime(2018, 5, 31, 23, 59, 59)),
    ("2018-06", datetime(2018, 6, 1), datetime(2018, 6, 30, 23, 59, 59)),
    ("2018-07", datetime(2018, 7, 1), datetime(2018, 7, 31, 23, 59, 59)),
    ("2018-08", datetime(2018, 8, 1), datetime(2018, 8, 31, 23, 59, 59)),
]
OUT_FILE = "marketing_leads.jsonl"
LEADS_CSV = "olist_marketing_qualified_leads_dataset.csv"
DEALS_CSV = "olist_closed_deals_dataset.csv"


def batch_of(ts):
    for name, start, end in BATCHES:
        if (start is None or ts >= start) and ts <= end:
            return name
    return None


def parse_ts(s):
    s = s.strip()
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S" if " " in s else "%Y-%m-%d")


def compact(d):
    """Drop empty values -> key absent."""
    return {k: v.strip() for k, v in d.items() if v is not None and v.strip() != ""}


def read_csv(path):
    with open(path, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="raw/json/archive")
    ap.add_argument("--output-root", default="raw/marketing_funnel")
    ap.add_argument("--include-post-window", action="store_true",
                    help="fold deals won after 2018-08-31 into batch 2018-08")
    a = ap.parse_args()

    leads = {r["mql_id"]: r for r in read_csv(os.path.join(a.src, LEADS_CSV))}
    deals = read_csv(os.path.join(a.src, DEALS_CSV))
    last = BATCHES[-1][0]

    out = {name: {} for name, _, _ in BATCHES}          # batch -> {mql_id: record}

    def lead_record(mql_id):
        rec = compact({k: v for k, v in leads[mql_id].items()})
        return rec

    for mql_id, r in leads.items():                      # leads by first_contact_date
        b = batch_of(parse_ts(r["first_contact_date"]))
        if b is None:
            print(f"[warn] lead {mql_id} first_contact_date outside all batches - skipped")
            continue
        out[b][mql_id] = lead_record(mql_id)

    dropped, seen = 0, set()
    for d in deals:                                      # deals by won_date
        mql_id = d["mql_id"]
        if mql_id in seen:
            print(f"[warn] duplicate deal for mql_id {mql_id} - keeping first")
            continue
        seen.add(mql_id)
        if mql_id not in leads:
            print(f"[warn] deal {mql_id} has no matching lead - skipped")
            continue
        b = batch_of(parse_ts(d["won_date"]))
        if b is None:
            if a.include_post_window:
                b = last
            else:
                dropped += 1
                continue
        rec = out[b].get(mql_id) or lead_record(mql_id)
        rec["deal"] = compact({k: v for k, v in d.items() if k != "mql_id"})
        out[b][mql_id] = rec

    for name, _, _ in BATCHES:
        folder = os.path.join(a.output_root, name)
        os.makedirs(folder, exist_ok=True)
        path = os.path.join(folder, OUT_FILE)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            for rec in out[name].values():
                f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        open(os.path.join(folder, "_SUCCESS"), "w").close()     # written last = batch ready
        n_deal = sum(1 for r in out[name].values() if "deal" in r)
        print(f"[+] {name:22s} leads={len(out[name]):>5,}  with_deal={n_deal:>4,}  -> {path}")
    if dropped:
        print(f"[!] {dropped} deals won after 2018-08-31 dropped (use --include-post-window to keep)")


if __name__ == "__main__":
    main()