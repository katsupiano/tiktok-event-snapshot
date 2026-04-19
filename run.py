"""
Orchestrator: discover active events → scrape each (alpha+ulysses) → save → optional Notion sync.

Usage:
    python3 run.py                           # auto-discover + scrape all active events
    python3 run.py --event-ids=ID1,ID2       # specify events explicitly
    python3 run.py --skip-notion             # no Notion writes
    python3 run.py --window-days=2           # tolerance for "active"
    NOTION_TOKEN env var required for Notion sync

Env:
    STORAGE_BCODE   path to bcode session (default ../realtime_scraper/storage_state.json)
    STORAGE_ULYSSES path to ulysses session
    HEADLESS        1/0 (default 1)
"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict

BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))

from scrape_event import scrape_event_with_session, merge_agency_results, build_summary
from discover_events import discover

REALTIME = BASE.parent / "realtime_scraper"
EVENTS_DIR = BASE / "events"
JST = timezone(timedelta(hours=9))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-ids", default="", help="Comma-separated event IDs; skip discovery")
    parser.add_argument("--skip-notion", action="store_true")
    parser.add_argument("--window-days", type=int, default=2)
    parser.add_argument("--only-with-katsu", action="store_true", default=True,
                        help="Skip Notion sync for events with 0 KATSU participants (default on)")
    parser.add_argument("--headless", default=os.environ.get("HEADLESS", "1"))
    args = parser.parse_args()

    headless = args.headless != "0"

    storage_bcode = Path(os.environ.get("STORAGE_BCODE", str(REALTIME / "storage_state.json")))
    storage_ulysses = Path(os.environ.get("STORAGE_ULYSSES", str(REALTIME / "storage_state_ulysses.json")))

    if not storage_bcode.exists():
        print(f"❌ bcode session not found: {storage_bcode}")
        sys.exit(1)

    # 1. Discover
    if args.event_ids:
        event_list = [{"eventId": eid.strip(), "name": ""} for eid in args.event_ids.split(",") if eid.strip()]
        print(f"[discover] using {len(event_list)} explicit event IDs")
    else:
        print(f"[discover] via bcode session, window ±{args.window_days}d")
        event_list = discover(storage_bcode, "alpha", headless=headless, include_window_days=args.window_days)
        now = int(datetime.now(JST).timestamp())
        # Keep only in-progress
        event_list = [e for e in event_list if (e.get("start") or 0) <= now <= (e.get("end") or 0)]
        print(f"[discover] {len(event_list)} in-progress events")

    if not event_list:
        print("[discover] nothing to scrape")
        return

    # 2. Scrape each event from both agencies
    agency_sessions = [("alpha", storage_bcode)]
    if storage_ulysses.exists():
        agency_sessions.append(("ulysses", storage_ulysses))
    else:
        print(f"⚠️  ulysses session not found ({storage_ulysses}) — scraping alpha only")

    all_summaries: List[Dict] = []
    ts = datetime.now(JST).strftime("%Y%m%dT%H%M")
    date_str = datetime.now(JST).strftime("%Y-%m-%d")

    for ev in event_list:
        event_id = ev["eventId"]
        print(f"\n{'='*60}\n[event] {event_id}  {ev.get('name','')}\n{'='*60}")
        agency_results = []
        for label, path in agency_sessions:
            try:
                r = scrape_event_with_session(event_id, path, label, headless=headless)
                hosts_n = len(r.get("hosts") or {})
                print(f"[{label}] collected {hosts_n} hosts")
                agency_results.append(r)
            except Exception as e:
                print(f"[{label}] scrape err: {e}")
                traceback.print_exc()

        if not agency_results:
            continue

        merged = merge_agency_results(agency_results)
        summary = build_summary(event_id, merged)
        summary["snapshotTimestamp"] = ts
        summary["snapshotDate"] = date_str

        ev_dir = EVENTS_DIR / event_id
        ev_dir.mkdir(parents=True, exist_ok=True)
        (ev_dir / f"summary_{ts}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        (ev_dir / "latest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(f"[save] {ev_dir.name}/summary_{ts}.json  total={summary['totalParticipants']} KATSU={summary['katsuCount']}")
        all_summaries.append(summary)

    # 3. Notion sync
    if not args.skip_notion and os.environ.get("NOTION_TOKEN"):
        try:
            from notion_sync import sync_summaries
            relevant = [s for s in all_summaries if not args.only_with_katsu or s["katsuCount"] > 0]
            print(f"\n[notion] syncing {len(relevant)} event summaries…")
            sync_summaries(relevant)
        except Exception as e:
            print(f"[notion] err: {e}")
            traceback.print_exc()
    else:
        reason = "--skip-notion" if args.skip_notion else "no NOTION_TOKEN"
        print(f"\n[notion] skipped ({reason})")

    # 4. Global snapshot index
    idx_path = EVENTS_DIR / "snapshots_index.json"
    idx = {"lastRun": ts, "date": date_str, "events": []}
    if idx_path.exists():
        try:
            idx = json.loads(idx_path.read_text())
        except Exception:
            pass
    idx["lastRun"] = ts
    idx["date"] = date_str
    idx["events"] = [
        {
            "eventId": s["eventId"],
            "name": s["eventName"],
            "total": s["totalParticipants"],
            "katsu": s["katsuCount"],
        }
        for s in all_summaries
    ]
    idx_path.write_text(json.dumps(idx, ensure_ascii=False, indent=2))
    print(f"\n[done] run complete @ {ts}: {len(all_summaries)} events scraped")


if __name__ == "__main__":
    main()
