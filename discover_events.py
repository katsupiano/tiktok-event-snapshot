"""
Discover active (in-progress) events on Backstage for an agency.

Uses activity_list API called from the activity portal page to enumerate all events.
Filters by ActivityStartTime/ActivityEndTime against now.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict

from playwright.sync_api import sync_playwright

BASE = Path(__file__).parent
REALTIME = BASE.parent / "realtime_scraper"
STORAGE_BCODE = REALTIME / "storage_state.json"

ACTIVITY_LIST_PATH = "/creators/live/union_platform_api/agency/activity/host/activity_list/"
# activity_list XHR only fires from fest-activity-detail pages.
# Use a known past event as a trampoline to trigger the list fetch.
SEED_EVENT_ID = "7621548004184670264"  # 4月の登竜門スタート (ended 2026-04-18)
ACTIVITY_PORTAL_URL = f"https://live-backstage.tiktok.com/portal/tools/activity/fest-activity-detail?id={SEED_EVENT_ID}"

JST = timezone(timedelta(hours=9))


def discover(storage_path: Path, agency_label: str = "alpha", headless: bool = True, include_window_days: int = 2) -> List[Dict]:
    """Return events whose period overlaps with [now - window, now + window]."""
    activities: List[Dict] = []

    def on_req(req):
        try:
            if ACTIVITY_LIST_PATH not in req.url:
                return
            resp = req.response()
            if not resp or resp.status != 200:
                return
            data = json.loads(resp.text())
            if data.get("BaseResp", {}).get("StatusCode") != 0:
                return
            acts = data.get("ActivityList") or []
            if acts and not activities:
                activities.extend(acts)
        except Exception:
            pass

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(storage_state=str(storage_path), locale="ja-JP", viewport={"width": 1400, "height": 900})
        page = context.new_page()
        page.on("requestfinished", on_req)
        try:
            page.goto(ACTIVITY_PORTAL_URL, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            time.sleep(5)
        except Exception as e:
            print(f"[discover/{agency_label}] nav err: {e}")
        try:
            context.storage_state(path=str(storage_path))
        except Exception:
            pass
        browser.close()

    now = int(datetime.now(JST).timestamp())
    window_sec = include_window_days * 86400
    active = []
    for a in activities:
        start = int(a.get("ActivityStartTime", 0) or 0)
        end = int(a.get("ActivityEndTime", 0) or 0)
        pid = a.get("PlatformActivityID") or a.get("AgencyActivityID")
        if not pid:
            continue
        # Include if event ends after (now - window) and starts before (now + window)
        if end < now - window_sec:
            continue
        if start > now + window_sec:
            continue
        active.append({
            "eventId": pid,
            "name": a.get("ActivityName", ""),
            "start": start,
            "end": end,
            "status": a.get("ActivityStatus"),
            "hasGameplay": a.get("HasGameplay"),
            "liveHostCount": a.get("LiveHostCount"),
            "registeredHostCount": a.get("RegisteredHostCount"),
        })
    active.sort(key=lambda e: e["start"])
    return active


def main():
    storage_path = Path(os.environ.get("STORAGE_STATE", str(STORAGE_BCODE)))
    label = os.environ.get("AGENCY_LABEL", "alpha")
    headless = os.environ.get("HEADLESS", "1") != "0"
    window = int(os.environ.get("WINDOW_DAYS", "2"))

    if not storage_path.exists():
        print(f"❌ {storage_path} not found")
        sys.exit(1)

    events = discover(storage_path, label, headless=headless, include_window_days=window)
    now = int(datetime.now(JST).timestamp())
    print(f"\n=== {label} active/recent events (window ±{window}d, {len(events)} hits) ===")
    for e in events:
        s = datetime.fromtimestamp(e["start"], JST).strftime("%m/%d %H:%M") if e["start"] else "?"
        end = datetime.fromtimestamp(e["end"], JST).strftime("%m/%d %H:%M") if e["end"] else "?"
        state = "進行中" if e["start"] <= now <= e["end"] else ("未開始" if now < e["start"] else "終了")
        print(f"  [{state}] {e['eventId']}  {s}〜{end}  登録{e.get('registeredHostCount','-')} 配信{e.get('liveHostCount','-')}  {e['name']}")

    # Also print as JSON for pipelining
    print("\n--- JSON ---")
    print(json.dumps(events, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
