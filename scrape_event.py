"""
Backstage fest-activity event ranking scraper.

Exposes scrape_event(event_id, storage_path, agency_label) library function.
CLI mode scrapes a single event with default bcode session.

Key findings (see BACKSTAGE_EVENT_API.md):
- POST /agency/activity/host/host_data/ with body {ActivityID, ComponentID, Offset, Limit, Page, SelectItemMap:{}}
- Multiple ComponentIDs per event (sub-leaderboards: tier-based + overall)
- Agent field contains manager email → filter directly
- UI-driven pagination only (page.evaluate fetch blocked by bot protection)
- Semi UI: Page N buttons don't fire XHR, only Next/Prev work
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from playwright.sync_api import sync_playwright, Browser, BrowserContext, Page

BASE = Path(__file__).parent
REALTIME = BASE.parent / "realtime_scraper"
EVENTS_DIR = BASE / "events"

JST = timezone(timedelta(hours=9))

KATSU_AGENTS = {
    "katsuaki.takizawa.bcode@gmail.com",
    "info@sinceed.co.jp",
    "katsuaki.takizawa.grove@gmail.com",
    "katsuaki.takizawa@bcode.co.jp",
}

API_PATH = "/creators/live/union_platform_api/agency/activity/host/host_data/"
ACTIVITY_LIST_PATH = "/creators/live/union_platform_api/agency/activity/host/activity_list/"


def is_katsu(agent: str) -> bool:
    return agent in KATSU_AGENTS


def _block_max_page(block) -> int:
    try:
        t = block.inner_text(timeout=2000)
    except Exception:
        return 0
    nums = []
    for tok in t.replace("\n", " ").split():
        if tok.isdigit():
            nums.append(int(tok))
    return max(nums) if nums else 0


def scrape_event_with_session(event_id: str, storage_path: Path, agency_label: str, headless: bool = True) -> dict:
    """Scrape one event with one agency session.
    Returns dict: {eventId, eventInfo, componentTotals, capturedAt, hosts: {HostID: entry}, userMap, agencyLabel}.
    """
    captures: Dict[tuple, dict] = {}
    event_info: Dict = {}
    component_totals: Dict[str, int] = {}

    def on_request_finished(req):
        try:
            u = req.url
            resp = req.response()
            if not resp or resp.status != 200:
                return
            if API_PATH in u:
                body_str = req.post_data or "{}"
                body = json.loads(body_str)
                cid = body.get("ComponentID")
                offset = int(body.get("Offset", 0) or 0)
                limit = int(body.get("Limit", 0) or 0)
                data = json.loads(resp.text())
                if data.get("BaseResp", {}).get("StatusCode") != 0:
                    return
                captures[(cid, offset, limit)] = data
                if cid:
                    component_totals[cid] = int(data.get("Total", 0) or 0)
            elif ACTIVITY_LIST_PATH in u:
                data = json.loads(resp.text())
                if data.get("BaseResp", {}).get("StatusCode") != 0:
                    return
                for act in data.get("ActivityList") or []:
                    if act.get("PlatformActivityID") == event_id or act.get("AgencyActivityID") == event_id:
                        event_info.update(act)
                        break
        except Exception:
            pass

    def has_key(cid: str, offset: int, limit: int) -> bool:
        return (cid, offset, limit) in captures

    def wait_for_any_new(before_n: int, timeout_sec: float = 8.0) -> bool:
        t = 0.0
        while t < timeout_sec:
            time.sleep(0.3)
            t += 0.3
            if len(captures) > before_n:
                return True
        return False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(storage_state=str(storage_path), locale="ja-JP", viewport={"width": 1600, "height": 1200})
        page = context.new_page()
        page.on("requestfinished", on_request_finished)

        url = f"https://live-backstage.tiktok.com/portal/tools/activity/fest-activity-detail?id={event_id}"
        print(f"[{agency_label}] nav: {url}")
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            time.sleep(5)
        except Exception as e:
            print(f"[{agency_label}] nav err: {e}")
            browser.close()
            return {
                "eventId": event_id,
                "agencyLabel": agency_label,
                "error": str(e),
                "hosts": {},
                "userMap": {},
                "componentTotals": {},
                "eventInfo": {},
                "capturedAt": datetime.now(JST).isoformat(timespec="seconds"),
            }

        if not component_totals:
            print(f"[{agency_label}] no host_data XHRs — skipping UI ops")
        else:
            print(f"[{agency_label}] components: {component_totals}")
            # Paginate each block via Next clicks
            try:
                pag_blocks = page.locator(".semi-page").all()
            except Exception:
                pag_blocks = []
            print(f"[{agency_label}] {len(pag_blocks)} pagination blocks")

            for idx, pag in enumerate(pag_blocks):
                mp = _block_max_page(pag)
                if mp <= 1:
                    continue
                try:
                    pag.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(0.3)
                    this_cid = None
                    for click_num in range(1, mp):
                        before_n = len(captures)
                        try:
                            pag.locator('li.semi-page-next[aria-label="Next"]:not([aria-disabled="true"])').click(timeout=6000)
                        except Exception:
                            break
                        if not wait_for_any_new(before_n, timeout_sec=12):
                            continue
                        new_keys = list(captures.keys())[before_n:]
                        for (cid, off2, lim2) in new_keys:
                            if this_cid is None and lim2 == 10:
                                this_cid = cid
                        time.sleep(0.2)
                    if this_cid:
                        print(f"  pag[{idx}] CID={this_cid} Total={component_totals.get(this_cid)}")
                except Exception as e:
                    print(f"  pag[{idx}] err: {e}")

        # Merge (prefer entry with Rank > 0)
        host_by_id: Dict[str, dict] = {}
        user_by_id: Dict[str, dict] = {}
        for (cid, offset, limit), data in captures.items():
            for h in data.get("HostDataList") or []:
                hid = h.get("HostID")
                if not hid:
                    continue
                lb = h.get("LeaderboardData") or {}
                this_rank = int(lb.get("Rank", 0) or 0)
                h2 = dict(h)
                h2["_componentId"] = cid
                prev = host_by_id.get(hid)
                if prev is None:
                    host_by_id[hid] = h2
                else:
                    prev_rank = int((prev.get("LeaderboardData") or {}).get("Rank", 0) or 0)
                    if this_rank > 0 and prev_rank == 0:
                        host_by_id[hid] = h2
            user_by_id.update(data.get("UserBaseInfoMap") or {})

        # Refresh session
        try:
            context.storage_state(path=str(storage_path))
        except Exception:
            pass
        browser.close()

    return {
        "eventId": event_id,
        "agencyLabel": agency_label,
        "eventInfo": event_info,
        "componentTotals": component_totals,
        "capturedAt": datetime.now(JST).isoformat(timespec="seconds"),
        "hosts": host_by_id,
        "userMap": user_by_id,
    }


def merge_agency_results(results: List[dict]) -> dict:
    """Merge results from multiple agency scrapes of the same event."""
    if not results:
        return {}
    # Event info from whichever succeeded first
    event_info = {}
    component_totals_by_agency = {}
    host_by_id: Dict[str, dict] = {}
    user_by_id: Dict[str, dict] = {}
    for r in results:
        if not event_info:
            event_info = r.get("eventInfo") or {}
        agency = r.get("agencyLabel", "")
        component_totals_by_agency[agency] = r.get("componentTotals", {})
        user_by_id.update(r.get("userMap") or {})
        for hid, h in (r.get("hosts") or {}).items():
            h2 = dict(h)
            h2["_agencyLabel"] = agency
            prev = host_by_id.get(hid)
            if prev is None:
                host_by_id[hid] = h2
            else:
                # Prefer entry with non-zero rank
                prev_rank = int((prev.get("LeaderboardData") or {}).get("Rank", 0) or 0)
                this_rank = int((h.get("LeaderboardData") or {}).get("Rank", 0) or 0)
                if this_rank > 0 and prev_rank == 0:
                    host_by_id[hid] = h2
    return {
        "eventInfo": event_info,
        "componentTotalsByAgency": component_totals_by_agency,
        "hosts": host_by_id,
        "userMap": user_by_id,
        "capturedAt": datetime.now(JST).isoformat(timespec="seconds"),
    }


def build_summary(event_id: str, merged: dict) -> dict:
    """Build KATSU-filtered summary from merged multi-agency data."""
    hosts = merged.get("hosts", {})
    user_map = merged.get("userMap", {})
    event_info = merged.get("eventInfo", {})

    all_entries = []
    katsu_entries = []
    for hid, h in hosts.items():
        agent = h.get("Agent", "")
        lb = h.get("LeaderboardData") or {}
        info = user_map.get(hid, {})
        rec = {
            "hostId": hid,
            "username": info.get("display_id", ""),
            "nickname": info.get("nickname", ""),
            "avatar": info.get("avatar", ""),
            "agent": agent,
            "agencyLabel": h.get("_agencyLabel", ""),
            "componentId": h.get("_componentId", ""),
            "hostIncomeTier": h.get("HostIncomeTier"),
            "rank": int(lb.get("Rank", 0) or 0),
            "score": int(lb.get("Score", 0) or 0),
            "behindBy": int(lb.get("BehindBy", 0) or 0),
            "isLive": info.get("IsLive", False),
        }
        all_entries.append(rec)
        if is_katsu(agent):
            katsu_entries.append(rec)

    all_entries.sort(key=lambda r: r["rank"] if r["rank"] > 0 else 999999)
    katsu_entries.sort(key=lambda r: r["rank"] if r["rank"] > 0 else 999999)

    return {
        "eventId": event_id,
        "eventName": event_info.get("ActivityName", ""),
        "eventStart": event_info.get("ActivityStartTime"),
        "eventEnd": event_info.get("ActivityEndTime"),
        "componentTotalsByAgency": merged.get("componentTotalsByAgency", {}),
        "capturedAt": merged.get("capturedAt"),
        "totalParticipants": len(all_entries),
        "katsuCount": len(katsu_entries),
        "katsuParticipants": katsu_entries,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scrape_event.py <EVENT_ID>")
        sys.exit(1)
    event_id = sys.argv[1]

    storage_path = REALTIME / "storage_state.json"
    if not storage_path.exists():
        print(f"❌ {storage_path} not found")
        sys.exit(1)

    headless = os.environ.get("HEADLESS", "1") != "0"
    ev_dir = EVENTS_DIR / event_id
    ev_dir.mkdir(parents=True, exist_ok=True)

    result = scrape_event_with_session(event_id, storage_path, "alpha", headless=headless)
    merged = merge_agency_results([result])
    summary = build_summary(event_id, merged)

    ts = datetime.now(JST).strftime("%Y%m%dT%H%M")
    (ev_dir / f"summary_{ts}.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    (ev_dir / "latest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    print(f"\n=== {summary['eventName']} | {summary['totalParticipants']}名中 KATSU {summary['katsuCount']}名 ===")
    for e in summary["katsuParticipants"]:
        tier = f"T{e['hostIncomeTier']}" if e.get('hostIncomeTier') else "--"
        print(f"  #{e['rank']:>4}  {e['nickname']:<20}  @{e['username']:<20}  {e['score']:>10,}pt  {tier}  [{e['agencyLabel']}]")


if __name__ == "__main__":
    main()
