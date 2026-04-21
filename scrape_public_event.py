"""
TikTok public event ranking scraper.

⚠️ 2026-04-21 保留: web版は top 200 × 1部門しか取れない（TikTokアプリ誘導で制限）。
Backstage一本化に方針変更。run.py 未統合。将来 Android UA偽装等で再開の可能性。

For events hosted on tiktok.com/falcon/campaign/fusion_cosmic_runtime/ (TikTok official campaigns).
API: POST /webcast/activity/dispatchv2/ returns user_rank__list_lead_players with top 200 by score.

Returns same dict shape as scrape_event.py's scrape_event_with_session, so run.py can reuse it.
"""
import json, os, sys, time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright

BASE = Path(__file__).parent
REALTIME = BASE.parent / "realtime_scraper"
MONTHLY_INTERNAL = REALTIME / "ranking_realtime_internal.json"

JST = timezone(timedelta(hours=9))


def _katsu_uids() -> set:
    """Load KATSU creators' TikTok user IDs (= anchorId in monthly data) from latest monthly snapshot."""
    if not MONTHLY_INTERNAL.exists():
        return set()
    try:
        data = json.loads(MONTHLY_INTERNAL.read_text())
    except Exception:
        return set()
    return {c["anchorId"] for c in data.get("creators", []) if c.get("managerDisplay") == "滝澤" and c.get("anchorId")}


def scrape_public_event(url: str, headless: bool = True) -> dict:
    """Scrape a TikTok public campaign URL.
    URL can be vt.tiktok.com shortlink OR direct falcon/campaign URL.
    Returns {eventId, hosts: {uid: entry}, capturedAt, eventInfo} in a Backstage-compatible shape.
    """
    captures: List[dict] = []
    activity_id = None

    def on_req(req):
        try:
            if "dispatchv2" not in req.url:
                return
            resp = req.response()
            if not resp or resp.status != 200:
                return
            data = json.loads(resp.text())
            d = data.get("data") or {}
            if any("list_lead_players" in k for k in d.keys()):
                captures.append(d)
        except Exception:
            pass

    with sync_playwright() as p:
        b = p.chromium.launch(headless=headless)
        ctx = b.new_context(
            locale="ja-JP",
            viewport={"width": 390, "height": 844},
            user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
        )
        page = ctx.new_page()
        page.on("requestfinished", on_req)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_load_state("networkidle", timeout=30000)
        except Exception:
            pass
        # Extract activityId from final URL
        final_url = page.url
        qs = parse_qs(urlparse(final_url).query)
        activity_id = (qs.get("activityId") or [None])[0]
        time.sleep(3)
        # Click ランキング tab to trigger list_lead_players XHR
        for kw in ["ランキング", "Leaderboard", "順位"]:
            try:
                el = page.get_by_text(kw).first
                if el.is_visible(timeout=2000):
                    el.click(timeout=5000)
                    time.sleep(5)
                    break
            except Exception:
                pass
        b.close()

    # Parse the lead_players data
    hosts: Dict[str, dict] = {}
    for c in captures:
        for k, v in c.items():
            if "list_lead_players" not in k or not isinstance(v, list):
                continue
            for blob in v:
                players = blob.get("players") or []
                for p in players:
                    uid = p.get("uid") or ""
                    if not uid:
                        continue
                    ui = p.get("user_info") or {}
                    avatar_urls = ((ui.get("avatar") or {}).get("url_list") or [])
                    hosts[uid] = {
                        "HostID": uid,
                        "Agent": "",  # not known from public API
                        "HostIncomeTier": None,
                        "LeaderboardData": {
                            "Rank": str(p.get("rank") or 0),
                            "Score": p.get("score") or "0",
                            "BehindBy": "0",
                        },
                        "_rawPlayer": p,
                    }
    user_map: Dict[str, dict] = {}
    for uid, h in hosts.items():
        p = h.pop("_rawPlayer", {})
        ui = p.get("user_info") or {}
        avatar_urls = ((ui.get("avatar") or {}).get("url_list") or [])
        user_map[uid] = {
            "display_id": ui.get("handle_name", ""),
            "nickname": ui.get("nickname", "") or ui.get("handle_name", ""),
            "avatar": avatar_urls[0] if avatar_urls else "",
            "IsLive": False,
        }

    event_info = {
        "ActivityName": "(TikTok公開ランキング)",
        "PlatformActivityID": activity_id,
        "ActivityStartTime": None,
        "ActivityEndTime": None,
    }

    return {
        "eventId": activity_id or "",
        "agencyLabel": "tiktok-public",
        "eventInfo": event_info,
        "componentTotals": {"public": len(hosts)},
        "capturedAt": datetime.now(JST).isoformat(timespec="seconds"),
        "hosts": hosts,
        "userMap": user_map,
    }


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 scrape_public_event.py <URL>")
        sys.exit(1)
    url = sys.argv[1]
    headless = os.environ.get("HEADLESS", "1") != "0"
    result = scrape_public_event(url, headless=headless)
    hosts = result["hosts"]
    katsu_uids = _katsu_uids()
    print(f"activity_id={result['eventId']}  total_hosts={len(hosts)}  katsu_set={len(katsu_uids)}")
    # Show top 10 overall
    items = sorted(hosts.values(), key=lambda h: int(h["LeaderboardData"]["Rank"]))[:10]
    print("\nTop 10 overall:")
    for h in items:
        uid = h["HostID"]
        ui = result["userMap"].get(uid, {})
        rank = h["LeaderboardData"]["Rank"]
        score = int(h["LeaderboardData"]["Score"])
        tag = " 🎯KATSU" if uid in katsu_uids else ""
        print(f"  #{rank:<4} @{ui.get('display_id','?'):<25} {score:>12,}pt  uid={uid}{tag}")
    # KATSU subset
    katsu_in = [h for uid, h in hosts.items() if uid in katsu_uids]
    print(f"\nKATSU 該当者 ({len(katsu_in)}名):")
    for h in sorted(katsu_in, key=lambda h: int(h["LeaderboardData"]["Rank"])):
        uid = h["HostID"]
        ui = result["userMap"].get(uid, {})
        print(f"  #{h['LeaderboardData']['Rank']:<4} @{ui.get('display_id','?')} {int(h['LeaderboardData']['Score']):,}pt")


if __name__ == "__main__":
    main()
