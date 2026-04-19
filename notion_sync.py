"""
Notion sync: upsert event snapshot rows into イベント実績 DB.

Strategy:
1. Load karte DB (クリエイターカルテ DB) → map TikTok ID → karte page_id
2. For each KATSU participant in each event:
   - Compute unique key = (eventId, hostId, date)
   - If a row already exists with that key → update (PATCH)
   - Else → create (POST)
3. "フェーズ" auto-derived from event start/end vs today

Environment:
    NOTION_TOKEN            Notion internal integration token (required)
    NOTION_KARTE_DB_ID      default: e65495b5dfc64a38a90018e40aaeeeab
    NOTION_RESULTS_DB_ID    default: 94bf21fce78f4fc093a128313a6c8da3
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import urllib.request
import urllib.error

JST = timezone(timedelta(hours=9))

NOTION_API = "https://api.notion.com/v1"
NOTION_VERSION = "2022-06-28"

KARTE_DB_ID = os.environ.get("NOTION_KARTE_DB_ID") or "e65495b5dfc64a38a90018e40aaeeeab"
RESULTS_DB_ID = os.environ.get("NOTION_RESULTS_DB_ID") or "94bf21fce78f4fc093a128313a6c8da3"

# Property names on カルテ DB used to match creator
KARTE_TITLE_PROP = "クリエイター名"
KARTE_TIKTOK_ID_PROP = "クリエイターID"

# Property names on イベント実績 DB
RES_TITLE = "タイトル"
RES_CREATOR = "クリエイター"
RES_EVENT_NAME = "イベント名"
RES_EVENT_ID = "イベントID"
RES_DATE = "日付"
RES_RANK = "順位"
RES_SCORE = "スコア"
RES_DIAMONDS = "ダイヤ"
RES_PK = "PKダイヤ"
RES_TIER = "Tier"
RES_PHASE = "フェーズ"
RES_CAPTURED_AT = "取得時刻"
RES_AGENCY = "エージェンシー"
RES_START = "開始日"
RES_END = "終了日"


def _token() -> str:
    tok = os.environ.get("NOTION_TOKEN")
    if not tok:
        raise RuntimeError("NOTION_TOKEN env var required")
    return tok


def _request(method: str, path: str, body: Optional[dict] = None, retries: int = 3) -> dict:
    """Simple Notion API request with retry on rate limits."""
    url = f"{NOTION_API}{path}"
    headers = {
        "Authorization": f"Bearer {_token()}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }
    data_bytes = json.dumps(body).encode("utf-8") if body is not None else None
    last_err = None
    for attempt in range(retries):
        req = urllib.request.Request(url, data=data_bytes, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            msg = e.read().decode("utf-8") if e.fp else ""
            if e.code == 429 and attempt < retries - 1:
                wait = 2 ** attempt
                print(f"[notion] 429 rate-limited, retry in {wait}s")
                time.sleep(wait)
                continue
            last_err = RuntimeError(f"HTTP {e.code}: {msg}")
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                time.sleep(1)
                continue
        break
    raise last_err  # type: ignore


def load_karte_index() -> Dict[str, str]:
    """Return {tiktok_id_lower: karte_page_id} map from クリエイターカルテ DB."""
    idx: Dict[str, str] = {}
    cursor = None
    page_count = 0
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = _request("POST", f"/databases/{KARTE_DB_ID}/query", body)
        for p in resp.get("results", []):
            props = p.get("properties", {})
            tiktok = ""
            prop = props.get(KARTE_TIKTOK_ID_PROP, {})
            for t in prop.get("rich_text", []) or []:
                tiktok += t.get("plain_text", "")
            tiktok = tiktok.strip().lower()
            if tiktok:
                idx[tiktok] = p["id"]
        page_count += 1
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        if page_count > 20:
            break
    return idx


def find_existing_row(event_id: str, host_id: str, date_iso: str) -> Optional[str]:
    """Look up an existing row for (event_id, host_id, date) — returns page_id or None."""
    # Match by event ID + title containing host_id fragment + date
    body = {
        "filter": {
            "and": [
                {"property": RES_EVENT_ID, "rich_text": {"equals": event_id}},
                {"property": RES_DATE, "date": {"equals": date_iso}},
                {"property": RES_TITLE, "title": {"contains": host_id}},
            ]
        },
        "page_size": 1,
    }
    try:
        resp = _request("POST", f"/databases/{RESULTS_DB_ID}/query", body)
        results = resp.get("results", [])
        return results[0]["id"] if results else None
    except Exception as e:
        print(f"[notion] find_existing_row err: {e}")
        return None


def compute_phase(event_start: Optional[int], event_end: Optional[int], now_ts: int) -> str:
    if not event_start or not event_end:
        return "中間"
    day = 86400
    if now_ts < event_start:
        return "中間"
    if now_ts > event_end:
        return "終了"
    if now_ts < event_start + day:
        return "初日"
    if event_end - now_ts < day:
        return "最終日"
    return "中間"


def build_properties(summary: dict, entry: dict, karte_id: Optional[str], date_iso: str, phase: str) -> dict:
    """Construct Notion properties payload for one row."""
    nickname = entry.get("nickname", "")
    host_id = entry.get("hostId", "")
    title = f"{nickname} / {summary['eventName']} / {date_iso} [{host_id}]"
    agency = "ユリシス" if entry.get("agencyLabel") == "ulysses" else "アルファ"
    tier_raw = entry.get("hostIncomeTier")
    tier_opt = f"T{tier_raw}" if tier_raw in ("1", "2", "3", "4", 1, 2, 3, 4) else None

    props: dict = {
        RES_TITLE: {"title": [{"text": {"content": title}}]},
        RES_EVENT_NAME: {"rich_text": [{"text": {"content": summary["eventName"] or ""}}]},
        RES_EVENT_ID: {"rich_text": [{"text": {"content": summary["eventId"]}}]},
        RES_DATE: {"date": {"start": date_iso}},
        RES_RANK: {"number": entry.get("rank") or None},
        RES_SCORE: {"number": entry.get("score", 0)},
        RES_DIAMONDS: {"number": entry.get("diamonds") or 0},
        RES_PK: {"number": entry.get("pkDiamonds") or 0},
        RES_PHASE: {"select": {"name": phase}},
        RES_CAPTURED_AT: {"rich_text": [{"text": {"content": summary.get("capturedAt", "")}}]},
        RES_AGENCY: {"select": {"name": agency}},
    }
    if tier_opt:
        props[RES_TIER] = {"select": {"name": tier_opt}}
    if karte_id:
        props[RES_CREATOR] = {"relation": [{"id": karte_id}]}
    # Event dates
    es = summary.get("eventStart")
    ee = summary.get("eventEnd")
    if es:
        props[RES_START] = {"date": {"start": datetime.fromtimestamp(int(es), JST).strftime("%Y-%m-%d")}}
    if ee:
        props[RES_END] = {"date": {"start": datetime.fromtimestamp(int(ee), JST).strftime("%Y-%m-%d")}}
    return props


def sync_summaries(summaries: List[dict]) -> dict:
    """Push each summary's katsuParticipants into イベント実績 DB.
    Returns stats dict.
    """
    stats = {"created": 0, "updated": 0, "failed": 0, "skipped_no_karte": 0}

    print("[notion] loading karte index…")
    karte_idx = load_karte_index()
    print(f"[notion] karte: {len(karte_idx)} creators indexed")

    now_ts = int(datetime.now(JST).timestamp())
    date_iso = datetime.now(JST).strftime("%Y-%m-%d")

    for s in summaries:
        phase = compute_phase(
            int(s.get("eventStart") or 0),
            int(s.get("eventEnd") or 0),
            now_ts,
        )
        participants = s.get("katsuParticipants", [])
        print(f"\n[notion] event '{s.get('eventName', '')}' phase={phase}  participants={len(participants)}")
        for e in participants:
            username = (e.get("username") or "").strip().lower()
            host_id = e.get("hostId", "")
            karte_id = karte_idx.get(username)
            if not karte_id:
                stats["skipped_no_karte"] += 1
                # Still proceed without relation (leave creator blank)
            props = build_properties(s, e, karte_id, date_iso, phase)

            try:
                existing = find_existing_row(s["eventId"], host_id, date_iso)
                if existing:
                    _request("PATCH", f"/pages/{existing}", {"properties": props})
                    stats["updated"] += 1
                else:
                    _request("POST", "/pages", {"parent": {"database_id": RESULTS_DB_ID}, "properties": props})
                    stats["created"] += 1
            except Exception as err:
                print(f"  ✗ {e.get('nickname')} (@{username}): {err}")
                stats["failed"] += 1

    print(f"\n[notion] done: {stats}")
    return stats


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 notion_sync.py <summary.json> [<summary2.json> ...]")
        sys.exit(1)
    summaries = [json.loads(Path(p).read_text()) for p in sys.argv[1:]]
    sync_summaries(summaries)


if __name__ == "__main__":
    main()
