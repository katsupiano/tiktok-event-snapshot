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
EVENTS_DB_ID = os.environ.get("NOTION_EVENTS_DB_ID") or "e0ff7ce241954338be8334b9063d1356"

# Property names on カルテ DB used to match creator
KARTE_TITLE_PROP = "クリエイター名"
KARTE_TIKTOK_ID_PROP = "クリエイターID"  # username (display_id, e.g. "milu.ami")
KARTE_HOST_ID_PROP = "hostId"  # numeric TikTok hostId (e.g. "7604791390933041168") — primary match key

# Property names on イベント DB used to match by name
EVENT_TITLE_PROP = "イベント名"
EVENT_BACKSTAGE_ID_PROP = "BackstageイベントID"

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
RES_EVENT_REL = "イベント"  # Relation → イベント DB


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


def load_karte_index() -> Dict[str, Dict[str, str]]:
    """Return {'by_host': {hostId: karte_page_id}, 'by_username': {username_lower: karte_page_id}}.

    hostId is the primary key — immutable per TikTok account. username (display_id)
    is fallback only; Backstage can return stale usernames for ex-managed creators
    (see 2026-04-19 incident: miruami wrong-linking).
    """
    by_host: Dict[str, str] = {}
    by_username: Dict[str, str] = {}
    cursor = None
    page_count = 0
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        resp = _request("POST", f"/databases/{KARTE_DB_ID}/query", body)
        for p in resp.get("results", []):
            props = p.get("properties", {})

            username = ""
            for t in (props.get(KARTE_TIKTOK_ID_PROP, {}).get("rich_text") or []):
                username += t.get("plain_text", "")
            username = username.strip().lower()

            host_id = ""
            for t in (props.get(KARTE_HOST_ID_PROP, {}).get("rich_text") or []):
                host_id += t.get("plain_text", "")
            host_id = host_id.strip()

            if host_id:
                by_host[host_id] = p["id"]
            if username:
                by_username[username] = p["id"]
        page_count += 1
        if not resp.get("has_more"):
            break
        cursor = resp.get("next_cursor")
        if page_count > 20:
            break
    return {"by_host": by_host, "by_username": by_username}


def normalize_event_name(name: str) -> str:
    """Lowercase, strip whitespace, remove emoji/symbol blocks, normalize digits. For fuzzy-match keying."""
    import re
    s = (name or "").strip().lower()
    # Remove emoji/pictograph ranges and variation selectors
    s = re.sub(r"[\U0001F300-\U0001FAFF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U0001F700-\U0001F77F\U0001F780-\U0001F7FF\U0001F800-\U0001F8FF\U0001F900-\U0001F9FF\U0001FA00-\U0001FA6F\U00002600-\U000027BF\uFE00-\uFE0F\u200D]", "", s)
    # Kanji digits → ASCII (一二三四五六七八九十)
    kanji_digits = {"一": "1", "二": "2", "三": "3", "四": "4", "五": "5",
                    "六": "6", "七": "7", "八": "8", "九": "9", "十": "10"}
    for k, v in kanji_digits.items():
        s = s.replace(k, v)
    # Remove punctuation/question marks/exclamation (full+half width) + year markers
    s = re.sub(r"[\s!?！？・\-—‐\u3000、。.,]", "", s)
    s = re.sub(r"20\d{2}年?", "", s)  # strip year prefix like "2026年"
    s = s.replace("第", "").replace("回", "")
    # Strip all remaining ASCII digits (position-sensitive e.g. "第3回" vs "第三回")
    s = re.sub(r"[0-9]+", "", s)
    return s


def _period_overlap_ratio(bs_start: str, bs_end: str, ev_start: Optional[str], ev_end: Optional[str]) -> float:
    """Return overlap ratio ∈ [0,1] between two YYYY-MM-DD ranges. 0 if either range missing."""
    if not (bs_start and bs_end and ev_start and ev_end):
        return 0.0
    try:
        from datetime import date
        def d(s: str) -> date:
            return date.fromisoformat(s)
        bs_s, bs_e = d(bs_start), d(bs_end)
        ev_s, ev_e = d(ev_start), d(ev_end)
    except Exception:
        return 0.0
    overlap_start = max(bs_s, ev_s)
    overlap_end = min(bs_e, ev_e)
    if overlap_start > overlap_end:
        return 0.0
    overlap_days = (overlap_end - overlap_start).days + 1
    bs_days = (bs_e - bs_s).days + 1
    ev_days = (ev_e - ev_s).days + 1
    return overlap_days / min(bs_days, ev_days)


def match_event(backstage_key: str, bs_start: Optional[str], bs_end: Optional[str], events: List[dict], backstage_event_id: Optional[str] = None) -> Optional[str]:
    """Match Backstage event to イベント DB entry.
    Priority:
      1. Exact match on BackstageイベントID (if user set it on the DB entry)
      2. Period overlap ≥ 60% + name tiebreaker
    No name-only fallback — too noisy (e.g. Music stars ≠ Music Rising).
    """
    # 1. Exact ID match (most reliable — user-set mapping)
    if backstage_event_id:
        for ev in events:
            if ev.get("backstageId") == backstage_event_id:
                return ev["pageId"]
    if not bs_start or not bs_end or not events:
        return None
    candidates = []
    for ev in events:
        ratio = _period_overlap_ratio(bs_start, bs_end, ev.get("start"), ev.get("end"))
        if ratio >= 0.6:
            # Name substring contribution
            nk = ev.get("key") or ""
            name_score = 0.0
            if nk and backstage_key:
                if nk in backstage_key or backstage_key in nk:
                    short, long = (nk, backstage_key) if len(nk) < len(backstage_key) else (backstage_key, nk)
                    name_score = len(short) / max(len(long), 1)
            candidates.append((ratio, name_score, ev))
    if not candidates:
        return None
    # Sort desc by (period, name)
    candidates.sort(key=lambda c: (c[0], c[1]), reverse=True)
    best = candidates[0]
    # If top candidate's period overlap is ≥0.9 AND uniquely best, accept
    # If lower overlap (0.6-0.9) and multiple similar candidates, require decent name match
    if len(candidates) == 1:
        return best[2]["pageId"]
    # Multiple: require a clear winner (either much better period OR good name match)
    second = candidates[1]
    if best[0] - second[0] >= 0.2:
        return best[2]["pageId"]
    if best[1] >= 0.3 and best[1] > second[1]:
        return best[2]["pageId"]
    return None  # ambiguous → unlinked


def load_event_index() -> List[dict]:
    """Return list of {page_id, name, key, start, end} dicts from イベント DB.
    Dates are 'YYYY-MM-DD' strings (or None). Used for period-overlap matching.
    """
    def _date(prop_name: str, props: dict) -> Optional[str]:
        d = (props.get(prop_name) or {}).get("date") or {}
        return d.get("start")

    events: List[dict] = []
    cursor = None
    pages = 0
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        try:
            resp = _request("POST", f"/databases/{EVENTS_DB_ID}/query", body)
        except Exception as e:
            print(f"[notion] load_event_index err: {e}")
            return events
        for p in resp.get("results", []):
            props = p.get("properties", {})
            title_prop = props.get(EVENT_TITLE_PROP, {})
            name = "".join(t.get("plain_text", "") for t in title_prop.get("title", []) or [])
            bs_prop = props.get(EVENT_BACKSTAGE_ID_PROP, {})
            bs_id = "".join(t.get("plain_text", "") for t in bs_prop.get("rich_text", []) or []).strip()
            events.append({
                "pageId": p["id"],
                "name": name,
                "key": normalize_event_name(name),
                "start": _date("開始日", props),
                "end": _date("終了日", props),
                "backstageId": bs_id,
            })
        pages += 1
        if not resp.get("has_more") or pages > 10:
            break
        cursor = resp.get("next_cursor")
    return events


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


def build_properties(summary: dict, entry: dict, karte_id: Optional[str], date_iso: str, phase: str, event_page_id: Optional[str] = None) -> dict:
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
    if event_page_id:
        props[RES_EVENT_REL] = {"relation": [{"id": event_page_id}]}
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
    stats = {"created": 0, "updated": 0, "failed": 0, "skipped_no_karte": 0, "username_fallbacks": 0, "events_linked": 0, "events_unlinked": 0, "unlinked_names": []}

    print("[notion] loading karte index…")
    karte_idx = load_karte_index()
    print(f"[notion] karte: {len(karte_idx['by_host'])} by hostId, {len(karte_idx['by_username'])} by username")

    print("[notion] loading event index…")
    event_idx = load_event_index()
    print(f"[notion] events: {len(event_idx)} entries indexed")

    now_ts = int(datetime.now(JST).timestamp())
    date_iso = datetime.now(JST).strftime("%Y-%m-%d")

    for s in summaries:
        phase = compute_phase(
            int(s.get("eventStart") or 0),
            int(s.get("eventEnd") or 0),
            now_ts,
        )
        event_key = normalize_event_name(s.get("eventName", ""))
        bs_start = datetime.fromtimestamp(int(s["eventStart"]), JST).strftime("%Y-%m-%d") if s.get("eventStart") else None
        bs_end = datetime.fromtimestamp(int(s["eventEnd"]), JST).strftime("%Y-%m-%d") if s.get("eventEnd") else None
        event_page_id = match_event(event_key, bs_start, bs_end, event_idx, backstage_event_id=s.get("eventId"))
        if event_page_id:
            stats["events_linked"] += 1
            print(f"\n[notion] event '{s.get('eventName', '')}' phase={phase}  → linked to イベントDB")
        else:
            stats["events_unlinked"] += 1
            stats["unlinked_names"].append(s.get("eventName", "(no name)"))
            print(f"\n[notion] event '{s.get('eventName', '')}' phase={phase}  → UNLINKED (no match in イベントDB, normalized key='{event_key}')")
        participants = s.get("katsuParticipants", [])
        print(f"[notion] participants={len(participants)}")
        for e in participants:
            username = (e.get("username") or "").strip().lower()
            host_id = (e.get("hostId") or "").strip()
            # Primary: hostId match. Fallback: username match (but only if the
            # matched karte has NO hostId set — otherwise it belongs to a different
            # TikTok account and we'd be mis-linking, like the 2026-04-19 miruami bug).
            karte_id = karte_idx["by_host"].get(host_id) if host_id else None
            if not karte_id:
                by_username_id = karte_idx["by_username"].get(username)
                if by_username_id and by_username_id not in karte_idx["by_host"].values():
                    # username-only match AND that karte has no hostId yet → safe fallback
                    karte_id = by_username_id
                    stats["username_fallbacks"] += 1
                    print(f"  ⚠ username-fallback link: @{username} (hostId {host_id}) → karte {by_username_id}")
                elif by_username_id:
                    # karte found by username but it already has a hostId pointing to someone else
                    # → this scraped entry is NOT the karte's real creator. SKIP the relation.
                    print(f"  ✗ username-match but hostId conflict: @{username} scraped hostId={host_id}, karte is another creator. Skipping relation.")
            if not karte_id:
                stats["skipped_no_karte"] += 1
                # Still proceed without relation (leave creator blank)
            props = build_properties(s, e, karte_id, date_iso, phase, event_page_id)

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
