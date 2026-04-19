# TikTok LIVE Event Snapshot Tracker

Daily scraper for Backstage fest-activity events. Captures ranking/score snapshots for KATSU-managed creators at 12:00 JST and 24:00 JST, writes results to a shared Notion database, and archives raw JSON to Git history.

## Architecture

```
discover_events.py  → list all in-progress events (via activity_list API)
scrape_event.py     → scrape one event with one agency session (Playwright)
run.py              → orchestrator: discover × (alpha + ulysses) sessions → save → Notion
notion_sync.py      → upsert rows into イベント実績 DB (unique key = event×host×date)
```

The scraper drives the Backstage UI (clicks pagination's "Next" button) to trigger XHRs. `page.evaluate` fetch() is blocked by bot protection, so only real UI-driven XHRs pass through.

## Running locally

```bash
pip install -r requirements.txt
python -m playwright install chromium

# Test: scrape a specific event (no Notion)
python run.py --event-ids=7621548004184670264 --skip-notion

# Full auto-run (discover + scrape + Notion)
export NOTION_TOKEN=secret_xxx
python run.py
```

## GitHub Actions

Runs at 12:00 and 24:00 JST (UTC 03:00 / 15:00). Triggered on schedule or manually via workflow_dispatch.

### Secrets required

| Secret | Purpose |
|---|---|
| `STORAGE_BCODE_B64` | base64-encoded `storage_state.json` for bcode session |
| `STORAGE_ULYSSES_B64` | base64-encoded `storage_state.json` for ulysses session (optional) |
| `NOTION_TOKEN` | Internal integration token (must have access to the Notion DBs below) |
| `NOTION_KARTE_DB_ID` | karte DB id (default works if not set) |
| `NOTION_RESULTS_DB_ID` | イベント実績 DB id (default works if not set) |

Generate session state locally (with Chrome/Chromium + Playwright), then:

```bash
base64 -i sessions/storage_state.json -o - | pbcopy
# Paste into GitHub → Settings → Secrets → STORAGE_BCODE_B64
```

### Session expiration

Session cookies live 1-few weeks. When they expire, workflow fails at scrape step. Re-login locally, re-export, update the secret.

## Output layout

```
events/
├── {event_id}/
│   ├── summary_YYYYMMDDTHHMM.json   # per-run snapshot (KATSU only)
│   └── latest.json                   # most recent
└── snapshots_index.json              # cross-event index
```

## Notion DB

- **イベント実績 DB** (`94bf21fce78f4fc093a128313a6c8da3`): one row per creator × event × date
  - Related to カルテ DB via `クリエイター` (two-way: `イベント実績` on karte side)
  - Unique key (for upsert): `(イベントID, hostId in title, 日付)`
