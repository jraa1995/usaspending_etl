#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
USASpending Data Pipeline (Awards) — Bulk Backfill + Incremental Search

Bulk backfill:
  - POST /api/v2/bulk_download/awards/
  - Poll  /api/v2/download/status?file_name=...
  - Correct payload per docs: filters.{prime_award_types,date_type,date_range,agencies?}, file_format, columns?
  - Resilient download: HEAD probe + retry GET on 403/404/429/5xx
  - Auto-split large ranges on backend generation errors until <= MIN_SPLIT_DAYS

Incremental (search API):
  - POST /api/v2/search/spending_by_award/
  - Sharded by date range & award groups

Defaults requested:
  - USER_AGENT = "justin@test.com"
  - DEFAULT_OUT = ./usaspending_data
  - Running with no args prints help
"""

import os
import re
import sys
import time
import json
import logging
import zipfile
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Iterable, Tuple

import httpx
import backoff
import pandas as pd

# Optional deps: if pyarrow is unavailable, CSV will be used instead of Parquet.
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:
    pa = None
    pq = None

# -----------------------------------------------------------------------------
# Configuration and Constants
# -----------------------------------------------------------------------------

API_ROOT = "https://api.usaspending.gov"

# Per request: use email only for User-Agent
USER_AGENT = "justinaguilab2@gmail.com"

# Default output folder at repo root
DEFAULT_OUT = Path("./usaspending_data")

# Global rate limit: ~1000 req / 300s (≈3.3 rps). Tweak with env.
MAX_RPS = float(os.getenv("USASPENDING_MAX_RPS", "3.0"))

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("usaspending")

# Award type groups for convenience; do not mix groups in one search request.
AWARD_TYPE_GROUPS: Dict[str, List[str]] = {
    "contracts": ["A", "B", "C", "D"],
    "loans": ["07", "08"],
    "idvs": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"],
    "grants": ["02", "03", "04", "05"],
    "other_fin_ast": ["06", "10"],
    "direct_payments": ["09", "11", "-1"],
}

# Default fields for search requests
SEARCH_FIELDS_BASE = [
    "Award ID",
    "Recipient Name",
    "Recipient UEI",
    "Awarding Agency",
    "Funding Agency",
    "Award Amount",
    "Start Date",
    "End Date",
    "Primary Place of Performance",
    "Description",
    "Last Modified Date",
]

# -----------------------------------------------------------------------------
# Rate Limiter
# -----------------------------------------------------------------------------

class RateLimiter:
    """Simple token-bucket-like rate limiter to respect MAX_RPS."""

    def __init__(self, rps: float) -> None:
        self.min_interval = 1.0 / max(rps, 0.1)
        self._last = 0.0

    def wait(self) -> None:
        now = time.time()
        delta = now - self._last
        if delta < self.min_interval:
            time.sleep(self.min_interval - delta)
        self._last = time.time()

# -----------------------------------------------------------------------------
# USAspending Client
# -----------------------------------------------------------------------------

class USAClient:
    """
    HTTP client for USAspending endpoints.
    Handles HTTP/2, gzip, retries/backoff, and rate limiting.
    """

    def __init__(self, timeout: float = 30.0, max_rps: float = MAX_RPS) -> None:
        self.client = httpx.Client(
            base_url=API_ROOT,
            http2=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )
        self.limiter = RateLimiter(max_rps)

    def close(self) -> None:
        self.client.close()

    @staticmethod
    def _log_req(resp: httpx.Response) -> None:
        logger.debug("HTTP %s %s -> %s", resp.request.method, resp.request.url, resp.status_code)

    @backoff.on_exception(backoff.expo, (httpx.HTTPError,), max_time=180)
    def _request(self, method: str, endpoint: str, *, json_body=None, params=None) -> dict:
        self.limiter.wait()
        resp = self.client.request(method, endpoint, json=json_body, params=params)
        self._log_req(resp)
        resp.raise_for_status()
        if resp.headers.get("Content-Type", "").startswith("application/json"):
            return resp.json()
        return {}

    def get_json(self, endpoint: str, *, params=None) -> dict:
        return self._request("GET", endpoint, params=params)

    def post_json(self, endpoint: str, *, json_body=None) -> dict:
        return self._request("POST", endpoint, json_body=json_body)

    # Bulk download endpoints (per documentation)
    def start_bulk_awards(self, payload: dict) -> dict:
        return self.post_json("/api/v2/bulk_download/awards/", json_body=payload)

    def download_status(self, file_name: str) -> dict:
        # Per docs, status lives under /api/v2/download/status
        return self.get_json("/api/v2/download/status", params={"file_name": file_name})

    # Search endpoint
    def search_spending_by_award(self, payload: dict) -> dict:
        return self.post_json("/api/v2/search/spending_by_award/", json_body=payload)

# -----------------------------------------------------------------------------
# Helper Functions
# -----------------------------------------------------------------------------

def ensure_out_dir(base: Path = DEFAULT_OUT) -> Path:
    base.mkdir(parents=True, exist_ok=True)
    return base

def to_parquet(df: pd.DataFrame, out_path: Path) -> None:
    """Write a DataFrame to Parquet if pyarrow is available; fallback to CSV."""
    if df.empty:
        logger.warning("No data to write: %s", out_path)
        return
    if pq is None:
        csv_path = out_path.with_suffix(".csv")
        df.to_csv(csv_path, index=False)
        logger.info("Wrote CSV: %s (rows=%d)", csv_path, len(df))
        return
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    logger.info("Wrote Parquet: %s (rows=%d)", out_path, len(df))

def unzip_to(folder: Path, zip_path: Path) -> None:
    """Extract a zip archive to the given folder."""
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(folder)
    logger.info("Extracted zip -> %s", folder)

def _ts_compact() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")

def _safe_slug(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s)).strip("_")

def dedupe_minimal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Basic deduplication by Award ID, Start Date, End Date, and Last Modified Date.
    Adjust keys as needed to your dedupe requirements.
    """
    keys = [c for c in ["Award ID", "Start Date", "End Date", "Last Modified Date"] if c in df.columns]
    if not keys:
        return df
    before = len(df)
    df2 = df.drop_duplicates(subset=keys)
    after = len(df2)
    if after != before:
        logger.info("Deduped rows: %d -> %d (-%d)", before, after, before - after)
    return df2

# -----------------------------------------------------------------------------
# Agencies parsing for Bulk Download filter
# -----------------------------------------------------------------------------

def _parse_agency_strings(agencies: Optional[List[str]], default_type: str) -> Optional[List[dict]]:
    """
    Convert a list of user strings into API 'Agency' filter dicts.

    Supported forms:
      - "Department of Defense"                      -> awarding/toptier
      - "funding:Department of Defense"              -> funding/toptier
      - "Animal and Plant Health Inspection Service|Department of Agriculture"
                                                     -> awarding/subtier + toptier_name
      - "funding:Animal and Plant Health...|Department of Agriculture"
                                                     -> funding/subtier + toptier_name
    """
    if not agencies:
        return None

    out: List[dict] = []
    for raw in agencies:
        s = raw.strip()
        a_type = default_type  # awarding|funding
        if s.lower().startswith("funding:"):
            a_type = "funding"
            s = s[len("funding:"):].strip()
        elif s.lower().startswith("awarding:"):
            a_type = "awarding"
            s = s[len("awarding:"):].strip()

        if "|" in s:
            subtier, toptier = [p.strip() for p in s.split("|", 1)]
            out.append({
                "type": a_type,
                "tier": "subtier",
                "name": subtier,
                "toptier_name": toptier
            })
        else:
            out.append({
                "type": a_type,
                "tier": "toptier",
                "name": s
            })

    return out if out else None

# -----------------------------------------------------------------------------
# Resilient file download (HEAD probe + retrying GET)
# -----------------------------------------------------------------------------

def _download_file(client: USAClient, file_url: str, dest: Path) -> None:
    """
    Download with resilience:
      - Probe availability using GET Range: bytes=0-0 (some CDNs 403 on HEAD)
      - Retry GET on 403/404/429/5xx with exponential backoff
      - Follow redirects
    """
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    }
    with httpx.Client(headers=headers, follow_redirects=True, timeout=60.0) as sess:
        max_wait_seconds = 45 * 60   # up to 45 minutes for very large jobs
        start = time.time()
        backoff_sec = 2.0

        def is_available(url: str) -> bool:
            try:
                # 206 (Partial) or 200 (no range support) both mean "ready"
                r = sess.get(url, headers={"Range": "bytes=0-0"})
                return r.status_code in (200, 206, 301, 302, 303, 307, 308)
            except httpx.HTTPError:
                return False

        while not is_available(file_url):
            logger.info("File not yet available (403/404 or similar). Retrying in %.1fs...", backoff_sec)
            time.sleep(backoff_sec)
            if time.time() - start > max_wait_seconds:
                raise RuntimeError(f"File not available after {max_wait_seconds}s: {file_url}")
            backoff_sec = min(backoff_sec * 1.5, 30.0)

        logger.info("Downloading: %s -> %s", file_url, dest)
        attempt = 0
        backoff_sec = 2.0
        while True:
            attempt += 1
            try:
                with sess.stream("GET", file_url) as r:
                    if r.status_code == 200:
                        with open(dest, "wb") as f:
                            for chunk in r.iter_bytes():
                                f.write(chunk)
                        size_mb = dest.stat().st_size / (1024 * 1024)
                        logger.info("Downloaded: %s (%.1f MB)", dest, size_mb)
                        return
                    elif r.status_code in (403, 404, 429) or 500 <= r.status_code < 600:
                        logger.info("GET status %s. Retrying in %.1fs (attempt %d)...",
                                    r.status_code, backoff_sec, attempt)
                    else:
                        r.raise_for_status()
            except httpx.HTTPError as e:
                logger.info("GET error: %s. Retrying in %.1fs (attempt %d)...", e, backoff_sec, attempt)

            time.sleep(backoff_sec)
            backoff_sec = min(backoff_sec * 1.5, 45.0)

# -----------------------------------------------------------------------------
# Bulk Backfill — auto-split logic for backend errors
# -----------------------------------------------------------------------------

def _days_between(start_date: str, end_date: str) -> int:
    s = dt.datetime.strptime(start_date, "%Y-%m-%d")
    e = dt.datetime.strptime(end_date, "%Y-%m-%d")
    return (e - s).days + 1

def _split_range(start_date: str, end_date: str) -> List[Tuple[str, str]]:
    s = dt.datetime.strptime(start_date, "%Y-%m-%d")
    e = dt.datetime.strptime(end_date, "%Y-%m-%d")
    mid = s + dt.timedelta(days=((e - s).days // 2))
    a = (s.strftime("%Y-%m-%d"), mid.strftime("%Y-%m-%d"))
    b = ((mid + dt.timedelta(days=1)).strftime("%Y-%m-%d"), e.strftime("%Y-%m-%d"))
    return [a, b]

def _run_bulk_job_or_raise(
    client: USAClient,
    payload: dict,
    out_dir: Path,
    group: str,
    start_date: str,
    end_date: str,
    file_format: str,
) -> None:
    start_resp = client.start_bulk_awards(payload)
    file_name = start_resp.get("file_name")
    status_url = start_resp.get("status_url")
    if not file_name:
        raise RuntimeError(f"Bulk start failed: {json.dumps(start_resp, indent=2)}")
    logger.info("Bulk started. file_name=%s status_url=%s", file_name, status_url or "n/a")

    consecutive_exception_msgs = 0
    while True:
        stat = client.download_status(file_name)
        status = str(stat.get("status", "")).lower()
        msg = stat.get("message") or ""
        if msg:
            m = msg.strip().splitlines()[0][:300]
            logger.info("Bulk status: %s (%s)", status or "unknown", m)
        else:
            logger.info("Bulk status: %s", status or "unknown")

        # Sometimes backend reports 'running' with an exception in the message
        if "exception" in msg.lower():
            consecutive_exception_msgs += 1
            if consecutive_exception_msgs >= 3:
                raise RuntimeError(f"Backend exception while generating file: {file_name}")

        if status in {"finished", "ready", "success"}:
            file_url = stat.get("file_url") or start_resp.get("file_url")
            if not file_url:
                raise RuntimeError(f"No file_url on finished job: {json.dumps(stat, indent=2)}")

            out_dir.mkdir(parents=True, exist_ok=True)
            dest_name = _safe_slug(file_name)
            if not dest_name.lower().endswith(".zip"):
                dest_name += ".zip"
            zip_path = out_dir / dest_name
            _download_file(client, file_url, zip_path)

            extract_dir = out_dir / f"bulk_{group}_{start_date}_to_{end_date}"
            extract_dir.mkdir(parents=True, exist_ok=True)
            unzip_to(extract_dir, zip_path)

            # Convert CSV/TSV to Parquet (keep originals)
            if file_format.lower() in {"csv", "tsv"}:
                sep = "," if file_format.lower() == "csv" else "\t"
                for txt_file in extract_dir.rglob(f"*.{file_format.lower()}"):
                    try:
                        df = pd.read_csv(txt_file, dtype=str, sep=sep)
                        df = dedupe_minimal(df)
                        if pq is not None:
                            to_parquet(df, txt_file.with_suffix(".parquet"))
                    except Exception as e:
                        logger.warning("Could not parse %s -> Parquet: %s", txt_file.name, e)
            return

        if status in {"failed", "error"}:
            raise RuntimeError(f"Bulk job failed: {json.dumps(stat, indent=2)}")

        time.sleep(5)

def bulk_backfill_awards(
    client: USAClient,
    out_dir: Path,
    start_date: str,
    end_date: str,
    award_types: List[str],
    agencies: Optional[List[str]] = None,
    date_type: str = "action_date",
    file_format: str = "csv",
    columns: Optional[List[str]] = None,
    min_split_days: int = 7,
) -> None:
    """
    Perform bulk award backfills by date range and award type group.

    - award_types: list of group keys like ["contracts","grants"] (mapped to codes)
    - agencies: optional list of strings (see _parse_agency_strings)
    - date_type: "action_date" or "last_modified_date"
    - file_format: "csv" | "tsv" | "pstxt"
    - columns: optional explicit column names
    - min_split_days: minimum shard size when auto-splitting on backend errors
    """
    ensure_out_dir(out_dir)
    groups = award_types or ["contracts"]
    agencies_filter = _parse_agency_strings(agencies, default_type="awarding")

    for group in groups:
        if group not in AWARD_TYPE_GROUPS:
            logger.warning("Unknown award group %s; skipping", group)
            continue

        payload_base = {
            "filters": {
                "prime_award_types": AWARD_TYPE_GROUPS[group],
                "date_type": date_type,
                "date_range": {"start_date": start_date, "end_date": end_date},
            },
            "file_format": file_format,
            "columns": [] if columns is None else columns,
        }
        if agencies_filter:
            payload_base["filters"]["agencies"] = agencies_filter

        def _attempt(s: str, e: str):
            p = dict(payload_base)
            p["filters"] = dict(payload_base["filters"])
            p["filters"]["date_range"] = {"start_date": s, "end_date": e}
            try:
                logger.info("Starting bulk job: group=%s | %s..%s", group, s, e)
                _run_bulk_job_or_raise(client, p, out_dir, group, s, e, file_format)
            except RuntimeError as rex:
                days = _days_between(s, e)
                if days > min_split_days:
                    logger.warning(
                        "Backend error on %s..%s (%d days). Splitting range and retrying.\n%s",
                        s, e, days, str(rex)[:2000]
                    )
                    for s2, e2 in _split_range(s, e):
                        _attempt(s2, e2)
                else:
                    raise

        _attempt(start_date, end_date)

# -----------------------------------------------------------------------------
# Incremental Search
# -----------------------------------------------------------------------------

def shard_iter(
    start_date: str,
    end_date: str,
    groups: List[str],
    agencies: Optional[List[str]],
    chunk_days: int = 7,
) -> Iterable[Tuple[str, str, str, Optional[str]]]:
    """
    Yield (group, shard_start, shard_end, agency) combinations for search.
    """
    groups = groups or ["contracts"]
    start = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end = dt.datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    agencies = agencies or [None]
    while current <= end:
        shard_end = min(current + dt.timedelta(days=chunk_days - 1), end)
        s = current.strftime("%Y-%m-%d")
        e = shard_end.strftime("%Y-%m-%d")
        for g in groups:
            for a in agencies:
                yield (g, s, e, a)
        current = shard_end + dt.timedelta(days=1)

def drain_spending_by_award(
    client: USAClient,
    award_codes: List[str],
    start_date: str,
    end_date: str,
    awarding_agency: Optional[str],
    fields: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Exhaust the paginated /spending_by_award/ endpoint for the given filters.
    """
    fields = fields or SEARCH_FIELDS_BASE
    page = 1
    rows: List[dict] = []
    while True:
        payload = {
            "filters": {
                "award_type_codes": award_codes,
                "time_period": [{"start_date": start_date, "end_date": end_date}],
            },
            "fields": fields,
            "limit": 500,
            "page": page,
            "sort": "Award Amount",
            "order": "desc",
        }
        if awarding_agency:
            payload["filters"]["agencies"] = [
                {"type": "awarding", "tier": "toptier", "name": awarding_agency}
            ]
        resp = client.search_spending_by_award(payload)
        results = resp.get("results") or []
        if not results:
            break
        rows.extend(results)
        meta = resp.get("page_metadata", {})
        if not meta.get("hasNext", False):
            break
        page += 1
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    return dedupe_minimal(df)

def incremental_awards(
    client: USAClient,
    out_dir: Path,
    start_date: str,
    end_date: str,
    award_groups: List[str],
    agencies: Optional[List[str]],
    chunk_days: int = 7,
    fmt: str = "parquet",
) -> None:
    """
    Fetch awards incrementally by sharded date ranges and groups.
    Outputs partitioned Parquet (or CSV) files.
    """
    ensure_out_dir(out_dir)
    for group, s, e, agency in shard_iter(start_date, end_date, award_groups, agencies, chunk_days=chunk_days):
        if group not in AWARD_TYPE_GROUPS:
            logger.warning("Unknown group %s; skipping", group)
            continue
        logger.info("Shard: %s | %s..%s | agency=%s", group, s, e, agency or "ALL")
        df = drain_spending_by_award(client, AWARD_TYPE_GROUPS[group], s, e, agency)
        if df.empty:
            logger.info("No results for shard.")
            continue
        base = out_dir / "incremental" / _safe_slug(group) / f"{s}_to_{e}"
        base.mkdir(parents=True, exist_ok=True)
        ts = _ts_compact()
        if fmt.lower() == "parquet":
            outp = base / f"awards_{_safe_slug(agency) if agency else 'all'}_{ts}.parquet"
            to_parquet(df, outp)
        else:
            outp = base / f"awards_{_safe_slug(agency) if agency else 'all'}_{ts}.csv"
            df.to_csv(outp, index=False)
            logger.info("Wrote CSV: %s (rows=%d)", outp, len(df))

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

import argparse

def main() -> None:
    parser = argparse.ArgumentParser(
        description="USASpending Awards Pipeline (Bulk Backfill + Incremental Search)"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # If user runs with no arguments, show help instead of argparse error
    if len(sys.argv) == 1:
        parser.print_help()
        parser.exit()

    # Bulk backfill (Bulk Award Download)
    p_bulk = sub.add_parser("bulk-backfill", help="Bulk backfill via /api/v2/bulk_download/awards/")
    p_bulk.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p_bulk.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p_bulk.add_argument("--groups", nargs="+", default=["contracts"],
                        help=f"Groups: {list(AWARD_TYPE_GROUPS.keys())}")
    p_bulk.add_argument("--agencies", nargs="*",
                        help=("Optional agencies filter. "
                              "Toptier: 'Department of Defense' | Subtier: 'Subtier Name|Toptier Name'. "
                              "Prefix with 'funding:' or 'awarding:' to set type."))
    p_bulk.add_argument("--agency-type", choices=["awarding", "funding"], default="awarding",
                        help="Default type for agencies provided without a prefix (default: awarding)")
    p_bulk.add_argument("--date-type", choices=["action_date", "last_modified_date"], default="action_date",
                        help="Date dimension for bulk download (default: action_date)")
    p_bulk.add_argument("--file-format", choices=["csv", "tsv", "pstxt"], default="csv",
                        help="Download file format (default: csv)")
    p_bulk.add_argument("--columns", nargs="*", help="Optional explicit list of columns to request")
    p_bulk.add_argument("--min-split-days", type=int, default=7,
                        help="Auto-split minimum window when backend errors occur (default: 7)")
    p_bulk.add_argument("--out", default=str(DEFAULT_OUT), help="Output directory for extracted files")

    # Incremental search (search API)
    p_incr = sub.add_parser("incremental", help="Incremental pull via /api/v2/search/spending_by_award/")
    p_incr.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    p_incr.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    p_incr.add_argument("--groups", nargs="+", default=["contracts"],
                        help=f"Groups: {list(AWARD_TYPE_GROUPS.keys())}")
    p_incr.add_argument("--agencies", nargs="*", help="Optional awarding toptier agency names (exact).")
    p_incr.add_argument("--chunk-days", type=int, default=7, help="Shard window in days (default 7)")
    p_incr.add_argument("--out", default=str(DEFAULT_OUT), help="Output directory")
    p_incr.add_argument("--fmt", default="parquet", choices=["parquet", "csv"], help="Output format")

    args = parser.parse_args()
    out_dir = Path(args.out)
    client = USAClient()
    try:
        if args.cmd == "bulk-backfill":
            bulk_backfill_awards(
                client=client,
                out_dir=out_dir,
                start_date=args.start_date,
                end_date=args.end_date,
                award_types=args.groups,
                agencies=args.agencies,
                date_type=args.date_type,
                file_format=args.file_format,
                columns=args.columns,
                min_split_days=args.min_split_days,
            )
        elif args.cmd == "incremental":
            incremental_awards(
                client=client,
                out_dir=out_dir,
                start_date=args.start_date,
                end_date=args.end_date,
                award_groups=args.groups,
                agencies=args.agencies,
                chunk_days=args.chunk_days,
                fmt=args.fmt,
            )
    finally:
        client.close()

if __name__ == "__main__":
    main()
