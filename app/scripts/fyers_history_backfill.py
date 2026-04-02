from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from fyers_apiv3 import fyersModel


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.auth import FyersAuthManager  # noqa: E402
from app.config import load_settings  # noqa: E402


CSV_COLUMNS = ("timestamp", "open", "high", "low", "close", "volume")


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_existing(path: Path) -> Dict[int, Dict[str, float]]:
    rows: Dict[int, Dict[str, float]] = {}
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = _to_int(row.get("timestamp"), 0)
            if ts <= 0:
                continue
            rows[ts] = {
                "timestamp": ts,
                "open": _to_float(row.get("open")),
                "high": _to_float(row.get("high")),
                "low": _to_float(row.get("low")),
                "close": _to_float(row.get("close")),
                "volume": _to_float(row.get("volume")),
            }
    return rows


def _write_rows(path: Path, rows: Dict[int, Dict[str, float]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = [rows[key] for key in sorted(rows.keys())]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(CSV_COLUMNS))
        writer.writeheader()
        for row in ordered:
            writer.writerow(
                {
                    "timestamp": int(row["timestamp"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": float(row["volume"]),
                }
            )


def _normalize_candles(raw_rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not isinstance(raw_rows, list):
        return out
    for item in raw_rows:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        ts = _to_int(item[0], 0)
        if ts > 10_000_000_000:
            ts = int(ts / 1000)
        if ts <= 0:
            continue
        out.append(
            {
                "timestamp": ts,
                "open": _to_float(item[1], 0.0),
                "high": _to_float(item[2], 0.0),
                "low": _to_float(item[3], 0.0),
                "close": _to_float(item[4], 0.0),
                "volume": _to_float(item[5], 0.0),
            }
        )
    return out


def _date_chunks(year: int, max_days: int) -> List[Tuple[date, date]]:
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    chunks: List[Tuple[date, date]] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=max_days - 1), end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks


def _history_payload(symbol: str, resolution: str, start: date, end: date) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "resolution": resolution,
        "date_format": "1",
        "range_from": start.isoformat(),
        "range_to": end.isoformat(),
        "cont_flag": "1",
    }


def _error_message(resp: Dict[str, Any]) -> str:
    return str(resp.get("message") or resp.get("error") or "FYERS history call failed")


@dataclass
class FetchStats:
    requested_chunks: int = 0
    successful_chunks: int = 0
    fetched_rows: int = 0


def _fetch_resolution_rows(
    *,
    client: Any,
    manager: FyersAuthManager,
    client_id: str,
    symbol: str,
    resolution: str,
    chunks: List[Tuple[date, date]],
    sleep_sec: float,
) -> Tuple[List[Dict[str, float]], FetchStats]:
    stats = FetchStats(requested_chunks=len(chunks))
    result: Dict[int, Dict[str, float]] = {}

    for idx, (start, end) in enumerate(chunks, start=1):
        payload = _history_payload(symbol=symbol, resolution=resolution, start=start, end=end)
        retries = 3
        while retries > 0:
            resp = client.history(data=payload)
            code = _to_int(resp.get("code"), 0) if isinstance(resp, dict) else 0
            status = str(resp.get("s", "")).strip().lower() if isinstance(resp, dict) else ""
            if isinstance(resp, dict) and code == 200 and status != "error":
                candles = _normalize_candles(resp.get("candles"))
                for row in candles:
                    result[int(row["timestamp"])] = row
                stats.successful_chunks += 1
                stats.fetched_rows += len(candles)
                print(
                    f"[{resolution}] chunk {idx}/{len(chunks)} {start.isoformat()} -> {end.isoformat()} "
                    f"rows={len(candles)}"
                )
                break

            message = _error_message(resp if isinstance(resp, dict) else {})
            auth_hint = "token" in message.lower() or "auth" in message.lower() or code in {401, 403}
            retries -= 1

            if auth_hint:
                raise RuntimeError(
                    f"[{resolution}] FYERS access token expired or is invalid. Re-authenticate and rerun the backfill."
                )

            if retries <= 0:
                raise RuntimeError(
                    f"[{resolution}] failed for {start.isoformat()} -> {end.isoformat()}: "
                    f"code={code}, status={status}, message={message}"
                )
            wait_sec = 1.5 * (4 - retries)
            print(
                f"[{resolution}] retrying {start.isoformat()} -> {end.isoformat()} "
                f"(remaining={retries}) message={message}"
            )
            time.sleep(wait_sec)

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    ordered = [result[key] for key in sorted(result.keys())]
    return ordered, stats


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-time FYERS history backfill with max 100-day chunks.")
    parser.add_argument("--year", type=int, default=2025, help="Year to fetch completely (default: 2025).")
    parser.add_argument("--symbol", default="", help="FYERS symbol. Default uses FYERS_SYMBOL from .env.")
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=100,
        help="Chunk size in days (FYERS limit is 100).",
    )
    parser.add_argument(
        "--sleep-sec",
        type=float,
        default=0.25,
        help="Pause between API calls.",
    )
    parser.add_argument(
        "--out-1m",
        default="data/training/candles_1m.csv",
        help="Output CSV path for 1m candles.",
    )
    parser.add_argument(
        "--out-5m",
        default="data/training/candles_5m.csv",
        help="Output CSV path for 5m candles.",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()

    chunk_days = max(1, min(100, int(args.chunk_days)))
    settings = load_settings()
    symbol = str(args.symbol or settings.fyers_symbol).strip()
    if not symbol:
        raise RuntimeError("symbol is required (set FYERS_SYMBOL in .env or pass --symbol)")

    manager = FyersAuthManager(
        auth_file=settings.fyers_auth_file,
        client_id=settings.fyers_client_id,
        secret_key=settings.fyers_secret_key,
        pin=settings.fyers_pin,
        redirect_uri=settings.fyers_redirect_uri,
        response_type=settings.fyers_response_type,
        scope=settings.fyers_scope,
        refresh_lead_sec=settings.fyers_refresh_lead_sec,
    )
    access_token = manager.get_active_access_token()
    if not access_token:
        status = manager.get_status()
        if not bool(status.get("authenticated")):
            raise RuntimeError("FYERS is not authenticated. Exchange a fresh auth code first.")
        raise RuntimeError("Could not load an active FYERS access token.")
    if not settings.fyers_client_id:
        raise RuntimeError("FYERS client_id is required.")

    client = fyersModel.FyersModel(
        client_id=settings.fyers_client_id,
        token=access_token,
        is_async=False,
        log_path="",
    )
    chunks = _date_chunks(year=int(args.year), max_days=chunk_days)

    print(f"Starting FYERS backfill for year={args.year}, symbol={symbol}, chunks={len(chunks)}, chunk_days={chunk_days}")

    fetched_1m, stats_1m = _fetch_resolution_rows(
        client=client,
        manager=manager,
        client_id=settings.fyers_client_id,
        symbol=symbol,
        resolution="1",
        chunks=chunks,
        sleep_sec=max(0.0, float(args.sleep_sec)),
    )
    fetched_5m, stats_5m = _fetch_resolution_rows(
        client=client,
        manager=manager,
        client_id=settings.fyers_client_id,
        symbol=symbol,
        resolution="5",
        chunks=chunks,
        sleep_sec=max(0.0, float(args.sleep_sec)),
    )

    out_1m = (PROJECT_ROOT / str(args.out_1m)).resolve()
    out_5m = (PROJECT_ROOT / str(args.out_5m)).resolve()

    existing_1m = _load_existing(out_1m)
    existing_5m = _load_existing(out_5m)
    before_1m = len(existing_1m)
    before_5m = len(existing_5m)

    for row in fetched_1m:
        existing_1m[int(row["timestamp"])] = row
    for row in fetched_5m:
        existing_5m[int(row["timestamp"])] = row

    _write_rows(out_1m, existing_1m)
    _write_rows(out_5m, existing_5m)

    after_1m = len(existing_1m)
    after_5m = len(existing_5m)
    add_1m = max(0, after_1m - before_1m)
    add_5m = max(0, after_5m - before_5m)

    first_1m = min(existing_1m.keys()) if existing_1m else 0
    last_1m = max(existing_1m.keys()) if existing_1m else 0
    first_5m = min(existing_5m.keys()) if existing_5m else 0
    last_5m = max(existing_5m.keys()) if existing_5m else 0

    print("")
    print("Backfill complete")
    print(
        f"1m: fetched={stats_1m.fetched_rows}, chunks_ok={stats_1m.successful_chunks}/{stats_1m.requested_chunks}, "
        f"added={add_1m}, total={after_1m}, ts_range={first_1m}->{last_1m}, file={out_1m}"
    )
    print(
        f"5m: fetched={stats_5m.fetched_rows}, chunks_ok={stats_5m.successful_chunks}/{stats_5m.requested_chunks}, "
        f"added={add_5m}, total={after_5m}, ts_range={first_5m}->{last_5m}, file={out_5m}"
    )


if __name__ == "__main__":
    main()
