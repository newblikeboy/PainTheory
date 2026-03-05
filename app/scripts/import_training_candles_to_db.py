from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import load_settings, mysql_connect_kwargs_from_parts  # noqa: E402
from app.main import MySQLCandleStore  # noqa: E402


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


def _chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    chunk: List[Dict[str, Any]] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _iter_candle_csv(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts = _to_int(row.get("timestamp"), 0)
            if ts <= 0:
                continue
            yield {
                "timestamp": ts,
                "open": _to_float(row.get("open"), 0.0),
                "high": _to_float(row.get("high"), 0.0),
                "low": _to_float(row.get("low"), 0.0),
                "close": _to_float(row.get("close"), 0.0),
                "volume": max(0.0, _to_float(row.get("volume"), 0.0)),
            }


def _count_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        count = -1
        for count, _ in enumerate(reader):
            pass
    return max(0, count)


def _verify_table_stats(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    connect_timeout_sec: int,
    ssl_mode: str = "",
    ssl_ca: str = "",
    ssl_disabled: bool = False,
    ssl_verify_cert: bool = False,
    ssl_verify_identity: bool = False,
    table: str,
) -> Dict[str, int]:
    import mysql.connector  # type: ignore

    conn = None
    cur = None
    try:
        kwargs = mysql_connect_kwargs_from_parts(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            connect_timeout_sec=connect_timeout_sec,
            with_database=True,
            autocommit=False,
            ssl_mode=ssl_mode,
            ssl_ca=ssl_ca,
            ssl_disabled=ssl_disabled,
            ssl_verify_cert=ssl_verify_cert,
            ssl_verify_identity=ssl_verify_identity,
        )
        conn = mysql.connector.connect(**kwargs)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
            """,
            (database, table),
        )
        cols = cur.fetchall() or []
        lower_to_real = {}
        for row in cols:
            name = str((row or {}).get("COLUMN_NAME") or "").strip()
            if name:
                lower_to_real[name.lower()] = name
        ts_col = ""
        for candidate in ("timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time"):
            if candidate in lower_to_real:
                ts_col = lower_to_real[candidate]
                break
        if not ts_col:
            raise RuntimeError(f"Unable to detect timestamp column in `{database}`.`{table}`")
        cur.execute(f"SELECT COUNT(*) AS cnt, MIN(`{ts_col}`) AS min_ts, MAX(`{ts_col}`) AS max_ts FROM `{table}`")
        row = cur.fetchone() or {}
        return {
            "count": _to_int(row.get("cnt"), 0),
            "min_ts": _to_int(row.get("min_ts"), 0),
            "max_ts": _to_int(row.get("max_ts"), 0),
        }
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-time import of training candle CSV into MySQL candle tables.")
    parser.add_argument("--csv-1m", default="data/training/candles_1m.csv", help="Path to 1m candle CSV file.")
    parser.add_argument("--csv-5m", default="data/training/candles_5m.csv", help="Path to 5m candle CSV file.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Rows per upsert batch.")
    parser.add_argument("--skip-1m", action="store_true", help="Skip importing 1m candles.")
    parser.add_argument("--skip-5m", action="store_true", help="Skip importing 5m candles.")
    return parser.parse_args()


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    args = parse_args()
    settings = load_settings()

    csv_1m = (PROJECT_ROOT / str(args.csv_1m)).resolve()
    csv_5m = (PROJECT_ROOT / str(args.csv_5m)).resolve()
    batch_size = max(100, int(args.batch_size))

    if not args.skip_1m and not csv_1m.exists():
        raise RuntimeError(f"1m csv not found: {csv_1m}")
    if not args.skip_5m and not csv_5m.exists():
        raise RuntimeError(f"5m csv not found: {csv_5m}")

    store = MySQLCandleStore(
        enabled=True,
        host=settings.auth_mysql_host,
        port=settings.auth_mysql_port,
        user=settings.auth_mysql_user,
        password=settings.auth_mysql_password,
        database=settings.auth_mysql_database,
        connect_timeout_sec=settings.auth_mysql_connect_timeout_sec,
        table_1m=settings.runtime_mysql_table_1m,
        table_5m=settings.runtime_mysql_table_5m,
    )
    if not store.enabled:
        raise RuntimeError("MySQL candle store is disabled. Check mysql-connector and DB settings.")

    start = time.time()
    print(
        "Import start:",
        f"db={settings.auth_mysql_database},",
        f"table_1m={settings.runtime_mysql_table_1m},",
        f"table_5m={settings.runtime_mysql_table_5m},",
        f"batch_size={batch_size}",
    )

    if not args.skip_1m:
        total_1m = _count_rows(csv_1m)
        done = 0
        print(f"1m csv rows={total_1m} file={csv_1m}")
        for chunk in _chunked(_iter_candle_csv(csv_1m), batch_size):
            store.persist_batch(candles_1m=chunk, candles_5m=[])
            done += len(chunk)
            print(f"1m imported {done}/{total_1m}")

    if not args.skip_5m:
        total_5m = _count_rows(csv_5m)
        done = 0
        print(f"5m csv rows={total_5m} file={csv_5m}")
        for chunk in _chunked(_iter_candle_csv(csv_5m), batch_size):
            store.persist_batch(candles_1m=[], candles_5m=chunk)
            done += len(chunk)
            print(f"5m imported {done}/{total_5m}")

    stats_1m = _verify_table_stats(
        host=settings.auth_mysql_host,
        port=settings.auth_mysql_port,
        user=settings.auth_mysql_user,
        password=settings.auth_mysql_password,
        database=settings.auth_mysql_database,
        connect_timeout_sec=settings.auth_mysql_connect_timeout_sec,
        ssl_mode=settings.auth_mysql_ssl_mode,
        ssl_ca=settings.auth_mysql_ssl_ca,
        ssl_disabled=settings.auth_mysql_ssl_disabled,
        ssl_verify_cert=settings.auth_mysql_ssl_verify_cert,
        ssl_verify_identity=settings.auth_mysql_ssl_verify_identity,
        table=settings.runtime_mysql_table_1m,
    )
    stats_5m = _verify_table_stats(
        host=settings.auth_mysql_host,
        port=settings.auth_mysql_port,
        user=settings.auth_mysql_user,
        password=settings.auth_mysql_password,
        database=settings.auth_mysql_database,
        connect_timeout_sec=settings.auth_mysql_connect_timeout_sec,
        ssl_mode=settings.auth_mysql_ssl_mode,
        ssl_ca=settings.auth_mysql_ssl_ca,
        ssl_disabled=settings.auth_mysql_ssl_disabled,
        ssl_verify_cert=settings.auth_mysql_ssl_verify_cert,
        ssl_verify_identity=settings.auth_mysql_ssl_verify_identity,
        table=settings.runtime_mysql_table_5m,
    )

    elapsed = time.time() - start
    print("")
    print(f"Import complete in {elapsed:.1f}s")
    print(
        f"DB 1m `{settings.runtime_mysql_table_1m}`: "
        f"count={stats_1m['count']}, min_ts={stats_1m['min_ts']}, max_ts={stats_1m['max_ts']}"
    )
    print(
        f"DB 5m `{settings.runtime_mysql_table_5m}`: "
        f"count={stats_5m['count']}, min_ts={stats_5m['min_ts']}, max_ts={stats_5m['max_ts']}"
    )


if __name__ == "__main__":
    main()
