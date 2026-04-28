from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .config import Settings, mysql_connect_kwargs_from_parts
from .pain_theory_ai.runtime import PainTheoryRuntime
from .paper_trade import PaperTradeEngine


try:
    _IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    _IST = timezone(timedelta(hours=5, minutes=30))


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


def _fmt_ist(ts: int) -> str:
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(int(ts), tz=_IST).isoformat()


class MySQLBacktestDataSource:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.host = str(settings.auth_mysql_host or "127.0.0.1")
        self.port = int(settings.auth_mysql_port or 3306)
        self.user = str(settings.auth_mysql_user or "")
        self.password = str(settings.auth_mysql_password or "")
        self.database = self._validate_identifier(settings.auth_mysql_database, "AUTH_MYSQL_DATABASE")
        self.connect_timeout_sec = max(2, int(settings.auth_mysql_connect_timeout_sec or 5))
        self.ssl_mode = str(settings.auth_mysql_ssl_mode or "").strip()
        self.ssl_ca = str(settings.auth_mysql_ssl_ca or "").strip()
        self.ssl_disabled = bool(settings.auth_mysql_ssl_disabled)
        self.ssl_verify_cert = bool(settings.auth_mysql_ssl_verify_cert)
        self.ssl_verify_identity = bool(settings.auth_mysql_ssl_verify_identity)
        self.table_1m = self._validate_identifier(settings.runtime_mysql_table_1m, "RUNTIME_MYSQL_TABLE_1M")
        self.options_table = self._validate_identifier(
            settings.runtime_mysql_options_table,
            "RUNTIME_MYSQL_OPTIONS_TABLE",
        )
        self.option_strikes_table = self._validate_identifier(
            settings.runtime_mysql_option_strikes_table,
            "RUNTIME_MYSQL_OPTION_STRIKES_TABLE",
        )

        try:
            import mysql.connector  # type: ignore
        except ImportError as exc:
            raise RuntimeError("mysql-connector-python is required for backtest") from exc
        self._mysql = mysql.connector

        if not self.user:
            raise RuntimeError("AUTH_MYSQL_USER is required for backtest")

    @staticmethod
    def _validate_identifier(value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise RuntimeError(f"{label} must contain only letters, numbers, and underscores")
        return text

    def _connect(self):
        kwargs = mysql_connect_kwargs_from_parts(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout_sec=self.connect_timeout_sec,
            with_database=True,
            autocommit=False,
            ssl_mode=self.ssl_mode,
            ssl_ca=self.ssl_ca,
            ssl_disabled=self.ssl_disabled,
            ssl_verify_cert=self.ssl_verify_cert,
            ssl_verify_identity=self.ssl_verify_identity,
        )
        return self._mysql.connect(**kwargs)

    def _resolve_candle_columns(self, conn: Any, table: str) -> Dict[str, str]:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.database, table),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        names = {str((row or {}).get("COLUMN_NAME", "")).strip().lower(): str((row or {}).get("COLUMN_NAME", "")).strip() for row in rows}

        def pick(candidates: List[str]) -> str:
            for candidate in candidates:
                if candidate in names:
                    return names[candidate]
            return ""

        mapping = {
            "ts": pick(["timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time"]),
            "o": pick(["open", "o"]),
            "h": pick(["high", "h"]),
            "l": pick(["low", "l"]),
            "c": pick(["close", "c"]),
            "v": pick(["volume", "vol", "v"]),
        }
        missing = [key for key, value in mapping.items() if not value]
        if missing:
            raise RuntimeError(f"table `{self.database}`.`{table}` missing candle columns: {','.join(missing)}")
        return mapping

    def fetch_candles_1m(self, *, start_ts: int, end_ts: int, max_rows: int) -> List[Dict[str, float]]:
        rows_limit = max(100, min(int(max_rows or 0), 500000))
        conn = None
        cur = None
        try:
            conn = self._connect()
            mapping = self._resolve_candle_columns(conn, self.table_1m)
            ts_col = mapping["ts"]
            o_col = mapping["o"]
            h_col = mapping["h"]
            l_col = mapping["l"]
            c_col = mapping["c"]
            v_col = mapping["v"]
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    `{ts_col}` AS ts,
                    `{o_col}` AS o,
                    `{h_col}` AS h,
                    `{l_col}` AS l,
                    `{c_col}` AS c,
                    `{v_col}` AS v
                FROM `{self.table_1m}`
                WHERE `{ts_col}` >= %s AND `{ts_col}` <= %s
                ORDER BY `{ts_col}` ASC
                LIMIT %s
                """,
                (int(start_ts), int(end_ts), rows_limit),
            )
            data = cur.fetchall() or []
            out: List[Dict[str, float]] = []
            for row in data:
                ts = _to_int(row.get("ts"), 0)
                if ts <= 0:
                    continue
                out.append(
                    {
                        "timestamp": ts,
                        "open": _to_float(row.get("o"), 0.0),
                        "high": _to_float(row.get("h"), 0.0),
                        "low": _to_float(row.get("l"), 0.0),
                        "close": _to_float(row.get("c"), 0.0),
                        "volume": max(0.0, _to_float(row.get("v"), 0.0)),
                    }
                )
            return out
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def fetch_option_snapshots(self, *, start_ts: int, end_ts: int, max_rows: int) -> List[Dict[str, Any]]:
        rows_limit = max(100, min(int(max_rows or 0), 200000))
        conn = None
        cur = None
        try:
            conn = self._connect()
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    snapshot_id,
                    timestamp,
                    atm_strike,
                    atm_ce_ltp,
                    atm_pe_ltp,
                    atm_ce_volume,
                    atm_pe_volume,
                    atm_ce_oi_change,
                    atm_pe_oi_change,
                    band_volume_near_atm,
                    band_volume_total
                FROM `{self.options_table}`
                WHERE timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
                LIMIT %s
                """,
                (int(start_ts), int(end_ts), rows_limit),
            )
            rows = cur.fetchall() or []
            out: List[Dict[str, Any]] = []
            for idx, row in enumerate(rows):
                ts = _to_int(row.get("timestamp"), 0)
                if ts <= 0:
                    continue
                snapshot_id = str(row.get("snapshot_id") or "").strip()
                if not snapshot_id:
                    snapshot_id = f"oc_{ts}_legacy_{idx}"
                out.append(
                    {
                        "snapshot_id": snapshot_id,
                        "timestamp": ts,
                        "atm_strike": _to_float(row.get("atm_strike"), 0.0),
                        "atm_ce_ltp": _to_float(row.get("atm_ce_ltp"), 0.0),
                        "atm_pe_ltp": _to_float(row.get("atm_pe_ltp"), 0.0),
                        "atm_ce_volume": _to_float(row.get("atm_ce_volume"), 0.0),
                        "atm_pe_volume": _to_float(row.get("atm_pe_volume"), 0.0),
                        "atm_ce_oi_change": _to_float(row.get("atm_ce_oi_change"), 0.0),
                        "atm_pe_oi_change": _to_float(row.get("atm_pe_oi_change"), 0.0),
                        "band_volume_near_atm": _to_float(row.get("band_volume_near_atm"), 0.0),
                        "band_volume_total": _to_float(row.get("band_volume_total"), 0.0),
                    }
                )
            return out
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def fetch_option_strikes(self, *, start_ts: int, end_ts: int, max_rows: int) -> Dict[str, List[Dict[str, Any]]]:
        rows_limit = max(200, min(int(max_rows or 0), 800000))
        conn = None
        cur = None
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        try:
            conn = self._connect()
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    snapshot_id,
                    timestamp,
                    option_type,
                    strike,
                    symbol,
                    ltp,
                    volume,
                    oi_change
                FROM `{self.option_strikes_table}`
                WHERE timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC, strike ASC, option_type ASC
                LIMIT %s
                """,
                (int(start_ts), int(end_ts), rows_limit),
            )
            rows = cur.fetchall() or []
            for row in rows:
                snapshot_id = str(row.get("snapshot_id") or "").strip()
                ts = _to_int(row.get("timestamp"), 0)
                if not snapshot_id or ts <= 0:
                    continue
                grouped.setdefault(snapshot_id, []).append(
                    {
                        "option_type": str(row.get("option_type") or "").upper().strip(),
                        "strike": _to_float(row.get("strike"), 0.0),
                        "symbol": str(row.get("symbol") or "").strip(),
                        "ltp": _to_float(row.get("ltp"), 0.0),
                        "volume": _to_float(row.get("volume"), 0.0),
                        "oi_change": _to_float(row.get("oi_change"), 0.0),
                        "timestamp": ts,
                    }
                )
            return grouped
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()


class MySQLBacktestResultStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.host = str(settings.auth_mysql_host or "127.0.0.1")
        self.port = int(settings.auth_mysql_port or 3306)
        self.user = str(settings.auth_mysql_user or "")
        self.password = str(settings.auth_mysql_password or "")
        self.database = self._validate_identifier(settings.auth_mysql_database, "AUTH_MYSQL_DATABASE")
        self.connect_timeout_sec = max(2, int(settings.auth_mysql_connect_timeout_sec or 5))
        self.ssl_mode = str(settings.auth_mysql_ssl_mode or "").strip()
        self.ssl_ca = str(settings.auth_mysql_ssl_ca or "").strip()
        self.ssl_disabled = bool(settings.auth_mysql_ssl_disabled)
        self.ssl_verify_cert = bool(settings.auth_mysql_ssl_verify_cert)
        self.ssl_verify_identity = bool(settings.auth_mysql_ssl_verify_identity)
        self.runs_table = self._validate_identifier(
            settings.backtest_mysql_runs_table,
            "BACKTEST_MYSQL_RUNS_TABLE",
        )
        self.trades_table = self._validate_identifier(
            settings.backtest_mysql_trades_table,
            "BACKTEST_MYSQL_TRADES_TABLE",
        )
        try:
            import mysql.connector  # type: ignore
        except ImportError as exc:
            raise RuntimeError("mysql-connector-python is required for backtest result storage") from exc
        self._mysql = mysql.connector

        if not self.user:
            raise RuntimeError("AUTH_MYSQL_USER is required for backtest result storage")
        self._initialize_schema()

    @staticmethod
    def _validate_identifier(value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise RuntimeError(f"{label} must contain only letters, numbers, and underscores")
        return text

    def _connect(self, with_database: bool = True):
        kwargs = mysql_connect_kwargs_from_parts(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            connect_timeout_sec=self.connect_timeout_sec,
            with_database=with_database,
            autocommit=False,
            ssl_mode=self.ssl_mode,
            ssl_ca=self.ssl_ca,
            ssl_disabled=self.ssl_disabled,
            ssl_verify_cert=self.ssl_verify_cert,
            ssl_verify_identity=self.ssl_verify_identity,
        )
        return self._mysql.connect(**kwargs)

    def _initialize_schema(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=False)
            cur = conn.cursor()
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            cur.close()
            conn.close()

            conn = self._connect(with_database=True)
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.runs_table}` (
                    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    job_id VARCHAR(96) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    error_message TEXT NULL,
                    started_at BIGINT NOT NULL,
                    completed_at BIGINT NOT NULL,
                    window_start_ts BIGINT NOT NULL DEFAULT 0,
                    window_end_ts BIGINT NOT NULL DEFAULT 0,
                    closed_trades INT NOT NULL DEFAULT 0,
                    win_rate_pct DOUBLE NOT NULL DEFAULT 0,
                    net_points DOUBLE NOT NULL DEFAULT 0,
                    params_json LONGTEXT NOT NULL,
                    summary_json LONGTEXT NOT NULL,
                    result_json LONGTEXT NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    UNIQUE KEY uq_{self.runs_table}_job (job_id),
                    INDEX idx_{self.runs_table}_completed (completed_at),
                    INDEX idx_{self.runs_table}_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.trades_table}` (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    run_id BIGINT NOT NULL,
                    seq_no INT NOT NULL,
                    trade_id BIGINT NULL,
                    direction VARCHAR(16) NULL,
                    entry_ts BIGINT NULL,
                    exit_ts BIGINT NULL,
                    entry_price DOUBLE NULL,
                    exit_price DOUBLE NULL,
                    points DOUBLE NULL,
                    outcome VARCHAR(16) NULL,
                    close_reason VARCHAR(64) NULL,
                    holding_seconds INT NULL,
                    signal_json LONGTEXT NULL,
                    created_at BIGINT NOT NULL,
                    UNIQUE KEY uq_{self.trades_table}_run_seq (run_id, seq_no),
                    INDEX idx_{self.trades_table}_run (run_id),
                    INDEX idx_{self.trades_table}_entry (entry_ts)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def save_run(
        self,
        *,
        job_id: str,
        started_at: int,
        completed_at: int,
        params: Dict[str, Any],
        result: Dict[str, Any],
        status: str = "completed",
        error_message: str = "",
    ) -> int:
        safe_job_id = str(job_id or "").strip()
        if not safe_job_id:
            raise ValueError("job_id is required")

        window = result.get("window") if isinstance(result, dict) else {}
        window_start_ts = _to_int((window or {}).get("start_ts"), 0)
        window_end_ts = _to_int((window or {}).get("end_ts"), 0)
        summary = result.get("summary") if isinstance(result, dict) else {}
        closed_trades = _to_int((summary or {}).get("closed_trades"), 0)
        win_rate_pct = _to_float((summary or {}).get("win_rate_pct"), 0.0)
        net_points = _to_float((summary or {}).get("net_points"), 0.0)
        params_json = json.dumps(params if isinstance(params, dict) else {}, ensure_ascii=True)
        summary_json = json.dumps(summary if isinstance(summary, dict) else {}, ensure_ascii=True)
        result_json = json.dumps(result if isinstance(result, dict) else {}, ensure_ascii=True)
        now_ts = max(_to_int(completed_at, 0), _to_int(started_at, 0), int(datetime.now(tz=timezone.utc).timestamp()))
        run_id = 0
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                INSERT INTO `{self.runs_table}` (
                    job_id,
                    status,
                    error_message,
                    started_at,
                    completed_at,
                    window_start_ts,
                    window_end_ts,
                    closed_trades,
                    win_rate_pct,
                    net_points,
                    params_json,
                    summary_json,
                    result_json,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    status = VALUES(status),
                    error_message = VALUES(error_message),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    window_start_ts = VALUES(window_start_ts),
                    window_end_ts = VALUES(window_end_ts),
                    closed_trades = VALUES(closed_trades),
                    win_rate_pct = VALUES(win_rate_pct),
                    net_points = VALUES(net_points),
                    params_json = VALUES(params_json),
                    summary_json = VALUES(summary_json),
                    result_json = VALUES(result_json),
                    updated_at = VALUES(updated_at)
                """,
                (
                    safe_job_id,
                    str(status or "completed")[:16],
                    str(error_message or "")[:4000] or None,
                    max(0, _to_int(started_at, 0)),
                    max(0, _to_int(completed_at, 0)),
                    max(0, window_start_ts),
                    max(0, window_end_ts),
                    max(0, closed_trades),
                    float(win_rate_pct),
                    float(net_points),
                    params_json,
                    summary_json,
                    result_json,
                    now_ts,
                    now_ts,
                ),
            )
            cur.execute(
                f"SELECT run_id FROM `{self.runs_table}` WHERE job_id = %s LIMIT 1",
                (safe_job_id,),
            )
            row = cur.fetchone() or {}
            run_id = max(0, _to_int(row.get("run_id"), 0))
            if run_id <= 0:
                raise RuntimeError("Unable to persist backtest run_id")

            cur.execute(f"DELETE FROM `{self.trades_table}` WHERE run_id = %s", (run_id,))

            trades = result.get("trades") if isinstance(result, dict) else []
            if isinstance(trades, list) and trades:
                rows: List[tuple[Any, ...]] = []
                for index, item in enumerate(trades, start=1):
                    if not isinstance(item, dict):
                        continue
                    rows.append(
                        (
                            run_id,
                            index,
                            _to_int(item.get("trade_id"), 0) or None,
                            str(item.get("direction") or "")[:16] or None,
                            _to_int(item.get("entry_ts"), 0) or None,
                            _to_int(item.get("exit_ts"), 0) or None,
                            _to_float(item.get("entry_price"), 0.0) if item.get("entry_price") is not None else None,
                            _to_float(item.get("exit_price"), 0.0) if item.get("exit_price") is not None else None,
                            _to_float(item.get("points"), 0.0) if item.get("points") is not None else None,
                            str(item.get("outcome") or "")[:16] or None,
                            str(item.get("close_reason") or "")[:64] or None,
                            _to_int(item.get("holding_seconds"), 0) or None,
                            json.dumps(item.get("signal") if isinstance(item.get("signal"), dict) else {}, ensure_ascii=True),
                            now_ts,
                        )
                    )
                if rows:
                    cur.executemany(
                        f"""
                        INSERT INTO `{self.trades_table}` (
                            run_id,
                            seq_no,
                            trade_id,
                            direction,
                            entry_ts,
                            exit_ts,
                            entry_price,
                            exit_price,
                            points,
                            outcome,
                            close_reason,
                            holding_seconds,
                            signal_json,
                            created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            trade_id = VALUES(trade_id),
                            direction = VALUES(direction),
                            entry_ts = VALUES(entry_ts),
                            exit_ts = VALUES(exit_ts),
                            entry_price = VALUES(entry_price),
                            exit_price = VALUES(exit_price),
                            points = VALUES(points),
                            outcome = VALUES(outcome),
                            close_reason = VALUES(close_reason),
                            holding_seconds = VALUES(holding_seconds),
                            signal_json = VALUES(signal_json),
                            created_at = VALUES(created_at)
                        """,
                        rows,
                    )
            conn.commit()
            return run_id
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def fetch_latest_completed(self) -> Optional[Dict[str, Any]]:
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT run_id, result_json
                FROM `{self.runs_table}`
                WHERE status = %s
                ORDER BY completed_at DESC, run_id DESC
                LIMIT 1
                """,
                ("completed",),
            )
            row = cur.fetchone()
            if not isinstance(row, dict):
                return None
            payload = row.get("result_json")
            if isinstance(payload, (bytes, bytearray)):
                payload = payload.decode("utf-8", errors="ignore")
            if not payload:
                return None
            loaded = json.loads(str(payload))
            if not isinstance(loaded, dict):
                return None
            loaded["db_run_id"] = _to_int(row.get("run_id"), 0)
            return loaded
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()


def _aggregate_5m_from_1m(candles: List[Dict[str, Any]]) -> List[Dict[str, float]]:
    if not candles:
        return []
    rows = sorted(candles, key=lambda item: _to_int(item.get("timestamp"), 0))
    result: List[Dict[str, float]] = []
    current: Optional[Dict[str, float]] = None
    for row in rows:
        ts = _to_int(row.get("timestamp"), 0)
        if ts <= 0:
            continue
        bucket_start = ts - (ts % 300)
        open_ = _to_float(row.get("open"), 0.0)
        high = _to_float(row.get("high"), open_)
        low = _to_float(row.get("low"), open_)
        close = _to_float(row.get("close"), open_)
        volume = max(0.0, _to_float(row.get("volume"), 0.0))
        if current is None or _to_int(current.get("timestamp"), 0) != bucket_start:
            if current is not None:
                result.append(current)
            current = {
                "timestamp": float(bucket_start),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
            continue
        current["high"] = max(float(current["high"]), high)
        current["low"] = min(float(current["low"]), low)
        current["close"] = close
        current["volume"] = float(current["volume"]) + volume
    if current is not None:
        result.append(current)
    return [
        {
            "timestamp": int(_to_float(row.get("timestamp"), 0.0)),
            "open": _to_float(row.get("open"), 0.0),
            "high": _to_float(row.get("high"), 0.0),
            "low": _to_float(row.get("low"), 0.0),
            "close": _to_float(row.get("close"), 0.0),
            "volume": max(0.0, _to_float(row.get("volume"), 0.0)),
        }
        for row in result
    ]


def _max_streak(flags: List[bool]) -> int:
    best = 0
    cur = 0
    for flag in flags:
        if flag:
            cur += 1
            if cur > best:
                best = cur
        else:
            cur = 0
    return best


def _extra_metrics(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not trades:
        return {
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "profit_factor": None,
            "avg_points": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "max_win_streak": 0,
            "max_loss_streak": 0,
            "long_trades": 0,
            "short_trades": 0,
            "close_reason_counts": {},
        }

    points = [_to_float(row.get("points"), 0.0) for row in trades]
    gross_profit = sum(item for item in points if item > 0)
    gross_loss_abs = abs(sum(item for item in points if item < 0))
    profit_factor: Optional[float]
    if gross_loss_abs > 0.0:
        profit_factor = gross_profit / gross_loss_abs
    elif gross_profit > 0.0:
        profit_factor = 999.0
    else:
        profit_factor = None

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for item in points:
        equity += item
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown

    outcomes = [str(row.get("outcome") or "").lower() for row in trades]
    max_win_streak = _max_streak([item == "profit" for item in outcomes])
    max_loss_streak = _max_streak([item == "loss" for item in outcomes])

    long_trades = sum(1 for row in trades if str(row.get("direction") or "").upper() == "LONG")
    short_trades = sum(1 for row in trades if str(row.get("direction") or "").upper() == "SHORT")

    close_reason_counts: Dict[str, int] = {}
    for row in trades:
        reason = str(row.get("close_reason") or "unknown").strip().lower() or "unknown"
        close_reason_counts[reason] = int(close_reason_counts.get(reason, 0)) + 1

    total = max(1, len(points))
    return {
        "gross_profit": round(float(gross_profit), 4),
        "gross_loss": round(float(gross_loss_abs), 4),
        "profit_factor": (round(float(profit_factor), 4) if profit_factor is not None else None),
        "avg_points": round(float(sum(points) / float(total)), 4),
        "expectancy": round(float(sum(points) / float(total)), 4),
        "max_drawdown": round(float(max_drawdown), 4),
        "max_win_streak": int(max_win_streak),
        "max_loss_streak": int(max_loss_streak),
        "long_trades": int(long_trades),
        "short_trades": int(short_trades),
        "close_reason_counts": close_reason_counts,
    }


class BacktestEngine:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.source = MySQLBacktestDataSource(settings)

    def run(
        self,
        *,
        start_ts: int,
        end_ts: int,
        max_candles: int = 120000,
        max_trades: int = 1000,
        close_open_at_end: bool = True,
        require_pain_release: Optional[bool] = None,
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        if start_ts <= 0 or end_ts <= 0 or end_ts <= start_ts:
            raise ValueError("Invalid backtest window")

        def emit_progress(processed: int, total: int, message: str) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(max(0, int(processed)), max(1, int(total)), str(message or "running"))
            except Exception:
                return

        emit_progress(0, 100, "loading_candles")
        candles_1m = self.source.fetch_candles_1m(start_ts=start_ts, end_ts=end_ts, max_rows=max_candles)
        if not candles_1m:
            raise ValueError("No 1m candles found in selected range")

        candles_5m = _aggregate_5m_from_1m(candles_1m)
        emit_progress(2, 100, "loading_option_snapshots")

        options = self.source.fetch_option_snapshots(
            start_ts=start_ts,
            end_ts=end_ts,
            max_rows=max(max_candles * 2, 5000),
        )
        emit_progress(4, 100, "loading_option_strikes")
        strike_map = self.source.fetch_option_strikes(
            start_ts=start_ts,
            end_ts=end_ts,
            max_rows=max(max_candles * 24, 20000),
        )
        option_strike_rows = 0
        for item in options:
            sid = str(item.get("snapshot_id") or "").strip()
            strikes = list(strike_map.get(sid, []))
            option_strike_rows += len(strikes)
            item["strikes"] = strikes

        runtime = PainTheoryRuntime(
            model_path=self.settings.pain_ai_model_path,
            use_model=self.settings.pain_ai_use_model,
            use_5m_primary=self.settings.runtime_use_5m_primary,
        )
        # Pain-release gating is intentionally disabled for backtest execution.
        require_release = False
        paper = PaperTradeEngine(
            enabled=True,
            symbol=self.settings.fyers_symbol,
            hold_minutes=max(1, int(self.settings.paper_trade_hold_minutes)),
            min_confidence=float(self.settings.paper_trade_min_confidence),
            entry_guidance=str(self.settings.paper_trade_entry_guidance),
            one_trade_at_a_time=bool(self.settings.paper_trade_one_trade_at_a_time),
            require_pain_release=require_release,
            entry_style=str(self.settings.paper_trade_entry_style),
            min_compression_bars=int(self.settings.paper_trade_min_compression_bars),
            target_r_multiple=float(self.settings.paper_trade_target_r_multiple),
            avoid_noon_chop=bool(self.settings.paper_trade_avoid_noon_chop),
            feedback_enabled=bool(self.settings.paper_trade_feedback_enabled),
            feedback_min_trades=int(self.settings.paper_trade_feedback_min_trades),
            feedback_block_win_rate=float(self.settings.paper_trade_feedback_block_win_rate),
            feedback_penalty_floor=float(self.settings.paper_trade_feedback_penalty_floor),
            feedback_reward_cap=float(self.settings.paper_trade_feedback_reward_cap),
            mysql_host=self.settings.auth_mysql_host,
            mysql_port=self.settings.auth_mysql_port,
            mysql_user=self.settings.auth_mysql_user,
            mysql_password=self.settings.auth_mysql_password,
            mysql_database=self.settings.auth_mysql_database,
            mysql_connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            mysql_ssl_mode=self.settings.auth_mysql_ssl_mode,
            mysql_ssl_ca=self.settings.auth_mysql_ssl_ca,
            mysql_ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            mysql_ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            mysql_ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
            mysql_state_table=self.settings.paper_trade_mysql_state_table,
            mysql_trades_table=self.settings.paper_trade_mysql_trades_table,
            mysql_feedback_table=self.settings.paper_trade_mysql_feedback_table,
            mysql_mistakes_table=self.settings.paper_trade_mysql_mistakes_table,
            option_upnl_exit_points=float(self.settings.paper_trade_option_upnl_exit_points),
            storage_backend="memory",
            model_driven_execution=True,
        )

        idx_5m = 0
        idx_opt = 0
        latest_option: Optional[Dict[str, Any]] = None

        total_candles = max(1, len(candles_1m))
        emit_progress(5, 100, "replaying_data")
        for index, candle in enumerate(candles_1m, start=1):
            candle_ts = _to_int(candle.get("timestamp"), 0)
            if candle_ts <= 0:
                continue

            matched_5m: List[Dict[str, Any]] = []
            while idx_5m < len(candles_5m):
                c5_ts = _to_int(candles_5m[idx_5m].get("timestamp"), 0)
                if c5_ts <= candle_ts:
                    matched_5m.append(candles_5m[idx_5m])
                    idx_5m += 1
                    continue
                break

            matched_options: List[Dict[str, Any]] = []
            while idx_opt < len(options):
                opt_ts = _to_int(options[idx_opt].get("timestamp"), 0)
                if opt_ts <= candle_ts:
                    row = options[idx_opt]
                    matched_options.append(row)
                    latest_option = row
                    idx_opt += 1
                    continue
                break

            ai_state = runtime.ingest(
                candles=[candle],
                candles_5m=matched_5m,
                option_snapshots=matched_options,
            )
            paper.on_candle(candle, ai_state, latest_option)
            if index == total_candles or index % 50 == 0:
                scaled = 5 + int((index / float(total_candles)) * 90)
                emit_progress(min(95, scaled), 100, "replaying_data")

        if close_open_at_end:
            state_now = paper.get_state()
            active = state_now.get("active_trade") if isinstance(state_now, dict) else None
            if isinstance(active, dict):
                final_ts = _to_int(candles_1m[-1].get("timestamp"), 0)
                final_px = _to_float(candles_1m[-1].get("close"), 0.0)
                if final_ts > 0 and final_px > 0.0:
                    paper._close_active_trade(exit_ts=final_ts, exit_price=final_px, reason="backtest_end")

        emit_progress(97, 100, "computing_metrics")
        paper_state = paper.get_state()
        closed_total = max(0, _to_int(paper_state.get("closed_trades"), 0))
        trades_limit = max(100, min(max(int(max_trades), closed_total), 5000))
        trades = paper.get_trades(limit=trades_limit)
        extras = _extra_metrics(trades)

        start_row_ts = _to_int(candles_1m[0].get("timestamp"), start_ts)
        end_row_ts = _to_int(candles_1m[-1].get("timestamp"), end_ts)

        summary = {
            "closed_trades": int(paper_state.get("closed_trades", 0)),
            "profit_trades": int(paper_state.get("profit_trades", 0)),
            "loss_trades": int(paper_state.get("loss_trades", 0)),
            "flat_trades": int(paper_state.get("flat_trades", 0)),
            "win_rate_pct": float(_to_float(paper_state.get("win_rate_pct"), 0.0)),
            "net_points": float(_to_float(paper_state.get("net_points"), 0.0)),
            "open_trade_present": bool(paper_state.get("active_trade") is not None),
            **extras,
        }

        result = {
            "ok": True,
            "window": {
                "start_ts": int(start_ts),
                "end_ts": int(end_ts),
                "start_ist": _fmt_ist(start_ts),
                "end_ist": _fmt_ist(end_ts),
                "data_start_ts": int(start_row_ts),
                "data_end_ts": int(end_row_ts),
                "data_start_ist": _fmt_ist(start_row_ts),
                "data_end_ist": _fmt_ist(end_row_ts),
            },
            "model": {
                "use_model": bool(self.settings.pain_ai_use_model),
                "model_path": str(self.settings.pain_ai_model_path),
                "analysis_timeframe": "5m" if self.settings.runtime_use_5m_primary else "1m",
                "execution_timeframe": "1m",
            },
            "data": {
                "candles_1m": len(candles_1m),
                "candles_5m": len(candles_5m),
                "option_snapshots": len(options),
                "option_strike_rows": int(option_strike_rows),
            },
            "paper_config": {
                "require_pain_release": bool(require_release),
                "entry_style": str(self.settings.paper_trade_entry_style),
                "min_confidence": float(self.settings.paper_trade_min_confidence),
                "entry_guidance": str(self.settings.paper_trade_entry_guidance),
                "target_r_multiple": float(self.settings.paper_trade_target_r_multiple),
                "avoid_noon_chop": bool(self.settings.paper_trade_avoid_noon_chop),
                "feedback_enabled": bool(self.settings.paper_trade_feedback_enabled),
                "model_driven_execution": True,
            },
            "summary": summary,
            "trades": trades[-max(1, min(int(max_trades), 2000)):],
        }
        emit_progress(100, 100, "completed")
        return result
