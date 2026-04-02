import asyncio
import calendar
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import struct
import time
import uuid
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests

from .auth import FyersAuthManager, MySQLUserAuthStore
from .backtest import BacktestEngine, MySQLBacktestResultStore
from .config import Settings, load_settings, mysql_connect_kwargs, mysql_connect_kwargs_from_parts
from .db_retention import MySQLRetentionService
from .ml.auto_retrain import AutoRetrainService
from .option_contracts import (
    build_contract_key,
    build_symbol_search_candidates,
    contract_tradeability,
    extract_option_contract,
    normalize_option_symbol,
)
from .paper_trade import PaperTradeEngine
from .pain_theory_ai import PainTheoryRuntime
from .stream.fyers_stream import FyersTickStream
from .stream.option_chain import OptionChainPoller
from .stream.fyers_quote import FyersQuoteClient

logger = logging.getLogger(__name__)
try:
    _IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    _IST = timezone(timedelta(hours=5, minutes=30))
_NSE_OPEN_TIME = dt_time(9, 15)
_NSE_CLOSE_TIME = dt_time(15, 30)
_SOS_DATA_STALE_SEC = 60
_SOS_WATCHDOG_POLL_SEC = 5
_SOS_RETRY_COOLDOWN_SEC = 15
_SOS_UNDERLYING_CLOSE_REASON = "SOS Close - Unablt to Fet Underlying Value of Order Strike"
_SOS_OPTION_QUOTE_CLOSE_REASON = "SOS Close - Unable to Fetch Option Quote of Order Strike"
_RAZORPAY_PLAN_CATALOG: Dict[str, Dict[str, Any]] = {
    "monthly": {
        "name": "Sensible Algo Monthly",
        "amount_paisa": 300000,
        "duration_days": 30,
    },
    "quarterly": {
        "name": "Sensible Algo Quarterly",
        "amount_paisa": 750000,
        "duration_days": 90,
    },
    "yearly": {
        "name": "Sensible Algo Yearly",
        "amount_paisa": 2400000,
        "duration_days": 365,
    },
}


class IngestCandle(BaseModel):
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class IngestOptionStrike(BaseModel):
    option_type: str
    strike: float
    symbol: str = ""
    symbol_token: str = ""
    exchange: str = ""
    underlying: str = ""
    expiry_date: str = ""
    expiry_ts: int = 0
    expiry_kind: str = ""
    contract_key: str = ""
    ltp: float = 0.0
    volume: float = 0.0
    oi_change: float = 0.0


class IngestOptionSnapshot(BaseModel):
    timestamp: int
    snapshot_id: str | None = None
    atm_strike: float | None = None
    atm_ce_ltp: float = 0.0
    atm_pe_ltp: float = 0.0
    atm_ce_volume: float = 0.0
    atm_pe_volume: float = 0.0
    atm_ce_oi_change: float = 0.0
    atm_pe_oi_change: float = 0.0
    band_volume_near_atm: float = 0.0
    band_volume_total: float = 0.0
    strikes: List[IngestOptionStrike] = []


class IngestPayload(BaseModel):
    candles: List[IngestCandle]
    option_snapshots: List[IngestOptionSnapshot] = []


class SignupPayload(BaseModel):
    full_name: str
    email: str
    mobile_number: str
    password: str
    confirm_password: str


class LoginPayload(BaseModel):
    email: str | None = None
    username: str | None = None
    password: str
    role: str | None = "user"


class BrokerClientIdPayload(BaseModel):
    client_id: str
    api_key: str = ""
    pin: str = ""
    totp_secret: str = ""


class TradingEnginePayload(BaseModel):
    enabled: bool


class LotSizePayload(BaseModel):
    lot_size_qty: int


class UserLotPayload(BaseModel):
    lot_count: int


class RazorpayOrderPayload(BaseModel):
    plan_code: str | None = None
    plan_name: str | None = None


class RazorpayVerifyPayload(BaseModel):
    razorpay_order_id: str
    razorpay_payment_id: str
    razorpay_signature: str
    plan_code: str | None = None
    plan_name: str | None = None


class FyersExchangePayload(BaseModel):
    auth_code: str


class FyersRefreshPayload(BaseModel):
    force: bool = False


class PaperResetPayload(BaseModel):
    close_open: bool = False


class RetrainTriggerPayload(BaseModel):
    force: bool = False


class RetentionTriggerPayload(BaseModel):
    reason: str = "manual"


class BacktestRunPayload(BaseModel):
    start: int | str
    end: int | str
    max_candles: int = 120000
    max_trades: int = 1000
    close_open_at_end: bool = True
    require_pain_release: bool = False


class MySQLCandleStore:
    def __init__(
        self,
        *,
        enabled: bool,
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
        table_1m: str,
        table_5m: str,
    ) -> None:
        self.enabled = bool(enabled)
        self.write_enabled = bool(enabled)
        self._mysql = None
        self.host = str(host or "127.0.0.1")
        self.port = int(port or 3306)
        self.user = str(user or "")
        self.password = str(password or "")
        self.database = str(database or "").strip()
        self.connect_timeout_sec = max(2, int(connect_timeout_sec))
        self.ssl_mode = str(ssl_mode or "").strip()
        self.ssl_ca = str(ssl_ca or "").strip()
        self.ssl_disabled = bool(ssl_disabled)
        self.ssl_verify_cert = bool(ssl_verify_cert)
        self.ssl_verify_identity = bool(ssl_verify_identity)
        self.table_1m = self._validate_identifier(table_1m, "RUNTIME_MYSQL_TABLE_1M")
        self.table_5m = self._validate_identifier(table_5m, "RUNTIME_MYSQL_TABLE_5M")
        self._column_cache: Dict[str, Dict[str, str]] = {}

        if not self.enabled:
            return
        try:
            import mysql.connector  # type: ignore
        except ImportError:
            logger.warning("Live candle persistence disabled: mysql-connector-python is not installed")
            self.enabled = False
            return
        if not self.user:
            logger.warning("Live candle persistence disabled: AUTH_MYSQL_USER is required")
            self.enabled = False
            return
        if not self.database or not re.fullmatch(r"[a-zA-Z0-9_]+", self.database):
            logger.warning("Live candle persistence disabled: AUTH_MYSQL_DATABASE is invalid")
            self.enabled = False
            return
        self._mysql = mysql.connector
        try:
            self._initialize_schema()
        except Exception as exc:
            logger.warning("Live candle persistence disabled due to schema init error: %s", exc)
            self.write_enabled = False

    @staticmethod
    def _validate_identifier(value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise ValueError(f"{label} must contain only letters, numbers, and underscores")
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
        if not self.enabled:
            return
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
            for table in (self.table_1m, self.table_5m):
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS `{table}` (
                        timestamp BIGINT PRIMARY KEY,
                        open DOUBLE NOT NULL,
                        high DOUBLE NOT NULL,
                        low DOUBLE NOT NULL,
                        close DOUBLE NOT NULL,
                        volume DOUBLE NOT NULL,
                        created_at BIGINT NOT NULL,
                        updated_at BIGINT NOT NULL,
                        INDEX idx_{table}_updated (updated_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _upsert_many(self, conn: Any, table: str, candles: List[Dict[str, Any]]) -> None:
        if not candles:
            return
        mapping = self._resolve_columns(conn, table)
        ts_col = mapping["ts"]
        o_col = mapping["o"]
        h_col = mapping["h"]
        l_col = mapping["l"]
        c_col = mapping["c"]
        v_col = mapping["v"]
        created_col = mapping.get("created_at", "")
        updated_col = mapping.get("updated_at", "")

        insert_columns = [ts_col, o_col, h_col, l_col, c_col, v_col]
        if created_col:
            insert_columns.append(created_col)
        if updated_col:
            insert_columns.append(updated_col)
        insert_columns_sql = ", ".join(f"`{col}`" for col in insert_columns)

        update_assignments = [
            f"`{o_col}` = VALUES(`{o_col}`)",
            f"`{h_col}` = VALUES(`{h_col}`)",
            f"`{l_col}` = VALUES(`{l_col}`)",
            f"`{c_col}` = VALUES(`{c_col}`)",
            f"`{v_col}` = VALUES(`{v_col}`)",
        ]
        if updated_col:
            update_assignments.append(f"`{updated_col}` = VALUES(`{updated_col}`)")
        update_sql = ", ".join(update_assignments)

        cur = conn.cursor()
        try:
            now = int(time.time())
            rows = []
            for candle in candles:
                ts = _to_int(candle.get("timestamp"), 0)
                if ts <= 0:
                    continue
                open_ = _to_float(candle.get("open"))
                high = _to_float(candle.get("high"))
                low = _to_float(candle.get("low"))
                close = _to_float(candle.get("close"))
                volume = max(0.0, _to_float(candle.get("volume")))
                payload = [ts, open_, high, low, close, volume]
                if created_col:
                    payload.append(now)
                if updated_col:
                    payload.append(now)
                rows.append(tuple(payload))
            if not rows:
                return
            placeholders = ", ".join(["%s"] * len(insert_columns))
            cur.executemany(
                f"""
                INSERT INTO `{table}` (
                    {insert_columns_sql}
                ) VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE
                    {update_sql}
                """,
                rows,
            )
        finally:
            cur.close()

    def persist_batch(
        self,
        candles_1m: Optional[List[Dict[str, Any]]] = None,
        candles_5m: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        if not self.enabled or not self.write_enabled:
            return
        conn = None
        try:
            conn = self._connect(with_database=True)
            self._upsert_many(conn, self.table_1m, list(candles_1m or []))
            self._upsert_many(conn, self.table_5m, list(candles_5m or []))
            conn.commit()
        except Exception as exc:
            logger.warning("Live candle persistence error: %s", exc)
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _pick_column(columns: Dict[str, str], candidates: List[str]) -> str:
        for key in candidates:
            if key in columns:
                return columns[key]
        return ""

    def _resolve_columns(self, conn: Any, table: str) -> Dict[str, str]:
        cached = self._column_cache.get(table)
        if cached:
            return dict(cached)

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

        if not rows:
            raise RuntimeError(f"table `{self.database}`.`{table}` not found")

        normalized: Dict[str, str] = {}
        for row in rows:
            name = str(row.get("COLUMN_NAME", "")).strip()
            if name:
                normalized[name.lower()] = name

        mapping = {
            "ts": self._pick_column(
                normalized,
                ["timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time"],
            ),
            "o": self._pick_column(normalized, ["open", "o"]),
            "h": self._pick_column(normalized, ["high", "h"]),
            "l": self._pick_column(normalized, ["low", "l"]),
            "c": self._pick_column(normalized, ["close", "c"]),
            "v": self._pick_column(normalized, ["volume", "vol", "v"]),
            "created_at": self._pick_column(normalized, ["created_at"]),
            "updated_at": self._pick_column(normalized, ["updated_at"]),
        }

        missing = [key for key in ("ts", "o", "h", "l", "c", "v") if not mapping.get(key)]
        if missing:
            raise RuntimeError(f"table `{self.database}`.`{table}` missing candle columns: {','.join(missing)}")

        self._column_cache[table] = dict(mapping)
        return mapping

    def fetch_recent(self, timeframe: str, limit: int, since_ts: int = 0) -> List[Dict[str, float]]:
        if not self.enabled or self._mysql is None:
            raise RuntimeError("Live candle MySQL store is not enabled")

        table = self.table_5m if str(timeframe).strip().lower() == "5m" else self.table_1m
        max_rows = max(1, min(int(limit), 5000))
        min_ts = max(0, int(since_ts or 0))
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            mapping = self._resolve_columns(conn, table)
            ts_col = mapping["ts"]
            o_col = mapping["o"]
            h_col = mapping["h"]
            l_col = mapping["l"]
            c_col = mapping["c"]
            v_col = mapping["v"]
            cur = conn.cursor(dictionary=True)
            if min_ts > 0:
                cur.execute(
                    f"""
                    SELECT
                        `{ts_col}` AS ts,
                        `{o_col}` AS o,
                        `{h_col}` AS h,
                        `{l_col}` AS l,
                        `{c_col}` AS c,
                        `{v_col}` AS v
                    FROM `{table}`
                    WHERE `{ts_col}` >= %s
                    ORDER BY `{ts_col}` DESC
                    LIMIT %s
                    """,
                    (min_ts, max_rows),
                )
            else:
                cur.execute(
                    f"""
                    SELECT
                        `{ts_col}` AS ts,
                        `{o_col}` AS o,
                        `{h_col}` AS h,
                        `{l_col}` AS l,
                        `{c_col}` AS c,
                        `{v_col}` AS v
                    FROM `{table}`
                    ORDER BY `{ts_col}` DESC
                    LIMIT %s
                    """,
                    (max_rows,),
                )
            rows = cur.fetchall() or []
            rows.reverse()
            return [
                {
                    "timestamp": _normalize_epoch_seconds(row.get("ts")),
                    "open": _to_float(row.get("o")),
                    "high": _to_float(row.get("h")),
                    "low": _to_float(row.get("l")),
                    "close": _to_float(row.get("c")),
                    "volume": max(0.0, _to_float(row.get("v"))),
                }
                for row in rows
            ]
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()


class MySQLOptionSnapshotStore:
    def __init__(
        self,
        *,
        enabled: bool,
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
    ) -> None:
        self.enabled = bool(enabled)
        self.write_enabled = bool(enabled)
        self._mysql = None
        self.host = str(host or "127.0.0.1")
        self.port = int(port or 3306)
        self.user = str(user or "")
        self.password = str(password or "")
        self.database = str(database or "").strip()
        self.connect_timeout_sec = max(2, int(connect_timeout_sec))
        self.ssl_mode = str(ssl_mode or "").strip()
        self.ssl_ca = str(ssl_ca or "").strip()
        self.ssl_disabled = bool(ssl_disabled)
        self.ssl_verify_cert = bool(ssl_verify_cert)
        self.ssl_verify_identity = bool(ssl_verify_identity)
        self.table = self._validate_identifier(table, "RUNTIME_MYSQL_OPTIONS_TABLE")

        if not self.enabled:
            return
        try:
            import mysql.connector  # type: ignore
        except ImportError:
            logger.warning("Option snapshot persistence disabled: mysql-connector-python is not installed")
            self.enabled = False
            return
        if not self.user:
            logger.warning("Option snapshot persistence disabled: AUTH_MYSQL_USER is required")
            self.enabled = False
            return
        if not self.database or not re.fullmatch(r"[a-zA-Z0-9_]+", self.database):
            logger.warning("Option snapshot persistence disabled: AUTH_MYSQL_DATABASE is invalid")
            self.enabled = False
            return
        self._mysql = mysql.connector
        try:
            self._initialize_schema()
        except Exception as exc:
            logger.warning("Option snapshot persistence disabled due to schema init error: %s", exc)
            self.write_enabled = False

    @staticmethod
    def _validate_identifier(value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise ValueError(f"{label} must contain only letters, numbers, and underscores")
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
        if not self.enabled:
            return
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
                CREATE TABLE IF NOT EXISTS `{self.table}` (
                    snapshot_id VARCHAR(48) NOT NULL,
                    timestamp BIGINT NOT NULL,
                    atm_strike DOUBLE NULL,
                    atm_ce_ltp DOUBLE NOT NULL,
                    atm_pe_ltp DOUBLE NOT NULL,
                    atm_ce_volume DOUBLE NOT NULL,
                    atm_pe_volume DOUBLE NOT NULL,
                    atm_ce_oi_change DOUBLE NOT NULL,
                    atm_pe_oi_change DOUBLE NOT NULL,
                    band_volume_near_atm DOUBLE NOT NULL,
                    band_volume_total DOUBLE NOT NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY (snapshot_id),
                    INDEX idx_{self.table}_ts (timestamp),
                    INDEX idx_{self.table}_updated (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            if self._is_modern_schema(conn):
                conn.commit()
                return
            self._ensure_column(
                conn,
                "snapshot_id",
                "VARCHAR(48) NOT NULL DEFAULT '' AFTER `timestamp`",
            )
            self._ensure_column(conn, "timestamp", "BIGINT NOT NULL")
            self._ensure_index(conn, f"idx_{self.table}_snapshot_id", "`snapshot_id`")
            self._ensure_index(conn, f"idx_{self.table}_ts", "`timestamp`")
            self._backfill_snapshot_ids(conn)
            self._ensure_primary_key(conn, ["snapshot_id"])
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _is_modern_schema(self, conn: Any) -> bool:
        cur = conn.cursor()
        try:
            cur.execute(f"SHOW CREATE TABLE `{self.table}`")
            row = cur.fetchone()
        finally:
            cur.close()
        if row is None:
            return False
        ddl = ""
        if isinstance(row, (tuple, list)) and len(row) >= 2:
            ddl = str(row[1] or "")
        elif isinstance(row, dict):
            ddl = str(row.get("Create Table") or row.get("create table") or "")
        text = ddl.lower()
        compact = text.replace(" ", "").replace("`", "").replace('"', "")
        required = (
            "snapshot_idvarchar(48)notnull",
            "primarykey(snapshot_id)",
            f"keyidx_{self.table}_ts(timestamp)",
        )
        return all(item in compact for item in required)

    def _ensure_column(self, conn: Any, column_name: str, definition_sql: str) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                LIMIT 1
                """,
                (self.database, self.table, column_name),
            )
            exists = cur.fetchone() is not None
        finally:
            cur.close()
        if exists:
            return
        cur = conn.cursor()
        try:
            cur.execute(f"ALTER TABLE `{self.table}` ADD COLUMN `{column_name}` {definition_sql}")
        finally:
            cur.close()

    def _ensure_index(self, conn: Any, index_name: str, columns_sql: str) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = %s
                LIMIT 1
                """,
                (self.database, self.table, index_name),
            )
            exists = cur.fetchone() is not None
        finally:
            cur.close()
        if exists:
            return
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE INDEX `{index_name}` ON `{self.table}` ({columns_sql})")
        finally:
            cur.close()

    def _backfill_snapshot_ids(self, conn: Any) -> None:
        probe = conn.cursor()
        try:
            probe.execute(
                f"""
                SELECT 1
                FROM `{self.table}`
                WHERE (snapshot_id IS NULL OR snapshot_id = '')
                LIMIT 1
                """
            )
            needed = probe.fetchone() is not None
        finally:
            probe.close()
        if not needed:
            return
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{self.table}`
                SET snapshot_id = CONCAT('oc_', timestamp, '_legacy')
                WHERE (snapshot_id IS NULL OR snapshot_id = '')
                """
            )
        finally:
            cur.close()

    def _ensure_primary_key(self, conn: Any, columns: List[str]) -> None:
        expected = [str(col).strip().lower() for col in columns]
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
                """,
                (self.database, self.table),
            )
            current = [str((row or {}).get("COLUMN_NAME", "")).strip().lower() for row in (cur.fetchall() or [])]
        finally:
            cur.close()
        if current == expected:
            return
        cur = conn.cursor()
        try:
            pk_sql = ", ".join(f"`{c}`" for c in columns)
            if current:
                cur.execute(f"ALTER TABLE `{self.table}` DROP PRIMARY KEY, ADD PRIMARY KEY ({pk_sql})")
            else:
                cur.execute(f"ALTER TABLE `{self.table}` ADD PRIMARY KEY ({pk_sql})")
        finally:
            cur.close()

    def persist_batch(self, rows: Optional[List[Dict[str, Any]]] = None) -> None:
        if not self.enabled or not self.write_enabled:
            return
        snapshots = list(rows or [])
        if not snapshots:
            return

        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor()
            now = int(time.time())
            payload_rows = []
            for row in snapshots:
                ts = _to_int(row.get("timestamp"), 0)
                if ts <= 0:
                    continue
                snapshot_id = str(row.get("snapshot_id") or "").strip()[:48]
                if not snapshot_id:
                    snapshot_id = _new_snapshot_id(ts)
                    row["snapshot_id"] = snapshot_id
                payload_rows.append(
                    (
                        ts,
                        snapshot_id,
                        _to_float(row.get("atm_strike"), 0.0),
                        _to_float(row.get("atm_ce_ltp")),
                        _to_float(row.get("atm_pe_ltp")),
                        _to_float(row.get("atm_ce_volume")),
                        _to_float(row.get("atm_pe_volume")),
                        _to_float(row.get("atm_ce_oi_change")),
                        _to_float(row.get("atm_pe_oi_change")),
                        _to_float(row.get("band_volume_near_atm")),
                        _to_float(row.get("band_volume_total")),
                        now,
                        now,
                    )
                )
            if not payload_rows:
                return

            cur.executemany(
                f"""
                INSERT INTO `{self.table}` (
                    timestamp,
                    snapshot_id,
                    atm_strike,
                    atm_ce_ltp,
                    atm_pe_ltp,
                    atm_ce_volume,
                    atm_pe_volume,
                    atm_ce_oi_change,
                    atm_pe_oi_change,
                    band_volume_near_atm,
                    band_volume_total,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    snapshot_id = VALUES(snapshot_id),
                    atm_strike = VALUES(atm_strike),
                    atm_ce_ltp = VALUES(atm_ce_ltp),
                    atm_pe_ltp = VALUES(atm_pe_ltp),
                    atm_ce_volume = VALUES(atm_ce_volume),
                    atm_pe_volume = VALUES(atm_pe_volume),
                    atm_ce_oi_change = VALUES(atm_ce_oi_change),
                    atm_pe_oi_change = VALUES(atm_pe_oi_change),
                    band_volume_near_atm = VALUES(band_volume_near_atm),
                    band_volume_total = VALUES(band_volume_total),
                    updated_at = VALUES(updated_at)
                """,
                payload_rows,
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Option snapshot persistence error: %s", exc)
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def fetch_recent(self, limit: int = 600) -> List[Dict[str, Any]]:
        if not self.enabled or self._mysql is None:
            return []
        max_rows = max(1, min(int(limit), 5000))
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    timestamp,
                    snapshot_id,
                    atm_strike,
                    atm_ce_ltp,
                    atm_pe_ltp,
                    atm_ce_volume,
                    atm_pe_volume,
                    atm_ce_oi_change,
                    atm_pe_oi_change,
                    band_volume_near_atm,
                    band_volume_total
                FROM `{self.table}`
                ORDER BY timestamp DESC
                LIMIT %s
                """,
                (max_rows,),
            )
            rows = cur.fetchall() or []
            rows.reverse()
            return [
                {
                    "timestamp": _normalize_epoch_seconds(row.get("timestamp")),
                    "snapshot_id": str(row.get("snapshot_id") or "").strip(),
                    "atm_strike": _to_float(row.get("atm_strike")),
                    "atm_ce_ltp": _to_float(row.get("atm_ce_ltp")),
                    "atm_pe_ltp": _to_float(row.get("atm_pe_ltp")),
                    "atm_ce_volume": _to_float(row.get("atm_ce_volume")),
                    "atm_pe_volume": _to_float(row.get("atm_pe_volume")),
                    "atm_ce_oi_change": _to_float(row.get("atm_ce_oi_change")),
                    "atm_pe_oi_change": _to_float(row.get("atm_pe_oi_change")),
                    "band_volume_near_atm": _to_float(row.get("band_volume_near_atm")),
                    "band_volume_total": _to_float(row.get("band_volume_total")),
                }
                for row in rows
            ]
        except Exception as exc:
            logger.warning("Option snapshot fetch error: %s", exc)
            return []
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()


class MySQLOptionStrikeStore:
    def __init__(
        self,
        *,
        enabled: bool,
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
    ) -> None:
        self.enabled = bool(enabled)
        self.write_enabled = bool(enabled)
        self._mysql = None
        self.host = str(host or "127.0.0.1")
        self.port = int(port or 3306)
        self.user = str(user or "")
        self.password = str(password or "")
        self.database = str(database or "").strip()
        self.connect_timeout_sec = max(2, int(connect_timeout_sec))
        self.ssl_mode = str(ssl_mode or "").strip()
        self.ssl_ca = str(ssl_ca or "").strip()
        self.ssl_disabled = bool(ssl_disabled)
        self.ssl_verify_cert = bool(ssl_verify_cert)
        self.ssl_verify_identity = bool(ssl_verify_identity)
        self.table = self._validate_identifier(table, "RUNTIME_MYSQL_OPTION_STRIKES_TABLE")

        if not self.enabled:
            return
        try:
            import mysql.connector  # type: ignore
        except ImportError:
            logger.warning("Option strike persistence disabled: mysql-connector-python is not installed")
            self.enabled = False
            return
        if not self.user:
            logger.warning("Option strike persistence disabled: AUTH_MYSQL_USER is required")
            self.enabled = False
            return
        if not self.database or not re.fullmatch(r"[a-zA-Z0-9_]+", self.database):
            logger.warning("Option strike persistence disabled: AUTH_MYSQL_DATABASE is invalid")
            self.enabled = False
            return
        self._mysql = mysql.connector
        try:
            self._initialize_schema()
        except Exception as exc:
            logger.warning("Option strike persistence disabled due to schema init error: %s", exc)
            self.write_enabled = False

    @staticmethod
    def _validate_identifier(value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise ValueError(f"{label} must contain only letters, numbers, and underscores")
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
        if not self.enabled:
            return
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
                CREATE TABLE IF NOT EXISTS `{self.table}` (
                    snapshot_id VARCHAR(48) NOT NULL,
                    contract_key VARCHAR(160) NOT NULL DEFAULT '',
                    timestamp BIGINT NOT NULL,
                    option_type VARCHAR(2) NOT NULL,
                    strike DOUBLE NOT NULL,
                    symbol VARCHAR(96) NOT NULL DEFAULT '',
                    symbol_token VARCHAR(64) NOT NULL DEFAULT '',
                    exchange VARCHAR(16) NOT NULL DEFAULT '',
                    underlying VARCHAR(32) NOT NULL DEFAULT '',
                    expiry_date VARCHAR(16) NOT NULL DEFAULT '',
                    expiry_ts BIGINT NULL,
                    expiry_kind VARCHAR(16) NOT NULL DEFAULT '',
                    ltp DOUBLE NOT NULL,
                    volume DOUBLE NOT NULL,
                    oi_change DOUBLE NOT NULL,
                    atm_strike DOUBLE NULL,
                    spot_price DOUBLE NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY (snapshot_id, contract_key),
                    INDEX idx_{self.table}_ts (timestamp),
                    INDEX idx_{self.table}_strike_ts (strike, timestamp),
                    INDEX idx_{self.table}_expiry_ts (expiry_ts),
                    INDEX idx_{self.table}_updated (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            if self._is_modern_schema(conn):
                conn.commit()
                return
            self._ensure_column(
                conn,
                "snapshot_id",
                "VARCHAR(48) NOT NULL DEFAULT '' FIRST",
            )
            self._ensure_column(
                conn,
                "contract_key",
                "VARCHAR(160) NOT NULL DEFAULT '' AFTER `snapshot_id`",
            )
            self._ensure_column(conn, "symbol_token", "VARCHAR(64) NOT NULL DEFAULT '' AFTER `symbol`")
            self._ensure_column(conn, "exchange", "VARCHAR(16) NOT NULL DEFAULT '' AFTER `symbol_token`")
            self._ensure_column(conn, "underlying", "VARCHAR(32) NOT NULL DEFAULT '' AFTER `exchange`")
            self._ensure_column(conn, "expiry_date", "VARCHAR(16) NOT NULL DEFAULT '' AFTER `underlying`")
            self._ensure_column(conn, "expiry_ts", "BIGINT NULL AFTER `expiry_date`")
            self._ensure_column(conn, "expiry_kind", "VARCHAR(16) NOT NULL DEFAULT '' AFTER `expiry_ts`")
            self._ensure_index(conn, f"idx_{self.table}_snapshot_id", "`snapshot_id`")
            self._ensure_index(conn, f"idx_{self.table}_ts", "`timestamp`")
            self._ensure_index(conn, f"idx_{self.table}_expiry_ts", "`expiry_ts`")
            self._backfill_snapshot_ids(conn)
            self._backfill_contract_keys(conn)
            self._ensure_primary_key(conn, ["snapshot_id", "contract_key"])
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _is_modern_schema(self, conn: Any) -> bool:
        cur = conn.cursor()
        try:
            cur.execute(f"SHOW CREATE TABLE `{self.table}`")
            row = cur.fetchone()
        finally:
            cur.close()
        if row is None:
            return False
        ddl = ""
        if isinstance(row, (tuple, list)) and len(row) >= 2:
            ddl = str(row[1] or "")
        elif isinstance(row, dict):
            ddl = str(row.get("Create Table") or row.get("create table") or "")
        text = ddl.lower()
        compact = text.replace(" ", "").replace("`", "").replace('"', "")
        required = (
            "snapshot_idvarchar(48)notnull",
            "contract_keyvarchar(160)notnull",
            "primarykey(snapshot_id,contract_key)",
            f"keyidx_{self.table}_ts(timestamp)",
        )
        return all(item in compact for item in required)

    def _ensure_column(self, conn: Any, column_name: str, definition_sql: str) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                LIMIT 1
                """,
                (self.database, self.table, column_name),
            )
            exists = cur.fetchone() is not None
        finally:
            cur.close()
        if exists:
            return
        cur = conn.cursor()
        try:
            cur.execute(f"ALTER TABLE `{self.table}` ADD COLUMN `{column_name}` {definition_sql}")
        finally:
            cur.close()

    def _ensure_index(self, conn: Any, index_name: str, columns_sql: str) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT 1
                FROM INFORMATION_SCHEMA.STATISTICS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND INDEX_NAME = %s
                LIMIT 1
                """,
                (self.database, self.table, index_name),
            )
            exists = cur.fetchone() is not None
        finally:
            cur.close()
        if exists:
            return
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE INDEX `{index_name}` ON `{self.table}` ({columns_sql})")
        finally:
            cur.close()

    def _backfill_snapshot_ids(self, conn: Any) -> None:
        probe = conn.cursor()
        try:
            probe.execute(
                f"""
                SELECT 1
                FROM `{self.table}`
                WHERE (snapshot_id IS NULL OR snapshot_id = '')
                LIMIT 1
                """
            )
            needed = probe.fetchone() is not None
        finally:
            probe.close()
        if not needed:
            return
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{self.table}`
                SET snapshot_id = CONCAT('oc_', timestamp, '_legacy')
                WHERE (snapshot_id IS NULL OR snapshot_id = '')
                """
            )
        finally:
            cur.close()

    def _backfill_contract_keys(self, conn: Any) -> None:
        probe = conn.cursor()
        try:
            probe.execute(
                f"""
                SELECT 1
                FROM `{self.table}`
                WHERE (contract_key IS NULL OR contract_key = '')
                LIMIT 1
                """
            )
            needed = probe.fetchone() is not None
        finally:
            probe.close()
        if not needed:
            return
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                UPDATE `{self.table}`
                SET contract_key = COALESCE(
                    NULLIF(TRIM(symbol), ''),
                    CONCAT(option_type, '|', CAST(strike AS CHAR), '|', CAST(timestamp AS CHAR))
                )
                WHERE (contract_key IS NULL OR contract_key = '')
                """
            )
        finally:
            cur.close()

    def _ensure_primary_key(self, conn: Any, columns: List[str]) -> None:
        expected = [str(col).strip().lower() for col in columns]
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
                """,
                (self.database, self.table),
            )
            current = [str((row or {}).get("COLUMN_NAME", "")).strip().lower() for row in (cur.fetchall() or [])]
        finally:
            cur.close()
        if current == expected:
            return
        cur = conn.cursor()
        try:
            pk_sql = ", ".join(f"`{c}`" for c in columns)
            if current:
                cur.execute(f"ALTER TABLE `{self.table}` DROP PRIMARY KEY, ADD PRIMARY KEY ({pk_sql})")
            else:
                cur.execute(f"ALTER TABLE `{self.table}` ADD PRIMARY KEY ({pk_sql})")
        finally:
            cur.close()

    def persist_batch(self, snapshots: Optional[List[Dict[str, Any]]] = None) -> None:
        if not self.enabled or not self.write_enabled:
            return
        rows_in = list(snapshots or [])
        if not rows_in:
            return

        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor()
            now = int(time.time())
            payload_rows = []
            for snapshot in rows_in:
                ts = _to_int(snapshot.get("timestamp"), 0)
                if ts <= 0:
                    continue
                snapshot_id = str(snapshot.get("snapshot_id") or "").strip()[:48]
                if not snapshot_id:
                    snapshot_id = _new_snapshot_id(ts)
                    snapshot["snapshot_id"] = snapshot_id
                atm_strike = _to_float(snapshot.get("atm_strike"), 0.0)
                spot_price = _to_float(snapshot.get("spot_price"), 0.0)
                strike_rows = snapshot.get("strikes") or []
                if not isinstance(strike_rows, list):
                    continue
                for strike_row in strike_rows:
                    if not isinstance(strike_row, dict):
                        continue
                    option_type = str(strike_row.get("option_type", "")).upper().strip()
                    if option_type not in {"CE", "PE"}:
                        continue
                    strike = _to_float(strike_row.get("strike"), 0.0)
                    if strike <= 0.0:
                        continue
                    contract = extract_option_contract(
                        strike_row,
                        snapshot_ts=ts,
                        explicit_option_type=option_type,
                        explicit_strike=strike,
                    )
                    symbol = str(contract.get("symbol") or strike_row.get("symbol") or "").strip()[:96]
                    contract_key = str(contract.get("contract_key") or build_contract_key(contract) or symbol or f"{option_type}|{strike}|{ts}")[:160]
                    symbol_token = str(contract.get("symbol_token") or "").strip()[:64]
                    exchange = str(contract.get("exchange") or "").strip().upper()[:16]
                    underlying = str(contract.get("underlying") or "").strip().upper()[:32]
                    expiry_date = str(contract.get("expiry_date") or "").strip()[:16]
                    expiry_ts = _to_int(contract.get("expiry_ts"), 0) or None
                    expiry_kind = str(contract.get("expiry_kind") or "").strip().lower()[:16]
                    ltp = _to_float(strike_row.get("ltp"), 0.0)
                    volume = max(0.0, _to_float(strike_row.get("volume"), 0.0))
                    oi_change = _to_float(strike_row.get("oi_change"), 0.0)
                    payload_rows.append(
                        (
                            snapshot_id,
                            contract_key,
                            ts,
                            option_type,
                            strike,
                            symbol,
                            symbol_token,
                            exchange,
                            underlying,
                            expiry_date,
                            expiry_ts,
                            expiry_kind,
                            ltp,
                            volume,
                            oi_change,
                            atm_strike if atm_strike > 0.0 else None,
                            spot_price if spot_price > 0.0 else None,
                            now,
                            now,
                        )
                    )
            if not payload_rows:
                return

            cur.executemany(
                f"""
                INSERT INTO `{self.table}` (
                    snapshot_id,
                    contract_key,
                    timestamp,
                    option_type,
                    strike,
                    symbol,
                    symbol_token,
                    exchange,
                    underlying,
                    expiry_date,
                    expiry_ts,
                    expiry_kind,
                    ltp,
                    volume,
                    oi_change,
                    atm_strike,
                    spot_price,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    snapshot_id = VALUES(snapshot_id),
                    contract_key = VALUES(contract_key),
                    symbol = VALUES(symbol),
                    symbol_token = VALUES(symbol_token),
                    exchange = VALUES(exchange),
                    underlying = VALUES(underlying),
                    expiry_date = VALUES(expiry_date),
                    expiry_ts = VALUES(expiry_ts),
                    expiry_kind = VALUES(expiry_kind),
                    ltp = VALUES(ltp),
                    volume = VALUES(volume),
                    oi_change = VALUES(oi_change),
                    atm_strike = VALUES(atm_strike),
                    spot_price = VALUES(spot_price),
                    updated_at = VALUES(updated_at)
                """,
                payload_rows,
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Option strike persistence error: %s", exc)
            try:
                if conn is not None:
                    conn.rollback()
            except Exception:
                pass
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def fetch_by_snapshot(self, snapshot_id: str, limit: int = 600) -> List[Dict[str, Any]]:
        if not self.enabled or self._mysql is None:
            return []
        sid = str(snapshot_id or "").strip()
        if not sid:
            return []
        max_rows = max(1, min(int(limit), 5000))
        conn = None
        cur = None
        try:
            conn = self._connect(with_database=True)
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    snapshot_id,
                    contract_key,
                    timestamp,
                    option_type,
                    strike,
                    symbol,
                    symbol_token,
                    exchange,
                    underlying,
                    expiry_date,
                    expiry_ts,
                    expiry_kind,
                    ltp,
                    volume,
                    oi_change,
                    atm_strike,
                    spot_price
                FROM `{self.table}`
                WHERE snapshot_id = %s
                ORDER BY COALESCE(expiry_ts, 0) ASC, strike ASC, option_type ASC, contract_key ASC
                LIMIT %s
                """,
                (sid, max_rows),
            )
            rows = cur.fetchall() or []
            return [
                {
                    "snapshot_id": str(row.get("snapshot_id") or "").strip(),
                    "contract_key": str(row.get("contract_key") or "").strip(),
                    "timestamp": _normalize_epoch_seconds(row.get("timestamp")),
                    "option_type": str(row.get("option_type") or "").upper().strip(),
                    "strike": _to_float(row.get("strike"), 0.0),
                    "symbol": str(row.get("symbol") or "").strip(),
                    "symbol_token": str(row.get("symbol_token") or "").strip(),
                    "exchange": str(row.get("exchange") or "").strip().upper(),
                    "underlying": str(row.get("underlying") or "").strip().upper(),
                    "expiry_date": str(row.get("expiry_date") or "").strip(),
                    "expiry_ts": _normalize_epoch_seconds(row.get("expiry_ts")),
                    "expiry_kind": str(row.get("expiry_kind") or "").strip().lower(),
                    "ltp": _to_float(row.get("ltp"), 0.0),
                    "volume": _to_float(row.get("volume"), 0.0),
                    "oi_change": _to_float(row.get("oi_change"), 0.0),
                    "atm_strike": _to_float(row.get("atm_strike"), 0.0),
                    "spot_price": _to_float(row.get("spot_price"), 0.0),
                }
                for row in rows
            ]
        except Exception as exc:
            logger.warning("Option strike fetch error: %s", exc)
            return []
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()


class AppState:
    def __init__(self) -> None:
        self.settings: Settings = load_settings()
        self.startup_ts: int = int(time.time())
        self.pain_runtime = PainTheoryRuntime(
            model_path=self.settings.pain_ai_model_path,
            use_model=self.settings.pain_ai_use_model,
            use_5m_primary=self.settings.runtime_use_5m_primary,
        )
        self.user_auth = MySQLUserAuthStore(
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            secret=self.settings.auth_secret,
            session_ttl_sec=self.settings.auth_session_ttl_sec,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
        )
        self.fyers_auth = FyersAuthManager(
            auth_file=self.settings.fyers_auth_file,
            client_id=self.settings.fyers_client_id,
            secret_key=self.settings.fyers_secret_key,
            pin=self.settings.fyers_pin,
            redirect_uri=self.settings.fyers_redirect_uri,
            response_type=self.settings.fyers_response_type,
            scope=self.settings.fyers_scope,
            refresh_lead_sec=self.settings.fyers_refresh_lead_sec,
        )
        self.current_1m: Optional[Dict[str, float]] = None
        self.current_5m: Optional[Dict[str, float]] = None
        self.last_tick: Optional[Dict[str, float]] = None
        self.last_underlying_data_received_at: int = 0
        self.last_underlying_source_ts: int = 0
        self.last_option_signature: Optional[tuple[Any, ...]] = None
        self.last_option_ts: int = 0
        self.last_option_quote_received_at: int = 0
        self.last_option_quote_source_ts: int = 0
        self.last_option_quote_trade_id: int = 0
        self.last_sos_underlying_trigger_at: int = 0
        self.last_sos_option_trigger_at: int = 0
        self.candle_store = MySQLCandleStore(
            enabled=self.settings.runtime_persist_live_candles,
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
            table_1m=self.settings.runtime_mysql_table_1m,
            table_5m=self.settings.runtime_mysql_table_5m,
        )
        self.option_store = MySQLOptionSnapshotStore(
            enabled=self.settings.runtime_persist_option_snapshots,
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
            table=self.settings.runtime_mysql_options_table,
        )
        self.option_strike_store = MySQLOptionStrikeStore(
            enabled=self.settings.runtime_persist_option_snapshots,
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
            table=self.settings.runtime_mysql_option_strikes_table,
        )
        self.paper_trade = PaperTradeEngine(
            enabled=self.settings.paper_trade_enabled,
            symbol=self.settings.fyers_symbol,
            hold_minutes=self.settings.paper_trade_hold_minutes,
            min_confidence=self.settings.paper_trade_min_confidence,
            entry_guidance=self.settings.paper_trade_entry_guidance,
            one_trade_at_a_time=self.settings.paper_trade_one_trade_at_a_time,
            require_pain_release=self.settings.paper_trade_require_pain_release,
            entry_style=self.settings.paper_trade_entry_style,
            min_compression_bars=self.settings.paper_trade_min_compression_bars,
            target_r_multiple=self.settings.paper_trade_target_r_multiple,
            avoid_noon_chop=self.settings.paper_trade_avoid_noon_chop,
            feedback_enabled=self.settings.paper_trade_feedback_enabled,
            feedback_min_trades=self.settings.paper_trade_feedback_min_trades,
            feedback_block_win_rate=self.settings.paper_trade_feedback_block_win_rate,
            feedback_penalty_floor=self.settings.paper_trade_feedback_penalty_floor,
            feedback_reward_cap=self.settings.paper_trade_feedback_reward_cap,
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
            model_driven_execution=True,
        )
        self.quote_client: Optional[FyersQuoteClient] = None
        self.fyers_token_generation: int = 0
        if (
            self.settings.stream_mode == "fyers"
            and self.settings.fyers_client_id
            and self.settings.fyers_access_token
        ):
            try:
                self.quote_client = FyersQuoteClient(
                    client_id=self.settings.fyers_client_id,
                    access_token=self.settings.fyers_access_token,
                    log_path=self.settings.fyers_log_path,
                )
            except Exception as exc:
                logger.warning("FYERS quote client init skipped: %s", exc)
        self.retrainer = AutoRetrainService(
            settings=self.settings,
            runtime=self.pain_runtime,
        )
        self.retention = MySQLRetentionService(self.settings)
        self.backtest_store: Optional[MySQLBacktestResultStore] = None
        self.backtest_store_init_attempted = False
        self.last_backtest: Optional[Dict[str, Any]] = None
        self.backtest_jobs: Dict[str, Dict[str, Any]] = {}
        self.backtest_active_job_id: str = ""
        self.backtest_task: Optional[asyncio.Task] = None
        self.live_order_store = MySQLLiveOrderStore(
            enabled=True,
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
        )
        self.angel_order_audit_store = MySQLAngelOrderAuditStore(
            enabled=True,
            host=self.settings.auth_mysql_host,
            port=self.settings.auth_mysql_port,
            user=self.settings.auth_mysql_user,
            password=self.settings.auth_mysql_password,
            database=self.settings.auth_mysql_database,
            connect_timeout_sec=self.settings.auth_mysql_connect_timeout_sec,
            ssl_mode=self.settings.auth_mysql_ssl_mode,
            ssl_ca=self.settings.auth_mysql_ssl_ca,
            ssl_disabled=self.settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=self.settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=self.settings.auth_mysql_ssl_verify_identity,
        )
        self.live_execution = CentralizedAngelExecutionManager(
            settings=self.settings,
            user_auth=self.user_auth,
            live_order_store=self.live_order_store,
            audit_store=self.angel_order_audit_store,
        )


class MySQLLiveOrderStore:
    """Persists per-user live orders dispatched by the AI execution engine."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout_sec: int = 10,
        ssl_mode: str = "",
        ssl_ca: str = "",
        ssl_disabled: bool = False,
        ssl_verify_cert: bool = False,
        ssl_verify_identity: bool = False,
        table: str = "user_live_orders",
    ) -> None:
        self.enabled = bool(enabled)
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout_sec = int(connect_timeout_sec)
        self.ssl_mode = ssl_mode
        self.ssl_ca = ssl_ca
        self.ssl_disabled = ssl_disabled
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_verify_identity = ssl_verify_identity
        self.table = table
        self._mysql: Any = None
        if self.enabled:
            try:
                import mysql.connector  # type: ignore
                self._mysql = mysql.connector
            except ImportError:
                logger.warning("MySQLLiveOrderStore disabled: mysql-connector-python not installed")
                self.enabled = False
            if self.enabled:
                try:
                    self._initialize_schema()
                except Exception as exc:
                    logger.warning("MySQLLiveOrderStore schema init failed: %s", exc)

    def _connect(self) -> Any:
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

    def _initialize_schema(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.table}` (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(190) NOT NULL,
                    trade_id BIGINT NOT NULL DEFAULT 0,
                    symbol VARCHAR(96) NOT NULL DEFAULT '',
                    exchange VARCHAR(16) NOT NULL DEFAULT '',
                    quantity INT NOT NULL DEFAULT 0,
                    direction VARCHAR(8) NOT NULL DEFAULT '',
                    entry_order_id VARCHAR(80) NOT NULL DEFAULT '',
                    entry_ts BIGINT NOT NULL DEFAULT 0,
                    exit_order_id VARCHAR(80) NOT NULL DEFAULT '',
                    exit_ts BIGINT NOT NULL DEFAULT 0,
                    status VARCHAR(16) NOT NULL DEFAULT 'open',
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    INDEX idx_ulo_username (username),
                    INDEX idx_ulo_trade_id (trade_id),
                    INDEX idx_ulo_entry_ts (entry_ts),
                    INDEX idx_ulo_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            self._ensure_close_reason_column(conn)
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _ensure_close_reason_column(self, conn: Any) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = 'close_reason'
                LIMIT 1
                """,
                (self.database, self.table),
            )
            row = cur.fetchone() or {}
        finally:
            cur.close()

        alter = conn.cursor()
        try:
            if not row:
                alter.execute(
                    f"ALTER TABLE `{self.table}` ADD COLUMN close_reason VARCHAR(128) NOT NULL DEFAULT '' AFTER status"
                )
                return
            data_type = str(row.get("DATA_TYPE") or "").strip().lower()
            length = _to_int(row.get("CHARACTER_MAXIMUM_LENGTH"), 0)
            if data_type == "varchar" and length >= 128:
                return
            alter.execute(
                f"ALTER TABLE `{self.table}` MODIFY COLUMN close_reason VARCHAR(128) NOT NULL DEFAULT ''"
            )
        finally:
            alter.close()

    def _try_connect(self) -> Optional[Any]:
        if not self.enabled:
            return None
        try:
            return self._connect()
        except Exception as exc:
            logger.warning("MySQLLiveOrderStore connect failed: %s", exc)
            return None

    def save_entry(self, *, username: str, trade_id: int, symbol: str, exchange: str, quantity: int, direction: str, entry_order_id: str, entry_ts: int) -> None:
        if not self.enabled:
            return
        conn = self._try_connect()
        if conn is None:
            return
        now = int(time.time())
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO `{self.table}`
                    (username, trade_id, symbol, exchange, quantity, direction,
                     entry_order_id, entry_ts, exit_order_id, exit_ts, status, close_reason, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '', 0, 'open', '', %s, %s)
                """,
                (
                    str(username).strip().lower(),
                    int(trade_id),
                    str(symbol),
                    str(exchange),
                    int(quantity),
                    str(direction).upper(),
                    str(entry_order_id),
                    int(entry_ts),
                    now,
                    now,
                ),
            )
            conn.commit()
            cur.close()
        except Exception as exc:
            logger.warning("MySQLLiveOrderStore.save_entry error: %s", exc)
        finally:
            conn.close()

    def save_exit(self, *, username: str, trade_id: int, exit_order_id: str, exit_ts: int, close_reason: str = "") -> None:
        if not self.enabled:
            return
        conn = self._try_connect()
        if conn is None:
            return
        now = int(time.time())
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                UPDATE `{self.table}`
                SET exit_order_id = %s, exit_ts = %s, status = 'closed', close_reason = %s, updated_at = %s
                WHERE username = %s AND trade_id = %s AND status = 'open'
                ORDER BY id DESC
                LIMIT 1
                """,
                (
                    str(exit_order_id),
                    int(exit_ts),
                    str(close_reason or "")[:128],
                    now,
                    str(username).strip().lower(),
                    int(trade_id),
                ),
            )
            conn.commit()
            cur.close()
        except Exception as exc:
            logger.warning("MySQLLiveOrderStore.save_exit error: %s", exc)
        finally:
            conn.close()

    def get_user_trades_in_range(self, *, username: str, start_ts: int, end_ts: int, limit: int = 500) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        conn = self._try_connect()
        if conn is None:
            return []
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT id, username, trade_id, symbol, exchange, quantity, direction,
                       entry_order_id, entry_ts, exit_order_id, exit_ts, status, close_reason, created_at, updated_at
                FROM `{self.table}`
                WHERE username = %s AND entry_ts >= %s AND entry_ts <= %s
                ORDER BY entry_ts DESC
                LIMIT %s
                """,
                (
                    str(username).strip().lower(),
                    int(start_ts),
                    int(end_ts),
                    int(max(1, min(limit, 1000))),
                ),
            )
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows] if rows else []
        except Exception as exc:
            logger.warning("MySQLLiveOrderStore.get_user_trades_in_range error: %s", exc)
            return []
        finally:
            conn.close()

    def get_open_trades(self, *, limit: int = 5000) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        conn = self._try_connect()
        if conn is None:
            return []
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT id, username, trade_id, symbol, exchange, quantity, direction,
                       entry_order_id, entry_ts, exit_order_id, exit_ts, status, close_reason, created_at, updated_at
                FROM `{self.table}`
                WHERE status = 'open'
                ORDER BY entry_ts DESC, id DESC
                LIMIT %s
                """,
                (int(max(1, min(limit, 20000))),),
            )
            rows = cur.fetchall()
            cur.close()
            return [dict(r) for r in rows] if rows else []
        except Exception as exc:
            logger.warning("MySQLLiveOrderStore.get_open_trades error: %s", exc)
            return []
        finally:
            conn.close()


class MySQLAngelOrderAuditStore:
    """Persists every Angel One place-order API hit with payload and response details."""

    def __init__(
        self,
        *,
        enabled: bool = True,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        connect_timeout_sec: int = 10,
        ssl_mode: str = "",
        ssl_ca: str = "",
        ssl_disabled: bool = False,
        ssl_verify_cert: bool = False,
        ssl_verify_identity: bool = False,
        table: str = "angel_order_api_hits",
    ) -> None:
        self.enabled = bool(enabled)
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.database = database
        self.connect_timeout_sec = int(connect_timeout_sec)
        self.ssl_mode = ssl_mode
        self.ssl_ca = ssl_ca
        self.ssl_disabled = ssl_disabled
        self.ssl_verify_cert = ssl_verify_cert
        self.ssl_verify_identity = ssl_verify_identity
        self.table = table
        self._mysql: Any = None
        if self.enabled:
            try:
                import mysql.connector  # type: ignore
                self._mysql = mysql.connector
            except ImportError:
                logger.warning("MySQLAngelOrderAuditStore disabled: mysql-connector-python not installed")
                self.enabled = False
            if self.enabled:
                try:
                    self._initialize_schema()
                except Exception as exc:
                    logger.warning("MySQLAngelOrderAuditStore schema init failed: %s", exc)

    def _connect(self) -> Any:
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

    def _initialize_schema(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect()
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.table}` (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    request_ts BIGINT NOT NULL DEFAULT 0,
                    username VARCHAR(190) NOT NULL DEFAULT '',
                    client_id VARCHAR(64) NOT NULL DEFAULT '',
                    trade_id BIGINT NOT NULL DEFAULT 0,
                    phase VARCHAR(16) NOT NULL DEFAULT '',
                    transaction_type VARCHAR(8) NOT NULL DEFAULT '',
                    symbol VARCHAR(96) NOT NULL DEFAULT '',
                    symbol_token VARCHAR(64) NOT NULL DEFAULT '',
                    exchange VARCHAR(16) NOT NULL DEFAULT '',
                    quantity INT NOT NULL DEFAULT 0,
                    request_payload_json LONGTEXT NOT NULL,
                    response_json LONGTEXT NOT NULL,
                    ok TINYINT(1) NOT NULL DEFAULT 0,
                    http_status INT NOT NULL DEFAULT 0,
                    error_message VARCHAR(255) NOT NULL DEFAULT '',
                    order_id VARCHAR(80) NOT NULL DEFAULT '',
                    created_at BIGINT NOT NULL,
                    INDEX idx_aoah_request_ts (request_ts),
                    INDEX idx_aoah_username (username),
                    INDEX idx_aoah_trade_id (trade_id),
                    INDEX idx_aoah_ok (ok),
                    INDEX idx_aoah_phase (phase)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _try_connect(self) -> Optional[Any]:
        if not self.enabled:
            return None
        try:
            return self._connect()
        except Exception as exc:
            logger.warning("MySQLAngelOrderAuditStore connect failed: %s", exc)
            return None

    @staticmethod
    def _json_text(payload: Any) -> str:
        try:
            return json.dumps(payload if payload is not None else {}, ensure_ascii=True, separators=(",", ":"), default=str)
        except Exception:
            return "{}"

    def save_hit(
        self,
        *,
        request_ts: int,
        username: str,
        client_id: str,
        trade_id: int,
        phase: str,
        transaction_type: str,
        symbol: str,
        symbol_token: str,
        exchange: str,
        quantity: int,
        request_payload: Any,
        response_payload: Any,
        ok: bool,
        http_status: int,
        error_message: str,
        order_id: str,
    ) -> None:
        if not self.enabled:
            return
        conn = self._try_connect()
        if conn is None:
            return
        now = int(time.time())
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO `{self.table}`
                    (request_ts, username, client_id, trade_id, phase, transaction_type,
                     symbol, symbol_token, exchange, quantity, request_payload_json,
                     response_json, ok, http_status, error_message, order_id, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    int(request_ts),
                    str(username or "").strip().lower(),
                    str(client_id or "").strip(),
                    int(trade_id),
                    str(phase or "").strip().lower()[:16],
                    str(transaction_type or "").strip().upper()[:8],
                    str(symbol or "").strip(),
                    str(symbol_token or "").strip(),
                    str(exchange or "").strip().upper(),
                    max(0, int(quantity or 0)),
                    self._json_text(request_payload),
                    self._json_text(response_payload),
                    1 if bool(ok) else 0,
                    max(0, int(http_status or 0)),
                    str(error_message or "").strip()[:255],
                    str(order_id or "").strip()[:80],
                    now,
                ),
            )
            conn.commit()
            cur.close()
        except Exception as exc:
            logger.warning("MySQLAngelOrderAuditStore.save_hit error: %s", exc)
        finally:
            conn.close()

    def get_hits_in_range(self, *, start_ts: int, end_ts: int, limit: int = 500) -> List[Dict[str, Any]]:
        if not self.enabled:
            return []
        conn = self._try_connect()
        if conn is None:
            return []
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT id, request_ts, username, client_id, trade_id, phase, transaction_type,
                       symbol, symbol_token, exchange, quantity, request_payload_json,
                       response_json, ok, http_status, error_message, order_id, created_at
                FROM `{self.table}`
                WHERE request_ts >= %s AND request_ts <= %s
                ORDER BY request_ts DESC, id DESC
                LIMIT %s
                """,
                (int(start_ts), int(end_ts), int(max(1, min(limit, 5000)))),
            )
            rows = cur.fetchall()
            cur.close()
            return [dict(row) for row in rows] if rows else []
        except Exception as exc:
            logger.warning("MySQLAngelOrderAuditStore.get_hits_in_range error: %s", exc)
            return []
        finally:
            conn.close()

    def get_summary_in_range(self, *, start_ts: int, end_ts: int) -> Dict[str, Any]:
        summary = {
            "total_hits": 0,
            "success_hits": 0,
            "failed_hits": 0,
            "unique_users": 0,
            "users": [],
        }
        if not self.enabled:
            return summary
        conn = self._try_connect()
        if conn is None:
            return summary
        cur = None
        user_cur = None
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    COUNT(*) AS total_hits,
                    COALESCE(SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END), 0) AS success_hits,
                    COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS failed_hits,
                    COUNT(DISTINCT CASE WHEN username <> '' THEN username END) AS unique_users
                FROM `{self.table}`
                WHERE request_ts >= %s AND request_ts <= %s
                """,
                (int(start_ts), int(end_ts)),
            )
            row = cur.fetchone() or {}
            summary.update(
                {
                    "total_hits": _to_int(row.get("total_hits"), 0),
                    "success_hits": _to_int(row.get("success_hits"), 0),
                    "failed_hits": _to_int(row.get("failed_hits"), 0),
                    "unique_users": _to_int(row.get("unique_users"), 0),
                }
            )
            user_cur = conn.cursor(dictionary=True)
            user_cur.execute(
                f"""
                SELECT
                    username,
                    COUNT(*) AS total_hits,
                    COALESCE(SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END), 0) AS success_hits,
                    COALESCE(SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END), 0) AS failed_hits
                FROM `{self.table}`
                WHERE request_ts >= %s AND request_ts <= %s
                  AND username <> ''
                GROUP BY username
                ORDER BY total_hits DESC, username ASC
                LIMIT 200
                """,
                (int(start_ts), int(end_ts)),
            )
            users = []
            for item in user_cur.fetchall() or []:
                users.append(
                    {
                        "username": str(item.get("username") or "").strip().lower(),
                        "total_hits": _to_int(item.get("total_hits"), 0),
                        "success_hits": _to_int(item.get("success_hits"), 0),
                        "failed_hits": _to_int(item.get("failed_hits"), 0),
                    }
                )
            summary["users"] = users
            return summary
        except Exception as exc:
            logger.warning("MySQLAngelOrderAuditStore.get_summary_in_range error: %s", exc)
            return summary
        finally:
            if user_cur is not None:
                user_cur.close()
            if cur is not None:
                cur.close()
            conn.close()


class CentralizedAngelExecutionManager:
    ORDER_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/placeOrder"
    SEARCH_URL = "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/searchScrip"
    _MONTH_CODE_MAP = {
        "1": "JAN",
        "2": "FEB",
        "3": "MAR",
        "4": "APR",
        "5": "MAY",
        "6": "JUN",
        "7": "JUL",
        "8": "AUG",
        "9": "SEP",
        "10": "OCT",
        "11": "NOV",
        "12": "DEC",
        "O": "OCT",
        "N": "NOV",
        "D": "DEC",
    }
    _MONTH_NAME_SET = {"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"}

    def __init__(
        self,
        *,
        settings: Settings,
        user_auth: MySQLUserAuthStore,
        live_order_store: Optional["MySQLLiveOrderStore"] = None,
        audit_store: Optional["MySQLAngelOrderAuditStore"] = None,
    ) -> None:
        self.settings = settings
        self.user_auth = user_auth
        self.live_order_store = live_order_store
        self.audit_store = audit_store
        self.enabled = bool(settings.central_live_execution_enabled)
        self.quantity = max(1, int(settings.central_live_execution_quantity))
        self.exchange = str(settings.central_live_execution_exchange or "NFO").strip().upper() or "NFO"
        self.product_type = str(settings.central_live_execution_product_type or "INTRADAY").strip().upper() or "INTRADAY"
        self.order_type = str(settings.central_live_execution_order_type or "MARKET").strip().upper() or "MARKET"
        self.variety = str(settings.central_live_execution_variety or "NORMAL").strip().upper() or "NORMAL"
        self.duration = str(settings.central_live_execution_duration or "DAY").strip().upper() or "DAY"
        self.timeout_sec = max(5, int(settings.central_live_execution_timeout_sec))
        self.api_key = str(settings.angel_api_key or "").strip()
        self._dispatch_lock = asyncio.Lock()
        self._active_trade_id = 0
        self._active_positions: Dict[str, Dict[str, Any]] = {}
        self._last_event: Dict[str, Any] = {
            "type": "idle",
            "ts": int(time.time()),
            "ok": False,
            "message": "No live dispatch yet.",
        }
        self._restore_open_positions_from_store()

    def _restore_open_positions_from_store(self) -> None:
        if not self.enabled or self.live_order_store is None:
            return
        rows = self.live_order_store.get_open_trades(limit=5000)
        if not rows:
            return

        latest_trade_id = 0
        latest_entry_ts = 0
        trade_ids: set[int] = set()
        for row in rows:
            trade_id = _to_int(row.get("trade_id"), 0)
            entry_ts = _to_int(row.get("entry_ts"), 0)
            if trade_id > 0:
                trade_ids.add(trade_id)
            if entry_ts > latest_entry_ts or (entry_ts == latest_entry_ts and trade_id > latest_trade_id):
                latest_entry_ts = entry_ts
                latest_trade_id = trade_id

        if latest_trade_id <= 0:
            logger.warning("Live execution recovery skipped: open rows exist but no valid trade_id was found.")
            return

        session_map: Dict[str, Dict[str, Any]] = {}
        try:
            for session in self.user_auth.list_connected_angel_sessions(5000):
                username = str(session.get("username") or "").strip().lower()
                if username:
                    session_map[username] = session
        except Exception as exc:
            logger.warning("Live execution recovery could not load Angel sessions: %s", exc)

        lot_size_qty = self._resolve_global_lot_size_qty()
        restored: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            trade_id = _to_int(row.get("trade_id"), 0)
            if trade_id != latest_trade_id:
                continue
            username = str(row.get("username") or "").strip().lower()
            if not username:
                continue
            session = session_map.get(username, {})
            restored[username] = {
                "trade_id": int(trade_id),
                "username": username,
                "client_id": str(session.get("client_id") or "").strip(),
                "api_key": str(session.get("api_key") or self.api_key or "").strip(),
                "access_token": str(session.get("access_token") or "").strip(),
                "symbol": str(row.get("symbol") or "").strip(),
                "symbol_token": "",
                "exchange": str(row.get("exchange") or self.exchange).strip().upper() or self.exchange,
                "user_lot_count": self._resolve_user_lot_count(session),
                "lot_size_qty": int(lot_size_qty),
                "quantity": max(1, _to_int(row.get("quantity"), self.quantity)),
                "entry_order_id": str(row.get("entry_order_id") or "").strip(),
                "entry_ts": _to_int(row.get("entry_ts"), 0),
                "restored_from_store": True,
                "store_row_id": _to_int(row.get("id"), 0),
            }

        if not restored:
            logger.warning(
                "Live execution recovery found %s open row(s) but could not reconstruct any active positions.",
                len(rows),
            )
            return

        self._active_trade_id = latest_trade_id
        self._active_positions = restored
        self._last_event = {
            "type": "recovery",
            "ts": int(time.time()),
            "ok": True,
            "trade_id": int(latest_trade_id),
            "restored_users": len(restored),
            "open_row_count": len(rows),
            "message": "Recovered unresolved live positions from MySQL store.",
        }
        if len(trade_ids) > 1:
            logger.warning(
                "Live execution recovery found multiple open trade_ids %s; restored only the latest trade_id=%s.",
                sorted(trade_ids),
                latest_trade_id,
            )
        logger.warning(
            "Live execution recovered %s unresolved live position(s) for trade_id=%s from MySQL store.",
            len(restored),
            latest_trade_id,
        )

    def _resolve_global_lot_size_qty(self) -> int:
        try:
            lot_size = _to_int(self.user_auth.get_global_lot_size_qty(), 0)
            if lot_size > 0:
                return lot_size
        except Exception:
            pass
        return max(1, int(self.quantity))

    @staticmethod
    def _resolve_user_lot_count(session: Dict[str, Any]) -> int:
        return max(1, _to_int((session or {}).get("user_lot_count"), 1))

    def _resolve_order_quantity_for_session(self, session: Dict[str, Any], lot_size_qty: int) -> int:
        lots = self._resolve_user_lot_count(session)
        lot_size = max(1, _to_int(lot_size_qty, 1))
        return max(1, lot_size * lots)

    @staticmethod
    def _clean_trading_symbol(value: Any) -> str:
        text = str(value or "").strip()
        if ":" in text:
            text = text.split(":", 1)[1].strip()
        return text

    @staticmethod
    def _normalize_option_trading_symbol(value: Any) -> str:
        return normalize_option_symbol(value)

    @staticmethod
    def _symbol_match_key(value: Any) -> str:
        text = str(value or "").strip().upper()
        if ":" in text:
            text = text.split(":", 1)[1].strip()
        return re.sub(r"[^A-Z0-9]", "", text)

    @classmethod
    def _looks_like_option_symbol(cls, value: Any) -> bool:
        text = cls._normalize_option_trading_symbol(value)
        if not text:
            return False
        if not (text.endswith("CE") or text.endswith("PE")):
            return False
        return any(ch.isdigit() for ch in text)

    @classmethod
    def _looks_like_angel_option_symbol(cls, value: Any) -> bool:
        text = cls._normalize_option_trading_symbol(value)
        if not text:
            return False
        return bool(re.fullmatch(r"[A-Z]+\d{2}(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\d{2}\d+(CE|PE)", text))

    @classmethod
    def _parse_option_symbol_parts(cls, value: Any) -> Dict[str, str]:
        contract = extract_option_contract(value)
        text = cls._normalize_option_trading_symbol(contract.get("symbol"))
        if not text:
            return {}
        strike = _to_float(contract.get("strike"), 0.0)
        strike_text = str(int(round(strike))) if strike > 0.0 and abs(strike - round(strike)) < 1e-6 else str(strike or "")
        month_name = str(contract.get("expiry_month_name") or "").strip().upper()
        return {
            "underlying": str(contract.get("underlying") or "").strip().upper(),
            "day": str(contract.get("expiry_day") or "").strip().zfill(2) if _to_int(contract.get("expiry_day"), 0) > 0 else "",
            "month": month_name,
            "year": str(contract.get("expiry_year_short") or "").strip().zfill(2) if contract.get("expiry_year_short") else "",
            "strike": strike_text,
            "side": str(contract.get("option_type") or "").strip().upper(),
            "expiry_kind": str(contract.get("expiry_kind") or "").strip().lower(),
            "expiry_date": str(contract.get("expiry_date") or "").strip(),
        }

    @classmethod
    def _build_angel_symbol_from_parts(cls, parts: Dict[str, str]) -> str:
        underlying = str(parts.get("underlying") or "").strip().upper()
        day = str(parts.get("day") or "").strip().zfill(2)
        month = str(parts.get("month") or "").strip().upper()
        year = str(parts.get("year") or "").strip().zfill(2)
        strike = str(parts.get("strike") or "").strip()
        side = str(parts.get("side") or "").strip().upper()
        if not underlying or not day or not month or not year or not strike or side not in {"CE", "PE"}:
            return ""
        if month not in cls._MONTH_NAME_SET:
            return ""
        if not strike.isdigit():
            try:
                strike = str(int(float(strike)))
            except (TypeError, ValueError):
                return ""
        return f"{underlying}{day}{month}{year}{strike}{side}"

    @classmethod
    def _symbol_search_candidates(cls, value: Any) -> List[str]:
        contract = extract_option_contract(value)
        out = [cls._normalize_option_trading_symbol(item) for item in build_symbol_search_candidates(contract)]
        return [item for item in out if item]

    @classmethod
    def _repair_option_symbol(
        cls,
        *,
        raw_symbol: Any,
        selected: Dict[str, Any],
        active_trade: Dict[str, Any],
    ) -> str:
        candidates = [
            raw_symbol,
            selected.get("symbol"),
            selected.get("tradingsymbol"),
            selected.get("symbol_name"),
            selected.get("name"),
            active_trade.get("option_symbol"),
        ]
        for candidate in candidates:
            normalized = cls._normalize_option_trading_symbol(candidate)
            if cls._looks_like_option_symbol(normalized):
                return normalized

        base = cls._normalize_option_trading_symbol(raw_symbol)
        if not base:
            base = cls._normalize_option_trading_symbol(active_trade.get("symbol"))
        side = str(
            active_trade.get("option_side")
            or selected.get("side")
            or selected.get("option_type")
            or ""
        ).strip().upper()
        strike = _to_float(active_trade.get("option_strike") or selected.get("strike"), 0.0)
        if base and side in {"CE", "PE"} and strike > 0.0:
            strike_text = str(int(round(strike))) if abs(strike - round(strike)) < 1e-9 else str(strike)
            fallback = f"{base}{strike_text}{side}"
            return cls._normalize_option_trading_symbol(fallback)
        return cls._normalize_option_trading_symbol(raw_symbol)

    @staticmethod
    def _extract_order_id(payload: Any) -> str:
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict):
                for key in ("orderid", "orderId", "oms_order_id", "uniqueorderid"):
                    value = str(data.get(key) or "").strip()
                    if value:
                        return value
            for key in ("orderid", "orderId", "oms_order_id", "uniqueorderid"):
                value = str(payload.get(key) or "").strip()
                if value:
                    return value
        return ""

    @staticmethod
    def _find_closed_trade(after_state: Dict[str, Any], trade_id: int) -> Dict[str, Any]:
        rows = after_state.get("recent_trades") if isinstance(after_state.get("recent_trades"), list) else []
        for row in reversed(rows):
            if not isinstance(row, dict):
                continue
            if _to_int(row.get("trade_id"), 0) == int(trade_id):
                return dict(row)
        return {}

    def _spawn(self, coro: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                coro.close()
            except Exception:
                pass
            return
        task = loop.create_task(coro)

        def _log_task_result(done: asyncio.Task) -> None:
            try:
                done.result()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.exception("Live execution async task failed: %s", exc)

        task.add_done_callback(_log_task_result)

    def _build_headers(self, access_token: str, api_key: str = "") -> Dict[str, str]:
        resolved_api_key = str(api_key or self.api_key or "").strip()
        return {
            "Authorization": f"Bearer {str(access_token or '').strip()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": str(self.settings.angel_client_local_ip or "127.0.0.1"),
            "X-ClientPublicIP": str(self.settings.angel_client_public_ip or "0.0.0.0"),
            "X-MACAddress": str(self.settings.angel_client_mac or "00:00:00:00:00:00"),
            "X-PrivateKey": resolved_api_key,
        }

    @staticmethod
    def _search_rows(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        data = payload.get("data")
        if isinstance(data, list):
            return [dict(item) for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("fetched", "rows", "scripts", "result"):
                rows = data.get(key)
                if isinstance(rows, list):
                    return [dict(item) for item in rows if isinstance(item, dict)]
            return [dict(data)]
        return []

    @classmethod
    def _score_search_row(
        cls,
        *,
        target_keys: List[str],
        target_parts: Dict[str, str],
        target_exchange: str,
        row: Dict[str, Any],
    ) -> int:
        row_symbol = (
            row.get("tradingsymbol")
            or row.get("symbol")
            or row.get("symbolname")
            or row.get("name")
        )
        row_key = cls._symbol_match_key(row_symbol)
        row_exchange = str(
            row.get("exchange")
            or row.get("exch_seg")
            or row.get("exchangeSegment")
            or ""
        ).strip().upper()
        row_parts = cls._parse_option_symbol_parts(row_symbol)
        score = 0
        if row_key and row_key in target_keys:
            score += 100
        if row_exchange and row_exchange == target_exchange:
            score += 5
        if target_parts and row_parts:
            if row_parts.get("underlying") == target_parts.get("underlying"):
                score += 25
            if row_parts.get("side") == target_parts.get("side"):
                score += 20
            if row_parts.get("strike") == target_parts.get("strike"):
                score += 25
            if row_parts.get("year") == target_parts.get("year"):
                score += 10
            if row_parts.get("month") == target_parts.get("month"):
                score += 10
            if target_parts.get("day") and row_parts.get("day") == target_parts.get("day"):
                score += 5
            if row_parts.get("expiry_kind") == target_parts.get("expiry_kind"):
                score += 5
        return score

    async def _resolve_option_instrument(
        self,
        *,
        access_token: str,
        api_key: str = "",
        symbol: str,
        exchange: str,
        contract_meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        token = str(access_token or "").strip()
        target_symbol = str(symbol or "").strip()
        target_exchange = str(exchange or self.exchange).strip().upper() or self.exchange
        if not token or not target_symbol:
            return {}
        target_contract = extract_option_contract(
            contract_meta or {"symbol": target_symbol},
            explicit_option_type=str((contract_meta or {}).get("option_type") or ""),
            explicit_strike=_to_float((contract_meta or {}).get("strike"), 0.0),
        )
        candidates = build_symbol_search_candidates(target_contract)
        target_keys = [self._symbol_match_key(item) for item in candidates if item]
        target_parts = self._parse_option_symbol_parts(target_contract.get("symbol") or target_symbol)
        best: Dict[str, str] = {}
        best_score = -1

        for query_symbol in candidates:
            payload = {
                "exchange": target_exchange,
                "searchscrip": query_symbol,
            }
            try:
                response = await asyncio.to_thread(
                    requests.post,
                    self.SEARCH_URL,
                    json=payload,
                    headers=self._build_headers(token, api_key=api_key),
                    timeout=self.timeout_sec,
                )
            except Exception:
                continue
            if int(getattr(response, "status_code", 500)) >= 400:
                continue
            try:
                body = response.json() if response.content else {}
            except ValueError:
                body = {}
            if isinstance(body, dict) and body.get("status") is False:
                continue
            rows = self._search_rows(body)
            for row in rows:
                row_token = str(
                    row.get("symboltoken")
                    or row.get("symbolToken")
                    or row.get("token")
                    or ""
                ).strip()
                row_symbol = str(
                    row.get("tradingsymbol")
                    or row.get("symbol")
                    or row.get("symbolname")
                    or row.get("name")
                    or ""
                ).strip()
                if not row_token or not row_symbol:
                    continue
                score = self._score_search_row(
                    target_keys=target_keys,
                    target_parts=target_parts,
                    target_exchange=target_exchange,
                    row=row,
                )
                if score <= best_score:
                    continue
                best_score = score
                best = {
                    "symbol": self._normalize_option_trading_symbol(row_symbol),
                    "symbol_token": row_token,
                    "exchange": str(
                        row.get("exchange")
                        or row.get("exch_seg")
                        or row.get("exchangeSegment")
                        or target_exchange
                    ).strip().upper() or target_exchange,
                }

        if not best:
            return {}
        if target_parts:
            best_parts = self._parse_option_symbol_parts(best.get("symbol"))
            if not best_parts:
                return {}
            must_match = ("underlying", "side", "strike")
            for key in must_match:
                if str(best_parts.get(key) or "") != str(target_parts.get(key) or ""):
                    return {}
            for key in ("year", "month"):
                target_value = str(target_parts.get(key) or "")
                if target_value and str(best_parts.get(key) or "") != target_value:
                    return {}
            target_day = str(target_parts.get("day") or "")
            if target_day and str(best_parts.get("day") or "") != target_day:
                return {}
        elif best_score < 100:
            return {}
        return best

    async def _search_symbol_token(
        self,
        *,
        access_token: str,
        api_key: str = "",
        symbol: str,
        exchange: str,
    ) -> str:
        resolved = await self._resolve_option_instrument(
            access_token=access_token,
            api_key=api_key,
            symbol=symbol,
            exchange=exchange,
            contract_meta={"symbol": symbol},
        )
        return str(resolved.get("symbol_token") or "").strip()

    @classmethod
    def _first_session_token(cls, sessions: List[Dict[str, Any]]) -> str:
        for session in sessions:
            access_token = str(session.get("access_token") or "").strip()
            if access_token:
                return access_token
        return ""

    @classmethod
    def _requires_angel_symbol_resolution(cls, symbol: str, symbol_token: str) -> bool:
        cleaned_symbol = cls._normalize_option_trading_symbol(symbol)
        if not cleaned_symbol:
            return True
        if not symbol_token:
            return True
        return not cls._looks_like_angel_option_symbol(cleaned_symbol)

    @classmethod
    def _apply_resolved_instrument(
        cls,
        *,
        symbol: str,
        symbol_token: str,
        exchange: str,
        resolved: Dict[str, str],
    ) -> Dict[str, str]:
        return {
            "symbol": str(resolved.get("symbol") or symbol or "").strip(),
            "symbol_token": str(resolved.get("symbol_token") or symbol_token or "").strip(),
            "exchange": str(resolved.get("exchange") or exchange or "").strip().upper(),
        }

    def _build_order_payload(
        self,
        *,
        symbol: str,
        symbol_token: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
    ) -> Dict[str, str]:
        return {
            "variety": self.variety,
            "tradingsymbol": symbol,
            "symboltoken": str(symbol_token),
            "transactiontype": str(transaction_type).upper(),
            "exchange": str(exchange or self.exchange).upper(),
            "ordertype": self.order_type,
            "producttype": self.product_type,
            "duration": self.duration,
            "quantity": str(max(1, int(quantity))),
            "price": "0",
            "squareoff": "0",
            "stoploss": "0",
            "triggerprice": "0",
        }

    async def _place_order(
        self,
        *,
        access_token: str,
        api_key: str = "",
        symbol: str,
        symbol_token: str,
        exchange: str,
        transaction_type: str,
        quantity: int,
        username: str = "",
        client_id: str = "",
        trade_id: int = 0,
        phase: str = "",
    ) -> Dict[str, Any]:
        payload = self._build_order_payload(
            symbol=symbol,
            symbol_token=symbol_token,
            exchange=exchange,
            transaction_type=transaction_type,
            quantity=quantity,
        )
        request_ts = int(time.time())

        def _audit(result: Dict[str, Any], response_payload: Any) -> None:
            if self.audit_store is None:
                return
            try:
                self.audit_store.save_hit(
                    request_ts=request_ts,
                    username=username,
                    client_id=client_id,
                    trade_id=int(trade_id),
                    phase=phase,
                    transaction_type=transaction_type,
                    symbol=symbol,
                    symbol_token=symbol_token,
                    exchange=exchange,
                    quantity=int(quantity),
                    request_payload=payload,
                    response_payload=response_payload,
                    ok=bool(result.get("ok")),
                    http_status=_to_int(result.get("http_status"), 0),
                    error_message=str(result.get("message") or "").strip(),
                    order_id=str(result.get("order_id") or "").strip(),
                )
            except Exception as exc:
                logger.warning("Angel order audit save failed for user=%s trade_id=%s: %s", username, int(trade_id), exc)

        try:
            response = await asyncio.to_thread(
                requests.post,
                self.ORDER_URL,
                json=payload,
                headers=self._build_headers(access_token, api_key=api_key),
                timeout=self.timeout_sec,
            )
        except Exception as exc:
            result = {"ok": False, "message": f"request_failed: {exc}"}
            _audit(result, {"error": str(exc), "stage": "request"})
            return result

        status_code = int(getattr(response, "status_code", 500))
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}
        if status_code >= 400:
            result = {"ok": False, "message": f"http_{status_code}", "http_status": status_code}
            _audit(result, body if body else {"status_code": status_code, "raw_text": str(getattr(response, "text", "") or "")[:4000]})
            return result
        if isinstance(body, dict) and body.get("status") is False:
            message = str(body.get("message") or body.get("errorcode") or "order_rejected").strip()
            result = {"ok": False, "message": message, "http_status": status_code}
            _audit(result, body)
            return result
        order_id = self._extract_order_id(body)
        if not order_id:
            result = {"ok": False, "message": "missing_order_id", "http_status": status_code}
            _audit(result, body)
            return result
        result = {
            "ok": True,
            "order_id": order_id,
            "http_status": status_code,
        }
        _audit(result, body)
        return result
    def observe_transition(self, before_state: Dict[str, Any], after_state: Dict[str, Any]) -> None:
        if not self.enabled:
            return
        before_active = before_state.get("active_trade") if isinstance(before_state.get("active_trade"), dict) else {}
        after_active = after_state.get("active_trade") if isinstance(after_state.get("active_trade"), dict) else {}
        before_trade_id = _to_int(before_active.get("trade_id"), 0)
        after_trade_id = _to_int(after_active.get("trade_id"), 0)
        if before_trade_id == after_trade_id:
            return

        if before_trade_id > 0:
            closed_trade = self._find_closed_trade(after_state, before_trade_id)
            self._spawn(self._dispatch_exit(before_trade_id, closed_trade))
        if after_trade_id > 0 and isinstance(after_active, dict):
            self._spawn(self._dispatch_entry(dict(after_active)))

    async def _dispatch_entry(self, active_trade: Dict[str, Any]) -> None:
        async with self._dispatch_lock:
            trade_id = _to_int(active_trade.get("trade_id"), 0)
            if trade_id <= 0:
                return
            if self._active_positions and self._active_trade_id != trade_id:
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": "Blocked: unresolved positions from previous trade.",
                    "open_positions": len(self._active_positions),
                }
                return

            signal = active_trade.get("signal") if isinstance(active_trade.get("signal"), dict) else {}
            selected = signal.get("selected_option") if isinstance(signal.get("selected_option"), dict) else {}
            raw_symbol = active_trade.get("option_symbol") or selected.get("symbol")
            raw_token = (
                active_trade.get("option_token")
                or selected.get("symbol_token")
                or selected.get("symboltoken")
                or selected.get("token")
            )
            raw_exchange = active_trade.get("option_exchange") or selected.get("exchange") or self.exchange

            symbol = self._repair_option_symbol(
                raw_symbol=raw_symbol,
                selected=selected,
                active_trade=active_trade,
            )
            symbol_token = str(raw_token or "").strip()
            exchange = str(raw_exchange or self.exchange).strip().upper() or self.exchange
            contract_meta = extract_option_contract(
                {
                    "symbol": symbol,
                    "symbol_token": symbol_token,
                    "exchange": exchange,
                    "option_type": active_trade.get("option_side") or selected.get("side"),
                    "strike": active_trade.get("option_strike") or selected.get("strike"),
                    "expiry_date": active_trade.get("option_expiry_date") or selected.get("expiry_date"),
                    "expiry_ts": active_trade.get("option_expiry_ts") or selected.get("expiry_ts"),
                    "expiry_kind": active_trade.get("option_expiry_kind") or selected.get("expiry_kind"),
                    "underlying": selected.get("underlying"),
                },
                snapshot_ts=_to_int(active_trade.get("entry_ts"), int(time.time())),
                explicit_option_type=str(active_trade.get("option_side") or selected.get("side") or ""),
                explicit_strike=_to_float(active_trade.get("option_strike") or selected.get("strike"), 0.0),
            )
            if not symbol:
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": "Missing option symbol for live dispatch.",
                    "symbol": symbol,
                }
                return
            tradeable, tradeable_reason = contract_tradeability(
                contract_meta,
                reference_ts=_to_int(active_trade.get("entry_ts"), int(time.time())),
                allow_expiry_day=True,
            )
            if not tradeable:
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": f"Option contract not tradeable for live dispatch: {tradeable_reason}",
                    "symbol": symbol,
                    "expiry_date": str(contract_meta.get("expiry_date") or ""),
                }
                return

            sessions = await asyncio.to_thread(self.user_auth.list_connected_angel_sessions, 5000)
            if not sessions:
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": "No connected users available for dispatch.",
                }
                self._active_trade_id = trade_id
                self._active_positions = {}
                return
            eligible_sessions = [
                session
                for session in sessions
                if str(session.get("access_token") or "").strip()
                and str(session.get("api_key") or self.api_key or "").strip()
            ]
            if not eligible_sessions:
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": "No connected users have both Angel access token and API key.",
                    "total_users": len(sessions),
                }
                self._active_trade_id = trade_id
                self._active_positions = {}
                return

            if self._requires_angel_symbol_resolution(symbol, symbol_token):
                resolved_instrument: Dict[str, str] = {}
                for session in eligible_sessions:
                    access_token = str(session.get("access_token") or "").strip()
                    api_key = str(session.get("api_key") or self.api_key or "").strip()
                    if not access_token or not api_key:
                        continue
                    resolved_instrument = await self._resolve_option_instrument(
                        access_token=access_token,
                        api_key=api_key,
                        symbol=symbol,
                        exchange=exchange,
                        contract_meta=contract_meta,
                    )
                    if resolved_instrument:
                        break
                if resolved_instrument:
                    normalized = self._apply_resolved_instrument(
                        symbol=symbol,
                        symbol_token=symbol_token,
                        exchange=exchange,
                        resolved=resolved_instrument,
                    )
                    symbol = normalized["symbol"]
                    symbol_token = normalized["symbol_token"]
                    exchange = normalized["exchange"] or exchange
            if not symbol_token or not self._looks_like_angel_option_symbol(symbol):
                self._last_event = {
                    "type": "entry",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": trade_id,
                    "message": "Missing Angel option symbol/token for live dispatch.",
                    "symbol": symbol,
                    "exchange": exchange,
                    "eligible_users": len(sessions),
                }
                return

            lot_size_qty = await asyncio.to_thread(self._resolve_global_lot_size_qty)
            placed: Dict[str, Dict[str, Any]] = {}
            failed = 0
            for session in eligible_sessions:
                username = str(session.get("username") or "").strip().lower()
                access_token = str(session.get("access_token") or "").strip()
                api_key = str(session.get("api_key") or self.api_key or "").strip()
                if not username or not access_token or not api_key:
                    failed += 1
                    continue
                order_quantity = self._resolve_order_quantity_for_session(session, lot_size_qty)
                result = await self._place_order(
                    access_token=access_token,
                    api_key=api_key,
                    symbol=symbol,
                    symbol_token=symbol_token,
                    exchange=exchange,
                    transaction_type="BUY",
                    quantity=order_quantity,
                    username=username,
                    client_id=str(session.get("client_id") or "").strip(),
                    trade_id=trade_id,
                    phase="entry",
                )
                if not bool(result.get("ok")):
                    failed += 1
                    continue
                entry_order_id = str(result.get("order_id") or "").strip()
                entry_ts_now = int(time.time())
                placed[username] = {
                    "trade_id": int(trade_id),
                    "username": username,
                    "client_id": str(session.get("client_id") or "").strip(),
                    "api_key": api_key,
                    "access_token": access_token,
                    "symbol": symbol,
                    "symbol_token": symbol_token,
                    "exchange": exchange,
                    "underlying": str(contract_meta.get("underlying") or ""),
                    "option_type": str(contract_meta.get("option_type") or ""),
                    "strike": _to_float(contract_meta.get("strike"), 0.0),
                    "expiry_date": str(contract_meta.get("expiry_date") or ""),
                    "expiry_ts": _to_int(contract_meta.get("expiry_ts"), 0),
                    "expiry_kind": str(contract_meta.get("expiry_kind") or ""),
                    "user_lot_count": self._resolve_user_lot_count(session),
                    "lot_size_qty": int(lot_size_qty),
                    "quantity": int(order_quantity),
                    "entry_order_id": entry_order_id,
                    "entry_ts": entry_ts_now,
                }
                if self.live_order_store is not None:
                    try:
                        self.live_order_store.save_entry(
                            username=username,
                            trade_id=trade_id,
                            symbol=symbol,
                            exchange=exchange,
                            quantity=order_quantity,
                            direction="BUY",
                            entry_order_id=entry_order_id,
                            entry_ts=entry_ts_now,
                        )
                    except Exception as _exc:
                        logger.warning("live_order_store.save_entry failed for %s: %s", username, _exc)

            self._active_trade_id = trade_id
            self._active_positions = placed
            self._last_event = {
                "type": "entry",
                "ts": int(time.time()),
                "ok": bool(placed),
                "trade_id": trade_id,
                "symbol": symbol,
                "exchange": exchange,
                "lot_size_qty": int(lot_size_qty),
                "quantity_mode": "lot_size_x_user_lots",
                "total_users": len(eligible_sessions),
                "placed_users": len(placed),
                "failed_users": int(failed),
            }

    async def _dispatch_exit(self, trade_id: int, closed_trade: Dict[str, Any]) -> None:
        async with self._dispatch_lock:
            open_positions = dict(self._active_positions)
            if not open_positions:
                self._active_trade_id = 0
                self._last_event = {
                    "type": "exit",
                    "ts": int(time.time()),
                    "ok": False,
                    "trade_id": int(trade_id),
                    "message": "No tracked open positions to close.",
                }
                logger.warning(
                    "Live exit skipped for trade_id=%s because no in-memory open positions were tracked.",
                    int(trade_id),
                )
                return

            resolved_trade_id = int(trade_id)
            if resolved_trade_id <= 0:
                for position in open_positions.values():
                    resolved_trade_id = _to_int(position.get("trade_id"), 0)
                    if resolved_trade_id > 0:
                        break

            sessions = await asyncio.to_thread(self.user_auth.list_connected_angel_sessions, 5000)
            session_map: Dict[str, Dict[str, Any]] = {}
            for row in sessions:
                username = str(row.get("username") or "").strip().lower()
                if username:
                    session_map[username] = row

            lot_size_qty = await asyncio.to_thread(self._resolve_global_lot_size_qty)
            remaining: Dict[str, Dict[str, Any]] = {}
            closed_count = 0
            failed_count = 0
            for username, position in open_positions.items():
                fresh = session_map.get(str(username).strip().lower(), {})
                access_token = str(fresh.get("access_token") or position.get("access_token") or "").strip()
                api_key = str(fresh.get("api_key") or position.get("api_key") or self.api_key or "").strip()
                symbol = str(position.get("symbol") or "").strip()
                symbol_token = str(position.get("symbol_token") or "").strip()
                exchange = str(position.get("exchange") or self.exchange).strip().upper() or self.exchange
                quantity = max(
                    1,
                    _to_int(
                        position.get("quantity"),
                        self._resolve_order_quantity_for_session(fresh or position, lot_size_qty),
                    ),
                )
                if access_token and self._requires_angel_symbol_resolution(symbol, symbol_token):
                    resolved = await self._resolve_option_instrument(
                        access_token=access_token,
                        api_key=api_key,
                        symbol=symbol,
                        exchange=exchange,
                        contract_meta=position,
                    )
                    if resolved:
                        normalized = self._apply_resolved_instrument(
                            symbol=symbol,
                            symbol_token=symbol_token,
                            exchange=exchange,
                            resolved=resolved,
                        )
                        symbol = normalized["symbol"]
                        symbol_token = normalized["symbol_token"]
                        exchange = normalized["exchange"] or exchange
                if not access_token or not api_key or not symbol or not symbol_token:
                    logger.warning(
                        "Live exit could not proceed for trade_id=%s user=%s symbol=%s access_token=%s api_key=%s symbol_token=%s",
                        int(trade_id),
                        username,
                        symbol,
                        bool(access_token),
                        bool(api_key),
                        bool(symbol_token),
                    )
                    failed_count += 1
                    saved = dict(position)
                    saved["api_key"] = api_key
                    saved["symbol"] = symbol
                    saved["symbol_token"] = symbol_token
                    saved["exchange"] = exchange
                    remaining[username] = saved
                    continue
                result = await self._place_order(
                    access_token=access_token,
                    api_key=api_key,
                    symbol=symbol,
                    symbol_token=symbol_token,
                    exchange=exchange,
                    transaction_type="SELL",
                    quantity=quantity,
                    username=username,
                    client_id=str(fresh.get("client_id") or position.get("client_id") or "").strip(),
                    trade_id=int(resolved_trade_id),
                    phase="exit",
                )
                if bool(result.get("ok")):
                    closed_count += 1
                    if self.live_order_store is not None:
                        try:
                            self.live_order_store.save_exit(
                                username=username,
                                trade_id=int(resolved_trade_id),
                                exit_order_id=str(result.get("order_id") or "").strip(),
                                exit_ts=int(time.time()),
                                close_reason=str(closed_trade.get("close_reason") or ""),
                            )
                        except Exception as _exc:
                            logger.warning("live_order_store.save_exit failed for %s: %s", username, _exc)
                else:
                    logger.warning(
                        "Live exit order failed for trade_id=%s user=%s symbol=%s message=%s",
                        int(resolved_trade_id),
                        username,
                        symbol,
                        str(result.get("message") or "unknown_error"),
                    )
                    failed_count += 1
                    remaining[username] = dict(position)

            self._active_trade_id = int(resolved_trade_id) if remaining else 0
            self._active_positions = remaining
            self._last_event = {
                "type": "exit",
                "ts": int(time.time()),
                "ok": failed_count == 0,
                "trade_id": int(resolved_trade_id),
                "closed_users": int(closed_count),
                "failed_users": int(failed_count),
                "remaining_open_positions": len(remaining),
                "close_reason": str(closed_trade.get("close_reason") or ""),
            }
            if failed_count > 0:
                logger.warning(
                    "Live exit completed with failures for trade_id=%s closed_users=%s failed_users=%s remaining=%s",
                    int(resolved_trade_id),
                    int(closed_count),
                    int(failed_count),
                    len(remaining),
                )

    def has_open_positions(self) -> bool:
        return bool(self._active_positions)

    def get_open_positions_snapshot(self) -> Dict[str, Dict[str, Any]]:
        return {username: dict(position) for username, position in self._active_positions.items()}

    async def force_exit_all(self, *, reason: str) -> Dict[str, Any]:
        trade_id = int(self._active_trade_id)
        if trade_id <= 0:
            for position in self._active_positions.values():
                trade_id = _to_int(position.get("trade_id"), 0)
                if trade_id > 0:
                    break
        await self._dispatch_exit(trade_id, {"close_reason": str(reason)})
        return dict(self._last_event)

    def get_status(self) -> Dict[str, Any]:
        lot_size_qty = self._resolve_global_lot_size_qty()
        return {
            "enabled": bool(self.enabled),
            "api_key_configured": bool(self.api_key),
            "active_trade_id": int(self._active_trade_id),
            "open_positions_count": len(self._active_positions),
            "open_position_users": sorted(self._active_positions.keys()),
            "config": {
                "quantity_mode": "lot_size_x_user_lots",
                "lot_size_qty": int(lot_size_qty),
                "fallback_quantity": int(self.quantity),
                "exchange": str(self.exchange),
                "product_type": str(self.product_type),
                "order_type": str(self.order_type),
                "variety": str(self.variety),
                "duration": str(self.duration),
                "timeout_sec": int(self.timeout_sec),
            },
            "last_event": dict(self._last_event),
        }


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _validate_identifier(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
        raise ValueError(f"{label} must contain only letters, numbers, and underscores")
    return text


def _normalize_epoch_seconds(value: Any) -> int:
    if value is None:
        return 0

    if isinstance(value, datetime):
        moment = value
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=_IST)
        return int(moment.timestamp())

    raw = _to_float(value, default=0.0)
    if raw > 0.0:
        if raw > 10_000_000_000:
            raw = raw / 1000.0
        return int(raw)

    text = str(value).strip()
    if not text:
        return 0

    iso_text = text.replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(iso_text)
    except ValueError:
        return 0
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=_IST)
    return int(moment.timestamp())


def _max_allowed_timestamp(now_ts: Optional[int] = None, future_slack_sec: int = 300) -> int:
    base_now = int(now_ts if now_ts is not None else time.time())
    return base_now + max(0, int(future_slack_sec))


def _new_snapshot_id(timestamp: int) -> str:
    ts = max(0, int(timestamp or 0))
    return f"oc_{ts}_{uuid.uuid4().hex[:10]}"


def _ensure_snapshot_id(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    ts = _to_int(snapshot.get("timestamp"), int(time.time()))
    snapshot_id = str(snapshot.get("snapshot_id") or "").strip()[:48]
    if not snapshot_id:
        snapshot_id = _new_snapshot_id(ts)
    snapshot["snapshot_id"] = snapshot_id
    return snapshot


def _new_backtest_job_id() -> str:
    return f"bt_{int(time.time())}_{uuid.uuid4().hex[:8]}"


def _backtest_public_status(job: Dict[str, Any]) -> Dict[str, Any]:
    status = str(job.get("status") or "unknown")
    out = {
        "job_id": str(job.get("job_id") or ""),
        "status": status,
        "running": status == "running",
        "progress_pct": max(0.0, min(100.0, _to_float(job.get("progress_pct"), 0.0))),
        "processed": max(0, _to_int(job.get("processed"), 0)),
        "total": max(1, _to_int(job.get("total"), 1)),
        "message": str(job.get("message") or ""),
        "started_at": max(0, _to_int(job.get("started_at"), 0)),
        "completed_at": max(0, _to_int(job.get("completed_at"), 0)),
        "error": str(job.get("error") or ""),
    }
    if status == "completed":
        result = job.get("result")
        if isinstance(result, dict):
            summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
            out["summary"] = {
                "closed_trades": _to_int(summary.get("closed_trades"), 0),
                "win_rate_pct": _to_float(summary.get("win_rate_pct"), 0.0),
                "net_points": _to_float(summary.get("net_points"), 0.0),
            }
    return out


def _db_readiness(state: AppState) -> Dict[str, Any]:
    settings = state.settings
    required_tables = {
        "auth_users": "auth_users",
        "auth_sessions": "auth_sessions",
        "candles_1m": settings.runtime_mysql_table_1m,
        "candles_5m": settings.runtime_mysql_table_5m,
        "option_snapshots": settings.runtime_mysql_options_table,
        "option_chain_strikes": settings.runtime_mysql_option_strikes_table,
        "paper_state": settings.paper_trade_mysql_state_table,
        "paper_trades": settings.paper_trade_mysql_trades_table,
        "paper_feedback": settings.paper_trade_mysql_feedback_table,
        "paper_mistakes": settings.paper_trade_mysql_mistakes_table,
        "backtest_runs": settings.backtest_mysql_runs_table,
        "backtest_trades": settings.backtest_mysql_trades_table,
    }
    out: Dict[str, Any] = {
        "ok": False,
        "database": settings.auth_mysql_database,
        "connected": False,
        "missing_tables": [],
        "table_counts": {},
        "future_rows": {"candles_1m": 0, "candles_5m": 0},
    }

    try:
        import mysql.connector  # type: ignore
    except ImportError:
        out["error"] = "mysql-connector-python is not installed"
        return out

    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(**mysql_connect_kwargs(settings, with_database=True, autocommit=True))
        cur = conn.cursor(dictionary=True)
        out["connected"] = True
        cur.execute("SHOW TABLES")
        table_keys = [str(v) for row in (cur.fetchall() or []) for v in row.values()]
        present_tables = set(table_keys)

        for key, table in required_tables.items():
            if table not in present_tables:
                out["missing_tables"].append(table)
                continue
            cur.execute(f"SELECT COUNT(*) AS c FROM `{table}`")
            row = cur.fetchone() or {}
            out["table_counts"][key] = int(row.get("c", 0) or 0)

        now_ts = int(time.time())
        max_ts = _max_allowed_timestamp(now_ts, settings.max_future_timestamp_sec)
        for key, table in (("candles_1m", settings.runtime_mysql_table_1m), ("candles_5m", settings.runtime_mysql_table_5m)):
            if table not in present_tables:
                continue
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (settings.auth_mysql_database, table),
            )
            names = {str((row or {}).get("COLUMN_NAME", "")).strip().lower() for row in (cur.fetchall() or [])}
            ts_col = ""
            for candidate in ("start", "timestamp", "start_ts", "ts", "time", "datetime", "date", "candle_time"):
                if candidate in names:
                    ts_col = candidate
                    break
            if not ts_col:
                continue
            cur.execute(f"SELECT COUNT(*) AS c FROM `{table}` WHERE `{ts_col}` > %s", (max_ts,))
            out["future_rows"][key] = int((cur.fetchone() or {}).get("c", 0) or 0)

        warmup_ok = (
            int(out["table_counts"].get("candles_1m", 0)) >= 30
            and int(out["table_counts"].get("candles_5m", 0)) >= 6
        )
        out["warmup_ok"] = warmup_ok
        out["ok"] = bool(not out["missing_tables"] and warmup_ok)
        return out
    except Exception as exc:
        out["error"] = str(exc)
        return out
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def _runtime_readiness(state: AppState) -> Dict[str, Any]:
    settings = state.settings
    return {
        "stream_mode": settings.stream_mode,
        "market_hours_only": bool(settings.market_hours_only),
        "options_enabled": bool(settings.options_enabled),
        "persist_live_candles": bool(settings.runtime_persist_live_candles),
        "persist_option_snapshots": bool(settings.runtime_persist_option_snapshots),
        "analysis_timeframe": "5m" if settings.runtime_use_5m_primary else "1m",
        "execution_timeframe": "1m",
    }


def _fyers_readiness(state: AppState) -> Dict[str, Any]:
    runtime = _runtime_readiness(state)
    if runtime["stream_mode"] != "fyers":
        return {"ok": True, "mode": runtime["stream_mode"], "detail": "FYERS stream disabled"}
    status = state.fyers_auth.get_status()
    access_sec = int(status.get("seconds_to_access_expiry") or 0)
    refresh_sec = int(status.get("seconds_to_refresh_expiry") or 0)
    has_refresh = bool(status.get("has_refresh_token"))
    refresh_valid = bool(status.get("refresh_valid"))
    authenticated = bool(status.get("authenticated"))
    has_secret = bool(str(state.settings.fyers_secret_key or "").strip())
    has_client_id = bool(str(state.settings.fyers_client_id or status.get("client_id") or "").strip())
    refresh_configured = bool(has_secret and has_client_id)
    # If access token is near expiry but refresh token is valid, runtime can recover automatically.
    ok = bool((authenticated and access_sec > 5) or (has_refresh and refresh_valid and refresh_configured))
    return {
        "ok": ok,
        "authenticated": authenticated,
        "has_access_token": bool(status.get("has_access_token")),
        "has_refresh_token": has_refresh,
        "refresh_valid": refresh_valid,
        "refresh_configured": refresh_configured,
        "has_secret_key": has_secret,
        "has_client_id": has_client_id,
        "needs_refresh": bool(status.get("needs_refresh")),
        "access_token_expires_at": int(status.get("access_token_expires_at") or 0),
        "refresh_token_expires_at": int(status.get("refresh_token_expires_at") or 0),
        "seconds_to_access_expiry": access_sec,
        "seconds_to_refresh_expiry": refresh_sec,
    }


def _system_ready_payload(state: AppState) -> Dict[str, Any]:
    now_ts = int(time.time())
    db_info = _db_readiness(state)
    runtime_info = _runtime_readiness(state)
    fyers_info = _fyers_readiness(state)
    issues: List[str] = []

    if not db_info.get("connected"):
        issues.append("Database connection failed.")
    if db_info.get("missing_tables"):
        issues.append(f"Missing tables: {', '.join(db_info['missing_tables'])}.")
    if not db_info.get("warmup_ok", False):
        issues.append("Not enough historical candles for warm-up (need >=30 of 1m and >=6 of 5m).")
    if int((db_info.get("future_rows", {}) or {}).get("candles_1m", 0)) > 0:
        issues.append("Future-dated rows found in 1m candles.")
    if int((db_info.get("future_rows", {}) or {}).get("candles_5m", 0)) > 0:
        issues.append("Future-dated rows found in 5m candles.")
    if not fyers_info.get("ok", False):
        issues.append("FYERS auth/refresh is not ready.")

    ready = bool(db_info.get("ok", False) and fyers_info.get("ok", False))
    summary = "Ready for market-open test." if ready else "Not ready. Fix listed issues."
    return {
        "ready": ready,
        "summary": summary,
        "checked_at": now_ts,
        "runtime": runtime_info,
        "database": db_info,
        "fyers": fyers_info,
        "issues": issues,
    }


def _is_nse_market_hours(timestamp: int) -> bool:
    moment = datetime.fromtimestamp(int(timestamp), tz=_IST)
    if moment.weekday() >= 5:
        return False
    current = moment.time()
    return _NSE_OPEN_TIME <= current <= _NSE_CLOSE_TIME


def _seconds_until_next_nse_market_open(now_ts: Optional[int] = None) -> int:
    moment = (
        datetime.fromtimestamp(int(now_ts), tz=_IST)
        if now_ts is not None
        else datetime.now(_IST)
    )
    if moment.weekday() < 5 and _NSE_OPEN_TIME <= moment.time() <= _NSE_CLOSE_TIME:
        return 0
    for day_offset in range(0, 8):
        day = (moment + timedelta(days=day_offset)).date()
        open_dt = datetime.combine(day, _NSE_OPEN_TIME, tzinfo=_IST)
        if open_dt.weekday() >= 5:
            continue
        if day_offset == 0:
            if moment < open_dt:
                return max(1, int((open_dt - moment).total_seconds()))
            continue
        return max(1, int((open_dt - moment).total_seconds()))
    return 3600


_FYERS_AUTH_ERROR_CODES = {-300, -99, -15, 401, 403}


def _fyers_error_text(payload: Any) -> str:
    if isinstance(payload, Exception):
        return str(payload)
    if isinstance(payload, dict):
        nested = payload.get("error")
        if isinstance(nested, dict):
            nested_msg = str(nested.get("message") or nested.get("error") or "").strip()
            if nested_msg:
                return nested_msg
        return str(payload.get("message") or payload.get("error") or payload.get("s") or payload)
    return str(payload)


def _is_fyers_auth_error(payload: Any) -> bool:
    if isinstance(payload, Exception):
        return _is_fyers_auth_error(str(payload))
    if isinstance(payload, dict):
        code = _to_int(payload.get("code"), 0)
        if code in _FYERS_AUTH_ERROR_CODES:
            return True
        nested = payload.get("error")
        if isinstance(nested, dict) and _is_fyers_auth_error(nested):
            return True
    text = _fyers_error_text(payload).lower()
    if "token" in text and ("expired" in text or "invalid" in text or "valid token" in text):
        return True
    return False


def _token_hash(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def _apply_fyers_access_token(state: AppState, token: str, *, reason: str) -> bool:
    fresh_token = str(token or "").strip()
    if not fresh_token:
        return False
    previous_token = str(state.settings.fyers_access_token or "").strip()
    changed = fresh_token != previous_token
    state.settings.fyers_access_token = fresh_token
    if state.quote_client is not None:
        try:
            state.quote_client.set_access_token(fresh_token)
        except Exception as exc:
            logger.warning("FYERS quote client token update failed after %s: %s", reason, exc)
    if changed:
        state.fyers_token_generation += 1
        logger.info(
            "FYERS token applied after %s; generation=%s token_hash=%s",
            reason,
            state.fyers_token_generation,
            _token_hash(fresh_token),
        )
    return changed


async def _force_fyers_token_refresh(state: AppState, reason: str) -> bool:
    if state.settings.stream_mode != "fyers":
        return False
    if not state.settings.fyers_refresh_enabled:
        return False
    for attempt in range(1, 4):
        try:
            refreshed = await asyncio.to_thread(state.fyers_auth.refresh_access_token, force=True)
            token = str(refreshed.get("access_token", "")).strip()
            if not token:
                if attempt < 3:
                    await asyncio.sleep(float(attempt))
                    continue
                return False
            _apply_fyers_access_token(state, token, reason=reason)
            logger.warning("FYERS token refreshed after %s (attempt=%s)", reason, attempt)
            return True
        except Exception as exc:
            if attempt >= 3:
                logger.warning("FYERS forced refresh failed after %s: %s", reason, exc)
                return False
            await asyncio.sleep(float(attempt))
    return False


async def _fyers_startup_token_bootstrap(state: AppState, *, max_wait_sec: int = 60) -> bool:
    if state.settings.stream_mode != "fyers":
        return False
    if not state.settings.fyers_refresh_enabled:
        return False

    deadline = int(time.time()) + max(5, int(max_wait_sec))
    while True:
        try:
            status = await asyncio.to_thread(state.fyers_auth.get_status)
        except Exception as exc:
            logger.warning("FYERS startup status check failed: %s", exc)
            status = {}

        access_valid = bool(status.get("authenticated"))
        has_refresh = bool(status.get("has_refresh_token"))
        refresh_valid = bool(status.get("refresh_valid"))
        if access_valid:
            # Pull current token payload from auth manager and apply it to all clients.
            try:
                current = await asyncio.to_thread(state.fyers_auth.refresh_access_token, force=False)
                token = str(current.get("access_token", "")).strip()
                if token:
                    _apply_fyers_access_token(state, token, reason="startup token bootstrap")
                    return True
            except Exception as exc:
                logger.warning("FYERS startup bootstrap read failed: %s", exc)

        if not has_refresh or not refresh_valid:
            # No valid refresh path left; require interactive re-auth.
            state.settings.fyers_access_token = ""
            logger.warning("FYERS refresh token unavailable/expired at startup; re-authentication is required.")
            return False

        if await _force_fyers_token_refresh(state, reason="startup bootstrap refresh"):
            return True

        if int(time.time()) >= deadline:
            # Continue startup with background retries; avoid using stale token immediately.
            state.settings.fyers_access_token = ""
            logger.warning("FYERS startup bootstrap timed out; background refresh loop will keep retrying.")
            return False
        await asyncio.sleep(3)


def _option_snapshot_signature(snapshot: Dict[str, Any]) -> tuple[Any, ...]:
    return (
        round(_to_float(snapshot.get("atm_strike"), 0.0), 2),
        round(_to_float(snapshot.get("atm_ce_ltp"), 0.0), 4),
        round(_to_float(snapshot.get("atm_pe_ltp"), 0.0), 4),
        round(_to_float(snapshot.get("atm_ce_volume"), 0.0), 4),
        round(_to_float(snapshot.get("atm_pe_volume"), 0.0), 4),
        round(_to_float(snapshot.get("atm_ce_oi_change"), 0.0), 4),
        round(_to_float(snapshot.get("atm_pe_oi_change"), 0.0), 4),
        round(_to_float(snapshot.get("band_volume_near_atm"), 0.0), 4),
        round(_to_float(snapshot.get("band_volume_total"), 0.0), 4),
    )


def _pain_state_response(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "pain_phase": snapshot.get("pain_phase", "comfort"),
        "dominant_pain_group": snapshot.get("dominant_pain_group", "none"),
        "next_likely_pain_group": snapshot.get("next_likely_pain_group", "none"),
        "confidence": snapshot.get("confidence", 0.0),
        "guidance": snapshot.get("guidance", "observe"),
        "entry_signal": snapshot.get("entry_signal", False),
        "entry_direction": snapshot.get("entry_direction", "NONE"),
        "entry_trigger_confidence": snapshot.get("entry_trigger_confidence", 0.0),
        "stop_loss_distance": snapshot.get("stop_loss_distance", 0.0),
        "target_level": snapshot.get("target_level", 0.0),
        "recommended_stop_loss": snapshot.get("recommended_stop_loss", 0.0),
        "recommended_targets": snapshot.get("recommended_targets", []),
        "explanation": snapshot.get("explanation", ""),
    }


def _ingest_runtime(
    state: AppState,
    candles: List[Dict[str, Any]],
    option_snapshots: List[Dict[str, Any]],
    candles_5m: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    candles_sorted = (
        candles
        if len(candles) <= 1
        else sorted(candles, key=lambda item: _to_int(item.get("timestamp"), 0))
    )
    candles_5m_rows = candles_5m or []
    candles_5m_sorted = (
        candles_5m_rows
        if len(candles_5m_rows) <= 1
        else sorted(candles_5m_rows, key=lambda item: _to_int(item.get("timestamp"), 0))
    )
    options_sorted = (
        option_snapshots
        if len(option_snapshots) <= 1
        else sorted(option_snapshots, key=lambda item: _to_int(item.get("timestamp"), 0))
    )

    latest = state.pain_runtime.get_state()
    candle_5m_idx = 0
    option_idx = 0
    seed_ts = _to_int(latest.get("timestamp"), 0)
    if candles_sorted:
        seed_ts = _to_int(candles_sorted[0].get("timestamp"), seed_ts)
    runtime_option_seed = state.pain_runtime.get_recent_options_for(
        seed_ts,
        limit=1,
    )
    last_option_seen = dict(runtime_option_seed[-1]) if runtime_option_seed else None

    for candle in candles_sorted:
        candle_ts = _to_int(candle.get("timestamp"), 0)
        matched_5m: List[Dict[str, Any]] = []
        while candle_5m_idx < len(candles_5m_sorted):
            candle_5m_ts = _to_int(candles_5m_sorted[candle_5m_idx].get("timestamp"), 0)
            if candle_5m_ts <= candle_ts:
                matched_5m.append(candles_5m_sorted[candle_5m_idx])
                candle_5m_idx += 1
                continue
            break
        matched_options: List[Dict[str, Any]] = []
        while option_idx < len(options_sorted):
            option_ts = _to_int(options_sorted[option_idx].get("timestamp"), 0)
            if option_ts <= candle_ts:
                matched_options.append(options_sorted[option_idx])
                option_idx += 1
                continue
            break
        if matched_options:
            last_option_seen = dict(matched_options[-1])
        latest = state.pain_runtime.ingest(
            candles=[candle],
            candles_5m=matched_5m,
            option_snapshots=matched_options,
        )
        state.paper_trade.on_candle(candle, latest, last_option_seen)

    if candle_5m_idx < len(candles_5m_sorted) or option_idx < len(options_sorted):
        pending_options = options_sorted[option_idx:]
        latest = state.pain_runtime.ingest(
            candles=[],
            candles_5m=candles_5m_sorted[candle_5m_idx:],
            option_snapshots=pending_options,
        )
    elif not candles_sorted and (candles_5m_sorted or options_sorted):
        latest = state.pain_runtime.ingest(
            candles=[],
            candles_5m=candles_5m_sorted,
            option_snapshots=options_sorted,
        )

    return latest


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
        open_ = _to_float(row.get("open"))
        high = _to_float(row.get("high"))
        low = _to_float(row.get("low"))
        close = _to_float(row.get("close"))
        volume = max(0.0, _to_float(row.get("volume")))
        if current is None or _to_int(current.get("timestamp"), 0) != bucket_start:
            if current is not None:
                result.append(current)
            current = {
                "timestamp": bucket_start,
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
    return result


def _next_closed_1m_candle(state: AppState, tick: Dict[str, float]) -> Optional[Dict[str, float]]:
    ts = _to_int(tick.get("timestamp"), 0)
    if ts <= 0:
        return None
    price = _to_float(tick.get("price"))
    volume = max(0.0, _to_float(tick.get("volume")))
    bucket_start = ts - (ts % 60)

    current = state.current_1m
    if current is None:
        state.current_1m = {
            "timestamp": bucket_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
        }
        return None

    if int(current["timestamp"]) == bucket_start:
        current["close"] = price
        current["high"] = max(float(current["high"]), price)
        current["low"] = min(float(current["low"]), price)
        current["volume"] = float(current["volume"]) + volume
        return None

    closed = dict(current)
    state.current_1m = {
        "timestamp": bucket_start,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
    }
    return closed


def _next_closed_5m_candle(state: AppState, tick: Dict[str, float]) -> Optional[Dict[str, float]]:
    ts = _to_int(tick.get("timestamp"), 0)
    if ts <= 0:
        return None
    price = _to_float(tick.get("price"))
    volume = max(0.0, _to_float(tick.get("volume")))
    bucket_start = ts - (ts % 300)

    current = state.current_5m
    if current is None:
        state.current_5m = {
            "timestamp": bucket_start,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume,
        }
        return None

    if int(current["timestamp"]) == bucket_start:
        current["close"] = price
        current["high"] = max(float(current["high"]), price)
        current["low"] = min(float(current["low"]), price)
        current["volume"] = float(current["volume"]) + volume
        return None

    closed = dict(current)
    state.current_5m = {
        "timestamp": bucket_start,
        "open": price,
        "high": price,
        "low": price,
        "close": price,
        "volume": volume,
    }
    return closed


def _option_snapshot_from_chain_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not payload or payload.get("code") != 200:
        return None
    data = payload.get("data") or {}
    chain = data.get("optionsChain") or []
    if not isinstance(chain, list) or not chain:
        return None

    ts = _to_int(data.get("timestamp"), int(time.time()))
    underlying = next((item for item in chain if not item.get("option_type")), None)
    spot = _to_float((underlying or {}).get("ltp"), _to_float((underlying or {}).get("fp")))

    # Derive the actual expiry date from the chain payload so holiday-shifted
    # expiries (e.g. monthly expiry moved from Tuesday to Monday) are correct.
    chain_expiry: Optional[str] = None
    expiry_data = data.get("expiryData")
    if isinstance(expiry_data, list) and expiry_data:
        first = expiry_data[0]
        if isinstance(first, dict):
            raw_date = str(first.get("date") or "").strip()   # "30-03-2026"
            raw_exp = first.get("expiry")                      # unix ts string
            if raw_date:
                chain_expiry = raw_date
            elif raw_exp:
                try:
                    from datetime import datetime as _dt
                    chain_expiry = _dt.fromtimestamp(int(raw_exp), _IST).strftime("%Y-%m-%d")
                except Exception:
                    pass

    strikes = [item for item in chain if item.get("option_type") in {"CE", "PE"}]
    strike_values = []
    for item in strikes:
        raw = item.get("strike_price")
        try:
            strike_values.append(float(raw))
        except (TypeError, ValueError):
            continue
    if not strike_values:
        return None

    if spot != 0.0:
        atm_strike = min(strike_values, key=lambda row: abs(row - spot))
    else:
        atm_strike = strike_values[len(strike_values) // 2]
    sorted_strikes = sorted(set(strike_values))
    strike_step = 0.0
    if len(sorted_strikes) >= 2:
        diffs = [sorted_strikes[idx + 1] - sorted_strikes[idx] for idx in range(len(sorted_strikes) - 1)]
        positives = [item for item in diffs if item > 0]
        if positives:
            strike_step = min(positives)

    band_steps = 5.0
    band_points = float(strike_step * band_steps) if strike_step > 0.0 else 250.0
    band_points = max(50.0, band_points)
    band_rows = [
        row
        for row in strikes
        if abs(_to_float(row.get("strike_price")) - float(atm_strike)) <= band_points
    ]
    ce_rows = [row for row in band_rows if row.get("option_type") == "CE"]
    pe_rows = [row for row in band_rows if row.get("option_type") == "PE"]
    total_volume = sum(_to_float(row.get("volume")) for row in ce_rows + pe_rows)
    near_volume = total_volume

    atm_ce = next((row for row in ce_rows if _to_float(row.get("strike_price")) == float(atm_strike)), None)
    atm_pe = next((row for row in pe_rows if _to_float(row.get("strike_price")) == float(atm_strike)), None)
    compact_strikes: List[Dict[str, Any]] = []
    for row in band_rows:
        strike = _to_float(row.get("strike_price"), 0.0)
        option_type = str(row.get("option_type", "")).upper()
        if strike <= 0.0 or option_type not in {"CE", "PE"}:
            continue
        contract = extract_option_contract(
            row,
            snapshot_ts=ts,
            explicit_option_type=option_type,
            explicit_strike=strike,
            explicit_expiry=chain_expiry or None,
        )
        compact_strikes.append(
            {
                "option_type": option_type,
                "strike": float(strike),
                "symbol": str(contract.get("symbol") or ""),
                "symbol_token": str(contract.get("symbol_token") or ""),
                "exchange": str(contract.get("exchange") or ""),
                "underlying": str(contract.get("underlying") or ""),
                "expiry_date": str(contract.get("expiry_date") or ""),
                "expiry_ts": _to_int(contract.get("expiry_ts"), 0),
                "expiry_kind": str(contract.get("expiry_kind") or ""),
                "contract_key": str(contract.get("contract_key") or ""),
                "ltp": _to_float(row.get("ltp"), 0.0),
                "volume": _to_float(row.get("volume"), 0.0),
                "oi_change": _to_float(row.get("oich"), 0.0),
            }
        )
    return {
        "timestamp": ts,
        "snapshot_id": _new_snapshot_id(ts),
        "spot_price": float(spot),
        "strike_step": float(strike_step),
        "atm_strike": atm_strike,
        "atm_ce_ltp": _to_float((atm_ce or {}).get("ltp")),
        "atm_pe_ltp": _to_float((atm_pe or {}).get("ltp")),
        "atm_ce_volume": _to_float((atm_ce or {}).get("volume")),
        "atm_pe_volume": _to_float((atm_pe or {}).get("volume")),
        "atm_ce_oi_change": _to_float((atm_ce or {}).get("oich")),
        "atm_pe_oi_change": _to_float((atm_pe or {}).get("oich")),
        "band_volume_near_atm": near_volume,
        "band_volume_total": total_volume,
        "band_steps": int(band_steps),
        "strikes": compact_strikes,
    }


async def process_tick(state: AppState, tick: Dict[str, float]) -> None:
    ts = _to_int(tick.get("timestamp"), 0)
    if ts <= 0:
        return
    price = _to_float(tick.get("price"))
    now_ts = int(time.time())
    max_ts = _max_allowed_timestamp(now_ts, state.settings.max_future_timestamp_sec)
    if ts > max_ts:
        logger.warning(
            "Dropping future tick ts=%s (now=%s, max_allowed=%s)",
            ts,
            now_ts,
            max_ts,
        )
        return
    state.last_underlying_data_received_at = now_ts
    state.last_underlying_source_ts = ts
    state.last_tick = {
        "timestamp": ts,
        "price": price,
        "volume": max(0.0, _to_float(tick.get("volume"))),
    }

    before_cutoff_state = state.paper_trade.get_state()
    after_cutoff_state = state.paper_trade.enforce_time_cutoffs(ts=ts, mark_price=price)
    state.live_execution.observe_transition(before_cutoff_state, after_cutoff_state)

    if state.settings.market_hours_only and not _is_nse_market_hours(ts):
        return

    closed_1m = _next_closed_1m_candle(state, tick)
    closed_5m = _next_closed_5m_candle(state, tick)
    if closed_1m is None and closed_5m is None:
        return
    before_paper_state = state.paper_trade.get_state()
    await asyncio.to_thread(
        state.candle_store.persist_batch,
        candles_1m=[closed_1m] if closed_1m is not None else [],
        candles_5m=[closed_5m] if closed_5m is not None else [],
    )
    _ingest_runtime(
        state,
        candles=[closed_1m] if closed_1m is not None else [],
        candles_5m=[closed_5m] if closed_5m is not None else [],
        option_snapshots=[],
    )
    after_paper_state = state.paper_trade.get_state()
    state.live_execution.observe_transition(before_paper_state, after_paper_state)


async def stream_loop(state: AppState) -> None:
    while True:
        try:
            if state.settings.market_hours_only:
                wait_sec = _seconds_until_next_nse_market_open()
                if wait_sec > 0:
                    resume_at = datetime.now(_IST) + timedelta(seconds=wait_sec)
                    logger.info(
                        "FYERS stream paused outside NSE market hours; retrying at %s IST.",
                        resume_at.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    await asyncio.sleep(wait_sec)
                    continue
            token_generation = int(state.fyers_token_generation)
            token = str(state.settings.fyers_access_token or "").strip()
            if not token:
                await asyncio.sleep(5)
                continue
            stream = FyersTickStream(
                access_token=token,
                symbol=state.settings.fyers_symbol,
                data_type=state.settings.fyers_data_type,
                log_path=state.settings.fyers_log_path,
                lite_mode=state.settings.fyers_lite_mode,
                reconnect=state.settings.fyers_reconnect,
                volume_mode=state.settings.fyers_volume_mode,
                debug=state.settings.fyers_debug,
            )
            await stream.run(
                lambda tick: process_tick(state, tick),
                should_stop=lambda: (
                    int(state.fyers_token_generation) != token_generation
                    or (state.settings.market_hours_only and not _is_nse_market_hours(int(time.time())))
                ),
            )
            if int(state.fyers_token_generation) != token_generation:
                logger.info("Restarting FYERS stream with refreshed token.")
                await asyncio.sleep(0.2)
                continue
        except Exception as exc:
            logger.exception("Stream loop crashed: %s", exc)
            if _is_fyers_auth_error(exc):
                await _force_fyers_token_refresh(state, reason="stream auth error")
            await asyncio.sleep(5)


async def option_chain_loop(state: AppState) -> None:
    while True:
        try:
            if not state.settings.options_enabled:
                return
            if state.settings.market_hours_only:
                wait_sec = _seconds_until_next_nse_market_open()
                if wait_sec > 0:
                    resume_at = datetime.now(_IST) + timedelta(seconds=wait_sec)
                    logger.info(
                        "FYERS option poller paused outside NSE market hours; retrying at %s IST.",
                        resume_at.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                    await asyncio.sleep(wait_sec)
                    continue
            if not state.settings.fyers_client_id or not state.settings.fyers_access_token:
                await asyncio.sleep(5)
                continue
            token_generation = int(state.fyers_token_generation)
            poller = OptionChainPoller(
                client_id=state.settings.fyers_client_id,
                access_token=state.settings.fyers_access_token,
                symbol=state.settings.fyers_option_symbol,
                strikecount=state.settings.options_strikecount,
                poll_interval_sec=state.settings.options_poll_interval_sec,
                log_path=state.settings.fyers_log_path,
            )

            async def on_option(payload: Dict[str, Any]) -> None:
                if _is_fyers_auth_error(payload):
                    raise RuntimeError(f"FYERS option auth error: {_fyers_error_text(payload)}")
                if _to_int(payload.get("code"), 0) != 200:
                    logger.warning(
                        "FYERS option chain rejected payload code=%s message=%s",
                        payload.get("code"),
                        _fyers_error_text(payload),
                    )
                    return
                snapshot = _option_snapshot_from_chain_payload(payload)
                if snapshot:
                    snapshot = _ensure_snapshot_id(snapshot)
                    snapshot_ts = _to_int(snapshot.get("timestamp"), 0)
                    if (
                        state.settings.market_hours_only
                        and snapshot_ts > 0
                        and not _is_nse_market_hours(snapshot_ts)
                    ):
                        return
                    signature = _option_snapshot_signature(snapshot)
                    if state.last_option_signature is not None and signature == state.last_option_signature:
                        return
                    await asyncio.to_thread(state.option_store.persist_batch, [snapshot])
                    await asyncio.to_thread(state.option_strike_store.persist_batch, [snapshot])
                    state.pain_runtime.ingest(candles=[], candles_5m=[], option_snapshots=[snapshot])
                    state.last_option_signature = signature
                    state.last_option_ts = snapshot_ts

            await poller.run(
                on_option,
                should_stop=lambda: (
                    int(state.fyers_token_generation) != token_generation
                    or (state.settings.market_hours_only and not _is_nse_market_hours(int(time.time())))
                ),
            )
            if int(state.fyers_token_generation) != token_generation:
                logger.info("Restarting FYERS option poller with refreshed token.")
                await asyncio.sleep(0.2)
                continue
        except Exception as exc:
            logger.exception("Option chain loop crashed: %s", exc)
            if _is_fyers_auth_error(exc):
                await _force_fyers_token_refresh(state, reason="option chain auth error")
            await asyncio.sleep(5)


async def fyers_refresh_loop(state: AppState) -> None:
    force_interval_sec = 3600
    last_forced_ts = 0
    while True:
        try:
            if state.settings.stream_mode != "fyers":
                await asyncio.sleep(15)
                continue
            if not state.settings.fyers_refresh_enabled:
                await asyncio.sleep(15)
                continue
            status = await asyncio.to_thread(state.fyers_auth.get_status)
            has_refresh = bool(status.get("has_refresh_token"))
            refresh_valid = bool(status.get("refresh_valid"))
            access_valid = bool(status.get("authenticated"))
            needs_refresh = bool(status.get("needs_refresh"))
            if not has_refresh:
                state.settings.fyers_access_token = ""
                await asyncio.sleep(max(15, int(state.settings.fyers_refresh_check_sec)))
                continue
            if not refresh_valid:
                state.settings.fyers_access_token = ""
                logger.warning("FYERS refresh token is expired/invalid; re-authentication required.")
                await asyncio.sleep(max(15, int(state.settings.fyers_refresh_check_sec)))
                continue
            now_ts = int(time.time())
            force_now = bool(
                (not access_valid)
                or needs_refresh
                or last_forced_ts <= 0
                or now_ts - last_forced_ts >= force_interval_sec
            )
            refreshed = await asyncio.to_thread(state.fyers_auth.refresh_access_token, force=force_now)
            token = str(refreshed.get("access_token", "")).strip()
            if token:
                _apply_fyers_access_token(state, token, reason="refresh loop")
                if force_now:
                    last_forced_ts = now_ts
                    logger.info("FYERS access token refreshed via hourly force cycle.")
        except Exception as exc:
            logger.exception("FYERS token refresh loop error: %s", exc)
        # Retry aggressively when refresh token is valid but access token is missing/expired.
        try:
            after = await asyncio.to_thread(state.fyers_auth.get_status)
            urgent = bool(after.get("has_refresh_token") and after.get("refresh_valid") and not after.get("authenticated"))
        except Exception:
            urgent = False
        await asyncio.sleep(5 if urgent else max(15, int(state.settings.fyers_refresh_check_sec)))


async def paper_quote_loop(state: AppState) -> None:
    while True:
        sleep_sec = max(1, int(state.settings.paper_trade_quote_poll_sec))
        try:
            if not state.settings.paper_trade_enabled:
                await asyncio.sleep(sleep_sec)
                continue
            if state.settings.stream_mode != "fyers":
                await asyncio.sleep(sleep_sec)
                continue
            token = str(state.settings.fyers_access_token or "").strip()
            client_id = str(state.settings.fyers_client_id or "").strip()
            if not token or not client_id:
                await asyncio.sleep(sleep_sec)
                continue
            if state.quote_client is None:
                state.quote_client = FyersQuoteClient(
                    client_id=client_id,
                    access_token=token,
                    log_path=state.settings.fyers_log_path,
                )
            else:
                state.quote_client.set_access_token(token)

            target = state.paper_trade.get_active_option_quote_target()
            if not target:
                await asyncio.sleep(sleep_sec)
                continue
            symbol = str(target.get("symbol") or "").strip()
            if not symbol:
                await asyncio.sleep(sleep_sec)
                continue
            fyers_symbol = symbol if symbol.upper().startswith("NSE:") else f"NSE:{symbol}"
            quote = await asyncio.to_thread(state.quote_client.fetch_ltp, fyers_symbol)
            if quote:
                quote_symbol = str(quote.get("symbol") or symbol).strip()
                quote_ts = _to_int(quote.get("timestamp"), int(time.time()))
                if quote_symbol.upper() == symbol.upper():
                    state.last_option_quote_received_at = int(time.time())
                    state.last_option_quote_source_ts = quote_ts
                    state.last_option_quote_trade_id = _to_int(target.get("trade_id"), 0)
                state.paper_trade.update_option_mark(
                    symbol=quote_symbol,
                    ltp=_to_float(quote.get("ltp")),
                    quote_ts=quote_ts,
                )
        except Exception as exc:
            logger.exception("Paper quote loop error: %s", exc)
        await asyncio.sleep(sleep_sec)


async def sos_data_watchdog_loop(state: AppState) -> None:
    while True:
        await asyncio.sleep(_SOS_WATCHDOG_POLL_SEC)
        try:
            now_ts = int(time.time())
            if state.settings.market_hours_only and not _is_nse_market_hours(now_ts):
                continue

            paper_state = state.paper_trade.get_state()
            paper_active = paper_state.get("active_trade") if isinstance(paper_state.get("active_trade"), dict) else {}
            live_positions = state.live_execution.get_open_positions_snapshot()
            has_open_exposure = bool(paper_active) or bool(live_positions)
            if not has_open_exposure:
                continue

            paper_entry_ts = _to_int((paper_active or {}).get("entry_ts"), 0)
            live_entry_ts = max((_to_int(row.get("entry_ts"), 0) for row in live_positions.values()), default=0)
            underlying_reference = _to_int(state.last_underlying_data_received_at, 0)
            if underlying_reference <= 0:
                underlying_reference = max(int(state.startup_ts), paper_entry_ts, live_entry_ts)
            underlying_age = max(0, now_ts - underlying_reference)
            if (
                underlying_age >= _SOS_DATA_STALE_SEC
                and now_ts - _to_int(state.last_sos_underlying_trigger_at, 0) >= _SOS_RETRY_COOLDOWN_SEC
            ):
                state.last_sos_underlying_trigger_at = now_ts
                mark_price = _to_float((state.last_tick or {}).get("price"), 0.0)
                if paper_active:
                    closed_trade = state.paper_trade.force_close_active_trade(
                        reason=_SOS_UNDERLYING_CLOSE_REASON,
                        exit_ts=now_ts,
                        exit_price=mark_price,
                    )
                    if isinstance(closed_trade, dict):
                        logger.warning(
                            "SOS forced paper close trade_id=%s reason=%s underlying_age_sec=%s",
                            _to_int(closed_trade.get("trade_id"), 0),
                            _SOS_UNDERLYING_CLOSE_REASON,
                            underlying_age,
                        )
                if live_positions:
                    exit_result = await state.live_execution.force_exit_all(reason=_SOS_UNDERLYING_CLOSE_REASON)
                    logger.warning(
                        "SOS forced live exit trade_id=%s reason=%s underlying_age_sec=%s ok=%s remaining=%s",
                        _to_int(exit_result.get("trade_id"), 0),
                        _SOS_UNDERLYING_CLOSE_REASON,
                        underlying_age,
                        bool(exit_result.get("ok")),
                        _to_int(exit_result.get("remaining_open_positions"), 0),
                    )
                continue

            if not paper_active:
                continue

            trade_id = _to_int(paper_active.get("trade_id"), 0)
            # Start stale-quote protection only after the first option LTP has
            # actually been attached to this trade. Entry time alone is not a
            # valid freshness baseline because the quote loop initializes
            # option_entry_ltp asynchronously after the trade opens.
            option_reference = _to_int(paper_active.get("option_quote_ts"), 0)
            if option_reference <= 0:
                if (
                    trade_id > 0
                    and _to_int(state.last_option_quote_trade_id, 0) == trade_id
                    and _to_int(state.last_option_quote_received_at, 0) > 0
                ):
                    option_reference = _to_int(state.last_option_quote_received_at, 0)
            if option_reference <= 0:
                continue
            option_age = max(0, now_ts - option_reference)
            if (
                option_age >= _SOS_DATA_STALE_SEC
                and now_ts - _to_int(state.last_sos_option_trigger_at, 0) >= _SOS_RETRY_COOLDOWN_SEC
            ):
                state.last_sos_option_trigger_at = now_ts
                mark_price = _to_float((state.last_tick or {}).get("price"), 0.0)
                closed_trade = state.paper_trade.force_close_active_trade(
                    reason=_SOS_OPTION_QUOTE_CLOSE_REASON,
                    exit_ts=now_ts,
                    exit_price=mark_price,
                )
                if isinstance(closed_trade, dict):
                    logger.warning(
                        "SOS forced paper close trade_id=%s reason=%s option_age_sec=%s",
                        _to_int(closed_trade.get("trade_id"), 0),
                        _SOS_OPTION_QUOTE_CLOSE_REASON,
                        option_age,
                    )
                if live_positions:
                    exit_result = await state.live_execution.force_exit_all(reason=_SOS_OPTION_QUOTE_CLOSE_REASON)
                    logger.warning(
                        "SOS forced live exit trade_id=%s reason=%s option_age_sec=%s ok=%s remaining=%s",
                        _to_int(exit_result.get("trade_id"), 0),
                        _SOS_OPTION_QUOTE_CLOSE_REASON,
                        option_age,
                        bool(exit_result.get("ok")),
                        _to_int(exit_result.get("remaining_open_positions"), 0),
                    )
        except Exception as exc:
            logger.exception("SOS watchdog loop error: %s", exc)


def _parse_retrain_daily_time(value: str) -> Optional[dt_time]:
    text = str(value or "").strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) != 2:
        logger.warning(
            "Invalid MODEL_AUTO_RETRAIN_DAILY_TIME=%r. Expected HH:MM in 24h format. Falling back to interval mode.",
            text,
        )
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        logger.warning(
            "Invalid MODEL_AUTO_RETRAIN_DAILY_TIME=%r. Expected HH:MM in 24h format. Falling back to interval mode.",
            text,
        )
        return None
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        logger.warning(
            "Invalid MODEL_AUTO_RETRAIN_DAILY_TIME=%r. Expected HH:MM in 24h format. Falling back to interval mode.",
            text,
        )
        return None
    return dt_time(hour=hour, minute=minute)


def _seconds_to_next_daily_retrain(target: dt_time) -> int:
    now = datetime.now(_IST)
    next_run = now.replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)
    if next_run <= now:
        next_run = next_run + timedelta(days=1)
    return max(1, int((next_run - now).total_seconds()))


async def model_retrain_loop(state: AppState) -> None:
    retrainer = state.retrainer
    if not retrainer.enabled:
        return
    daily_time = _parse_retrain_daily_time(state.settings.model_auto_retrain_daily_time)
    if state.settings.model_auto_retrain_run_on_start:
        try:
            await retrainer.run_once(reason="startup", force=False)
        except Exception as exc:
            logger.exception("Auto retrain startup run failed: %s", exc)
    while True:
        if daily_time is not None:
            sleep_sec = _seconds_to_next_daily_retrain(daily_time)
            await asyncio.sleep(sleep_sec)
        else:
            await asyncio.sleep(retrainer.interval_sec)
        try:
            await retrainer.run_once(reason="daily_scheduled" if daily_time is not None else "scheduled", force=False)
        except Exception as exc:
            logger.exception("Auto retrain scheduled run failed: %s", exc)


async def db_retention_loop(state: AppState) -> None:
    retention = state.retention
    if not retention.enabled:
        return
    if retention.run_on_start:
        try:
            await asyncio.to_thread(retention.run_once, reason="startup")
        except Exception as exc:
            logger.exception("DB retention startup run failed: %s", exc)
    while True:
        await asyncio.sleep(max(300, int(retention.interval_sec)))
        try:
            await asyncio.to_thread(retention.run_once, reason="scheduled")
        except Exception as exc:
            logger.exception("DB retention scheduled run failed: %s", exc)


def _require_user(state: AppState, authorization: Optional[str]) -> Dict[str, Any]:
    user = state.user_auth.get_current_user(authorization or "")
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid or expired auth token")
    return user


def _require_admin(state: AppState, authorization: Optional[str]) -> Dict[str, Any]:
    user = _require_user(state, authorization)
    role = str(user.get("role") or "").strip().lower()
    if role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="admin access required")
    return user


def _authorization_from_request(authorization: Optional[str], request: Optional[Request]) -> str:
    header = str(authorization or "").strip()
    if header:
        return header
    if request is None:
        return ""
    token = str(request.cookies.get("pta_session", "")).strip()
    if token:
        return f"Bearer {token}"
    return ""


def _urlsafe_b64_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _urlsafe_b64_decode(text: str) -> bytes:
    value = str(text or "").strip()
    if not value:
        return b""
    padding = "=" * ((4 - (len(value) % 4)) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def _normalize_base32_secret(secret: str) -> str:
    text = re.sub(r"\s+", "", str(secret or "").strip()).upper()
    padding = "=" * ((8 - (len(text) % 8)) % 8)
    return text + padding


def _generate_totp_code(secret: str, *, period: int = 30, digits: int = 6) -> str:
    key = base64.b32decode(_normalize_base32_secret(secret), casefold=True)
    counter = int(time.time() // max(1, int(period)))
    message = struct.pack(">Q", counter)
    digest = hmac.new(key, message, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    code = struct.unpack(">I", digest[offset : offset + 4])[0] & 0x7FFFFFFF
    return str(code % (10**digits)).zfill(digits)


def _build_angel_state(username: str, secret: str, ttl_sec: int = 900) -> str:
    uname = str(username or "").strip().lower()
    if not uname:
        return ""
    payload = {
        "u": uname,
        "exp": int(time.time()) + max(60, int(ttl_sec or 900)),
        "n": secrets.token_urlsafe(12),
    }
    body = _urlsafe_b64_no_pad(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(str(secret or "").encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    return f"{body}.{_urlsafe_b64_no_pad(signature)}"


def _parse_angel_state(token: str, secret: str) -> str:
    value = str(token or "").strip()
    if not value or "." not in value:
        return ""
    body, sig = value.split(".", 1)
    if not body or not sig:
        return ""
    expected_sig = hmac.new(str(secret or "").encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()
    provided_sig = _urlsafe_b64_decode(sig)
    if not provided_sig or not hmac.compare_digest(expected_sig, provided_sig):
        return ""
    try:
        payload = json.loads(_urlsafe_b64_decode(body).decode("utf-8"))
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    exp = _to_int(payload.get("exp"), 0)
    if exp <= int(time.time()):
        return ""
    return str(payload.get("u") or "").strip().lower()


def _resolve_angel_redirect_url(settings: Settings, request: Request) -> str:
    configured = str(settings.angel_redirect_url or "").strip()
    if configured:
        return configured
    app_host = str(settings.app_host or "").strip().rstrip("/")
    if app_host:
        return f"{app_host}/auth/angel/callback"
    scheme = str(request.url.scheme or "http")
    host = str(request.headers.get("host", "")).strip()
    if host:
        return f"{scheme}://{host}/auth/angel/callback"
    return "http://localhost:8000/auth/angel/callback"


def _append_query(base_url: str, params: Dict[str, str]) -> str:
    split = urlsplit(base_url)
    existing = parse_qs(split.query, keep_blank_values=True)
    for key, value in params.items():
        existing[str(key)] = [str(value)]
    pairs: List[tuple[str, str]] = []
    for key, values in existing.items():
        for value in values:
            pairs.append((str(key), str(value)))
    query = urlencode(pairs)
    return urlunsplit((split.scheme, split.netloc, split.path, query, split.fragment))


def _parse_angel_callback_query(request: Request) -> Dict[str, Any]:
    raw_query = str(request.url.query or "")
    parsed = parse_qs(raw_query, keep_blank_values=True)

    def pick(*names: str) -> str:
        for name in names:
            values = parsed.get(name)
            if not values:
                continue
            for value in values:
                text = str(value or "").strip()
                if text:
                    return text
        return ""

    return {
        "auth_token": pick("auth_token"),
        "feed_token": pick("feed_token"),
        "refresh_token": pick("refresh_token"),
        "state": pick("state"),
        "raw": {
            "clientcode": pick("clientcode"),
            "client_id": pick("clientId", "client_id"),
            "username": pick("username"),
            "user_id": pick("userId", "user_id"),
        },
    }


def _jwt_payload(token: str) -> Dict[str, Any]:
    parts = str(token or "").split(".")
    if len(parts) < 2:
        return {}
    try:
        decoded = _urlsafe_b64_decode(parts[1]).decode("utf-8")
        data = json.loads(decoded)
        if isinstance(data, dict):
            return data
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return {}


def _extract_angel_client_id(decoded: Dict[str, Any], raw: Dict[str, Any]) -> str:
    candidates = [
        decoded.get("username"),
        decoded.get("clientcode"),
        decoded.get("clientCode"),
        decoded.get("clientId"),
        decoded.get("clientID"),
        decoded.get("client_id"),
        decoded.get("userId"),
        decoded.get("userID"),
        decoded.get("user_id"),
        raw.get("clientcode"),
        raw.get("client_id"),
        raw.get("username"),
        raw.get("user_id"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _parse_angel_trade_timestamp(row: Dict[str, Any]) -> int:
    candidates = [
        row.get("filltime"),
        row.get("fill_time"),
        row.get("trade_time"),
        row.get("tradetime"),
        row.get("updatetime"),
        row.get("exchange_time"),
        row.get("timestamp"),
    ]
    for value in candidates:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            ts = _to_int(value, 0)
            if ts > 1_000_000_000_000:
                ts = int(ts / 1000)
            if ts > 0:
                return ts
        text = str(value).strip()
        if not text:
            continue
        if text.isdigit():
            ts_num = _to_int(text, 0)
            if ts_num > 1_000_000_000_000:
                ts_num = int(ts_num / 1000)
            if ts_num > 0:
                return ts_num
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%d-%b-%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ):
            try:
                dt = datetime.strptime(text, fmt).replace(tzinfo=_IST)
                return int(dt.timestamp())
            except ValueError:
                pass
        try:
            iso = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if iso.tzinfo is None:
                iso = iso.replace(tzinfo=_IST)
            return int(iso.timestamp())
        except ValueError:
            pass
    return 0


def _load_runtime_preload_candles(state: AppState) -> List[Dict[str, float]]:
    settings = state.settings
    try:
        table_1m = _validate_identifier(settings.runtime_preload_table_1m, "RUNTIME_PRELOAD_TABLE_1M")
        database = _validate_identifier(settings.auth_mysql_database, "AUTH_MYSQL_DATABASE")
    except ValueError as exc:
        logger.warning("Runtime preload skipped: %s", exc)
        return []

    try:
        import mysql.connector  # type: ignore
    except ImportError:
        logger.warning("Runtime preload skipped: mysql-connector-python is not installed")
        return []

    max_rows = max(30, min(5000, int(settings.runtime_preload_candles)))
    conn = None
    cur = None
    rows: List[Dict[str, Any]] = []
    try:
        conn_kwargs = mysql_connect_kwargs(settings, with_database=True, autocommit=True)
        conn_kwargs["database"] = database
        conn = mysql.connector.connect(**conn_kwargs)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (database, table_1m),
        )
        found = cur.fetchall() or []
        if not found:
            logger.warning("Runtime preload skipped: table `%s`.`%s` does not exist", database, table_1m)
            return []

        columns: Dict[str, str] = {}
        for row in found:
            col = str(row.get("COLUMN_NAME", "")).strip()
            if col:
                columns[col.lower()] = col

        def _pick(*names: str) -> str:
            for name in names:
                if name in columns:
                    return columns[name]
            return ""

        ts_col = _pick("timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time")
        open_col = _pick("open", "o")
        high_col = _pick("high", "h")
        low_col = _pick("low", "l")
        close_col = _pick("close", "c")
        volume_col = _pick("volume", "vol", "v")

        missing = []
        if not ts_col:
            missing.append("timestamp")
        if not open_col:
            missing.append("open")
        if not high_col:
            missing.append("high")
        if not low_col:
            missing.append("low")
        if not close_col:
            missing.append("close")
        if not volume_col:
            missing.append("volume")
        if missing:
            logger.warning(
                "Runtime preload skipped: missing columns %s in `%s`.`%s`",
                ",".join(missing),
                database,
                table_1m,
            )
            return []

        sql = (
            f"SELECT `{ts_col}` AS ts, `{open_col}` AS o, `{high_col}` AS h, `{low_col}` AS l, "
            f"`{close_col}` AS c, `{volume_col}` AS v "
            f"FROM `{table_1m}` ORDER BY `{ts_col}` DESC LIMIT %s"
        )
        cur.execute(sql, (max_rows,))
        rows = cur.fetchall() or []
    except Exception as exc:
        logger.warning("Runtime preload skipped due to database error: %s", exc)
        return []
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    candles: List[Dict[str, float]] = []
    last_ts = 0
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    for row in reversed(rows):
        ts = _normalize_epoch_seconds(row.get("ts"))
        if ts <= 0:
            continue
        if ts > max_ts:
            continue
        if ts == last_ts:
            continue
        candle = {
            "timestamp": ts,
            "open": _to_float(row.get("o")),
            "high": _to_float(row.get("h")),
            "low": _to_float(row.get("l")),
            "close": _to_float(row.get("c")),
            "volume": max(0.0, _to_float(row.get("v"))),
        }
        candles.append(candle)
        last_ts = ts
    return candles


def _load_runtime_preload_options(state: AppState) -> List[Dict[str, float]]:
    rows = state.option_store.fetch_recent(limit=state.settings.runtime_preload_candles)
    if not rows:
        return []
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    return [row for row in rows if _to_int(row.get("timestamp"), 0) <= max_ts]


def _preload_runtime_from_db(state: AppState) -> None:
    if not state.settings.runtime_preload_enabled:
        return

    candles = _load_runtime_preload_candles(state)
    if not candles:
        return
    candles_5m = _aggregate_5m_from_1m(candles)
    options = _load_runtime_preload_options(state)

    # Preload only AI runtime context. Do not replay paper-trade entries on restart.
    snapshot = state.pain_runtime.ingest(candles=candles, candles_5m=candles_5m, option_snapshots=options)
    first_ts = int(candles[0]["timestamp"])
    last_ts = int(candles[-1]["timestamp"])
    logger.info(
        "Runtime preloaded 1m=%d candles, 5m=%d candles, options=%d from `%s` (%d -> %d), state_ts=%s",
        len(candles),
        len(candles_5m),
        len(options),
        state.settings.runtime_preload_table_1m,
        first_ts,
        last_ts,
        snapshot.get("timestamp"),
    )


async def _ensure_backtest_store(state: AppState) -> Optional[MySQLBacktestResultStore]:
    if state.backtest_store is not None:
        return state.backtest_store
    if bool(state.backtest_store_init_attempted):
        return None
    state.backtest_store_init_attempted = True
    try:
        state.backtest_store = await asyncio.to_thread(MySQLBacktestResultStore, state.settings)
    except Exception as exc:
        logger.warning("Backtest result persistence disabled: %s", exc)
        state.backtest_store = None
    return state.backtest_store


load_dotenv()
app = FastAPI(title="Pain Theory AI", version="1.0.0")
app.state.state = AppState()
_FRONTEND_DIR = Path(__file__).resolve().parent / "frontend"
_FRONTEND_STATIC_DIR = _FRONTEND_DIR / "static"
_FRONTEND_INDEX = _FRONTEND_DIR / "index.html"
_FRONTEND_APP = _FRONTEND_DIR / "app.html"
_FRONTEND_DASHBOARD = _FRONTEND_DIR / "dashboard.html"

if _FRONTEND_STATIC_DIR.exists():
    app.mount("/ui/static", StaticFiles(directory=str(_FRONTEND_STATIC_DIR)), name="ui-static")


@app.on_event("startup")
async def startup_event() -> None:
    state: AppState = app.state.state
    _preload_runtime_from_db(state)
    if state.live_execution.enabled:
        if not str(state.settings.angel_api_key or "").strip():
            logger.warning("Central live execution is enabled without a global ANGEL_API_KEY. Connected users must have saved Angel API keys.")
        else:
            logger.warning("Central live execution is ENABLED. Live orders will fan out to connected Angel users.")
    if state.settings.stream_mode == "fyers" and state.settings.fyers_refresh_enabled:
        await _fyers_startup_token_bootstrap(state, max_wait_sec=60)
    if state.settings.stream_mode == "fyers":
        asyncio.create_task(stream_loop(state))
        asyncio.create_task(option_chain_loop(state))
        asyncio.create_task(fyers_refresh_loop(state))
        asyncio.create_task(paper_quote_loop(state))
        if state.paper_trade.enabled or state.live_execution.enabled:
            asyncio.create_task(sos_data_watchdog_loop(state))
    if state.settings.model_auto_retrain_enabled:
        asyncio.create_task(model_retrain_loop(state))
    if state.settings.db_retention_enabled:
        asyncio.create_task(db_retention_loop(state))


@app.get("/health")
async def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
async def ready(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    return _system_ready_payload(state)


@app.get("/")
async def root() -> Any:
    if _FRONTEND_INDEX.exists():
        return FileResponse(str(_FRONTEND_INDEX))
    return {
        "service": "Pain Theory AI",
        "status": "ok",
        "endpoints": [
            "/health",
            "/ready",
            "/ingest",
            "/state",
            "/market/candles",
            "/market/options",
            "/market/option-strikes",
            "/backtest/run",
            "/backtest/progress",
            "/backtest/latest",
            "/paper/state",
            "/paper/trades",
            "/ml/retrain/status",
            "/ml/retrain/trigger",
            "/docs",
        ],
    }


@app.get("/ui")
async def ui(request: Request, authorization: Optional[str] = Header(default=None)) -> Any:
    state: AppState = app.state.state
    if state.user_auth.get_current_user(_authorization_from_request(authorization, request)) is None:
        return RedirectResponse(url="/", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
    if _FRONTEND_APP.exists():
        return FileResponse(str(_FRONTEND_APP))
    if _FRONTEND_DASHBOARD.exists():
        return FileResponse(str(_FRONTEND_DASHBOARD))
    return {
        "service": "Pain Theory AI",
        "status": "ok",
        "endpoints": [
            "/health",
            "/ready",
            "/ingest",
            "/state",
            "/market/candles",
            "/market/options",
            "/market/option-strikes",
            "/backtest/run",
            "/backtest/progress",
            "/backtest/latest",
            "/paper/state",
            "/paper/trades",
            "/ml/retrain/status",
            "/ml/retrain/trigger",
            "/docs",
        ],
    }


@app.get("/ui/admin")
async def ui_admin(request: Request, authorization: Optional[str] = Header(default=None)) -> Any:
    state: AppState = app.state.state
    auth_value = _authorization_from_request(authorization, request)
    try:
        _require_admin(state, auth_value)
    except HTTPException:
        return RedirectResponse(url="/", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
    if _FRONTEND_DASHBOARD.exists():
        return FileResponse(str(_FRONTEND_DASHBOARD))
    return {
        "service": "Pain Theory AI",
        "status": "ok",
        "endpoints": [
            "/health",
            "/ready",
            "/ingest",
            "/state",
            "/market/candles",
            "/market/options",
            "/market/option-strikes",
            "/backtest/run",
            "/backtest/progress",
            "/backtest/latest",
            "/paper/state",
            "/paper/trades",
            "/ml/retrain/status",
            "/ml/retrain/trigger",
            "/docs",
        ],
    }


@app.post("/ingest")
async def ingest(payload: IngestPayload) -> Dict[str, Any]:
    state: AppState = app.state.state
    if not payload.candles:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="candles is required")
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    candles_1m = [row.dict() for row in payload.candles if _to_int(row.timestamp, 0) <= max_ts]
    if not candles_1m:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="all candles are future-dated; check timestamp source",
        )
    candles_5m = _aggregate_5m_from_1m(candles_1m)
    option_rows = [
        _ensure_snapshot_id(row.dict())
        for row in payload.option_snapshots
        if _to_int(row.timestamp, 0) <= max_ts
    ]
    await asyncio.to_thread(state.candle_store.persist_batch, candles_1m=candles_1m, candles_5m=candles_5m)
    await asyncio.to_thread(state.option_store.persist_batch, option_rows)
    await asyncio.to_thread(state.option_strike_store.persist_batch, option_rows)
    snapshot = _ingest_runtime(
        state=state,
        candles=candles_1m,
        candles_5m=candles_5m,
        option_snapshots=option_rows,
    )
    return _pain_state_response(snapshot)


@app.get("/state")
async def current_state() -> Dict[str, Any]:
    state: AppState = app.state.state
    return _pain_state_response(state.pain_runtime.get_state())


@app.get("/state/raw")
async def raw_state() -> Dict[str, Any]:
    state: AppState = app.state.state
    return state.pain_runtime.get_state()


@app.get("/market/candles")
async def market_candles(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    limit: int = 500,
    timeframe: str = "both",
    since: int = 0,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    max_rows = max(60, min(int(limit), 2000))
    timeframe_key = str(timeframe or "both").strip().lower()
    since_ts = max(0, int(since or 0))
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    raw_state_obj = state.pain_runtime.get_state()
    try:
        rows_1m = (
            state.candle_store.fetch_recent(timeframe="1m", limit=max_rows, since_ts=since_ts)
            if timeframe_key in {"both", "1m"}
            else []
        )
        rows_5m = (
            state.candle_store.fetch_recent(timeframe="5m", limit=max_rows, since_ts=since_ts)
            if timeframe_key in {"both", "5m"}
            else []
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=f"database candles unavailable: {exc}") from exc
    rows_1m = [row for row in rows_1m if _to_int(row.get("timestamp"), 0) <= max_ts]
    rows_5m = [row for row in rows_5m if _to_int(row.get("timestamp"), 0) <= max_ts]
    return {
        "analysis_timeframe": str(raw_state_obj.get("analysis_timeframe", "5m")),
        "execution_timeframe": str(raw_state_obj.get("execution_timeframe", "1m")),
        "candles_1m": rows_1m,
        "candles_5m": rows_5m,
        "current_1m": dict(state.current_1m) if state.current_1m else None,
        "last_tick": dict(state.last_tick) if state.last_tick else None,
    }


@app.get("/market/options")
async def market_options(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    limit: int = 300,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    max_rows = max(20, min(int(limit), 2000))
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    rows = [
        row
        for row in state.option_store.fetch_recent(limit=max_rows)
        if _to_int(row.get("timestamp"), 0) <= max_ts
    ]
    return {"count": len(rows), "options": rows}


@app.get("/market/option-strikes")
async def market_option_strikes(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    snapshot_id: str = "",
    limit: int = 600,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    max_rows = max(20, min(int(limit), 5000))
    max_ts = _max_allowed_timestamp(future_slack_sec=state.settings.max_future_timestamp_sec)
    selected_snapshot_id = str(snapshot_id or "").strip()
    if not selected_snapshot_id:
        latest = state.option_store.fetch_recent(limit=1)
        if latest:
            selected_snapshot_id = str(latest[-1].get("snapshot_id") or "").strip()
    rows = state.option_strike_store.fetch_by_snapshot(selected_snapshot_id, limit=max_rows)
    rows = [row for row in rows if _to_int(row.get("timestamp"), 0) <= max_ts]
    return {
        "snapshot_id": selected_snapshot_id,
        "count": len(rows),
        "strikes": rows,
    }


@app.post("/backtest/run")
async def backtest_run(
    payload: BacktestRunPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    await _ensure_backtest_store(state)

    start_ts = _normalize_epoch_seconds(payload.start)
    end_ts = _normalize_epoch_seconds(payload.end)
    if start_ts <= 0 or end_ts <= 0 or end_ts <= start_ts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid backtest window. Use valid start/end timestamps or ISO datetime strings.",
        )
    max_candles = max(500, min(int(payload.max_candles or 0), 500000))
    max_trades = max(100, min(int(payload.max_trades or 0), 5000))
    # Keep request field for backward compatibility, but enforce no release gating.
    require_pain_release = False

    if state.backtest_task is not None and not state.backtest_task.done():
        active = state.backtest_jobs.get(state.backtest_active_job_id) if state.backtest_active_job_id else None
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Backtest already running: {state.backtest_active_job_id or 'active_job'}",
        )

    loop = asyncio.get_running_loop()
    job_id = _new_backtest_job_id()
    now_ts = int(time.time())
    state.backtest_jobs[job_id] = {
        "job_id": job_id,
        "status": "running",
        "progress_pct": 0.0,
        "processed": 0,
        "total": 100,
        "message": "queued",
        "started_at": now_ts,
        "completed_at": 0,
        "error": "",
        "result": None,
        "params": {
            "start_ts": start_ts,
            "end_ts": end_ts,
            "max_candles": max_candles,
            "max_trades": max_trades,
            "close_open_at_end": bool(payload.close_open_at_end),
            "require_pain_release": require_pain_release,
        },
    }
    state.backtest_active_job_id = job_id
    if len(state.backtest_jobs) > 12:
        keys_sorted = sorted(state.backtest_jobs.keys())
        for key in keys_sorted[:-12]:
            if key != state.backtest_active_job_id:
                state.backtest_jobs.pop(key, None)

    def on_progress(processed: int, total: int, message: str) -> None:
        safe_total = max(1, int(total))
        safe_processed = max(0, min(int(processed), safe_total))
        pct = round((safe_processed * 100.0) / float(safe_total), 2)

        def update() -> None:
            job = state.backtest_jobs.get(job_id)
            if not isinstance(job, dict):
                return
            job["status"] = "running"
            job["processed"] = safe_processed
            job["total"] = safe_total
            job["progress_pct"] = pct
            job["message"] = str(message or "running")

        loop.call_soon_threadsafe(update)

    async def _job_worker() -> None:
        try:
            runner = BacktestEngine(state.settings)
            result = await asyncio.to_thread(
                runner.run,
                start_ts=start_ts,
                end_ts=end_ts,
                max_candles=max_candles,
                max_trades=max_trades,
                close_open_at_end=bool(payload.close_open_at_end),
                require_pain_release=require_pain_release,
                progress_callback=on_progress,
            )
        except ValueError as exc:
            completed_at = int(time.time())
            job = state.backtest_jobs.get(job_id)
            if isinstance(job, dict):
                job["status"] = "failed"
                job["error"] = str(exc)
                job["message"] = "failed"
                job["completed_at"] = completed_at
                job["progress_pct"] = max(_to_float(job.get("progress_pct"), 0.0), 0.0)
            if state.backtest_store is not None:
                try:
                    await asyncio.to_thread(
                        state.backtest_store.save_run,
                        job_id=job_id,
                        started_at=now_ts,
                        completed_at=completed_at,
                        params=dict(state.backtest_jobs.get(job_id, {}).get("params") or {}),
                        result={},
                        status="failed",
                        error_message=str(exc),
                    )
                except Exception as store_exc:
                    logger.warning("Backtest failed run persistence skipped: %s", store_exc)
            return
        except Exception as exc:
            completed_at = int(time.time())
            job = state.backtest_jobs.get(job_id)
            if isinstance(job, dict):
                job["status"] = "failed"
                job["error"] = f"Backtest failed: {exc}"
                job["message"] = "failed"
                job["completed_at"] = completed_at
            if state.backtest_store is not None:
                try:
                    await asyncio.to_thread(
                        state.backtest_store.save_run,
                        job_id=job_id,
                        started_at=now_ts,
                        completed_at=completed_at,
                        params=dict(state.backtest_jobs.get(job_id, {}).get("params") or {}),
                        result={},
                        status="failed",
                        error_message=f"Backtest failed: {exc}",
                    )
                except Exception as store_exc:
                    logger.warning("Backtest failed run persistence skipped: %s", store_exc)
            return

        completed_at = int(time.time())
        persisted = False
        persisted_run_id = 0
        result_out = dict(result)
        if state.backtest_store is not None:
            try:
                persisted_run_id = await asyncio.to_thread(
                    state.backtest_store.save_run,
                    job_id=job_id,
                    started_at=now_ts,
                    completed_at=completed_at,
                    params=dict(state.backtest_jobs.get(job_id, {}).get("params") or {}),
                    result=result_out,
                    status="completed",
                    error_message="",
                )
                persisted = True
            except Exception as exc:
                logger.warning("Backtest persistence failed: %s", exc)
        result_out["storage"] = {
            "backend": "mysql" if state.backtest_store is not None else "memory",
            "persisted": bool(persisted),
            "run_id": int(persisted_run_id),
        }
        if persisted_run_id > 0:
            result_out["db_run_id"] = int(persisted_run_id)

        state.last_backtest = dict(result_out)
        job = state.backtest_jobs.get(job_id)
        if isinstance(job, dict):
            job["status"] = "completed"
            job["error"] = ""
            job["message"] = "completed"
            job["completed_at"] = completed_at
            job["progress_pct"] = 100.0
            job["processed"] = max(job.get("processed", 0), 100)
            job["total"] = max(job.get("total", 100), 100)
            job["result"] = dict(result_out)

    task = asyncio.create_task(_job_worker())
    state.backtest_task = task

    def _clear_active(_task: asyncio.Task) -> None:
        if state.backtest_active_job_id == job_id:
            state.backtest_active_job_id = ""
        if state.backtest_task is _task:
            state.backtest_task = None

    task.add_done_callback(_clear_active)
    return {"ok": True, "job": _backtest_public_status(state.backtest_jobs[job_id])}


@app.get("/backtest/progress")
async def backtest_progress(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    job_id: str = "",
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))

    target_job_id = str(job_id or "").strip()
    if not target_job_id:
        if state.backtest_active_job_id:
            target_job_id = state.backtest_active_job_id
        elif state.backtest_jobs:
            target_job_id = sorted(state.backtest_jobs.keys())[-1]
    job = state.backtest_jobs.get(target_job_id)
    if not isinstance(job, dict):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Backtest job not found.")
    return _backtest_public_status(job)


@app.get("/backtest/latest")
async def backtest_latest(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    await _ensure_backtest_store(state)
    if isinstance(state.last_backtest, dict):
        return dict(state.last_backtest)
    if state.backtest_store is not None:
        try:
            latest = await asyncio.to_thread(state.backtest_store.fetch_latest_completed)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Latest backtest load failed: {exc}",
            ) from exc
        if isinstance(latest, dict):
            state.last_backtest = dict(latest)
            return dict(latest)
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No backtest run found in current session or database.",
    )


@app.get("/paper/state")
async def paper_state(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    return state.paper_trade.get_state()


@app.get("/paper/trades")
async def paper_trades(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    limit: int = 200,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    rows = state.paper_trade.get_trades(limit=limit)
    return {"count": len(rows), "trades": rows}


@app.post("/paper/reset")
async def paper_reset(
    payload: PaperResetPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_user(state, _authorization_from_request(authorization, request))
    snapshot = state.paper_trade.reset(close_open=bool(payload.close_open))
    return {"ok": True, "paper": snapshot}


@app.get("/ml/retrain/status")
async def retrain_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    return state.retrainer.get_status()


@app.get("/ml/retrain/history")
async def retrain_history(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    limit: int = 60,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    max_rows = max(1, min(int(limit), 200))
    rows = await asyncio.to_thread(state.retrainer.get_history, max_rows)
    return {
        "ok": True,
        "count": len(rows),
        "history": rows,
    }


@app.get("/ml/retrain/mistakes/summary")
async def retrain_mistakes_summary(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    days: int = 30,
    limit: int = 5000,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    lookback_days = max(1, min(int(days), 365))
    max_rows = max(100, min(int(limit), 50000))
    summary = await asyncio.to_thread(
        state.paper_trade.get_mistake_summary,
        days=lookback_days,
        max_rows=max_rows,
    )
    return {
        "ok": True,
        "summary": summary,
    }


@app.post("/ml/retrain/trigger")
async def retrain_trigger(
    payload: RetrainTriggerPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    result = await state.retrainer.run_once(reason="manual", force=bool(payload.force))
    return {"ok": bool(result.get("ok")), "result": result}


@app.get("/maintenance/retention/status")
async def retention_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    return state.retention.get_status()


@app.post("/maintenance/retention/run")
async def retention_run(
    payload: RetentionTriggerPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    result = await asyncio.to_thread(state.retention.run_once, reason=str(payload.reason or "manual"))
    return {"ok": bool(result.get("ok")), "result": result}


@app.get("/admin/execution/live/status")
async def live_execution_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    return state.live_execution.get_status()


@app.get("/admin/execution/lot-size")
async def admin_execution_lot_size(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    lot_size_qty = await asyncio.to_thread(state.user_auth.get_global_lot_size_qty)
    return {
        "ok": True,
        "lot_size_qty": int(max(1, _to_int(lot_size_qty, 1))),
    }


@app.post("/admin/execution/lot-size")
async def admin_execution_lot_size_update(
    payload: LotSizePayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    try:
        saved = await asyncio.to_thread(state.user_auth.set_global_lot_size_qty, int(payload.lot_size_qty))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "ok": True,
        "lot_size_qty": int(_to_int(saved.get("lot_size_qty"), 1)),
        "updated_at": int(_to_int(saved.get("updated_at"), 0)),
    }


@app.get("/admin/execution/angel-api-hits")
async def admin_execution_angel_api_hits(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    date: str = "",
    limit: int = 500,
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))

    target_date_text = str(date or "").strip()
    today_ist = datetime.now(_IST).date()
    if target_date_text:
        try:
            target_day = datetime.strptime(target_date_text, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid date format. Use YYYY-MM-DD.") from exc
    else:
        target_day = today_ist
    if target_day > today_ist:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"date cannot be after {today_ist.isoformat()}.")

    start_ts = int(datetime.combine(target_day, dt_time(0, 0, 0), tzinfo=_IST).timestamp())
    end_ts = int(datetime.combine(target_day, dt_time(23, 59, 59), tzinfo=_IST).timestamp())
    max_rows = max(1, min(int(limit), 2000))

    rows = await asyncio.to_thread(
        state.angel_order_audit_store.get_hits_in_range,
        start_ts=start_ts,
        end_ts=end_ts,
        limit=max_rows,
    )
    summary = await asyncio.to_thread(
        state.angel_order_audit_store.get_summary_in_range,
        start_ts=start_ts,
        end_ts=end_ts,
    )

    out_rows: List[Dict[str, Any]] = []
    for row in rows:
        request_payload_raw = str(row.get("request_payload_json") or "").strip()
        response_payload_raw = str(row.get("response_json") or "").strip()
        try:
            request_payload = json.loads(request_payload_raw) if request_payload_raw else {}
        except Exception:
            request_payload = {"raw": request_payload_raw}
        try:
            response_payload = json.loads(response_payload_raw) if response_payload_raw else {}
        except Exception:
            response_payload = {"raw": response_payload_raw}
        request_ts_val = _to_int(row.get("request_ts"), 0)
        request_dt = datetime.fromtimestamp(request_ts_val, tz=_IST) if request_ts_val > 0 else None
        out_rows.append(
            {
                "id": _to_int(row.get("id"), 0),
                "request_ts": request_ts_val,
                "request_time_ist": request_dt.strftime("%Y-%m-%d %H:%M:%S") if request_dt else "--",
                "username": str(row.get("username") or "").strip().lower(),
                "client_id": str(row.get("client_id") or "").strip(),
                "trade_id": _to_int(row.get("trade_id"), 0),
                "phase": str(row.get("phase") or "").strip().lower(),
                "transaction_type": str(row.get("transaction_type") or "").strip().upper(),
                "symbol": str(row.get("symbol") or "").strip(),
                "symbol_token": str(row.get("symbol_token") or "").strip(),
                "exchange": str(row.get("exchange") or "").strip().upper(),
                "quantity": _to_int(row.get("quantity"), 0),
                "ok": bool(_to_int(row.get("ok"), 0)),
                "http_status": _to_int(row.get("http_status"), 0),
                "error_message": str(row.get("error_message") or "").strip(),
                "order_id": str(row.get("order_id") or "").strip(),
                "request_payload": request_payload,
                "response_payload": response_payload,
            }
        )

    return {
        "ok": True,
        "enabled": bool(state.angel_order_audit_store.enabled),
        "date": target_day.isoformat(),
        "today_ist": today_ist.isoformat(),
        "limit": int(max_rows),
        "summary": summary,
        "count": len(out_rows),
        "rows": out_rows,
    }


@app.post("/auth/signup")
async def signup(payload: SignupPayload) -> Dict[str, Any]:
    state: AppState = app.state.state
    try:
        user = state.user_auth.signup(
            full_name=payload.full_name,
            email=payload.email,
            mobile_number=payload.mobile_number,
            password=payload.password,
            confirm_password=payload.confirm_password,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "user": user}


@app.post("/auth/login")
async def login(payload: LoginPayload, response: Response) -> Dict[str, Any]:
    state: AppState = app.state.state
    identifier = str(payload.email or payload.username or "").strip()
    if not identifier:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="email is required")
    requested_role = str(payload.role or "user").strip().lower() or "user"
    if requested_role not in {"user", "admin"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="role must be user or admin")
    try:
        session = state.user_auth.login(
            email_or_username=identifier,
            password=payload.password,
            required_role=requested_role,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc)) from exc
    token = str(session.get("access_token", "")).strip()
    if token:
        response.set_cookie(
            key="pta_session",
            value=token,
            httponly=True,
            secure=False,
            samesite="lax",
            max_age=int(state.settings.auth_session_ttl_sec),
        )
    return session


@app.get("/auth/me")
async def auth_me(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    return {"authenticated": True, "user": user}


@app.get("/user/broker/angel-one")
async def user_broker_angel_one(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.get_user_broker_config(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/broker/angel-one")
async def user_broker_angel_one_update(
    payload: BrokerClientIdPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.set_user_broker_profile(
            username,
            client_id=payload.client_id,
            api_key=payload.api_key,
            pin=payload.pin,
            totp_secret=payload.totp_secret,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/broker/angel-one/login")
async def user_broker_angel_one_login(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        session = state.user_auth.get_user_angel_session(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    client_id = str(session.get("client_id") or "").strip()
    api_key = str(session.get("api_key") or state.settings.angel_api_key or "").strip()
    pin = str(session.get("pin") or "").strip()
    totp_secret = str(session.get("totp_secret") or "").strip()
    if not client_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set Angel client id first.")
    if not api_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set Angel API key first.")
    if not pin:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set Angel PIN first.")
    if not totp_secret:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set Angel TOTP secret first.")

    try:
        totp_code = _generate_totp_code(totp_secret)
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid Angel TOTP secret: {exc}") from exc

    payload = {
        "clientcode": client_id,
        "password": pin,
        "totp": totp_code,
        "state": f"terminal-{int(time.time())}",
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": str(state.settings.angel_client_local_ip or "127.0.0.1"),
        "X-ClientPublicIP": str(state.settings.angel_client_public_ip or "0.0.0.0"),
        "X-MACAddress": str(state.settings.angel_client_mac or "00:00:00:00:00:00"),
        "X-PrivateKey": api_key,
    }
    try:
        resp = await asyncio.to_thread(
            requests.post,
            "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword",
            json=payload,
            headers=headers,
            timeout=15,
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Angel terminal login request failed: {exc}") from exc

    try:
        body = resp.json() if resp.content else {}
    except ValueError:
        body = {}
    if int(getattr(resp, "status_code", 500)) >= 400:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Angel terminal login HTTP {resp.status_code}.")
    if isinstance(body, dict) and body.get("status") is False:
        message = str(body.get("message") or body.get("errorcode") or "Angel login failed").strip()
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)

    data = body.get("data") if isinstance(body, dict) else {}
    access_token = str((data or {}).get("jwtToken") or "").strip()
    refresh_token = str((data or {}).get("refreshToken") or "").strip()
    feed_token = str((data or {}).get("feedToken") or "").strip()
    if not access_token:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Angel login succeeded but jwtToken was missing.")

    decoded = _jwt_payload(access_token)
    token_expires_at = _to_int(decoded.get("exp"), 0)
    try:
        state.user_auth.save_user_angel_session(
            username=username,
            client_id=client_id,
            auth_token="",
            access_token=access_token,
            feed_token=feed_token,
            refresh_token=refresh_token,
            token_expires_at=token_expires_at,
        )
        config = state.user_auth.get_user_broker_config(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/broker/angel-one/disconnect")
async def user_broker_angel_one_disconnect(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.clear_user_angel_session(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.get("/user/trading-engine")
async def user_trading_engine_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.get_user_trading_engine_config(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/trading-engine")
async def user_trading_engine_update(
    payload: TradingEnginePayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.set_user_trading_engine_enabled(username, bool(payload.enabled))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.get("/user/lots")
async def user_lot_config(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = await asyncio.to_thread(state.user_auth.get_user_lot_config, username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/lots")
async def user_lot_config_update(
    payload: UserLotPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = await asyncio.to_thread(state.user_auth.set_user_lot_count, username, int(payload.lot_count))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"ok": True, "config": config}


@app.post("/user/subscription/razorpay/order")
async def user_subscription_razorpay_order(
    payload: RazorpayOrderPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    key_id = str(state.settings.razorpay_key_id or "").strip()
    key_secret = str(state.settings.razorpay_key_secret or "").strip()
    if not key_id or not key_secret:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Razorpay is not configured.")

    plan_code = str(payload.plan_code or "").strip().lower() or "monthly"
    selected_plan = _RAZORPAY_PLAN_CATALOG.get(plan_code)
    if not isinstance(selected_plan, dict):
        allowed = ", ".join(sorted(_RAZORPAY_PLAN_CATALOG.keys()))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid plan_code. Allowed: {allowed}.")
    plan_name = str(payload.plan_name or selected_plan.get("name") or state.settings.razorpay_subscribe_plan_name or "Sensible Algo Pro").strip()
    if not plan_name:
        plan_name = "Sensible Algo Pro"
    amount_paisa = max(100, _to_int(selected_plan.get("amount_paisa"), int(state.settings.razorpay_subscribe_amount_paisa)))
    currency = str(state.settings.razorpay_currency or "INR").strip().upper() or "INR"
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    receipt = f"sa_{int(time.time())}_{secrets.token_hex(4)}"

    order_payload = {
        "amount": amount_paisa,
        "currency": currency,
        "receipt": receipt,
        "notes": {
            "username": username,
            "plan_code": plan_code,
            "plan_name": plan_name,
        },
    }

    try:
        resp = await asyncio.to_thread(
            requests.post,
            "https://api.razorpay.com/v1/orders",
            json=order_payload,
            auth=(key_id, key_secret),
            timeout=20,
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Razorpay order request failed: {exc}") from exc

    try:
        body = resp.json() if resp.content else {}
    except ValueError:
        body = {}

    if int(getattr(resp, "status_code", 500)) >= 400:
        message = ""
        if isinstance(body, dict):
            err = body.get("error")
            if isinstance(err, dict):
                message = str(err.get("description") or err.get("reason") or "").strip()
            if not message:
                message = str(body.get("message") or "").strip()
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=message or f"Razorpay order failed with HTTP {resp.status_code}.",
        )

    order_id = str((body or {}).get("id") or "").strip()
    if not order_id:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Razorpay order id missing.")

    return {
        "ok": True,
        "provider": "razorpay",
        "key_id": key_id,
        "order_id": order_id,
        "amount": amount_paisa,
        "currency": currency,
        "plan_code": plan_code,
        "plan_name": plan_name,
        "receipt": receipt,
        "prefill": {
            "name": str(user.get("full_name") or "").strip(),
            "email": str(user.get("email") or "").strip(),
            "contact": str(user.get("mobile_number") or "").strip(),
        },
    }


@app.post("/user/subscription/razorpay/verify")
async def user_subscription_razorpay_verify(
    payload: RazorpayVerifyPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    key_secret = str(state.settings.razorpay_key_secret or "").strip()
    if not key_secret:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Razorpay is not configured.")

    order_id = str(payload.razorpay_order_id or "").strip()
    payment_id = str(payload.razorpay_payment_id or "").strip()
    signature = str(payload.razorpay_signature or "").strip()
    if not order_id or not payment_id or not signature:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Missing payment verification fields.")

    signed_text = f"{order_id}|{payment_id}"
    expected_signature = hmac.new(
        key_secret.encode("utf-8"),
        signed_text.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_signature, signature):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Razorpay signature.")

    plan_code = str(payload.plan_code or "").strip().lower()
    selected_plan = _RAZORPAY_PLAN_CATALOG.get(plan_code) if plan_code else None
    if plan_code and not isinstance(selected_plan, dict):
        allowed = ", ".join(sorted(_RAZORPAY_PLAN_CATALOG.keys()))
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid plan_code. Allowed: {allowed}.")
    plan_name = str(
        payload.plan_name
        or (selected_plan or {}).get("name")
        or state.settings.razorpay_subscribe_plan_name
        or "Sensible Algo Pro"
    ).strip()
    if not plan_name:
        plan_name = "Sensible Algo Pro"
    duration_days = max(1, _to_int((selected_plan or {}).get("duration_days"), 30))
    try:
        subscription = state.user_auth.activate_user_subscription(
            username=username,
            plan_code=plan_code or "monthly",
            plan_name=plan_name,
            duration_days=duration_days,
            provider="razorpay",
            payment_order_id=order_id,
            payment_id=payment_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "ok": True,
        "verified": True,
        "provider": "razorpay",
        "order_id": order_id,
        "payment_id": payment_id,
        "plan_code": plan_code or "monthly",
        "plan_name": plan_name,
        "subscription": subscription,
    }


@app.get("/user/broker/angel-one/login-url")
async def user_broker_angel_one_login_url(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        config = state.user_auth.get_user_broker_config(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    client_id = str(config.get("client_id") or "").strip()
    if not client_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Set Angel One client id first.")

    api_key = str(state.settings.angel_api_key or "").strip()
    if not api_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="ANGEL_API_KEY is not configured.")

    redirect_url = _resolve_angel_redirect_url(state.settings, request)
    state_token = _build_angel_state(username=username, secret=state.settings.auth_secret, ttl_sec=900)
    if not state_token:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create login state.")

    publisher_base = str(state.settings.angel_publisher_login or "").strip()
    if not publisher_base:
        publisher_base = "https://smartapi.angelbroking.com/publisher-login/"

    final_url = _append_query(
        publisher_base,
        {
            "api_key": api_key,
            "redirect_url": redirect_url,
            "state": state_token,
        },
    )
    return {
        "ok": True,
        "broker": "angel_one",
        "auth_url": final_url,
        "client_id": client_id,
    }


@app.get("/user/report/trades")
async def user_report_trades(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    start_date: str = "",
    end_date: str = "",
) -> Dict[str, Any]:
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()

    try:
        start_day = datetime.strptime(str(start_date or "").strip(), "%Y-%m-%d").date()
        end_day = datetime.strptime(str(end_date or "").strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid date format. Use YYYY-MM-DD.") from exc
    if end_day < start_day:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="end_date must be on or after start_date.")
    if (end_day - start_day).days > 29:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Maximum report range is 30 days.")

    today_ist = datetime.now(_IST).date()
    min_allowed = today_ist - timedelta(days=29)
    if start_day < min_allowed:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"start_date must be on or after {min_allowed.isoformat()}.")
    if end_day > today_ist:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"end_date cannot be after {today_ist.isoformat()}.")

    try:
        session = state.user_auth.get_user_angel_session(username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    if not bool(session.get("connected")):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Angel terminal is not connected.")

    access_token = str(session.get("access_token") or "").strip()
    client_id = str(session.get("client_id") or "").strip()
    api_key = str(session.get("api_key") or state.settings.angel_api_key or "").strip()
    if not access_token or not client_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Angel session is incomplete. Reconnect terminal login.")
    if not api_key:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Angel API key is not configured.")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": str(state.settings.angel_client_local_ip or "127.0.0.1"),
        "X-ClientPublicIP": str(state.settings.angel_client_public_ip or "0.0.0.0"),
        "X-MACAddress": str(state.settings.angel_client_mac or "00:00:00:00:00:00"),
        "X-PrivateKey": api_key,
    }

    try:
        resp = await asyncio.to_thread(
            requests.post,
            "https://apiconnect.angelone.in/rest/secure/angelbroking/order/v1/getTradeBook",
            json={},
            headers=headers,
            timeout=15,
        )
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Angel tradebook request failed: {exc}") from exc

    if int(getattr(resp, "status_code", 500)) >= 400:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=f"Angel tradebook HTTP {resp.status_code}.")

    try:
        body = resp.json() if resp.content else {}
    except ValueError:
        body = {}

    if isinstance(body, dict) and body.get("status") is False:
        msg = str(body.get("message") or body.get("errorcode") or "Angel tradebook failed").strip()
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=msg)

    data = body.get("data") if isinstance(body, dict) else []
    raw_rows: List[Dict[str, Any]] = []
    if isinstance(data, list):
        raw_rows = [row for row in data if isinstance(row, dict)]
    elif isinstance(data, dict):
        for key in ("trades", "tradeBook", "tradebook", "rows"):
            value = data.get(key)
            if isinstance(value, list):
                raw_rows = [row for row in value if isinstance(row, dict)]
                break

    range_start = int(datetime.combine(start_day, dt_time(0, 0, 0), tzinfo=_IST).timestamp())
    range_end = int(datetime.combine(end_day, dt_time(23, 59, 59), tzinfo=_IST).timestamp())

    out_rows: List[Dict[str, Any]] = []
    for row in raw_rows:
        ts = _parse_angel_trade_timestamp(row)
        if ts <= 0 or ts < range_start or ts > range_end:
            continue
        trade_dt = datetime.fromtimestamp(ts, tz=_IST)
        symbol = str(row.get("tradingsymbol") or row.get("symbol") or row.get("tsym") or "").strip()
        side = str(row.get("transactiontype") or row.get("transactionType") or row.get("side") or "").strip().upper()
        qty = _to_int(row.get("quantity") or row.get("fillshares") or row.get("filledshares"), 0)
        price = _to_float(row.get("averageprice") or row.get("fillprice") or row.get("price"), 0.0)
        order_id = str(row.get("orderid") or row.get("orderId") or row.get("oms_order_id") or "").strip()
        exchange = str(row.get("exchange") or "").strip().upper()
        product = str(row.get("producttype") or row.get("productType") or "").strip()
        out_rows.append(
            {
                "trade_time_ts": ts,
                "trade_time_ist": trade_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "side": side,
                "quantity": qty,
                "price": round(float(price), 4),
                "order_id": order_id,
                "exchange": exchange,
                "product": product,
            }
        )

    out_rows.sort(key=lambda item: _to_int(item.get("trade_time_ts"), 0), reverse=True)
    return {
        "ok": True,
        "username": username,
        "broker": "angel_one",
        "client_id": client_id,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "count": len(out_rows),
        "trades": out_rows,
    }


def _query_paper_trades_in_range(state: "AppState", start_ts: int, end_ts: int, limit: int = 500) -> List[Dict[str, Any]]:
    """Read paper trades from MySQL within an epoch-second range. Returns raw dicts."""
    try:
        import mysql.connector  # type: ignore
    except ImportError:
        return []
    settings = state.settings
    table = str(settings.paper_trade_mysql_trades_table or "").strip()
    if not table:
        return []
    try:
        conn_kwargs = mysql_connect_kwargs_from_parts(
            host=settings.auth_mysql_host,
            port=settings.auth_mysql_port,
            user=settings.auth_mysql_user,
            password=settings.auth_mysql_password,
            database=settings.auth_mysql_database,
            connect_timeout_sec=settings.auth_mysql_connect_timeout_sec,
            with_database=True,
            autocommit=False,
            ssl_mode=settings.auth_mysql_ssl_mode,
            ssl_ca=settings.auth_mysql_ssl_ca,
            ssl_disabled=settings.auth_mysql_ssl_disabled,
            ssl_verify_cert=settings.auth_mysql_ssl_verify_cert,
            ssl_verify_identity=settings.auth_mysql_ssl_verify_identity,
        )
        conn = mysql.connector.connect(**conn_kwargs)
        cur = conn.cursor(dictionary=True)
        cur.execute(
            f"""
            SELECT trade_id, symbol, status, direction, entry_ts, entry_price,
                   exit_ts, exit_price, points, outcome, close_reason, created_at, closed_at,
                   signal_json
            FROM `{table}`
            WHERE entry_ts >= %s AND entry_ts <= %s
            ORDER BY entry_ts DESC
            LIMIT %s
            """,
            (int(start_ts), int(end_ts), max(1, min(int(limit), 1000))),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        out = []
        for r in (rows or []):
            d = dict(r)
            # Parse signal_json to extract the real option symbol
            sig_raw = d.pop("signal_json", None)
            if isinstance(sig_raw, (bytes, bytearray)):
                sig_raw = sig_raw.decode("utf-8", errors="ignore")
            option_symbol = ""
            try:
                sig = json.loads(str(sig_raw or "{}"))
                if isinstance(sig, dict):
                    sel = sig.get("selected_option")
                    if isinstance(sel, dict):
                        option_symbol = str(
                            sel.get("symbol") or sel.get("tradingsymbol") or ""
                        ).strip()
            except Exception:
                pass
            d["option_symbol"] = option_symbol
            out.append(d)
        return out
    except Exception as exc:
        logger.warning("_query_paper_trades_in_range failed: %s", exc)
        return []


@app.get("/user/report/ai-trades")
async def user_report_ai_trades(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    start_date: str = "",
    end_date: str = "",
    mode: str = "all",
) -> Dict[str, Any]:
    """Return AI-generated paper trades and the current user's personal live orders."""
    state: AppState = app.state.state
    user = _require_user(state, _authorization_from_request(authorization, request))
    username = str(user.get("email") or user.get("username") or "").strip().lower()
    try:
        lot_cfg = await asyncio.to_thread(state.user_auth.get_user_lot_config, username)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    lot_size_qty = max(1, _to_int(lot_cfg.get("lot_size_qty"), 1))
    user_lot_count = max(1, _to_int(lot_cfg.get("user_lot_count"), 1))
    quantity_multiplier = max(1, lot_size_qty * user_lot_count)

    try:
        start_day = datetime.strptime(str(start_date or "").strip(), "%Y-%m-%d").date()
        end_day = datetime.strptime(str(end_date or "").strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid date format. Use YYYY-MM-DD.") from exc
    if end_day < start_day:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="end_date must be on or after start_date.")
    if (end_day - start_day).days > 89:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Maximum report range is 90 days.")

    start_ts = int(datetime.combine(start_day, dt_time(0, 0, 0), tzinfo=_IST).timestamp())
    end_ts = int(datetime.combine(end_day, dt_time(23, 59, 59), tzinfo=_IST).timestamp())

    mode_filter = str(mode or "all").strip().lower()
    out_rows: List[Dict[str, Any]] = []

    # --- Paper trades (system-wide, same for all users) ---
    if mode_filter in ("all", "paper"):
        raw_paper = await asyncio.to_thread(_query_paper_trades_in_range, state, start_ts, end_ts)
        for row in raw_paper:
            entry_ts_val = _to_int(row.get("entry_ts"), 0)
            exit_ts_val = _to_int(row.get("exit_ts"), 0)
            entry_dt = datetime.fromtimestamp(entry_ts_val, tz=_IST) if entry_ts_val > 0 else None
            exit_dt = datetime.fromtimestamp(exit_ts_val, tz=_IST) if exit_ts_val > 0 else None
            option_sym = str(row.get("option_symbol") or "").strip()
            display_symbol = option_sym if option_sym else str(row.get("symbol") or "--")
            out_rows.append({
                "mode": "paper",
                "trade_id": str(row.get("trade_id") or "--"),
                "entry_time_ist": entry_dt.strftime("%Y-%m-%d %H:%M:%S") if entry_dt else "--",
                "exit_time_ist": exit_dt.strftime("%Y-%m-%d %H:%M:%S") if exit_dt else "--",
                "symbol": display_symbol,
                "direction": str(row.get("direction") or "--").capitalize(),
                "entry_price": (
                    round(_to_float(row.get("entry_price"), 0.0), 2)
                    if _to_float(row.get("entry_price"), 0.0) > 0.0
                    else None
                ),
                "exit_price": (
                    round(_to_float(row.get("exit_price"), 0.0), 2)
                    if row.get("exit_price") is not None and _to_float(row.get("exit_price"), 0.0) > 0.0
                    else None
                ),
                "points": (
                    round(_to_float(row.get("points"), 0.0) * float(quantity_multiplier), 2)
                    if row.get("points") is not None
                    else None
                ),
                "outcome": str(row.get("outcome") or "open").capitalize(),
                "close_reason": str(row.get("close_reason") or ""),
                "status": str(row.get("status") or ""),
                "entry_ts": entry_ts_val,
                "quantity": int(quantity_multiplier),
            })

    # --- User's personal live orders ---
    if mode_filter in ("all", "live"):
        live_rows = await asyncio.to_thread(
            state.live_order_store.get_user_trades_in_range,
            username=username,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        for row in live_rows:
            entry_ts_val = _to_int(row.get("entry_ts"), 0)
            exit_ts_val = _to_int(row.get("exit_ts"), 0)
            entry_dt = datetime.fromtimestamp(entry_ts_val, tz=_IST) if entry_ts_val > 0 else None
            exit_dt = datetime.fromtimestamp(exit_ts_val, tz=_IST) if exit_ts_val > 0 else None
            out_rows.append({
                "mode": "live",
                "trade_id": str(row.get("trade_id") or "--"),
                "entry_time_ist": entry_dt.strftime("%Y-%m-%d %H:%M:%S") if entry_dt else "--",
                "exit_time_ist": exit_dt.strftime("%Y-%m-%d %H:%M:%S") if exit_dt else "--",
                "symbol": str(row.get("symbol") or "--"),
                "direction": str(row.get("direction") or "--").capitalize(),
                "entry_price": None,
                "exit_price": None,
                "points": None,
                "outcome": "Open" if str(row.get("status") or "open") == "open" else "Closed",
                "close_reason": str(row.get("close_reason") or ""),
                "status": str(row.get("status") or "open"),
                "entry_order_id": str(row.get("entry_order_id") or ""),
                "exit_order_id": str(row.get("exit_order_id") or ""),
                "exchange": str(row.get("exchange") or ""),
                "quantity": _to_int(row.get("quantity"), 0),
                "entry_ts": entry_ts_val,
            })

    out_rows.sort(key=lambda r: _to_int(r.get("entry_ts"), 0), reverse=True)
    return {
        "ok": True,
        "username": username,
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "mode": mode_filter,
        "lot_size_qty": int(lot_size_qty),
        "user_lot_count": int(user_lot_count),
        "quantity_multiplier": int(quantity_multiplier),
        "count": len(out_rows),
        "trades": out_rows,
    }


@app.get("/auth/angel/callback")
async def angel_callback(request: Request) -> Any:
    state: AppState = app.state.state
    parsed = _parse_angel_callback_query(request)
    auth_token = str(parsed.get("auth_token") or "").strip()
    refresh_token = str(parsed.get("refresh_token") or "").strip()
    feed_token = str(parsed.get("feed_token") or "").strip()
    state_token = str(parsed.get("state") or "").strip()
    raw = parsed.get("raw") if isinstance(parsed.get("raw"), dict) else {}
    redirect_base = "/ui"

    if not auth_token or not refresh_token:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=missing_tokens", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    username = _parse_angel_state(state_token, state.settings.auth_secret)

    api_key = str(state.settings.angel_api_key or "").strip()
    if not api_key:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=missing_api_key", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    headers = {
        "Authorization": f"Bearer {auth_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": str(state.settings.angel_client_local_ip or "127.0.0.1"),
        "X-ClientPublicIP": str(state.settings.angel_client_public_ip or "0.0.0.0"),
        "X-MACAddress": str(state.settings.angel_client_mac or "00:00:00:00:00:00"),
        "X-PrivateKey": api_key,
    }
    payload = {"refreshToken": refresh_token}

    try:
        token_resp = await asyncio.to_thread(
            requests.post,
            "https://apiconnect.angelone.in/rest/auth/angelbroking/jwt/v1/generateTokens",
            json=payload,
            headers=headers,
            timeout=15,
        )
        body = token_resp.json() if token_resp.content else {}
    except Exception:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=exchange_failed", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    if int(getattr(token_resp, "status_code", 500)) >= 400:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=exchange_http", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    data = body.get("data") if isinstance(body, dict) else {}
    access_token = str((data or {}).get("jwtToken") or "").strip()
    new_feed_token = str((data or {}).get("feedToken") or feed_token).strip()
    new_refresh_token = str((data or {}).get("refreshToken") or refresh_token).strip()
    if not access_token:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=no_access_token", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    decoded = _jwt_payload(access_token)
    extracted_client_id = _extract_angel_client_id(decoded, raw)
    if not extracted_client_id:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=no_clientid", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    if not username:
        username = state.user_auth.find_username_by_angel_client_id(extracted_client_id)
    if not username:
        return RedirectResponse(
            url=f"{redirect_base}?angel=failed&reason=client_not_linked&clientId={extracted_client_id}",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    try:
        cfg = state.user_auth.get_user_broker_config(username)
    except ValueError:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=user_not_found", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    expected_client_id = str(cfg.get("client_id") or "").strip()
    if not expected_client_id:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=client_not_set", status_code=status.HTTP_307_TEMPORARY_REDIRECT)
    if expected_client_id.lower() != extracted_client_id.lower():
        return RedirectResponse(
            url=f"{redirect_base}?angel=failed&reason=client_mismatch&clientId={extracted_client_id}",
            status_code=status.HTTP_307_TEMPORARY_REDIRECT,
        )

    token_expires_at = _to_int(decoded.get("exp"), 0)
    try:
        state.user_auth.save_user_angel_session(
            username=username,
            client_id=extracted_client_id,
            auth_token=auth_token,
            access_token=access_token,
            feed_token=new_feed_token,
            refresh_token=new_refresh_token,
            token_expires_at=token_expires_at,
        )
    except ValueError:
        return RedirectResponse(url=f"{redirect_base}?angel=failed&reason=save_failed", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    return RedirectResponse(url=f"{redirect_base}?angel=connected", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@app.post("/auth/logout")
async def auth_logout(
    request: Request,
    response: Response,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    auth_value = _authorization_from_request(authorization, request)
    user = _require_user(state, auth_value)
    state.user_auth.logout(auth_value)
    response.delete_cookie("pta_session")
    return {"ok": True, "username": user.get("username", "")}


@app.get("/auth/fyers/status")
async def fyers_status(
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    return state.fyers_auth.get_status()


@app.get("/auth/fyers/url")
async def fyers_url(
    request: Request,
    authorization: Optional[str] = Header(default=None),
    state_key: str = "pain-theory-ai",
) -> Dict[str, Any]:
    state_obj: AppState = app.state.state
    _require_admin(state_obj, _authorization_from_request(authorization, request))
    try:
        auth_url = state_obj.fyers_auth.generate_login_url(state=state_key)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {"auth_url": auth_url}


@app.post("/auth/fyers/exchange")
async def fyers_exchange(
    payload: FyersExchangePayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    try:
        exchanged = state.fyers_auth.exchange_auth_code(payload.auth_code)
        _apply_fyers_access_token(
            state,
            str(exchanged.get("access_token", "")).strip(),
            reason="manual exchange",
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return {
        "ok": True,
        "access_token_expires_at": exchanged.get("access_token_expires_at", 0),
        "refresh_token_expires_at": exchanged.get("refresh_token_expires_at", 0),
        "authenticated": bool(exchanged.get("authenticated", False)),
    }


@app.post("/auth/fyers/refresh")
async def fyers_refresh(
    payload: FyersRefreshPayload,
    request: Request,
    authorization: Optional[str] = Header(default=None),
) -> Dict[str, Any]:
    state: AppState = app.state.state
    _require_admin(state, _authorization_from_request(authorization, request))
    try:
        refreshed = await asyncio.to_thread(state.fyers_auth.refresh_access_token, force=bool(payload.force))
        token = str(refreshed.get("access_token", "")).strip()
        if token:
            _apply_fyers_access_token(state, token, reason="manual refresh endpoint")
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return refreshed
