from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import deque
from datetime import datetime, timedelta, timezone
import json
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional
import uuid
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .config import mysql_connect_kwargs_from_parts
from .option_contracts import contract_days_to_expiry, contract_tradeability, extract_option_contract


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


try:
    _IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    _IST = timezone(timedelta(hours=5, minutes=30))


_MISTAKE_TYPES = {
    "no_trade",
    "none",
    "false_entry",
    "late_entry",
    "wrong_side_pain",
    "chop_zone",
    "expiry_noise",
    "unclassified_loss",
}
_MISTAKE_ORDER = (
    "none",
    "false_entry",
    "late_entry",
    "wrong_side_pain",
    "chop_zone",
    "expiry_noise",
)
_QUOTE_SOURCE_MAX_AGE_SEC = 120
_QUOTE_SOURCE_FUTURE_SLACK_SEC = 60


def _effective_quote_timestamp(source_ts: Any, *, received_ts: int) -> int:
    """Prefer source exchange timestamp when fresh; otherwise use receive time."""
    recv = max(0, _to_int(received_ts, int(time.time())))
    src = _to_int(source_ts, 0)
    if src <= 0:
        return recv
    if src > recv + _QUOTE_SOURCE_FUTURE_SLACK_SEC:
        return recv
    if recv - src > _QUOTE_SOURCE_MAX_AGE_SEC:
        return recv
    return src


class PaperTradeEngine:
    def __init__(
        self,
        *,
        enabled: bool,
        symbol: str,
        hold_minutes: int,
        min_confidence: float,
        entry_guidance: str,
        one_trade_at_a_time: bool,
        require_pain_release: bool = False,
        entry_style: str = "aggressive",
        min_compression_bars: int = 3,
        target_r_multiple: float = 1.5,
        avoid_noon_chop: bool = True,
        feedback_enabled: bool = True,
        feedback_min_trades: int = 8,
        feedback_block_win_rate: float = 0.28,
        feedback_penalty_floor: float = 0.55,
        feedback_reward_cap: float = 1.12,
        mysql_host: str = "127.0.0.1",
        mysql_port: int = 3306,
        mysql_user: str = "",
        mysql_password: str = "",
        mysql_database: str = "",
        mysql_connect_timeout_sec: int = 5,
        mysql_ssl_mode: str = "",
        mysql_ssl_ca: str = "",
        mysql_ssl_disabled: bool = False,
        mysql_ssl_verify_cert: bool = False,
        mysql_ssl_verify_identity: bool = False,
        mysql_state_table: str = "paper_trade_state",
        mysql_trades_table: str = "paper_trade_trades",
        mysql_feedback_table: str = "paper_trade_feedback",
        mysql_mistakes_table: str = "paper_trade_mistakes",
        storage_backend: str = "mysql",
        model_driven_execution: bool = False,
        quote_fetcher: Optional[Callable[[str], Optional[Dict[str, Any]]]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.symbol = str(symbol or "").strip()
        self.hold_seconds = max(60, int(hold_minutes) * 60)
        self.min_confidence = max(0.0, min(1.0, float(min_confidence)))
        self.entry_guidance = str(entry_guidance or "caution").strip().lower()
        self.one_trade_at_a_time = bool(one_trade_at_a_time)
        # Pain-release gating is disabled by design. Keep the parameter only for API compatibility.
        self.require_pain_release = False
        self.entry_style = "conservative" if str(entry_style or "").strip().lower() == "conservative" else "aggressive"
        self.min_compression_bars = max(2, int(min_compression_bars))
        self.target_r_multiple = max(1.0, float(target_r_multiple))
        self.avoid_noon_chop = bool(avoid_noon_chop)
        self.feedback_enabled = bool(feedback_enabled)
        self.feedback_min_trades = max(3, int(feedback_min_trades))
        self.feedback_block_win_rate = max(0.05, min(0.60, float(feedback_block_win_rate)))
        self.feedback_penalty_floor = max(0.35, min(0.95, float(feedback_penalty_floor)))
        self.feedback_reward_cap = max(1.0, min(1.5, float(feedback_reward_cap)))
        self.model_driven_execution = bool(model_driven_execution)
        self._quote_fetcher = quote_fetcher
        backend = str(storage_backend or "mysql").strip().lower()
        if backend not in {"mysql", "memory"}:
            backend = "mysql"
        self._storage_backend = backend

        self._lock = threading.Lock()
        self._trades: List[Dict[str, Any]] = []
        self._active_trade: Optional[Dict[str, Any]] = None
        self._last_candle_ts = 0
        self._last_price = 0.0
        self._persisted_closed_count = 0
        self._recent_candles: deque[Dict[str, float]] = deque(maxlen=90)
        self._compression_count = 0
        self._last_release_ts = 0
        self._pending_release: Optional[Dict[str, Any]] = None
        self._last_option_snapshot: Optional[Dict[str, Any]] = None
        self._feedback_stats: Dict[str, Dict[str, Any]] = {}
        self._last_feedback_decision: Dict[str, Any] = {
            "applied": False,
            "reason": "not_evaluated",
        }
        self._gate_stats: Dict[str, int] = self._new_gate_stats()
        self._last_gate_event: Dict[str, Any] = {
            "reason": "not_evaluated",
            "ts": 0,
            "detail": "",
        }

        self._mysql = None
        self._mysql_host = str(mysql_host or "127.0.0.1")
        self._mysql_port = int(mysql_port or 3306)
        self._mysql_user = str(mysql_user or "")
        self._mysql_password = str(mysql_password or "")
        self._mysql_database = str(mysql_database or "").strip()
        self._mysql_connect_timeout_sec = max(2, int(mysql_connect_timeout_sec))
        self._mysql_ssl_mode = str(mysql_ssl_mode or "").strip()
        self._mysql_ssl_ca = str(mysql_ssl_ca or "").strip()
        self._mysql_ssl_disabled = bool(mysql_ssl_disabled)
        self._mysql_ssl_verify_cert = bool(mysql_ssl_verify_cert)
        self._mysql_ssl_verify_identity = bool(mysql_ssl_verify_identity)
        self._mysql_state_table = str(mysql_state_table or "paper_trade_state").strip()
        self._mysql_trades_table = str(mysql_trades_table or "paper_trade_trades").strip()
        self._mysql_feedback_table = str(mysql_feedback_table or "paper_trade_feedback").strip()
        self._mysql_mistakes_table = str(mysql_mistakes_table or "paper_trade_mistakes").strip()

        if self._storage_backend == "mysql":
            self._setup_mysql()
            self._initialize_mysql_schema()
            self._load()

    def _validate_identifier(self, value: str, label: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            raise RuntimeError(f"{label} must contain only letters, numbers, and underscores")
        return text

    def set_quote_fetcher(self, quote_fetcher: Optional[Callable[[str], Optional[Dict[str, Any]]]]) -> None:
        self._quote_fetcher = quote_fetcher

    @staticmethod
    def _new_gate_stats() -> Dict[str, int]:
        return {
            "candles_seen": 0,
            "with_active_trade": 0,
            "noon_chop_block": 0,
            "entry_cutoff_block": 0,
            "forced_squareoff": 0,
            "guidance_mismatch": 0,
            "entry_signal_off": 0,
            "direction_missing": 0,
            "feedback_block": 0,
            "confidence_below_min": 0,
            "pain_release_detected": 0,
            "pain_release_missing": 0,
            "pending_release_wait": 0,
            "entry_opened": 0,
        }

    def _note_gate(self, reason: str, ts: int = 0, detail: str = "") -> None:
        key = str(reason or "").strip().lower()
        if key:
            self._gate_stats[key] = max(0, _to_int(self._gate_stats.get(key), 0)) + 1
        self._last_gate_event = {
            "reason": key or "unknown",
            "ts": max(0, _to_int(ts, 0)),
            "detail": str(detail or "").strip(),
        }

    def _gate_diagnostics_locked(self) -> Dict[str, Any]:
        return {
            "counts": {str(key): int(_to_int(value, 0)) for key, value in self._gate_stats.items()},
            "last_event": dict(self._last_gate_event),
            "entry_guidance": self.entry_guidance,
            "min_confidence": float(self.min_confidence),
            "require_pain_release": bool(self.require_pain_release),
            "model_driven_execution": bool(self.model_driven_execution),
            "entry_style": self.entry_style,
            "min_compression_bars": int(self.min_compression_bars),
        }

    def _setup_mysql(self) -> None:
        try:
            import mysql.connector  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "mysql-connector-python is required for paper-trade database storage. "
                "Install dependencies from requirements.txt."
            ) from exc

        self._mysql = mysql.connector
        self._mysql_database = self._validate_identifier(self._mysql_database, "AUTH_MYSQL_DATABASE")
        self._mysql_state_table = self._validate_identifier(self._mysql_state_table, "PAPER_TRADE_MYSQL_STATE_TABLE")
        self._mysql_trades_table = self._validate_identifier(self._mysql_trades_table, "PAPER_TRADE_MYSQL_TRADES_TABLE")
        self._mysql_feedback_table = self._validate_identifier(self._mysql_feedback_table, "PAPER_TRADE_MYSQL_FEEDBACK_TABLE")
        self._mysql_mistakes_table = self._validate_identifier(self._mysql_mistakes_table, "PAPER_TRADE_MYSQL_MISTAKES_TABLE")
        if not self._mysql_user:
            raise RuntimeError("AUTH_MYSQL_USER is required")

    def _connect_mysql(self, with_database: bool = True):
        kwargs = mysql_connect_kwargs_from_parts(
            host=self._mysql_host,
            port=self._mysql_port,
            user=self._mysql_user,
            password=self._mysql_password,
            database=self._mysql_database,
            connect_timeout_sec=self._mysql_connect_timeout_sec,
            with_database=with_database,
            autocommit=False,
            ssl_mode=self._mysql_ssl_mode,
            ssl_ca=self._mysql_ssl_ca,
            ssl_disabled=self._mysql_ssl_disabled,
            ssl_verify_cert=self._mysql_ssl_verify_cert,
            ssl_verify_identity=self._mysql_ssl_verify_identity,
        )
        return self._mysql.connect(**kwargs)

    def _initialize_mysql_schema(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect_mysql(with_database=False)
            cur = conn.cursor()
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self._mysql_database}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
            conn.commit()
            cur.close()
            conn.close()

            conn = self._connect_mysql(with_database=True)
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._mysql_state_table}` (
                    id TINYINT PRIMARY KEY,
                    symbol VARCHAR(80) NOT NULL,
                    hold_seconds INT NOT NULL,
                    min_confidence DOUBLE NOT NULL,
                    entry_guidance VARCHAR(32) NOT NULL,
                    one_trade_at_a_time TINYINT(1) NOT NULL DEFAULT 1,
                    last_candle_ts BIGINT NOT NULL DEFAULT 0,
                    last_price DOUBLE NOT NULL DEFAULT 0,
                    active_trade_json LONGTEXT NULL,
                    updated_at BIGINT NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._mysql_trades_table}` (
                    trade_id BIGINT PRIMARY KEY,
                    symbol VARCHAR(80) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    direction VARCHAR(16) NOT NULL,
                    entry_ts BIGINT NOT NULL,
                    entry_price DOUBLE NOT NULL,
                    planned_exit_ts BIGINT NOT NULL,
                    hold_seconds INT NOT NULL,
                    signal_json LONGTEXT NOT NULL,
                    exit_ts BIGINT NULL,
                    exit_price DOUBLE NULL,
                    holding_seconds INT NULL,
                    points DOUBLE NULL,
                    outcome VARCHAR(16) NULL,
                    close_reason VARCHAR(40) NULL,
                    created_at BIGINT NOT NULL,
                    closed_at BIGINT NULL,
                    INDEX idx_entry_ts (entry_ts),
                    INDEX idx_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._mysql_feedback_table}` (
                    feedback_key VARCHAR(190) PRIMARY KEY,
                    pain_phase VARCHAR(32) NOT NULL,
                    next_group VARCHAR(32) NOT NULL,
                    direction VARCHAR(16) NOT NULL,
                    regime_tag VARCHAR(80) NOT NULL,
                    trades_count INT NOT NULL DEFAULT 0,
                    wins_count INT NOT NULL DEFAULT 0,
                    losses_count INT NOT NULL DEFAULT 0,
                    flat_count INT NOT NULL DEFAULT 0,
                    net_points DOUBLE NOT NULL DEFAULT 0,
                    avg_points DOUBLE NOT NULL DEFAULT 0,
                    updated_at BIGINT NOT NULL,
                    INDEX idx_feedback_phase_dir (pain_phase, direction),
                    INDEX idx_feedback_updated (updated_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self._mysql_mistakes_table}` (
                    trade_id BIGINT PRIMARY KEY,
                    prediction_id VARCHAR(80) NULL,
                    prediction_ts BIGINT NOT NULL DEFAULT 0,
                    entry_ts BIGINT NOT NULL DEFAULT 0,
                    exit_ts BIGINT NOT NULL DEFAULT 0,
                    holding_seconds INT NOT NULL DEFAULT 0,
                    direction VARCHAR(16) NOT NULL,
                    outcome VARCHAR(16) NOT NULL,
                    close_reason VARCHAR(40) NOT NULL,
                    points DOUBLE NOT NULL DEFAULT 0,
                    mistake_type VARCHAR(40) NOT NULL DEFAULT 'none',
                    mistake_score DOUBLE NOT NULL DEFAULT 0,
                    mistake_reason VARCHAR(255) NULL,
                    mistake_flags_json LONGTEXT NULL,
                    pain_phase VARCHAR(32) NULL,
                    dominant_pain_group VARCHAR(32) NULL,
                    next_likely_pain_group VARCHAR(32) NULL,
                    guidance VARCHAR(32) NULL,
                    confidence DOUBLE NOT NULL DEFAULT 0,
                    entry_trigger_confidence DOUBLE NOT NULL DEFAULT 0,
                    stop_loss_distance DOUBLE NOT NULL DEFAULT 0,
                    target_level DOUBLE NOT NULL DEFAULT 0,
                    regime_tag VARCHAR(80) NULL,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    INDEX idx_mistake_prediction_ts (prediction_ts),
                    INDEX idx_mistake_entry_ts (entry_ts),
                    INDEX idx_mistake_type (mistake_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            self._ensure_mysql_varchar_column(
                conn,
                table=self._mysql_trades_table,
                column="close_reason",
                length=128,
                nullable=True,
            )
            self._ensure_mysql_varchar_column(
                conn,
                table=self._mysql_mistakes_table,
                column="close_reason",
                length=128,
                nullable=False,
                default="",
            )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _ensure_mysql_varchar_column(
        self,
        conn: Any,
        *,
        table: str,
        column: str,
        length: int,
        nullable: bool,
        default: Optional[str] = None,
    ) -> None:
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                LIMIT 1
                """,
                (self._mysql_database, table, column),
            )
            row = cur.fetchone() or {}
        finally:
            cur.close()

        if not row:
            return

        data_type = str(row.get("DATA_TYPE") or "").strip().lower()
        current_length = _to_int(row.get("CHARACTER_MAXIMUM_LENGTH"), 0)
        current_nullable = str(row.get("IS_NULLABLE") or "").strip().upper() == "YES"
        if data_type == "varchar" and current_length >= int(length) and current_nullable == bool(nullable):
            return

        nullable_sql = "NULL" if nullable else "NOT NULL"
        default_sql = ""
        if default is not None:
            escaped = str(default).replace("\\", "\\\\").replace("'", "\\'")
            default_sql = f" DEFAULT '{escaped}'"

        alter = conn.cursor()
        try:
            alter.execute(
                f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` VARCHAR({int(max(1, length))}) {nullable_sql}{default_sql}"
            )
        finally:
            alter.close()

    def _load(self) -> None:
        if self._storage_backend == "mysql":
            self._load_mysql()

    def _load_mysql(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect_mysql(with_database=True)
            cur = conn.cursor(dictionary=True)

            cur.execute(
                f"SELECT last_candle_ts, last_price, active_trade_json "
                f"FROM `{self._mysql_state_table}` WHERE id = 1 LIMIT 1"
            )
            row = cur.fetchone()
            if isinstance(row, dict):
                self._last_candle_ts = _to_int(row.get("last_candle_ts"), 0)
                self._last_price = _to_float(row.get("last_price"), 0.0)
                active_json = row.get("active_trade_json")
                if isinstance(active_json, (bytes, bytearray)):
                    active_json = active_json.decode("utf-8", errors="ignore")
                if active_json:
                    try:
                        active_obj = json.loads(str(active_json))
                        self._active_trade = active_obj if isinstance(active_obj, dict) else None
                    except (ValueError, TypeError):
                        self._active_trade = None

            cur.execute(
                f"""
                SELECT
                    trade_id, symbol, status, direction,
                    entry_ts, entry_price, planned_exit_ts, hold_seconds,
                    signal_json, exit_ts, exit_price, holding_seconds,
                    points, outcome, close_reason
                FROM `{self._mysql_trades_table}`
                ORDER BY trade_id ASC
                """
            )
            fetched = cur.fetchall() or []
            self._trades = []
            for item in fetched:
                signal_json = item.get("signal_json")
                if isinstance(signal_json, (bytes, bytearray)):
                    signal_json = signal_json.decode("utf-8", errors="ignore")
                try:
                    signal = json.loads(str(signal_json or "{}"))
                    if not isinstance(signal, dict):
                        signal = {}
                except (ValueError, TypeError):
                    signal = {}

                self._trades.append(
                    {
                        "trade_id": _to_int(item.get("trade_id"), 0),
                        "symbol": str(item.get("symbol") or self.symbol),
                        "status": str(item.get("status") or "closed"),
                        "direction": str(item.get("direction") or ""),
                        "entry_ts": _to_int(item.get("entry_ts"), 0),
                        "entry_price": _to_float(item.get("entry_price"), 0.0),
                        "planned_exit_ts": _to_int(item.get("planned_exit_ts"), 0),
                        "hold_seconds": _to_int(item.get("hold_seconds"), 0),
                        "signal": signal,
                        "exit_ts": _to_int(item.get("exit_ts"), 0) if item.get("exit_ts") is not None else None,
                        "exit_price": (
                            _to_float(item.get("exit_price"), 0.0)
                            if item.get("exit_price") is not None
                            else None
                        ),
                        "holding_seconds": (
                            _to_int(item.get("holding_seconds"), 0)
                            if item.get("holding_seconds") is not None
                            else None
                        ),
                        "points": _to_float(item.get("points"), 0.0) if item.get("points") is not None else 0.0,
                        "outcome": str(item.get("outcome") or "flat"),
                        "close_reason": str(item.get("close_reason") or ""),
                    }
                )
            self._persisted_closed_count = len(self._trades)

            cur.execute(
                f"""
                SELECT
                    feedback_key,
                    pain_phase,
                    next_group,
                    direction,
                    regime_tag,
                    trades_count,
                    wins_count,
                    losses_count,
                    flat_count,
                    net_points,
                    avg_points
                FROM `{self._mysql_feedback_table}`
                """
            )
            feedback_rows = cur.fetchall() or []
            self._feedback_stats = {}
            for row in feedback_rows:
                key = str(row.get("feedback_key") or "").strip()
                if not key:
                    continue
                trades_count = max(0, _to_int(row.get("trades_count"), 0))
                wins_count = max(0, _to_int(row.get("wins_count"), 0))
                losses_count = max(0, _to_int(row.get("losses_count"), 0))
                flat_count = max(0, _to_int(row.get("flat_count"), 0))
                net_points = float(_to_float(row.get("net_points"), 0.0))
                avg_points = float(_to_float(row.get("avg_points"), 0.0))
                if trades_count <= 0 and (wins_count or losses_count or flat_count):
                    trades_count = wins_count + losses_count + flat_count
                if trades_count > 0 and avg_points == 0.0 and net_points != 0.0:
                    avg_points = net_points / float(trades_count)
                self._feedback_stats[key] = {
                    "feedback_key": key,
                    "pain_phase": str(row.get("pain_phase") or "comfort"),
                    "next_group": str(row.get("next_group") or "none"),
                    "direction": str(row.get("direction") or ""),
                    "regime_tag": str(row.get("regime_tag") or "neutral"),
                    "trades_count": trades_count,
                    "wins_count": wins_count,
                    "losses_count": losses_count,
                    "flat_count": flat_count,
                    "net_points": net_points,
                    "avg_points": avg_points,
                }
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _persist(self) -> None:
        if self._storage_backend == "mysql":
            self._persist_mysql()

    def _persist_mysql(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect_mysql(with_database=True)
            cur = conn.cursor()

            if len(self._trades) < self._persisted_closed_count:
                cur.execute(f"DELETE FROM `{self._mysql_trades_table}`")
                self._persisted_closed_count = 0

            for row in self._trades[self._persisted_closed_count :]:
                cur.execute(
                    f"""
                    INSERT INTO `{self._mysql_trades_table}` (
                        trade_id, symbol, status, direction,
                        entry_ts, entry_price, planned_exit_ts, hold_seconds,
                        signal_json, exit_ts, exit_price, holding_seconds,
                        points, outcome, close_reason, created_at, closed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        symbol = VALUES(symbol),
                        status = VALUES(status),
                        direction = VALUES(direction),
                        entry_ts = VALUES(entry_ts),
                        entry_price = VALUES(entry_price),
                        planned_exit_ts = VALUES(planned_exit_ts),
                        hold_seconds = VALUES(hold_seconds),
                        signal_json = VALUES(signal_json),
                        exit_ts = VALUES(exit_ts),
                        exit_price = VALUES(exit_price),
                        holding_seconds = VALUES(holding_seconds),
                        points = VALUES(points),
                        outcome = VALUES(outcome),
                        close_reason = VALUES(close_reason),
                        created_at = VALUES(created_at),
                        closed_at = VALUES(closed_at)
                    """,
                    (
                        _to_int(row.get("trade_id"), 0),
                        str(row.get("symbol") or self.symbol),
                        str(row.get("status") or "closed"),
                        str(row.get("direction") or ""),
                        _to_int(row.get("entry_ts"), 0),
                        _to_float(row.get("entry_price"), 0.0),
                        _to_int(row.get("planned_exit_ts"), 0),
                        _to_int(row.get("hold_seconds"), self.hold_seconds),
                        json.dumps(row.get("signal") if isinstance(row.get("signal"), dict) else {}),
                        (
                            _to_int(row.get("exit_ts"), 0)
                            if row.get("exit_ts") is not None
                            else None
                        ),
                        (
                            _to_float(row.get("exit_price"), 0.0)
                            if row.get("exit_price") is not None
                            else None
                        ),
                        (
                            _to_int(row.get("holding_seconds"), 0)
                            if row.get("holding_seconds") is not None
                            else None
                        ),
                        _to_float(row.get("points"), 0.0),
                        str(row.get("outcome") or "flat"),
                        str(row.get("close_reason") or ""),
                        _to_int(row.get("entry_ts"), int(time.time())),
                        (
                            _to_int(row.get("exit_ts"), 0)
                            if row.get("exit_ts") is not None
                            else None
                        ),
                    ),
                )

            self._persisted_closed_count = len(self._trades)

            cur.execute(
                f"""
                INSERT INTO `{self._mysql_state_table}` (
                    id, symbol, hold_seconds, min_confidence, entry_guidance,
                    one_trade_at_a_time, last_candle_ts, last_price,
                    active_trade_json, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    symbol = VALUES(symbol),
                    hold_seconds = VALUES(hold_seconds),
                    min_confidence = VALUES(min_confidence),
                    entry_guidance = VALUES(entry_guidance),
                    one_trade_at_a_time = VALUES(one_trade_at_a_time),
                    last_candle_ts = VALUES(last_candle_ts),
                    last_price = VALUES(last_price),
                    active_trade_json = VALUES(active_trade_json),
                    updated_at = VALUES(updated_at)
                """,
                (
                    1,
                    self.symbol,
                    int(self.hold_seconds),
                    float(self.min_confidence),
                    self.entry_guidance,
                    1 if self.one_trade_at_a_time else 0,
                    int(self._last_candle_ts),
                    float(self._last_price),
                    json.dumps(self._active_trade) if self._active_trade is not None else None,
                    int(time.time()),
                ),
            )
            conn.commit()
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    @staticmethod
    def _direction_from_features(features: Dict[str, Any]) -> int:
        return int(round(_to_float((features or {}).get("current_direction"), 0.0)))

    @staticmethod
    def _is_compression(features: Dict[str, Any]) -> bool:
        overlap = _to_float((features or {}).get("overlap_ratio"), 0.0)
        expansion = _to_float((features or {}).get("range_expansion"), 0.0)
        speed = _to_float((features or {}).get("candle_speed"), 0.0)
        wick = _to_float((features or {}).get("wick_dominance"), 0.0)
        return bool(overlap >= 0.58 and expansion <= 1.08 and speed <= 1.06 and wick >= 0.45)

    @staticmethod
    def _is_displacement(features: Dict[str, Any]) -> bool:
        expansion = _to_float((features or {}).get("range_expansion"), 0.0)
        speed = _to_float((features or {}).get("candle_speed"), 0.0)
        wick = _to_float((features or {}).get("wick_dominance"), 1.0)
        return bool(expansion >= 1.30 and speed >= 1.20 and wick <= 0.72)

    def _is_noon_chop(self, ts: int) -> bool:
        if not self.avoid_noon_chop:
            return False
        moment = datetime.fromtimestamp(int(ts), tz=_IST)
        minutes = int(moment.hour) * 60 + int(moment.minute)
        # Avoid typical midday chop where option buyers often get theta-trapped.
        return 12 * 60 <= minutes <= 13 * 60 + 15

    @staticmethod
    def _ist_minutes(ts: int) -> int:
        moment = datetime.fromtimestamp(int(ts), tz=_IST)
        return int(moment.hour) * 60 + int(moment.minute)

    def _entry_cutoff_reached(self, ts: int) -> bool:
        # No new entries from 2:45 PM IST onward.
        return self._ist_minutes(ts) >= (14 * 60 + 45)

    def _forced_squareoff_reached(self, ts: int) -> bool:
        # Hard square-off at/after 3:20 PM IST.
        return self._ist_minutes(ts) >= (15 * 60 + 20)

    def _push_recent_candle(self, candle: Dict[str, Any]) -> None:
        self._recent_candles.append(
            {
                "timestamp": _to_int(candle.get("timestamp"), 0),
                "open": _to_float(candle.get("open"), 0.0),
                "high": _to_float(candle.get("high"), 0.0),
                "low": _to_float(candle.get("low"), 0.0),
                "close": _to_float(candle.get("close"), 0.0),
                "volume": _to_float(candle.get("volume"), 0.0),
            }
        )

    @staticmethod
    def _regime_tag(features: Dict[str, Any]) -> str:
        speed = _to_float((features or {}).get("candle_speed"), 0.0)
        expansion = _to_float((features or {}).get("range_expansion"), 0.0)
        overlap = _to_float((features or {}).get("overlap_ratio"), 0.0)
        if speed >= 1.25:
            speed_tag = "fast"
        elif speed <= 0.9:
            speed_tag = "slow"
        else:
            speed_tag = "normal"
        if expansion >= 1.25:
            range_tag = "wide"
        elif expansion <= 0.95:
            range_tag = "tight"
        else:
            range_tag = "normal"
        if overlap >= 0.58:
            overlap_tag = "compressed"
        elif overlap <= 0.40:
            overlap_tag = "clean"
        else:
            overlap_tag = "mixed"
        return f"{speed_tag}_{range_tag}_{overlap_tag}"

    def _feedback_key(self, *, pain_phase: str, next_group: str, direction: str, regime_tag: str) -> str:
        phase_text = str(pain_phase or "comfort").strip().lower() or "comfort"
        next_text = str(next_group or "none").strip().lower() or "none"
        dir_text = str(direction or "").strip().upper()
        regime_text = str(regime_tag or "neutral").strip().lower() or "neutral"
        return f"{phase_text}|{next_text}|{dir_text}|{regime_text}"

    @staticmethod
    def _new_prediction_id(ts: int) -> str:
        return f"pred_{max(0, _to_int(ts, 0))}_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _is_expiry_day_ist(ts: int) -> bool:
        if _to_int(ts, 0) <= 0:
            return False
        try:
            moment = datetime.fromtimestamp(int(ts), tz=_IST)
        except Exception:
            return False
        # Weekly expiry (indices) is typically Thursday.
        return int(moment.weekday()) == 3

    @staticmethod
    def _normalize_mistake_type(value: Any) -> str:
        text = str(value or "").strip().lower()
        if text in _MISTAKE_TYPES:
            return text
        return "none"

    @staticmethod
    def _step_sign(value: float) -> int:
        val = float(_to_float(value, 0.0))
        if val > 1e-9:
            return 1
        if val < -1e-9:
            return -1
        return 0

    @staticmethod
    def _expected_sign(next_group: str, direction: str) -> int:
        group = str(next_group or "").strip().lower()
        # Support both old 6-class and new 3-class predictions
        if group in {"put_buyers", "bearish"}:
            return 1
        if group in {"call_buyers", "bullish"}:
            return -1
        direct = str(direction or "").strip().upper()
        if direct == "LONG":
            return 1
        if direct == "SHORT":
            return -1
        return 0

    @staticmethod
    def _score_from_points(points: float, baseline: float) -> float:
        base = max(1.0, abs(float(_to_float(baseline, 1.0))))
        return max(0.0, min(1.0, abs(float(_to_float(points, 0.0))) / base))

    def _classify_mistake_from_trade(self, row: Dict[str, Any]) -> Dict[str, Any]:
        signal = row.get("signal") if isinstance(row.get("signal"), dict) else {}
        existing_type = self._normalize_mistake_type(signal.get("mistake_type"))
        if existing_type != "none":
            return {
                "mistake_type": existing_type,
                "mistake_score": max(0.0, min(1.0, _to_float(signal.get("mistake_score"), 0.0))),
                "mistake_reason": str(signal.get("mistake_reason") or "").strip(),
                "flags": signal.get("mistake_flags") if isinstance(signal.get("mistake_flags"), dict) else {},
            }

        entry_ts = _to_int(row.get("entry_ts"), 0)
        exit_ts = _to_int(row.get("exit_ts"), 0)
        holding = _to_int(row.get("holding_seconds"), max(0, exit_ts - entry_ts))
        points = _to_float(row.get("points"), 0.0)
        close_reason = str(row.get("close_reason") or "").strip().lower()
        direction = str(row.get("direction") or "").strip().upper()
        next_group = str(signal.get("next_likely_pain_group") or "").strip().lower()
        regime_tag = str(signal.get("regime_tag") or "").strip().lower()

        candles = list(self._recent_candles)
        ts_values = [_to_int(item.get("timestamp"), 0) for item in candles]
        idx_entry_left = bisect_left(ts_values, entry_ts)
        idx_entry_right = bisect_right(ts_values, entry_ts)
        pre20 = candles[max(0, idx_entry_left - 20) : idx_entry_left]
        pre5 = candles[max(0, idx_entry_left - 5) : idx_entry_left]
        post5 = candles[idx_entry_right : min(len(candles), idx_entry_right + 5)]

        entry_ref = _to_float(row.get("underlying_entry_price"), _to_float(row.get("entry_price"), 0.0))
        if entry_ref <= 0.0 and pre5:
            entry_ref = _to_float(pre5[-1].get("close"), 0.0)

        atr_prev = 0.0
        if pre20:
            atr_prev = sum(
                max(0.0, _to_float(item.get("high"), 0.0) - _to_float(item.get("low"), 0.0))
                for item in pre20
            ) / float(max(1, len(pre20)))
        if atr_prev <= 0.0:
            atr_prev = max(5.0, abs(entry_ref) * 0.0005 if entry_ref > 0.0 else 5.0)

        expected_sign = self._expected_sign(next_group, direction)
        pre_move = 0.0
        if pre5 and entry_ref > 0.0:
            if expected_sign >= 0:
                pre_move = max(0.0, entry_ref - min(_to_float(item.get("low"), entry_ref) for item in pre5))
            else:
                pre_move = max(0.0, max(_to_float(item.get("high"), entry_ref) for item in pre5) - entry_ref)

        fut_move = 0.0
        opposite_count = 0
        flip_count = 0
        if post5 and entry_ref > 0.0:
            fut_close = _to_float(post5[-1].get("close"), entry_ref)
            fut_move = fut_close - entry_ref
            prev_close = entry_ref
            prev_step = 0
            for item in post5:
                now_close = _to_float(item.get("close"), prev_close)
                step = self._step_sign(now_close - prev_close)
                if expected_sign != 0 and step == -expected_sign:
                    opposite_count += 1
                if prev_step != 0 and step != 0 and step != prev_step:
                    flip_count += 1
                if step != 0:
                    prev_step = step
                prev_close = now_close

        expiry_day = self._is_expiry_day_ist(entry_ts)
        flags = {
            "fast_sl": bool(close_reason == "stop_loss_hit" and 0 < holding <= 5 * 60),
            "range_regime": bool("range" in regime_tag or "chop" in regime_tag or "compress" in regime_tag),
            "expiry_day": bool(expiry_day),
            "opposite_drift": bool(expected_sign != 0 and (expected_sign * fut_move) < 0.0),
            "late_chase": bool(pre_move >= max(1.5 * atr_prev, 10.0)),
        }

        mistake_type = "none"
        mistake_reason = ""
        if flags["fast_sl"]:
            mistake_type = "false_entry"
            mistake_reason = f"stop_loss_hit in {holding}s"
            score = self._score_from_points(points, max(5.0, 0.8 * atr_prev))
        elif (
            flags["expiry_day"]
            and points < 0.0
            and abs(fut_move) <= max(0.5 * atr_prev, 6.0)
        ):
            mistake_type = "expiry_noise"
            mistake_reason = f"expiry_day low follow-through move={fut_move:.2f}"
            score = self._score_from_points(points, max(5.0, 0.6 * atr_prev))
        elif (
            points <= 0.0
            and (
                flags["range_regime"]
                or close_reason in {"compression_return", "speed_fade"}
                or (close_reason == "time_exit" and flip_count >= 2)
            )
        ):
            mistake_type = "chop_zone"
            mistake_reason = f"range/compression trap reason={close_reason}; flips={flip_count}"
            score = self._score_from_points(points, max(6.0, atr_prev))
        elif (
            points < 0.0
            and expected_sign != 0
            and (expected_sign * fut_move) < -max(0.35 * atr_prev, 6.0)
            and opposite_count >= 3
        ):
            mistake_type = "wrong_side_pain"
            mistake_reason = f"opposite drift in first bars; move={fut_move:.2f}"
            score = self._score_from_points(points, max(6.0, atr_prev))
        elif points <= 0.0 and flags["late_chase"]:
            mistake_type = "late_entry"
            mistake_reason = f"pre-move already extended pre={pre_move:.2f} atr={atr_prev:.2f}"
            score = self._score_from_points(points, max(6.0, atr_prev))
        else:
            score = 0.0

        return {
            "mistake_type": mistake_type,
            "mistake_score": float(max(0.0, min(1.0, score))),
            "mistake_reason": str(mistake_reason),
            "flags": flags,
        }

    def _feedback_context(self, ai_state: Dict[str, Any], direction: str, features: Dict[str, Any]) -> Dict[str, str]:
        pain_phase = str(ai_state.get("pain_phase", "comfort") or "comfort")
        next_group = str(ai_state.get("next_likely_pain_group", "none") or "none")
        regime_tag = self._regime_tag(features)
        feedback_key = self._feedback_key(
            pain_phase=pain_phase,
            next_group=next_group,
            direction=direction,
            regime_tag=regime_tag,
        )
        return {
            "feedback_key": feedback_key,
            "pain_phase": pain_phase,
            "next_group": next_group,
            "direction": str(direction or "").upper(),
            "regime_tag": regime_tag,
        }

    def _feedback_adjustment(
        self,
        *,
        ai_state: Dict[str, Any],
        direction: str,
        confidence: float,
        features: Dict[str, Any],
    ) -> tuple[float, str, Dict[str, Any]]:
        decision: Dict[str, Any] = {
            "applied": False,
            "reason": "disabled",
            "confidence_in": float(confidence),
            "confidence_out": float(confidence),
            "direction_in": str(direction or "").upper(),
            "direction_out": str(direction or "").upper(),
        }
        if not self.feedback_enabled:
            return confidence, direction, decision
        if direction not in {"LONG", "SHORT"}:
            decision["reason"] = "no_direction"
            return confidence, direction, decision

        context = self._feedback_context(ai_state, direction, features)
        decision.update(context)
        stats = self._feedback_stats.get(context["feedback_key"])
        if not isinstance(stats, dict):
            decision["reason"] = "no_history"
            return confidence, direction, decision

        trades_count = max(0, _to_int(stats.get("trades_count"), 0))
        if trades_count < self.feedback_min_trades:
            decision["reason"] = "insufficient_history"
            decision["trades_count"] = trades_count
            return confidence, direction, decision

        wins = max(0, _to_int(stats.get("wins_count"), 0))
        losses = max(0, _to_int(stats.get("losses_count"), 0))
        net_points = float(_to_float(stats.get("net_points"), 0.0))
        avg_points = float(_to_float(stats.get("avg_points"), 0.0))
        win_rate = float(wins) / float(max(1, trades_count))
        decision.update(
            {
                "trades_count": trades_count,
                "wins_count": wins,
                "losses_count": losses,
                "net_points": net_points,
                "avg_points": avg_points,
                "win_rate": win_rate,
            }
        )

        next_direction = direction
        multiplier = 1.0
        reason = "neutral"
        if (
            win_rate < self.feedback_block_win_rate
            and avg_points < 0.0
            and losses >= max(3, wins + 1)
            and trades_count >= self.feedback_min_trades + 3
        ):
            next_direction = ""
            multiplier = self.feedback_penalty_floor
            reason = "blocked_weak_setup"
        elif win_rate < 0.45 and avg_points < 0.0:
            gap = min(0.35, max(0.0, 0.45 - win_rate))
            penalty = 1.0 - (gap * 1.25)
            multiplier = max(self.feedback_penalty_floor, penalty)
            reason = "penalized_after_losses"
        elif win_rate > 0.62 and avg_points > 0.0:
            bonus = min(self.feedback_reward_cap - 1.0, max(0.0, win_rate - 0.62) * 0.35)
            multiplier = min(self.feedback_reward_cap, 1.0 + bonus)
            reason = "rewarded_after_wins"

        adjusted_confidence = max(0.0, min(1.0, float(confidence) * float(multiplier)))
        decision["applied"] = bool(reason != "neutral")
        decision["reason"] = reason
        decision["multiplier"] = float(multiplier)
        decision["confidence_out"] = adjusted_confidence
        decision["direction_out"] = next_direction
        return adjusted_confidence, next_direction, decision

    def _persist_feedback_row(self, row: Dict[str, Any]) -> None:
        if self._storage_backend != "mysql":
            return
        conn = None
        cur = None
        try:
            conn = self._connect_mysql(with_database=True)
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO `{self._mysql_feedback_table}` (
                    feedback_key,
                    pain_phase,
                    next_group,
                    direction,
                    regime_tag,
                    trades_count,
                    wins_count,
                    losses_count,
                    flat_count,
                    net_points,
                    avg_points,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    pain_phase = VALUES(pain_phase),
                    next_group = VALUES(next_group),
                    direction = VALUES(direction),
                    regime_tag = VALUES(regime_tag),
                    trades_count = VALUES(trades_count),
                    wins_count = VALUES(wins_count),
                    losses_count = VALUES(losses_count),
                    flat_count = VALUES(flat_count),
                    net_points = VALUES(net_points),
                    avg_points = VALUES(avg_points),
                    updated_at = VALUES(updated_at)
                """,
                (
                    str(row.get("feedback_key") or ""),
                    str(row.get("pain_phase") or "comfort"),
                    str(row.get("next_group") or "none"),
                    str(row.get("direction") or ""),
                    str(row.get("regime_tag") or "neutral"),
                    max(0, _to_int(row.get("trades_count"), 0)),
                    max(0, _to_int(row.get("wins_count"), 0)),
                    max(0, _to_int(row.get("losses_count"), 0)),
                    max(0, _to_int(row.get("flat_count"), 0)),
                    float(_to_float(row.get("net_points"), 0.0)),
                    float(_to_float(row.get("avg_points"), 0.0)),
                    int(time.time()),
                ),
            )
            conn.commit()
        except Exception:
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

    def _persist_mistake_row(self, row: Dict[str, Any]) -> None:
        if self._storage_backend != "mysql":
            return
        conn = None
        cur = None
        try:
            conn = self._connect_mysql(with_database=True)
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO `{self._mysql_mistakes_table}` (
                    trade_id,
                    prediction_id,
                    prediction_ts,
                    entry_ts,
                    exit_ts,
                    holding_seconds,
                    direction,
                    outcome,
                    close_reason,
                    points,
                    mistake_type,
                    mistake_score,
                    mistake_reason,
                    mistake_flags_json,
                    pain_phase,
                    dominant_pain_group,
                    next_likely_pain_group,
                    guidance,
                    confidence,
                    entry_trigger_confidence,
                    stop_loss_distance,
                    target_level,
                    regime_tag,
                    created_at,
                    updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    prediction_id = VALUES(prediction_id),
                    prediction_ts = VALUES(prediction_ts),
                    entry_ts = VALUES(entry_ts),
                    exit_ts = VALUES(exit_ts),
                    holding_seconds = VALUES(holding_seconds),
                    direction = VALUES(direction),
                    outcome = VALUES(outcome),
                    close_reason = VALUES(close_reason),
                    points = VALUES(points),
                    mistake_type = VALUES(mistake_type),
                    mistake_score = VALUES(mistake_score),
                    mistake_reason = VALUES(mistake_reason),
                    mistake_flags_json = VALUES(mistake_flags_json),
                    pain_phase = VALUES(pain_phase),
                    dominant_pain_group = VALUES(dominant_pain_group),
                    next_likely_pain_group = VALUES(next_likely_pain_group),
                    guidance = VALUES(guidance),
                    confidence = VALUES(confidence),
                    entry_trigger_confidence = VALUES(entry_trigger_confidence),
                    stop_loss_distance = VALUES(stop_loss_distance),
                    target_level = VALUES(target_level),
                    regime_tag = VALUES(regime_tag),
                    created_at = VALUES(created_at),
                    updated_at = VALUES(updated_at)
                """,
                (
                    _to_int(row.get("trade_id"), 0),
                    str(row.get("prediction_id") or "").strip() or None,
                    _to_int(row.get("prediction_ts"), 0),
                    _to_int(row.get("entry_ts"), 0),
                    _to_int(row.get("exit_ts"), 0),
                    max(0, _to_int(row.get("holding_seconds"), 0)),
                    str(row.get("direction") or ""),
                    str(row.get("outcome") or ""),
                    str(row.get("close_reason") or ""),
                    _to_float(row.get("points"), 0.0),
                    self._normalize_mistake_type(row.get("mistake_type")),
                    max(0.0, min(1.0, _to_float(row.get("mistake_score"), 0.0))),
                    str(row.get("mistake_reason") or "")[:255] if row.get("mistake_reason") else None,
                    json.dumps(row.get("mistake_flags") if isinstance(row.get("mistake_flags"), dict) else {}, ensure_ascii=True),
                    str(row.get("pain_phase") or ""),
                    str(row.get("dominant_pain_group") or ""),
                    str(row.get("next_likely_pain_group") or ""),
                    str(row.get("guidance") or ""),
                    _to_float(row.get("confidence"), 0.0),
                    _to_float(row.get("entry_trigger_confidence"), 0.0),
                    _to_float(row.get("stop_loss_distance"), 0.0),
                    _to_float(row.get("target_level"), 0.0),
                    str(row.get("regime_tag") or ""),
                    _to_int(row.get("created_at"), int(time.time())),
                    int(time.time()),
                ),
            )
            conn.commit()
        except Exception:
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

    def _record_feedback_from_trade(self, row: Dict[str, Any]) -> None:
        if not self.feedback_enabled:
            return
        signal = row.get("signal") if isinstance(row.get("signal"), dict) else {}
        feedback_key = str(signal.get("feedback_key") or "").strip()
        if not feedback_key:
            return
        direction = str(row.get("direction") or signal.get("release_direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            return
        pain_phase = str(signal.get("pain_phase") or "comfort")
        next_group = str(signal.get("next_likely_pain_group") or "none")
        regime_tag = str(signal.get("regime_tag") or "neutral")
        outcome = str(row.get("outcome") or "flat").strip().lower()
        points = float(_to_float(row.get("points"), 0.0))

        stats = self._feedback_stats.get(feedback_key)
        if not isinstance(stats, dict):
            stats = {
                "feedback_key": feedback_key,
                "pain_phase": pain_phase,
                "next_group": next_group,
                "direction": direction,
                "regime_tag": regime_tag,
                "trades_count": 0,
                "wins_count": 0,
                "losses_count": 0,
                "flat_count": 0,
                "net_points": 0.0,
                "avg_points": 0.0,
            }
            self._feedback_stats[feedback_key] = stats
        stats["trades_count"] = max(0, _to_int(stats.get("trades_count"), 0)) + 1
        if outcome == "profit":
            stats["wins_count"] = max(0, _to_int(stats.get("wins_count"), 0)) + 1
        elif outcome == "loss":
            stats["losses_count"] = max(0, _to_int(stats.get("losses_count"), 0)) + 1
        else:
            stats["flat_count"] = max(0, _to_int(stats.get("flat_count"), 0)) + 1
        stats["net_points"] = float(_to_float(stats.get("net_points"), 0.0)) + points
        trades_count = max(1, _to_int(stats.get("trades_count"), 1))
        stats["avg_points"] = float(stats["net_points"]) / float(trades_count)
        stats["pain_phase"] = pain_phase
        stats["next_group"] = next_group
        stats["direction"] = direction
        stats["regime_tag"] = regime_tag
        self._persist_feedback_row(stats)

    def _entry_levels(
        self,
        *,
        direction: str,
        entry_price: float,
        origin_low: float,
        origin_high: float,
        ai_state: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        if self.model_driven_execution:
            ai_levels = self._entry_levels_from_ai(
                direction=direction,
                entry_price=entry_price,
                ai_state=ai_state if isinstance(ai_state, dict) else {},
            )
            if ai_levels is not None:
                return ai_levels

        recent = list(self._recent_candles)
        lookback = recent[-30:] if len(recent) > 30 else recent
        recent_high = max((_to_float(row.get("high"), entry_price) for row in lookback), default=entry_price)
        recent_low = min((_to_float(row.get("low"), entry_price) for row in lookback), default=entry_price)

        candle_range = max(0.5, origin_high - origin_low)
        buffer_pts = max(0.25, candle_range * 0.08)
        if direction == "LONG":
            stop_loss = origin_low - buffer_pts
            risk = max(0.5, entry_price - stop_loss)
            r_target = entry_price + risk * self.target_r_multiple
            pocket_target = max(r_target, recent_high)
            target_price = max(entry_price + 0.5, pocket_target)
        else:
            stop_loss = origin_high + buffer_pts
            risk = max(0.5, stop_loss - entry_price)
            r_target = entry_price - risk * self.target_r_multiple
            pocket_target = min(r_target, recent_low)
            target_price = min(entry_price - 0.5, pocket_target)

        return {
            "stop_loss": float(stop_loss),
            "target_price": float(target_price),
            "risk_points": float(risk),
        }

    def _entry_levels_from_ai(
        self,
        *,
        direction: str,
        entry_price: float,
        ai_state: Dict[str, Any],
    ) -> Optional[Dict[str, float]]:
        if direction not in {"LONG", "SHORT"}:
            return None

        stop_loss = _to_float(ai_state.get("recommended_stop_loss"), 0.0)
        target_price = 0.0
        raw_targets = ai_state.get("recommended_targets")
        targets: List[float] = []
        if isinstance(raw_targets, list):
            targets = [float(_to_float(item, 0.0)) for item in raw_targets if _to_float(item, 0.0) > 0.0]

        if direction == "LONG":
            valid_targets = sorted(item for item in targets if item > entry_price)
            if valid_targets:
                target_price = float(valid_targets[0])
        else:
            valid_targets = sorted((item for item in targets if item < entry_price), reverse=True)
            if valid_targets:
                target_price = float(valid_targets[0])

        stop_distance = max(0.0, _to_float(ai_state.get("stop_loss_distance"), 0.0))
        target_distance = max(0.0, _to_float(ai_state.get("target_level"), 0.0))

        if direction == "LONG":
            if stop_loss <= 0.0 or stop_loss >= entry_price:
                if stop_distance > 0.0:
                    stop_loss = entry_price - stop_distance
            if target_price <= entry_price and target_distance > 0.0:
                target_price = entry_price + target_distance
        else:
            if stop_loss <= entry_price:
                if stop_distance > 0.0:
                    stop_loss = entry_price + stop_distance
            if target_price >= entry_price or target_price <= 0.0:
                if target_distance > 0.0:
                    target_price = max(0.0, entry_price - target_distance)

        risk = abs(entry_price - stop_loss)
        if stop_loss <= 0.0 or risk <= 0.0:
            return None
        if (direction == "LONG" and target_price <= entry_price) or (direction == "SHORT" and target_price >= entry_price):
            return None

        return {
            "stop_loss": float(stop_loss),
            "target_price": float(target_price),
            "risk_points": float(risk),
        }

    def _build_release_signal(
        self,
        *,
        ts: int,
        candle: Dict[str, Any],
        ai_state: Dict[str, Any],
        option_snapshot: Optional[Dict[str, Any]],
        direction: str,
        confidence: float,
        guidance: str,
        compression_bars: int,
    ) -> Dict[str, Any]:
        release_open = _to_float(candle.get("open"), _to_float(candle.get("close"), 0.0))
        release_close = _to_float(candle.get("close"), release_open)
        release_high = _to_float(candle.get("high"), max(release_open, release_close))
        release_low = _to_float(candle.get("low"), min(release_open, release_close))
        release_range = max(0.01, release_high - release_low)
        features = ai_state.get("features") if isinstance(ai_state.get("features"), dict) else {}
        feedback_ctx = self._feedback_context(ai_state, direction, features)
        selected_option = self._select_option_contract(
            direction=direction,
            spot_price=release_close,
            confidence=confidence,
            ai_state=ai_state,
            option_snapshot=option_snapshot,
        )
        return {
            "prediction_id": self._new_prediction_id(ts),
            "release_ts": int(ts),
            "release_direction": direction,
            "release_open": float(release_open),
            "release_close": float(release_close),
            "release_high": float(release_high),
            "release_low": float(release_low),
            "release_range": float(release_range),
            "compression_bars": int(compression_bars),
            "pain_phase": str(ai_state.get("pain_phase", "")),
            "dominant_pain_group": str(ai_state.get("dominant_pain_group", "")),
            "next_likely_pain_group": str(ai_state.get("next_likely_pain_group", "")),
            "guidance": guidance,
            "confidence": float(confidence),
            "entry_signal": bool(ai_state.get("entry_signal", False)),
            "entry_direction": str(ai_state.get("entry_direction", "NONE")).upper(),
            "entry_trigger_confidence": float(_to_float(ai_state.get("entry_trigger_confidence"), confidence)),
            "predicted_mistake_type": str(ai_state.get("mistake_type", "none") or "none").strip().lower(),
            "predicted_mistake_confidence": float(_to_float(ai_state.get("mistake_confidence"), 0.0)),
            "stop_loss_distance": float(_to_float(ai_state.get("stop_loss_distance"), 0.0)),
            "target_level": float(_to_float(ai_state.get("target_level"), 0.0)),
            "recommended_stop_loss": float(_to_float(ai_state.get("recommended_stop_loss"), 0.0)),
            "recommended_targets": list(ai_state.get("recommended_targets") or []),
            "pullback_seen": False,
            "regime_tag": feedback_ctx["regime_tag"],
            "feedback_key": feedback_ctx["feedback_key"],
            "selected_option": selected_option,
        }

    def _fallback_step_for_symbol(self) -> float:
        text = str(self.symbol or "").upper()
        if "BANKNIFTY" in text:
            return 100.0
        if "MIDCPNIFTY" in text:
            return 25.0
        if "FINNIFTY" in text:
            return 50.0
        if "NIFTY" in text:
            return 50.0
        return 50.0

    @staticmethod
    def _infer_step_from_strikes(values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        ordered = sorted(set(float(item) for item in values))
        diffs = [ordered[idx + 1] - ordered[idx] for idx in range(len(ordered) - 1)]
        positive = [item for item in diffs if item > 0]
        if not positive:
            return 0.0
        return min(positive)

    def _select_option_contract(
        self,
        *,
        direction: str,
        spot_price: float,
        confidence: float,
        ai_state: Dict[str, Any],
        option_snapshot: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        side = "CE" if str(direction).upper() == "LONG" else "PE"
        snapshot = option_snapshot if isinstance(option_snapshot, dict) else {}
        raw_atm = _to_float(snapshot.get("atm_strike"), 0.0)
        strikes = snapshot.get("strikes")
        snapshot_ts = _to_int(snapshot.get("timestamp"), 0)
        contract_rows: List[Dict[str, Any]] = []
        strike_values: List[float] = []
        if isinstance(strikes, list):
            for row in strikes:
                if not isinstance(row, dict):
                    continue
                contract = extract_option_contract(
                    row,
                    snapshot_ts=snapshot_ts,
                    explicit_option_type=str(row.get("option_type") or ""),
                    explicit_strike=_to_float(row.get("strike"), 0.0),
                )
                tradeable, tradeable_reason = contract_tradeability(
                    contract,
                    reference_ts=snapshot_ts,
                    allow_expiry_day=True,
                )
                enriched = dict(row)
                enriched["_contract"] = contract
                enriched["_tradeable"] = bool(tradeable)
                enriched["_tradeable_reason"] = str(tradeable_reason or "")
                contract_rows.append(enriched)
                strike = _to_float(row.get("strike"), 0.0)
                if strike > 0.0 and tradeable:
                    strike_values.append(strike)
        if not strike_values and contract_rows:
            for row in contract_rows:
                strike = _to_float(row.get("strike"), 0.0)
                if strike > 0.0:
                    strike_values.append(strike)

        strike_step = self._infer_step_from_strikes(strike_values)
        if strike_step <= 0.0:
            strike_step = self._fallback_step_for_symbol()

        if raw_atm > 0.0:
            atm_strike = raw_atm
        else:
            atm_strike = round(float(spot_price) / strike_step) * strike_step

        features = ai_state.get("features") if isinstance(ai_state.get("features"), dict) else {}
        speed = _to_float(features.get("candle_speed"), 0.0)
        expansion = _to_float(features.get("range_expansion"), 0.0)
        style = "ATM"
        offset = 0
        if self.entry_style == "conservative" or confidence < 0.62:
            style = "ATM"
            offset = 0
        elif confidence >= 0.78 and speed >= 1.35 and expansion >= 1.45:
            style = "OTM_1"
            offset = 1
        else:
            style = "ITM_1"
            offset = -1

        side_sign = 1 if side == "CE" else -1
        selected_strike = float(atm_strike + float(offset) * float(side_sign) * float(strike_step))
        if strike_values:
            selected_strike = min(strike_values, key=lambda row: abs(row - selected_strike))

        ltp_value = 0.0
        selected_symbol = ""
        selected_symbol_token = ""
        selected_exchange = ""
        selected_underlying = ""
        selected_expiry_date = ""
        selected_expiry_ts = 0
        selected_expiry_kind = ""
        selected_contract_key = ""
        selected_tradeable = False
        selected_reject_reason = ""
        if side == "CE":
            if abs(selected_strike - atm_strike) <= 1e-9:
                ltp_value = _to_float(snapshot.get("atm_ce_ltp"), 0.0)
        else:
            if abs(selected_strike - atm_strike) <= 1e-9:
                ltp_value = _to_float(snapshot.get("atm_pe_ltp"), 0.0)
        candidate_pool = [
            row
            for row in contract_rows
            if str(row.get("option_type", "")).upper() == side
            and abs(_to_float(row.get("strike"), 0.0) - selected_strike) <= 1e-9
            and bool(row.get("_tradeable"))
        ]
        if not candidate_pool:
            candidate_pool = [
                row
                for row in contract_rows
                if str(row.get("option_type", "")).upper() == side
                and abs(_to_float(row.get("strike"), 0.0) - selected_strike) <= 1e-9
            ]
        if candidate_pool:
            best_row = min(
                candidate_pool,
                key=lambda row: (
                    0 if bool(row.get("_tradeable")) else 1,
                    contract_days_to_expiry(
                        row.get("_contract") if isinstance(row.get("_contract"), dict) else {},
                        reference_ts=snapshot_ts,
                    ),
                    0 if str((row.get("_contract") or {}).get("symbol_token") or "").strip() else 1,
                    0 if str((row.get("_contract") or {}).get("symbol") or "").strip() else 1,
                ),
            )
            contract = best_row.get("_contract") if isinstance(best_row.get("_contract"), dict) else {}
            ltp_value = _to_float(best_row.get("ltp"), ltp_value)
            selected_symbol = str(contract.get("symbol") or best_row.get("symbol") or "").strip()
            selected_symbol_token = str(
                contract.get("symbol_token")
                or best_row.get("symbol_token")
                or best_row.get("symboltoken")
                or best_row.get("token")
                or best_row.get("fy_token")
                or ""
            ).strip()
            selected_exchange = str(contract.get("exchange") or best_row.get("exchange") or "").strip().upper()
            selected_underlying = str(contract.get("underlying") or "").strip().upper()
            selected_expiry_date = str(contract.get("expiry_date") or "").strip()
            selected_expiry_ts = _to_int(contract.get("expiry_ts"), 0)
            selected_expiry_kind = str(contract.get("expiry_kind") or "").strip().lower()
            selected_contract_key = str(contract.get("contract_key") or "").strip()
            selected_tradeable = bool(best_row.get("_tradeable"))
            selected_reject_reason = str(best_row.get("_tradeable_reason") or "")

        return {
            "side": side,
            "strike": float(selected_strike),
            "atm_strike": float(atm_strike),
            "step": float(strike_step),
            "style": style,
            "symbol": selected_symbol,
            "symbol_token": selected_symbol_token,
            "exchange": selected_exchange,
            "underlying": selected_underlying,
            "expiry_date": selected_expiry_date,
            "expiry_ts": int(selected_expiry_ts),
            "expiry_kind": selected_expiry_kind,
            "contract_key": selected_contract_key,
            "tradeable": bool(selected_tradeable),
            "reject_reason": selected_reject_reason,
            "ltp_ref": float(ltp_value),
            "snapshot_ts": _to_int(snapshot.get("timestamp"), 0),
            "source": "option_snapshot" if snapshot else "spot_fallback",
        }

    def _conservative_entry_ready(
        self,
        *,
        pending: Dict[str, Any],
        candle: Dict[str, Any],
        features: Dict[str, Any],
        direction: str,
    ) -> bool:
        sign = 1 if direction == "LONG" else -1
        close_price = _to_float(candle.get("close"), 0.0)
        release_close = _to_float(pending.get("release_close"), close_price)
        release_range = max(0.01, _to_float(pending.get("release_range"), 0.01))
        release_low = _to_float(pending.get("release_low"), close_price)
        release_high = _to_float(pending.get("release_high"), close_price)
        current_dir = self._direction_from_features(features)
        displacement_now = self._is_displacement(features)

        if not bool(pending.get("pullback_seen", False)):
            if sign > 0:
                retrace = close_price <= (release_close - 0.2 * release_range) and close_price >= release_low
            else:
                retrace = close_price >= (release_close + 0.2 * release_range) and close_price <= release_high
            if retrace or (current_dir != 0 and current_dir == -sign):
                pending["pullback_seen"] = True
            return False

        return bool(displacement_now and current_dir == sign)

    def _enter_trade(
        self,
        *,
        ts: int,
        close_price: float,
        direction: str,
        signal: Dict[str, Any],
        origin_low: float,
        origin_high: float,
        ai_state: Optional[Dict[str, Any]] = None,
    ) -> bool:
        levels = self._entry_levels(
            direction=direction,
            entry_price=close_price,
            origin_low=origin_low,
            origin_high=origin_high,
            ai_state=ai_state if isinstance(ai_state, dict) else {},
        )
        candle_ts = _to_int(ts, 0)
        now_ts = int(time.time())
        # Use runtime wall-clock for near-real-time execution timestamps so
        # paper/live latency reflects actual processing time. For historical
        # replay (e.g. backtest), preserve candle-time semantics.
        if candle_ts > 0 and abs(now_ts - candle_ts) <= 300:
            entry_ts = now_ts
        elif candle_ts > 0:
            entry_ts = candle_ts
        else:
            entry_ts = max(1, now_ts)
            candle_ts = entry_ts
        selected = signal.get("selected_option") if isinstance(signal.get("selected_option"), dict) else {}
        option_symbol = str(selected.get("symbol") or "").strip()
        option_token = str(
            selected.get("symbol_token")
            or selected.get("symboltoken")
            or selected.get("token")
            or ""
        ).strip()
        option_exchange = str(selected.get("exchange") or "").strip().upper()
        contract_tradeable, contract_reason = contract_tradeability(
            selected,
            reference_ts=candle_ts,
            allow_expiry_day=True,
        )
        if not option_symbol or not contract_tradeable:
            reason = str(contract_reason or "missing_option_symbol")
            self._note_gate("option_contract_blocked", entry_ts, reason)
            return False
        entry_quote = self._resolve_entry_option_quote(
            option_symbol=option_symbol,
            selected_option=selected,
        )
        option_entry_ltp = _to_float(entry_quote.get("ltp"), 0.0)
        option_quote_ts = _to_int(entry_quote.get("timestamp"), 0)
        resolved_symbol = str(entry_quote.get("symbol") or "").strip()
        if resolved_symbol:
            option_symbol = resolved_symbol
        trade_id = len(self._trades) + (1 if self._active_trade else 0) + 1
        self._active_trade = {
            "trade_id": trade_id,
            "symbol": self.symbol,
            "status": "open",
            "direction": direction,
            "entry_ts": entry_ts,
            "signal_ts": candle_ts,
            "entry_price": close_price,
            "planned_exit_ts": entry_ts + self.hold_seconds,
            "hold_seconds": self.hold_seconds,
            "stop_loss": levels["stop_loss"],
            "target_price": levels["target_price"],
            "risk_points": levels["risk_points"],
            "option_symbol": option_symbol,
            "option_token": option_token,
            "option_exchange": option_exchange,
            "option_side": str(selected.get("side") or "").upper(),
            "option_strike": _to_float(selected.get("strike"), 0.0),
            "option_underlying": str(selected.get("underlying") or "").strip().upper(),
            "option_expiry_date": str(selected.get("expiry_date") or "").strip(),
            "option_expiry_ts": _to_int(selected.get("expiry_ts"), 0),
            "option_expiry_kind": str(selected.get("expiry_kind") or "").strip().lower(),
            "option_contract_key": str(selected.get("contract_key") or "").strip(),
            # Entry is locked from the latest available option quote at trade-open time.
            "option_entry_ltp": option_entry_ltp,
            "option_mark_ltp": option_entry_ltp,
            "option_quote_ts": option_quote_ts,
            "signal": dict(signal),
        }
        return True

    def _resolve_entry_option_quote(
        self,
        *,
        option_symbol: str,
        selected_option: Dict[str, Any],
    ) -> Dict[str, Any]:
        fetcher = self._quote_fetcher
        text_symbol = str(option_symbol or "").strip()
        if callable(fetcher) and text_symbol:
            try:
                quote = fetcher(text_symbol)
            except Exception:
                quote = None
            if isinstance(quote, dict):
                mark = _to_float(quote.get("ltp"), 0.0)
                if mark > 0.0:
                    quote_ts = _effective_quote_timestamp(
                        quote.get("timestamp"),
                        received_ts=int(time.time()),
                    )
                    return {
                        "symbol": str(
                            quote.get("resolved_symbol")
                            or quote.get("symbol")
                            or text_symbol
                        ).strip(),
                        "ltp": mark,
                        "timestamp": quote_ts,
                    }

        fallback_ltp = _to_float(selected_option.get("ltp_ref"), 0.0)
        if fallback_ltp > 0.0:
            return {
                "symbol": text_symbol,
                "ltp": fallback_ltp,
                "timestamp": _to_int(selected_option.get("snapshot_ts"), 0),
            }
        return {}

    def _manage_open_trade(self, candle: Dict[str, Any], ai_state: Dict[str, Any]) -> bool:
        if self._active_trade is None:
            return False

        ts = _to_int(candle.get("timestamp"), 0)
        close_price = _to_float(candle.get("close"), 0.0)
        high_price = _to_float(candle.get("high"), close_price)
        low_price = _to_float(candle.get("low"), close_price)
        direction = str(self._active_trade.get("direction", "")).upper()
        if direction not in {"LONG", "SHORT"}:
            return False

        if self._forced_squareoff_reached(ts):
            self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="session_squareoff_1520")
            self._note_gate("forced_squareoff", ts, "candle_cutoff_15_20")
            return True

        stop_loss = _to_float(self._active_trade.get("stop_loss"), 0.0)
        target_price = _to_float(self._active_trade.get("target_price"), 0.0)
        if stop_loss > 0.0 and target_price > 0.0:
            if direction == "LONG":
                if low_price <= stop_loss:
                    self._close_active_trade(exit_ts=ts, exit_price=stop_loss, reason="stop_loss_hit")
                    return True
                if high_price >= target_price:
                    self._close_active_trade(exit_ts=ts, exit_price=target_price, reason="target_hit")
                    return True
            else:
                if high_price >= stop_loss:
                    self._close_active_trade(exit_ts=ts, exit_price=stop_loss, reason="stop_loss_hit")
                    return True
                if low_price <= target_price:
                    self._close_active_trade(exit_ts=ts, exit_price=target_price, reason="target_hit")
                    return True

        if self.model_driven_execution:
            if ts >= _to_int(self._active_trade.get("planned_exit_ts"), 0):
                self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="time_exit")
                return True
            return False

        features = ai_state.get("features") if isinstance(ai_state.get("features"), dict) else {}
        speed = _to_float(features.get("candle_speed"), 0.0)
        overlap = _to_float(features.get("overlap_ratio"), 0.0)
        expansion = _to_float(features.get("range_expansion"), 0.0)
        displacement = self._is_displacement(features)
        current_dir = self._direction_from_features(features)
        sign = 1 if direction == "LONG" else -1

        if speed < 0.95 and overlap >= 0.56:
            self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="speed_fade")
            return True
        if overlap >= 0.62 and expansion <= 1.06:
            self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="compression_return")
            return True
        if displacement and current_dir != 0 and current_dir == -sign:
            self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="opposite_displacement")
            return True
        if ts >= _to_int(self._active_trade.get("planned_exit_ts"), 0):
            self._close_active_trade(exit_ts=ts, exit_price=close_price, reason="time_exit")
            return True
        return False

    def _close_active_trade(self, exit_ts: int, exit_price: float, reason: str) -> Optional[Dict[str, Any]]:
        if self._active_trade is None:
            return None
        active = dict(self._active_trade)
        direction = str(active.get("direction", "")).upper()
        sign = 1.0 if direction == "LONG" else -1.0
        underlying_entry_price = _to_float(active.get("entry_price"), 0.0)
        underlying_exit_price = float(exit_price)
        underlying_points = (underlying_exit_price - underlying_entry_price) * sign

        # Paper execution enters via BUY option and exits via SELL option for both LONG/SHORT views.
        # Closed PnL must be based on option premium only; never fall back to underlying points.
        option_entry_price = _to_float(active.get("option_entry_ltp"), 0.0)
        option_exit_price = _to_float(active.get("option_mark_ltp"), 0.0)
        if option_entry_price <= 0.0 and option_exit_price > 0.0:
            option_entry_price = option_exit_price
        if option_exit_price <= 0.0 and option_entry_price > 0.0:
            option_exit_price = option_entry_price

        entry_price = option_entry_price
        final_exit_price = option_exit_price
        if entry_price > 0.0 and final_exit_price > 0.0:
            points = final_exit_price - entry_price
            pnl_source = "option_quote"
        else:
            points = 0.0
            pnl_source = "option_unavailable"

        outcome = "flat"
        if points > 0:
            outcome = "profit"
        elif points < 0:
            outcome = "loss"
        active.update(
            {
                "status": "closed",
                "exit_ts": int(exit_ts),
                "entry_price": float(entry_price),
                "exit_price": float(final_exit_price),
                "holding_seconds": max(0, int(exit_ts) - _to_int(active.get("entry_ts"), int(exit_ts))),
                "points": float(points),
                "outcome": outcome,
                "close_reason": str(reason),
                "pnl_source": str(pnl_source),
                "underlying_entry_price": float(underlying_entry_price),
                "underlying_exit_price": float(underlying_exit_price),
                "underlying_points": float(underlying_points),
            }
        )
        signal = active.get("signal") if isinstance(active.get("signal"), dict) else {}
        mistake = self._classify_mistake_from_trade(active)
        signal["mistake_type"] = str(mistake.get("mistake_type") or "none")
        signal["mistake_score"] = float(_to_float(mistake.get("mistake_score"), 0.0))
        signal["mistake_reason"] = str(mistake.get("mistake_reason") or "")
        signal["mistake_flags"] = mistake.get("flags") if isinstance(mistake.get("flags"), dict) else {}
        signal["mistake_evaluated_at"] = int(time.time())
        active["signal"] = signal
        self._trades.append(active)
        self._record_feedback_from_trade(active)
        self._persist_mistake_row(
            {
                "trade_id": _to_int(active.get("trade_id"), 0),
                "prediction_id": str(signal.get("prediction_id") or ""),
                "prediction_ts": _to_int(signal.get("release_ts"), _to_int(active.get("entry_ts"), 0)),
                "entry_ts": _to_int(active.get("entry_ts"), 0),
                "exit_ts": _to_int(active.get("exit_ts"), 0),
                "holding_seconds": _to_int(active.get("holding_seconds"), 0),
                "direction": str(active.get("direction") or ""),
                "outcome": str(active.get("outcome") or ""),
                "close_reason": str(active.get("close_reason") or ""),
                "points": _to_float(active.get("points"), 0.0),
                "mistake_type": str(signal.get("mistake_type") or "none"),
                "mistake_score": _to_float(signal.get("mistake_score"), 0.0),
                "mistake_reason": str(signal.get("mistake_reason") or ""),
                "mistake_flags": signal.get("mistake_flags") if isinstance(signal.get("mistake_flags"), dict) else {},
                "pain_phase": str(signal.get("pain_phase") or ""),
                "dominant_pain_group": str(signal.get("dominant_pain_group") or ""),
                "next_likely_pain_group": str(signal.get("next_likely_pain_group") or ""),
                "guidance": str(signal.get("guidance") or ""),
                "confidence": _to_float(signal.get("confidence"), 0.0),
                "entry_trigger_confidence": _to_float(signal.get("entry_trigger_confidence"), 0.0),
                "stop_loss_distance": _to_float(signal.get("stop_loss_distance"), 0.0),
                "target_level": _to_float(signal.get("target_level"), 0.0),
                "regime_tag": str(signal.get("regime_tag") or ""),
                "created_at": _to_int(active.get("entry_ts"), int(time.time())),
            }
        )
        self._active_trade = None
        return active

    def _signal_to_direction(self, next_group: str) -> str:
        text = str(next_group or "").strip().lower()
        # Support both old 6-class and new 3-class predictions
        if text in {"put_buyers", "call_sellers", "bearish"}:
            return "LONG"
        if text in {"call_buyers", "put_sellers", "bullish"}:
            return "SHORT"
        return ""

    def _ai_direction(self, ai_state: Dict[str, Any]) -> str:
        entry_direction = str(ai_state.get("entry_direction", "")).strip().upper()
        if entry_direction in {"LONG", "SHORT"}:
            return entry_direction
        return self._signal_to_direction(str(ai_state.get("next_likely_pain_group", "")))

    def on_candle(
        self,
        candle: Dict[str, Any],
        ai_state: Dict[str, Any],
        option_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.enabled:
            return self.get_state()

        ts = _to_int(candle.get("timestamp"), 0)
        close_price = _to_float(candle.get("close"), 0.0)
        if ts <= 0:
            return self.get_state()

        with self._lock:
            if ts <= self._last_candle_ts:
                return self._state_locked()
            if isinstance(option_snapshot, dict):
                self._last_option_snapshot = dict(option_snapshot)
            self._last_candle_ts = ts
            self._last_price = close_price
            self._push_recent_candle(candle)
            self._note_gate("candles_seen", ts, "candle_processed")
            state_changed = False

            if self._manage_open_trade(candle, ai_state):
                state_changed = True

            if self._active_trade:
                self._note_gate("with_active_trade", ts, "managing_open_trade")
                if state_changed:
                    self._persist()
                return self._state_locked()

            if self._entry_cutoff_reached(ts):
                self._note_gate("entry_cutoff_block", ts, "entry_block_after_14_45_ist")
                if state_changed:
                    self._persist()
                return self._state_locked()

            guidance = str(ai_state.get("guidance", "")).strip().lower()
            confidence = _to_float(ai_state.get("confidence"), 0.0)
            entry_signal = bool(ai_state.get("entry_signal", False))
            trigger_confidence = _to_float(ai_state.get("entry_trigger_confidence"), confidence)
            direction = self._ai_direction(ai_state)
            features = ai_state.get("features") if isinstance(ai_state.get("features"), dict) else {}
            self._last_feedback_decision = {
                "applied": False,
                "reason": "not_evaluated",
                "confidence_in": float(confidence),
                "confidence_out": float(confidence),
                "direction_in": str(direction or "").upper(),
                "direction_out": str(direction or "").upper(),
            }
            prev_compression_count = self._compression_count
            is_compression = self._is_compression(features)
            is_displacement = self._is_displacement(features)
            if is_compression:
                self._compression_count += 1
            else:
                self._compression_count = 0

            if self._pending_release and ts > _to_int(self._pending_release.get("release_ts"), 0) + 6 * 60:
                self._pending_release = None

            if self._is_noon_chop(ts):
                self._note_gate("noon_chop_block", ts, "noon_chop_window")
                if state_changed:
                    self._persist()
                return self._state_locked()

            if guidance == "observe":
                self._note_gate("guidance_observe_block", ts, "observe_is_non_tradable")
                if state_changed:
                    self._persist()
                return self._state_locked()

            if self.model_driven_execution and not entry_signal:
                self._note_gate("entry_signal_off", ts, "model_entry_signal_false")
                if state_changed:
                    self._persist()
                return self._state_locked()

            if not self.model_driven_execution and guidance != self.entry_guidance:
                self._note_gate("guidance_mismatch", ts, f"expected={self.entry_guidance};got={guidance or 'none'}")
            if not direction:
                self._note_gate(
                    "direction_missing",
                    ts,
                    f"entry_direction={str(ai_state.get('entry_direction', 'NONE') or 'NONE')};"
                    f"next_group={str(ai_state.get('next_likely_pain_group', 'none') or 'none')}",
                )
            if (not self.model_driven_execution and guidance != self.entry_guidance) or not direction:
                if state_changed:
                    self._persist()
                return self._state_locked()

            if self.model_driven_execution:
                self._last_feedback_decision = {
                    "applied": False,
                    "reason": "bypassed_model_driven",
                    "confidence_in": float(trigger_confidence),
                    "confidence_out": float(trigger_confidence),
                    "direction_in": str(direction or "").upper(),
                    "direction_out": str(direction or "").upper(),
                }
                confidence = trigger_confidence
            else:
                adjusted_conf, adjusted_direction, feedback_decision = self._feedback_adjustment(
                    ai_state=ai_state,
                    direction=direction,
                    confidence=confidence,
                    features=features,
                )
                self._last_feedback_decision = feedback_decision
                confidence = adjusted_conf
                direction = adjusted_direction

            if not direction:
                self._note_gate(
                    "feedback_block",
                    ts,
                    str(feedback_decision.get("reason") or "direction_removed"),
                )
            if confidence < self.min_confidence or not direction:
                if confidence < self.min_confidence:
                    self._note_gate(
                        "confidence_below_min",
                        ts,
                        f"confidence={confidence:.4f};min={self.min_confidence:.4f}",
                    )
                if state_changed:
                    self._persist()
                return self._state_locked()

            if self._active_trade is None and not self.require_pain_release:
                fallback_signal = self._build_release_signal(
                    ts=ts,
                    candle=candle,
                    ai_state=ai_state,
                    option_snapshot=(option_snapshot if isinstance(option_snapshot, dict) else self._last_option_snapshot),
                    direction=direction,
                    confidence=confidence,
                    guidance=guidance,
                    compression_bars=prev_compression_count,
                )
                opened = self._enter_trade(
                    ts=ts,
                    close_price=close_price,
                    direction=direction,
                    signal=fallback_signal,
                    origin_low=_to_float(candle.get("low"), close_price),
                    origin_high=_to_float(candle.get("high"), close_price),
                    ai_state=ai_state,
                )
                if opened:
                    self._note_gate("entry_opened", ts, "release_gate_disabled")
                    state_changed = True
                if state_changed:
                    self._persist()
                return self._state_locked()

            current_dir = self._direction_from_features(features)
            align_sign = 1 if direction == "LONG" else -1
            pain_release = bool(
                is_displacement
                and prev_compression_count >= self.min_compression_bars
                and ts != self._last_release_ts
                and (current_dir == 0 or current_dir == align_sign)
            )

            if pain_release:
                self._note_gate("pain_release_detected", ts, f"compression_bars={prev_compression_count}")
                self._last_release_ts = ts
                release_signal = self._build_release_signal(
                    ts=ts,
                    candle=candle,
                    ai_state=ai_state,
                    option_snapshot=(option_snapshot if isinstance(option_snapshot, dict) else self._last_option_snapshot),
                    direction=direction,
                    confidence=confidence,
                    guidance=guidance,
                    compression_bars=prev_compression_count,
                )
                self._compression_count = 0
                if not self.require_pain_release or self.entry_style == "aggressive":
                    opened = self._enter_trade(
                        ts=ts,
                        close_price=close_price,
                        direction=direction,
                        signal=release_signal,
                        origin_low=_to_float(release_signal.get("release_low"), close_price),
                        origin_high=_to_float(release_signal.get("release_high"), close_price),
                        ai_state=ai_state,
                    )
                    if opened:
                        self._note_gate("entry_opened", ts, "release_entry")
                        self._pending_release = None
                        state_changed = True
                else:
                    self._pending_release = release_signal
            elif self.require_pain_release:
                self._note_gate("pain_release_missing", ts, f"compression_bars={prev_compression_count}")

            if (
                self._active_trade is None
                and self.require_pain_release
                and self.entry_style == "conservative"
                and self._pending_release is not None
            ):
                pending = self._pending_release
                pending_dir = str(pending.get("release_direction", "")).upper()
                if direction == pending_dir and self._conservative_entry_ready(
                    pending=pending,
                    candle=candle,
                    features=features,
                    direction=direction,
                ):
                    opened = self._enter_trade(
                        ts=ts,
                        close_price=close_price,
                        direction=direction,
                        signal=pending,
                        origin_low=_to_float(pending.get("release_low"), close_price),
                        origin_high=_to_float(pending.get("release_high"), close_price),
                        ai_state=ai_state,
                    )
                    if opened:
                        self._note_gate("entry_opened", ts, "conservative_confirmed")
                        self._pending_release = None
                        state_changed = True
                else:
                    self._note_gate("pending_release_wait", ts, "awaiting_conservative_confirmation")

            if state_changed:
                self._persist()
            return self._state_locked()

    def _state_locked(self) -> Dict[str, Any]:
        closed = list(self._trades)
        wins = sum(1 for row in closed if _to_float(row.get("points"), 0.0) > 0)
        losses = sum(1 for row in closed if _to_float(row.get("points"), 0.0) < 0)
        flats = sum(1 for row in closed if _to_float(row.get("points"), 0.0) == 0)
        net_points = float(sum(_to_float(row.get("points"), 0.0) for row in closed))
        closed_count = len(closed)
        win_rate = (float(wins) * 100.0 / float(closed_count)) if closed_count else 0.0

        open_trade = dict(self._active_trade) if self._active_trade else None
        if open_trade is not None:
            direction = str(open_trade.get("direction", "")).upper()
            sign = 1.0 if direction == "LONG" else -1.0
            underlying_unrealized = (self._last_price - _to_float(open_trade.get("entry_price"), 0.0)) * sign
            open_trade["underlying_mark_price"] = self._last_price
            open_trade["underlying_unrealized_points"] = underlying_unrealized
            option_entry = _to_float(open_trade.get("option_entry_ltp"), 0.0)
            option_mark = _to_float(open_trade.get("option_mark_ltp"), 0.0)
            if option_entry > 0.0 and option_mark > 0.0:
                option_unrealized = option_mark - option_entry
                open_trade["option_unrealized_points"] = option_unrealized
                open_trade["unrealized_points"] = option_unrealized
                open_trade["mark_price"] = option_mark
                open_trade["pnl_source"] = "option_quote"
            else:
                # Do not fall back to underlying for option trade PnL.
                open_trade["option_unrealized_points"] = 0.0
                open_trade["unrealized_points"] = 0.0
                open_trade["mark_price"] = option_mark if option_mark > 0.0 else 0.0
                open_trade["pnl_source"] = "option_quote_pending"

        return {
            "enabled": self.enabled,
            "storage_backend": self._storage_backend,
            "symbol": self.symbol,
            "entry_guidance": self.entry_guidance,
            "min_confidence": self.min_confidence,
            "hold_seconds": self.hold_seconds,
            "one_trade_at_a_time": self.one_trade_at_a_time,
            "require_pain_release": self.require_pain_release,
            "model_driven_execution": self.model_driven_execution,
            "entry_style": self.entry_style,
            "min_compression_bars": self.min_compression_bars,
            "target_r_multiple": self.target_r_multiple,
            "avoid_noon_chop": self.avoid_noon_chop,
            "feedback_enabled": self.feedback_enabled,
            "feedback_min_trades": self.feedback_min_trades,
            "feedback_setups": len(self._feedback_stats),
            "feedback_last_decision": dict(self._last_feedback_decision),
            "last_candle_ts": self._last_candle_ts,
            "last_price": self._last_price,
            "closed_trades": closed_count,
            "profit_trades": wins,
            "loss_trades": losses,
            "flat_trades": flats,
            "win_rate_pct": round(win_rate, 4),
            "net_points": round(net_points, 4),
            "active_trade": open_trade,
            "pending_release": dict(self._pending_release) if self._pending_release else None,
            "recent_trades": closed[-20:],
            "gate_diagnostics": self._gate_diagnostics_locked(),
        }

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            return self._state_locked()

    def has_active_trade(self) -> bool:
        with self._lock:
            return self._active_trade is not None

    def force_close_active_trade(
        self,
        *,
        reason: str,
        exit_ts: int = 0,
        exit_price: float = 0.0,
    ) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._active_trade is None:
                return None
            resolved_exit_ts = _to_int(exit_ts, int(time.time()))
            resolved_exit_price = _to_float(exit_price, 0.0)
            if resolved_exit_price > 0.0:
                self._last_price = resolved_exit_price
            else:
                resolved_exit_price = _to_float(self._last_price, 0.0)
            if resolved_exit_price <= 0.0:
                resolved_exit_price = _to_float(self._active_trade.get("entry_price"), 0.0)
            closed = self._close_active_trade(
                exit_ts=resolved_exit_ts,
                exit_price=resolved_exit_price,
                reason=str(reason),
            )
            self._persist()
            return dict(closed) if isinstance(closed, dict) else None

    def get_gate_diagnostics(self) -> Dict[str, Any]:
        with self._lock:
            return self._gate_diagnostics_locked()

    def get_trades(self, limit: int = 200) -> List[Dict[str, Any]]:
        max_rows = max(1, min(int(limit), 5000))
        with self._lock:
            return [dict(row) for row in self._trades[-max_rows:]]

    def get_mistake_summary(self, *, days: int = 30, max_rows: int = 5000) -> Dict[str, Any]:
        lookback_days = max(1, min(int(days), 365))
        row_limit = max(100, min(int(max_rows), 50000))
        now_ts = int(time.time())
        since_ts = max(0, now_ts - (lookback_days * 86400))

        rows: List[Dict[str, Any]] = []
        if self._storage_backend == "mysql":
            conn = None
            cur = None
            try:
                conn = self._connect_mysql(with_database=True)
                cur = conn.cursor(dictionary=True)
                cur.execute(
                    f"""
                    SELECT
                        trade_id,
                        entry_ts,
                        outcome,
                        close_reason,
                        points,
                        mistake_type,
                        mistake_score
                    FROM `{self._mysql_mistakes_table}`
                    WHERE entry_ts >= %s
                    ORDER BY entry_ts DESC
                    LIMIT %s
                    """,
                    (since_ts, row_limit),
                )
                rows = cur.fetchall() or []
                conn.commit()
            finally:
                if cur is not None:
                    cur.close()
                if conn is not None:
                    conn.close()
        else:
            with self._lock:
                for trade in reversed(self._trades):
                    entry_ts = _to_int(trade.get("entry_ts"), 0)
                    if entry_ts < since_ts:
                        continue
                    signal = trade.get("signal") if isinstance(trade.get("signal"), dict) else {}
                    rows.append(
                        {
                            "trade_id": _to_int(trade.get("trade_id"), 0),
                            "entry_ts": entry_ts,
                            "outcome": str(trade.get("outcome") or "flat"),
                            "close_reason": str(trade.get("close_reason") or ""),
                            "points": _to_float(trade.get("points"), 0.0),
                            "mistake_type": self._normalize_mistake_type(signal.get("mistake_type")),
                            "mistake_score": _to_float(signal.get("mistake_score"), 0.0),
                        }
                    )
                    if len(rows) >= row_limit:
                        break

        bucket_map: Dict[str, Dict[str, Any]] = {}
        for name in _MISTAKE_ORDER:
            bucket_map[name] = {
                "mistake_type": name,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "flats": 0,
                "net_points": 0.0,
                "score_sum": 0.0,
                "reason_counts": {},
                "latest_entry_ts": 0,
            }

        for item in rows:
            name = self._normalize_mistake_type(item.get("mistake_type"))
            points = _to_float(item.get("points"), 0.0)
            score = max(0.0, min(1.0, _to_float(item.get("mistake_score"), 0.0)))
            entry_ts = _to_int(item.get("entry_ts"), 0)
            close_reason = str(item.get("close_reason") or "").strip().lower()
            bucket = bucket_map.get(name)
            if bucket is None:
                continue
            bucket["trades"] = _to_int(bucket.get("trades"), 0) + 1
            bucket["net_points"] = _to_float(bucket.get("net_points"), 0.0) + points
            bucket["score_sum"] = _to_float(bucket.get("score_sum"), 0.0) + score
            bucket["latest_entry_ts"] = max(_to_int(bucket.get("latest_entry_ts"), 0), entry_ts)
            if points > 0.0:
                bucket["wins"] = _to_int(bucket.get("wins"), 0) + 1
            elif points < 0.0:
                bucket["losses"] = _to_int(bucket.get("losses"), 0) + 1
            else:
                bucket["flats"] = _to_int(bucket.get("flats"), 0) + 1
            if close_reason:
                reason_counts = bucket.get("reason_counts") if isinstance(bucket.get("reason_counts"), dict) else {}
                reason_counts[close_reason] = _to_int(reason_counts.get(close_reason), 0) + 1
                bucket["reason_counts"] = reason_counts

        totals = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "flats": 0,
            "net_points": 0.0,
            "active_buckets": 0,
        }
        mistake_totals = {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "flats": 0,
            "net_points": 0.0,
            "active_buckets": 0,
        }
        buckets: List[Dict[str, Any]] = []
        for name in _MISTAKE_ORDER:
            bucket = bucket_map[name]
            trades = _to_int(bucket.get("trades"), 0)
            wins = _to_int(bucket.get("wins"), 0)
            losses = _to_int(bucket.get("losses"), 0)
            flats = _to_int(bucket.get("flats"), 0)
            net_points = _to_float(bucket.get("net_points"), 0.0)
            avg_points = (net_points / float(trades)) if trades > 0 else 0.0
            avg_score = (_to_float(bucket.get("score_sum"), 0.0) / float(trades)) if trades > 0 else 0.0
            win_rate_pct = (float(wins) * 100.0 / float(trades)) if trades > 0 else 0.0
            reason_counts = bucket.get("reason_counts") if isinstance(bucket.get("reason_counts"), dict) else {}
            top_reason = "--"
            if reason_counts:
                top_reason = str(max(reason_counts.items(), key=lambda kv: _to_int(kv[1], 0))[0])

            if trades > 0:
                totals["active_buckets"] = _to_int(totals.get("active_buckets"), 0) + 1
                if name != "none":
                    mistake_totals["active_buckets"] = _to_int(mistake_totals.get("active_buckets"), 0) + 1
            totals["trades"] = _to_int(totals.get("trades"), 0) + trades
            totals["wins"] = _to_int(totals.get("wins"), 0) + wins
            totals["losses"] = _to_int(totals.get("losses"), 0) + losses
            totals["flats"] = _to_int(totals.get("flats"), 0) + flats
            totals["net_points"] = _to_float(totals.get("net_points"), 0.0) + net_points
            if name != "none":
                mistake_totals["trades"] = _to_int(mistake_totals.get("trades"), 0) + trades
                mistake_totals["wins"] = _to_int(mistake_totals.get("wins"), 0) + wins
                mistake_totals["losses"] = _to_int(mistake_totals.get("losses"), 0) + losses
                mistake_totals["flats"] = _to_int(mistake_totals.get("flats"), 0) + flats
                mistake_totals["net_points"] = _to_float(mistake_totals.get("net_points"), 0.0) + net_points

            buckets.append(
                {
                    "mistake_type": name,
                    "trades": trades,
                    "wins": wins,
                    "losses": losses,
                    "flats": flats,
                    "win_rate_pct": round(win_rate_pct, 4),
                    "net_points": round(net_points, 4),
                    "avg_points": round(avg_points, 4),
                    "avg_mistake_score": round(avg_score, 4),
                    "top_close_reason": top_reason,
                    "latest_entry_ts": _to_int(bucket.get("latest_entry_ts"), 0),
                }
            )

        totals_trades = max(0, _to_int(totals.get("trades"), 0))
        totals["win_rate_pct"] = round((float(_to_int(totals.get("wins"), 0)) * 100.0 / float(totals_trades)), 4) if totals_trades else 0.0
        totals["avg_points"] = round((_to_float(totals.get("net_points"), 0.0) / float(totals_trades)), 4) if totals_trades else 0.0
        totals["net_points"] = round(_to_float(totals.get("net_points"), 0.0), 4)

        mistake_trades = max(0, _to_int(mistake_totals.get("trades"), 0))
        mistake_totals["win_rate_pct"] = (
            round((float(_to_int(mistake_totals.get("wins"), 0)) * 100.0 / float(mistake_trades)), 4)
            if mistake_trades
            else 0.0
        )
        mistake_totals["avg_points"] = (
            round((_to_float(mistake_totals.get("net_points"), 0.0) / float(mistake_trades)), 4)
            if mistake_trades
            else 0.0
        )
        mistake_totals["net_points"] = round(_to_float(mistake_totals.get("net_points"), 0.0), 4)

        return {
            "days": lookback_days,
            "since_ts": since_ts,
            "generated_at": now_ts,
            "total_rows": len(rows),
            "totals": totals,
            "mistake_totals": mistake_totals,
            "buckets": buckets,
        }

    def get_active_option_quote_target(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            if self._active_trade is None:
                return None
            active = self._active_trade
            signal = active.get("signal") if isinstance(active.get("signal"), dict) else {}
            selected = signal.get("selected_option") if isinstance(signal.get("selected_option"), dict) else {}
            symbol = str(active.get("option_symbol") or selected.get("symbol") or "").strip()
            if not symbol:
                return None
            return {
                "trade_id": _to_int(active.get("trade_id"), 0),
                "symbol": symbol,
                "option_side": str(active.get("option_side") or selected.get("side") or "").upper(),
                "option_strike": _to_float(active.get("option_strike"), _to_float(selected.get("strike"), 0.0)),
                "option_entry_ltp": _to_float(active.get("option_entry_ltp"), 0.0),
            }

    def update_option_mark(self, *, symbol: str, ltp: float, quote_ts: int = 0) -> bool:
        mark = _to_float(ltp, 0.0)
        if mark <= 0.0:
            return False
        text_symbol = str(symbol or "").strip()
        with self._lock:
            if self._active_trade is None:
                return False
            active = self._active_trade
            current_symbol = str(active.get("option_symbol") or "").strip()
            if current_symbol and text_symbol and current_symbol.upper() != text_symbol.upper():
                return False
            changed = False
            if text_symbol and current_symbol != text_symbol:
                active["option_symbol"] = text_symbol
                changed = True
            prev_entry = _to_float(active.get("option_entry_ltp"), 0.0)
            prev_mark = _to_float(active.get("option_mark_ltp"), 0.0)
            if prev_entry <= 0.0:
                active["option_entry_ltp"] = mark
                changed = True
            if abs(prev_mark - mark) > 1e-9:
                active["option_mark_ltp"] = mark
                changed = True
            qts = _to_int(quote_ts, int(time.time()))
            prev_qts = _to_int(active.get("option_quote_ts"), 0)
            if qts > 0 and qts > prev_qts:
                active["option_quote_ts"] = qts
                changed = True
            if changed:
                self._persist()
            return changed

    def enforce_time_cutoffs(self, *, ts: int, mark_price: float = 0.0) -> Dict[str, Any]:
        if not self.enabled:
            return self.get_state()
        runtime_ts = _to_int(ts, 0)
        if runtime_ts <= 0:
            return self.get_state()

        with self._lock:
            mark = _to_float(mark_price, 0.0)
            if mark > 0.0:
                self._last_price = mark
            if self._active_trade is not None and self._forced_squareoff_reached(runtime_ts):
                exit_price = self._last_price
                if exit_price <= 0.0:
                    exit_price = _to_float(self._active_trade.get("entry_price"), 0.0)
                self._close_active_trade(
                    exit_ts=runtime_ts,
                    exit_price=exit_price,
                    reason="session_squareoff_1520",
                )
                self._note_gate("forced_squareoff", runtime_ts, "runtime_tick_cutoff_15_20")
                self._persist()
            return self._state_locked()

    def reset(self, close_open: bool = False) -> Dict[str, Any]:
        with self._lock:
            if close_open and self._active_trade is not None and self._last_candle_ts > 0:
                self._close_active_trade(
                    exit_ts=self._last_candle_ts,
                    exit_price=self._last_price,
                    reason="reset",
                )
            self._trades = []
            self._active_trade = None
            self._pending_release = None
            self._compression_count = 0
            self._last_release_ts = 0
            self._last_feedback_decision = {
                "applied": False,
                "reason": "not_evaluated",
            }
            self._gate_stats = self._new_gate_stats()
            self._last_gate_event = {
                "reason": "not_evaluated",
                "ts": 0,
                "detail": "",
            }
            self._persist()
            return self._state_locked()
