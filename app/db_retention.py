from __future__ import annotations

import logging
import re
import threading
import time
from typing import Any, Dict, List

from .config import Settings, mysql_connect_kwargs

logger = logging.getLogger(__name__)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


class MySQLRetentionService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.enabled = bool(settings.db_retention_enabled)
        self.run_on_start = bool(settings.db_retention_run_on_start)
        self.interval_sec = max(300, int(settings.db_retention_interval_sec))

        self._lock = threading.Lock()
        self._running = False
        self._run_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._last_started_ts = 0
        self._last_finished_ts = 0
        self._last_result: Dict[str, Any] = {
            "ok": False,
            "status": "idle",
            "message": "DB retention has not run yet.",
        }

    @staticmethod
    def _validate_identifier(value: str) -> str:
        text = str(value or "").strip()
        if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
            return ""
        return text

    @staticmethod
    def _pick(columns: Dict[str, str], candidates: List[str]) -> str:
        for name in candidates:
            key = str(name).strip().lower()
            if key in columns:
                return columns[key]
        return ""

    def _connect(self):
        import mysql.connector  # type: ignore

        return mysql.connector.connect(**mysql_connect_kwargs(self.settings, with_database=True, autocommit=False))

    def _columns(self, conn: Any, table: str) -> Dict[str, str]:
        safe_table = self._validate_identifier(table)
        if not safe_table:
            return {}
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.settings.auth_mysql_database, safe_table),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        out: Dict[str, str] = {}
        for row in rows:
            col = str((row or {}).get("COLUMN_NAME", "")).strip()
            if col:
                out[col.lower()] = col
        return out

    def _delete_by_ts(
        self,
        conn: Any,
        *,
        table: str,
        ts_candidates: List[str],
        keep_days: int,
        now_ts: int,
    ) -> Dict[str, Any]:
        safe_table = self._validate_identifier(table)
        result = {
            "table": str(table or ""),
            "keep_days": max(0, int(keep_days)),
            "cutoff_ts": max(0, int(now_ts - max(0, int(keep_days)) * 86400)),
            "deleted_rows": 0,
            "status": "skipped",
            "reason": "",
            "ts_column": "",
        }
        if not safe_table:
            result["reason"] = "invalid_table_name"
            return result
        if keep_days <= 0:
            result["reason"] = "disabled_keep_days"
            return result

        columns = self._columns(conn, safe_table)
        if not columns:
            result["reason"] = "table_missing"
            return result
        ts_col = self._pick(columns, ts_candidates)
        if not ts_col:
            result["reason"] = "timestamp_column_missing"
            return result

        cutoff_ts = int(result["cutoff_ts"])
        cur = conn.cursor()
        try:
            cur.execute(
                f"DELETE FROM `{safe_table}` WHERE `{ts_col}` < %s",
                (cutoff_ts,),
            )
            result["deleted_rows"] = int(cur.rowcount or 0)
            result["status"] = "ok"
            result["ts_column"] = ts_col
            result["reason"] = "deleted"
            return result
        finally:
            cur.close()

    def _cleanup_auth_sessions(self, conn: Any, *, keep_days: int, now_ts: int) -> Dict[str, Any]:
        table = "auth_sessions"
        safe_table = self._validate_identifier(table)
        result = {
            "table": table,
            "keep_days": max(0, int(keep_days)),
            "cutoff_ts": max(0, int(now_ts - max(0, int(keep_days)) * 86400)),
            "deleted_rows": 0,
            "status": "skipped",
            "reason": "",
        }
        columns = self._columns(conn, safe_table)
        if not columns:
            result["reason"] = "table_missing"
            return result

        predicates: List[str] = []
        params: List[Any] = []
        if "expires_at" in columns:
            predicates.append(f"`{columns['expires_at']}` <= %s")
            params.append(int(now_ts))
        if "revoked" in columns:
            predicates.append(f"`{columns['revoked']}` = 1")
        if keep_days > 0 and "created_at" in columns:
            predicates.append(f"`{columns['created_at']}` < %s")
            params.append(int(result["cutoff_ts"]))
        if not predicates:
            result["reason"] = "cleanup_columns_missing"
            return result

        sql = f"DELETE FROM `{safe_table}` WHERE " + " OR ".join(f"({item})" for item in predicates)
        cur = conn.cursor()
        try:
            cur.execute(sql, tuple(params))
            result["deleted_rows"] = int(cur.rowcount or 0)
            result["status"] = "ok"
            result["reason"] = "deleted"
            return result
        finally:
            cur.close()

    def _cleanup_backtest_trades_by_run_age(
        self,
        conn: Any,
        *,
        runs_table: str,
        trades_table: str,
        keep_days: int,
        now_ts: int,
    ) -> Dict[str, Any]:
        safe_runs = self._validate_identifier(runs_table)
        safe_trades = self._validate_identifier(trades_table)
        result = {
            "table": safe_trades or str(trades_table or ""),
            "keep_days": max(0, int(keep_days)),
            "cutoff_ts": max(0, int(now_ts - max(0, int(keep_days)) * 86400)),
            "deleted_rows": 0,
            "status": "skipped",
            "reason": "",
            "joined_on": "",
            "run_ts_column": "",
        }
        if not safe_runs or not safe_trades:
            result["reason"] = "invalid_table_name"
            return result
        if keep_days <= 0:
            result["reason"] = "disabled_keep_days"
            return result

        runs_cols = self._columns(conn, safe_runs)
        trades_cols = self._columns(conn, safe_trades)
        if not runs_cols or not trades_cols:
            result["reason"] = "table_missing"
            return result
        run_fk = self._pick(trades_cols, ["run_id"])
        run_pk = self._pick(runs_cols, ["run_id"])
        run_ts = self._pick(runs_cols, ["completed_at", "updated_at", "created_at", "started_at"])
        if not run_fk or not run_pk or not run_ts:
            result["reason"] = "join_columns_missing"
            return result

        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                DELETE t
                FROM `{safe_trades}` t
                INNER JOIN `{safe_runs}` r ON t.`{run_fk}` = r.`{run_pk}`
                WHERE r.`{run_ts}` < %s
                """,
                (int(result["cutoff_ts"]),),
            )
            result["deleted_rows"] = int(cur.rowcount or 0)
            result["status"] = "ok"
            result["reason"] = "deleted"
            result["joined_on"] = f"{safe_trades}.{run_fk}={safe_runs}.{run_pk}"
            result["run_ts_column"] = run_ts
            return result
        finally:
            cur.close()

    def run_once(self, *, reason: str = "scheduled") -> Dict[str, Any]:
        if not self.enabled:
            return {
                "ok": False,
                "status": "disabled",
                "message": "DB retention is disabled.",
            }

        with self._lock:
            if self._running:
                return {
                    "ok": False,
                    "status": "busy",
                    "message": "DB retention is already running.",
                }
            self._running = True
            self._run_count += 1
            self._last_started_ts = int(time.time())

        conn = None
        try:
            now_ts = int(time.time())
            conn = self._connect()

            table_results: Dict[str, Dict[str, Any]] = {}
            table_results["candles_1m"] = self._delete_by_ts(
                conn,
                table=self.settings.runtime_mysql_table_1m,
                ts_candidates=["timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time"],
                keep_days=self.settings.db_retention_candles_1m_days,
                now_ts=now_ts,
            )
            table_results["candles_5m"] = self._delete_by_ts(
                conn,
                table=self.settings.runtime_mysql_table_5m,
                ts_candidates=["timestamp", "start", "start_ts", "ts", "time", "datetime", "date", "candle_time"],
                keep_days=self.settings.db_retention_candles_5m_days,
                now_ts=now_ts,
            )
            table_results["option_snapshots"] = self._delete_by_ts(
                conn,
                table=self.settings.runtime_mysql_options_table,
                ts_candidates=["timestamp", "ts", "start", "time", "created_at", "updated_at"],
                keep_days=self.settings.db_retention_option_snapshots_days,
                now_ts=now_ts,
            )
            table_results["option_strikes"] = self._delete_by_ts(
                conn,
                table=self.settings.runtime_mysql_option_strikes_table,
                ts_candidates=["timestamp", "ts", "start", "time", "created_at", "updated_at"],
                keep_days=self.settings.db_retention_option_strikes_days,
                now_ts=now_ts,
            )
            table_results["paper_trades"] = self._delete_by_ts(
                conn,
                table=self.settings.paper_trade_mysql_trades_table,
                ts_candidates=["entry_ts", "closed_at", "exit_ts", "created_at", "updated_at"],
                keep_days=self.settings.db_retention_paper_trades_days,
                now_ts=now_ts,
            )
            table_results["paper_feedback"] = self._delete_by_ts(
                conn,
                table=self.settings.paper_trade_mysql_feedback_table,
                ts_candidates=["updated_at", "created_at"],
                keep_days=self.settings.db_retention_paper_feedback_days,
                now_ts=now_ts,
            )
            table_results["backtest_trades"] = self._cleanup_backtest_trades_by_run_age(
                conn,
                runs_table=self.settings.backtest_mysql_runs_table,
                trades_table=self.settings.backtest_mysql_trades_table,
                keep_days=self.settings.db_retention_backtest_trades_days,
                now_ts=now_ts,
            )
            table_results["backtest_runs"] = self._delete_by_ts(
                conn,
                table=self.settings.backtest_mysql_runs_table,
                ts_candidates=["completed_at", "updated_at", "created_at", "started_at"],
                keep_days=self.settings.db_retention_backtest_runs_days,
                now_ts=now_ts,
            )
            table_results["auth_sessions"] = self._cleanup_auth_sessions(
                conn,
                keep_days=self.settings.db_retention_auth_sessions_days,
                now_ts=now_ts,
            )

            conn.commit()
            total_deleted = int(
                sum(max(0, _to_int((item or {}).get("deleted_rows"), 0)) for item in table_results.values())
            )
            finished = int(time.time())
            result = {
                "ok": True,
                "status": "completed",
                "trigger": str(reason or "scheduled"),
                "started_at": now_ts,
                "finished_at": finished,
                "deleted_rows_total": total_deleted,
                "table_results": table_results,
            }
            self._last_result = dict(result)
            self._success_count += 1
            self._last_finished_ts = finished
            logger.info("DB retention completed: deleted_rows_total=%d", total_deleted)
            return result
        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            finished = int(time.time())
            result = {
                "ok": False,
                "status": "failed",
                "trigger": str(reason or "scheduled"),
                "started_at": self._last_started_ts,
                "finished_at": finished,
                "error": str(exc),
            }
            self._last_result = dict(result)
            self._failure_count += 1
            self._last_finished_ts = finished
            logger.warning("DB retention failed: %s", exc)
            return result
        finally:
            if conn is not None:
                conn.close()
            self._running = False

    def get_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "running": self._running,
            "run_on_start": self.run_on_start,
            "interval_sec": self.interval_sec,
            "policies": {
                "candles_1m_days": self.settings.db_retention_candles_1m_days,
                "candles_5m_days": self.settings.db_retention_candles_5m_days,
                "option_snapshots_days": self.settings.db_retention_option_snapshots_days,
                "option_strikes_days": self.settings.db_retention_option_strikes_days,
                "paper_trades_days": self.settings.db_retention_paper_trades_days,
                "paper_feedback_days": self.settings.db_retention_paper_feedback_days,
                "backtest_runs_days": self.settings.db_retention_backtest_runs_days,
                "backtest_trades_days": self.settings.db_retention_backtest_trades_days,
                "auth_sessions_days": self.settings.db_retention_auth_sessions_days,
            },
            "run_count": self._run_count,
            "success_count": self._success_count,
            "failure_count": self._failure_count,
            "last_started_ts": self._last_started_ts,
            "last_finished_ts": self._last_finished_ts,
            "last_result": dict(self._last_result),
        }

