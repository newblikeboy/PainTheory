from __future__ import annotations

import asyncio
from bisect import bisect_left, bisect_right
import csv
from datetime import datetime, timedelta, timezone
import json
import os
import re
import shutil
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib

from ..config import Settings, mysql_connect_kwargs
from ..pain_theory_ai.runtime import PainTheoryRuntime
from .train_pain_theory import train as train_model


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


def _validate_identifier(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not text or not re.fullmatch(r"[a-zA-Z0-9_]+", text):
        raise ValueError(f"{label} must contain only letters, numbers, and underscores")
    return text


def _target_accuracy(metrics: Dict[str, Any], target: str) -> float:
    targets = metrics.get("targets") if isinstance(metrics.get("targets"), dict) else {}
    row = targets.get(target) if isinstance(targets, dict) else {}
    return float(_to_float((row or {}).get("accuracy"), 0.0))


def _target_report(metrics: Dict[str, Any], target: str) -> Dict[str, Any]:
    targets = metrics.get("targets") if isinstance(metrics.get("targets"), dict) else {}
    row = targets.get(target) if isinstance(targets, dict) else {}
    report = (row or {}).get("report")
    return report if isinstance(report, dict) else {}


def _target_macro_f1(metrics: Dict[str, Any], target: str) -> float:
    report = _target_report(metrics, target)
    macro = report.get("macro avg") if isinstance(report.get("macro avg"), dict) else {}
    value = _to_float((macro or {}).get("f1-score"), -1.0)
    if value < 0.0:
        return _target_accuracy(metrics, target)
    return max(0.0, min(1.0, float(value)))


def _target_class_recall(metrics: Dict[str, Any], target: str, class_label: str) -> float:
    report = _target_report(metrics, target)
    row = report.get(str(class_label)) if isinstance(report.get(str(class_label)), dict) else {}
    value = _to_float((row or {}).get("recall"), 0.0)
    return max(0.0, min(1.0, float(value)))


def _target_class_support(metrics: Dict[str, Any], target: str, class_label: str) -> int:
    report = _target_report(metrics, target)
    row = report.get(str(class_label)) if isinstance(report.get(str(class_label)), dict) else {}
    return max(0, _to_int((row or {}).get("support"), 0))


def _target_quality(metrics: Dict[str, Any], target: str) -> float:
    accuracy = _target_accuracy(metrics, target)
    macro_f1 = _target_macro_f1(metrics, target)
    # Blend weighted accuracy with macro-F1 so minority-class collapse is penalized.
    return (accuracy * 0.65) + (macro_f1 * 0.35)


def _min_required_recall(*, baseline_recall: float, floor: float, max_drop: float) -> float:
    return max(float(floor), float(baseline_recall) - max(0.0, float(max_drop)))


def _weighted_score(metrics: Dict[str, Any]) -> float:
    phase = _target_quality(metrics, "pain_phase")
    dominant = _target_quality(metrics, "dominant_pain_group")
    next_group = _target_quality(metrics, "next_likely_pain_group")
    guidance = _target_quality(metrics, "guidance")
    mistake = _target_quality(metrics, "mistake_type")
    core = (phase * 0.27) + (dominant * 0.27) + (next_group * 0.31) + (guidance * 0.15)
    if mistake <= 0.0:
        return core
    return (core * 0.90) + (mistake * 0.10)


def _read_artifact_metrics(path: str) -> Dict[str, Any]:
    model_path = Path(str(path or ""))
    if not model_path.exists():
        return {}
    try:
        artifact = joblib.load(model_path)
    except Exception:
        return {}
    metrics = artifact.get("metrics") if isinstance(artifact, dict) else {}
    return metrics if isinstance(metrics, dict) else {}


class AutoRetrainService:
    def __init__(self, *, settings: Settings, runtime: PainTheoryRuntime) -> None:
        self.settings = settings
        self.runtime = runtime
        self.enabled = bool(settings.model_auto_retrain_enabled)
        self.interval_sec = max(300, int(settings.model_auto_retrain_interval_sec))
        self.lookback_days = max(7, int(settings.model_auto_retrain_lookback_days))
        self.min_rows = max(500, int(settings.model_auto_retrain_min_rows))
        self.min_test_rows = max(100, int(settings.model_auto_retrain_min_test_rows))
        self.min_improvement = max(0.0, float(settings.model_auto_retrain_min_improvement))
        self.max_core_drop = max(0.0, float(settings.model_auto_retrain_max_core_drop))
        self.window_1m = max(5, int(settings.model_auto_retrain_window_1m))
        self.window_5m = max(10, int(settings.model_auto_retrain_window_5m))
        self.next_horizon = max(1, int(settings.model_auto_retrain_next_horizon))
        self.test_size = max(0.05, min(0.5, float(settings.model_auto_retrain_test_size)))
        self.candidate_path = str(settings.model_auto_retrain_candidate_path or "models/pain_theory_ai_candidate.pkl")
        self.backup_dir = str(settings.model_auto_retrain_backup_dir or "models/backups")
        self.runs_table = _validate_identifier(
            str(settings.model_auto_retrain_runs_table or "model_retrain_runs"),
            "MODEL_AUTO_RETRAIN_RUNS_TABLE",
        )

        self._lock = asyncio.Lock()
        self._running = False
        self._last_started_ts = 0
        self._last_finished_ts = 0
        self._last_success_ts = 0
        self._run_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._last_result: Dict[str, Any] = {
            "ok": False,
            "status": "idle",
            "message": "Auto retrain not executed yet.",
        }
        self._run_store_ready = False
        self._run_store_error = ""
        self._init_run_store()

    def _connect_mysql(self):
        import mysql.connector  # type: ignore

        return mysql.connector.connect(**mysql_connect_kwargs(self.settings, with_database=True, autocommit=False))

    def _init_run_store(self) -> None:
        conn = None
        cur = None
        try:
            conn = self._connect_mysql()
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS `{self.runs_table}` (
                    run_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    started_at BIGINT NOT NULL,
                    finished_at BIGINT NOT NULL,
                    trigger_source VARCHAR(40) NOT NULL,
                    status VARCHAR(24) NOT NULL,
                    ok TINYINT(1) NOT NULL DEFAULT 0,
                    deployed TINYINT(1) NOT NULL DEFAULT 0,
                    reason VARCHAR(120) NULL,
                    candles_rows INT NOT NULL DEFAULT 0,
                    options_rows INT NOT NULL DEFAULT 0,
                    candidate_score DOUBLE NULL,
                    baseline_score DOUBLE NULL,
                    improvement DOUBLE NULL,
                    phase_delta DOUBLE NULL,
                    dominant_delta DOUBLE NULL,
                    guidance_delta DOUBLE NULL,
                    next_delta DOUBLE NULL,
                    learned_text VARCHAR(512) NULL,
                    failure_text VARCHAR(512) NULL,
                    result_json LONGTEXT NULL,
                    created_at BIGINT NOT NULL,
                    INDEX idx_{self.runs_table}_started (started_at),
                    INDEX idx_{self.runs_table}_trigger (trigger_source),
                    INDEX idx_{self.runs_table}_status (status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            conn.commit()
            self._run_store_ready = True
            self._run_store_error = ""
        except Exception as exc:
            self._run_store_ready = False
            self._run_store_error = str(exc)
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    @staticmethod
    def _safe_json(data: Any) -> str:
        try:
            return json.dumps(data, ensure_ascii=True)
        except Exception:
            return "{}"

    @staticmethod
    def _clip_text(value: Any, limit: int = 500) -> str:
        text = str(value or "").strip().replace("\n", " ").replace("\r", " ")
        if len(text) <= limit:
            return text
        return f"{text[: max(0, limit - 3)]}..."

    @staticmethod
    def _fmt_signed(value: float) -> str:
        number = _to_float(value, 0.0)
        sign = "+" if number >= 0 else ""
        return f"{sign}{number:.4f}"

    def _learning_notes(self, result: Dict[str, Any]) -> Tuple[str, str]:
        status_text = str(result.get("status") or "").strip().lower()
        reason = str(result.get("reason") or result.get("message") or result.get("error") or "").strip()
        if status_text == "completed":
            decision = result.get("decision") if isinstance(result.get("decision"), dict) else {}
            candidate_score = _to_float(decision.get("candidate_score"), 0.0)
            baseline_score = _to_float(decision.get("baseline_score"), 0.0)
            improvement = _to_float(decision.get("improvement"), candidate_score - baseline_score)
            phase_delta = _to_float(decision.get("phase_delta"), 0.0)
            dominant_delta = _to_float(decision.get("dominant_delta"), 0.0)
            next_delta = _to_float(decision.get("next_delta"), 0.0)
            guidance_delta = _to_float(decision.get("guidance_delta"), 0.0)
            if bool(result.get("deployed")):
                learned = (
                    f"Deployed. score {candidate_score:.4f} vs {baseline_score:.4f} ({self._fmt_signed(improvement)}), "
                    f"phase {self._fmt_signed(phase_delta)}, dominant {self._fmt_signed(dominant_delta)}, "
                    f"next {self._fmt_signed(next_delta)}, guidance {self._fmt_signed(guidance_delta)}."
                )
                return self._clip_text(learned), ""
            not_learned = reason or "candidate_not_deployed"
            detail = (
                f"Not deployed: {not_learned}. score {candidate_score:.4f} vs {baseline_score:.4f} "
                f"({self._fmt_signed(improvement)})."
            )
            return "", self._clip_text(detail)
        if status_text == "skipped":
            candles_rows = _to_int(result.get("candles_rows"), 0)
            required_rows = _to_int(result.get("required_rows"), self.min_rows)
            detail = f"Skipped: {reason or 'insufficient_data'} ({candles_rows}/{required_rows} candles)."
            return "", self._clip_text(detail)
        if status_text == "failed":
            detail = f"Failed: {reason or 'unknown_error'}"
            return "", self._clip_text(detail)
        if status_text == "busy":
            return "", "Skipped: retrain already running."
        if status_text == "disabled":
            return "", "Skipped: auto retrain disabled."
        return "", self._clip_text(reason or "No retrain outcome available.")

    def _persist_run(self, result: Dict[str, Any]) -> None:
        if not self._run_store_ready:
            return
        decision = result.get("decision") if isinstance(result.get("decision"), dict) else {}
        learned_text, failure_text = self._learning_notes(result)
        started_at = _to_int(result.get("started_at"), int(time.time()))
        finished_at = _to_int(result.get("finished_at"), started_at)
        trigger_source = str(result.get("trigger") or "manual").strip() or "manual"
        status_text = str(result.get("status") or "unknown").strip() or "unknown"
        reason = str(result.get("reason") or result.get("message") or result.get("error") or "").strip()

        conn = None
        cur = None
        try:
            conn = self._connect_mysql()
            cur = conn.cursor()
            cur.execute(
                f"""
                INSERT INTO `{self.runs_table}` (
                    started_at,
                    finished_at,
                    trigger_source,
                    status,
                    ok,
                    deployed,
                    reason,
                    candles_rows,
                    options_rows,
                    candidate_score,
                    baseline_score,
                    improvement,
                    phase_delta,
                    dominant_delta,
                    guidance_delta,
                    next_delta,
                    learned_text,
                    failure_text,
                    result_json,
                    created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    started_at,
                    finished_at,
                    trigger_source[:40],
                    status_text[:24],
                    1 if bool(result.get("ok")) else 0,
                    1 if bool(result.get("deployed")) else 0,
                    reason[:120] if reason else None,
                    _to_int(result.get("candles_rows"), 0),
                    _to_int(result.get("options_rows"), 0),
                    _to_float(decision.get("candidate_score"), 0.0),
                    _to_float(decision.get("baseline_score"), 0.0),
                    _to_float(decision.get("improvement"), 0.0),
                    _to_float(decision.get("phase_delta"), 0.0),
                    _to_float(decision.get("dominant_delta"), 0.0),
                    _to_float(decision.get("guidance_delta"), 0.0),
                    _to_float(decision.get("next_delta"), 0.0),
                    learned_text[:512] if learned_text else None,
                    failure_text[:512] if failure_text else None,
                    self._safe_json(result),
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

    def get_history(self, limit: int = 60) -> List[Dict[str, Any]]:
        if not self._run_store_ready:
            return []
        max_rows = max(1, min(int(limit), 500))
        conn = None
        cur = None
        try:
            conn = self._connect_mysql()
            cur = conn.cursor(dictionary=True)
            cur.execute(
                f"""
                SELECT
                    run_id,
                    started_at,
                    finished_at,
                    trigger_source,
                    status,
                    ok,
                    deployed,
                    reason,
                    candles_rows,
                    options_rows,
                    candidate_score,
                    baseline_score,
                    improvement,
                    phase_delta,
                    dominant_delta,
                    guidance_delta,
                    next_delta,
                    learned_text,
                    failure_text
                FROM `{self.runs_table}`
                ORDER BY run_id DESC
                LIMIT %s
                """,
                (max_rows,),
            )
            rows = cur.fetchall() or []
            out: List[Dict[str, Any]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                out.append(
                    {
                        "run_id": _to_int(row.get("run_id"), 0),
                        "started_at": _to_int(row.get("started_at"), 0),
                        "finished_at": _to_int(row.get("finished_at"), 0),
                        "trigger": str(row.get("trigger_source") or ""),
                        "status": str(row.get("status") or ""),
                        "ok": _to_int(row.get("ok"), 0) > 0,
                        "deployed": _to_int(row.get("deployed"), 0) > 0,
                        "reason": str(row.get("reason") or ""),
                        "candles_rows": _to_int(row.get("candles_rows"), 0),
                        "options_rows": _to_int(row.get("options_rows"), 0),
                        "candidate_score": _to_float(row.get("candidate_score"), 0.0),
                        "baseline_score": _to_float(row.get("baseline_score"), 0.0),
                        "improvement": _to_float(row.get("improvement"), 0.0),
                        "phase_delta": _to_float(row.get("phase_delta"), 0.0),
                        "dominant_delta": _to_float(row.get("dominant_delta"), 0.0),
                        "guidance_delta": _to_float(row.get("guidance_delta"), 0.0),
                        "next_delta": _to_float(row.get("next_delta"), 0.0),
                        "learned_text": str(row.get("learned_text") or ""),
                        "failure_text": str(row.get("failure_text") or ""),
                    }
                )
            return out
        except Exception:
            return []
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    def _table_columns(self, conn: Any, table: str) -> set[str]:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                """,
                (self.settings.auth_mysql_database, str(table)),
            )
            return {str(row[0]) for row in (cur.fetchall() or []) if row and row[0]}
        finally:
            cur.close()

    @staticmethod
    def _pick(columns: set[str], *candidates: str) -> str:
        for name in candidates:
            if name in columns:
                return name
        return ""

    def _fetch_candles(self, conn: Any, since_ts: int, until_ts: int) -> List[Dict[str, Any]]:
        table = self.settings.runtime_mysql_table_1m
        columns = self._table_columns(conn, table)
        ts_col = self._pick(columns, "timestamp", "ts", "start", "time")
        o_col = self._pick(columns, "open", "o")
        h_col = self._pick(columns, "high", "h")
        l_col = self._pick(columns, "low", "l")
        c_col = self._pick(columns, "close", "c")
        v_col = self._pick(columns, "volume", "v")
        if not (ts_col and o_col and h_col and l_col and c_col and v_col):
            raise RuntimeError(f"Unable to resolve candle columns in table `{table}`.")
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT
                    `{ts_col}` AS timestamp,
                    `{o_col}` AS open,
                    `{h_col}` AS high,
                    `{l_col}` AS low,
                    `{c_col}` AS close,
                    `{v_col}` AS volume
                FROM `{table}`
                WHERE `{ts_col}` >= %s AND `{ts_col}` <= %s
                ORDER BY `{ts_col}` ASC
                """,
                (int(since_ts), int(until_ts)),
            )
            rows = cur.fetchall() or []
            out: List[Dict[str, Any]] = []
            for row in rows:
                ts = _to_int(row.get("timestamp"), 0)
                if ts <= 0:
                    continue
                out.append(
                    {
                        "timestamp": ts,
                        "open": _to_float(row.get("open")),
                        "high": _to_float(row.get("high")),
                        "low": _to_float(row.get("low")),
                        "close": _to_float(row.get("close")),
                        "volume": _to_float(row.get("volume")),
                    }
                )
            return out
        finally:
            cur.close()

    def _fetch_options(self, conn: Any, since_ts: int, until_ts: int) -> List[Dict[str, Any]]:
        table = self.settings.runtime_mysql_options_table
        columns = self._table_columns(conn, table)
        ts_col = self._pick(columns, "timestamp", "ts", "start", "time")
        if not ts_col:
            return []
        mappings = {
            "atm_ce_ltp": self._pick(columns, "atm_ce_ltp"),
            "atm_pe_ltp": self._pick(columns, "atm_pe_ltp"),
            "atm_ce_volume": self._pick(columns, "atm_ce_volume"),
            "atm_pe_volume": self._pick(columns, "atm_pe_volume"),
            "atm_ce_oi_change": self._pick(columns, "atm_ce_oi_change"),
            "atm_pe_oi_change": self._pick(columns, "atm_pe_oi_change"),
            "band_volume_near_atm": self._pick(columns, "band_volume_near_atm"),
            "band_volume_total": self._pick(columns, "band_volume_total"),
        }
        fields = [f"`{ts_col}` AS timestamp"]
        for key, col in mappings.items():
            if col:
                fields.append(f"`{col}` AS {key}")
        sql = ", ".join(fields)
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT {sql}
                FROM `{table}`
                WHERE `{ts_col}` >= %s AND `{ts_col}` <= %s
                ORDER BY `{ts_col}` ASC
                """,
                (int(since_ts), int(until_ts)),
            )
            rows = cur.fetchall() or []
            out: List[Dict[str, Any]] = []
            for row in rows:
                ts = _to_int(row.get("timestamp"), 0)
                if ts <= 0:
                    continue
                out.append(
                    {
                        "timestamp": ts,
                        "atm_ce_ltp": _to_float(row.get("atm_ce_ltp")),
                        "atm_pe_ltp": _to_float(row.get("atm_pe_ltp")),
                        "atm_ce_volume": _to_float(row.get("atm_ce_volume")),
                        "atm_pe_volume": _to_float(row.get("atm_pe_volume")),
                        "atm_ce_oi_change": _to_float(row.get("atm_ce_oi_change")),
                        "atm_pe_oi_change": _to_float(row.get("atm_pe_oi_change")),
                        "band_volume_near_atm": _to_float(row.get("band_volume_near_atm")),
                        "band_volume_total": _to_float(row.get("band_volume_total")),
                    }
                )
            return out
        finally:
            cur.close()

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
        if group == "put_buyers":
            return 1
        if group == "call_buyers":
            return -1
        direct = str(direction or "").strip().upper()
        if direct == "LONG":
            return 1
        if direct == "SHORT":
            return -1
        return 0

    @staticmethod
    def _is_expiry_day_ist(ts: int) -> bool:
        if _to_int(ts, 0) <= 0:
            return False
        try:
            moment = datetime.fromtimestamp(int(ts), tz=_IST)
        except Exception:
            return False
        return int(moment.weekday()) == 3

    @staticmethod
    def _score_from_points(points: float, baseline: float) -> float:
        base = max(1.0, abs(float(_to_float(baseline, 1.0))))
        return max(0.0, min(1.0, abs(float(_to_float(points, 0.0))) / base))

    def _classify_trade_mistake(
        self,
        *,
        row: Dict[str, Any],
        signal: Dict[str, Any],
        candles: List[Dict[str, Any]],
        ts_values: List[int],
    ) -> Dict[str, Any]:
        existing_type = self._normalize_mistake_type(signal.get("mistake_type"))
        if existing_type != "none":
            return {
                "mistake_type": existing_type,
                "mistake_score": max(0.0, min(1.0, _to_float(signal.get("mistake_score"), 0.0))),
                "mistake_reason": str(signal.get("mistake_reason") or "").strip(),
            }

        entry_ts = _to_int(row.get("entry_ts"), 0)
        exit_ts = _to_int(row.get("exit_ts"), 0)
        holding = _to_int(row.get("holding_seconds"), max(0, exit_ts - entry_ts))
        points = _to_float(row.get("points"), 0.0)
        close_reason = str(row.get("close_reason") or "").strip().lower()
        direction = str(row.get("direction") or "").strip().upper()
        next_group = str(signal.get("next_likely_pain_group") or "").strip().lower()
        regime_tag = str(signal.get("regime_tag") or "").strip().lower()

        idx_left = bisect_left(ts_values, entry_ts)
        idx_right = bisect_right(ts_values, entry_ts)
        pre20 = candles[max(0, idx_left - 20) : idx_left]
        pre5 = candles[max(0, idx_left - 5) : idx_left]
        post5 = candles[idx_right : min(len(candles), idx_right + 5)]

        entry_ref = _to_float(row.get("entry_price"), 0.0)
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
        if close_reason == "stop_loss_hit" and 0 < holding <= 5 * 60:
            return {
                "mistake_type": "false_entry",
                "mistake_score": self._score_from_points(points, max(5.0, 0.8 * atr_prev)),
                "mistake_reason": f"stop_loss_hit in {holding}s",
            }
        if expiry_day and points < 0.0 and abs(fut_move) <= max(0.5 * atr_prev, 6.0):
            return {
                "mistake_type": "expiry_noise",
                "mistake_score": self._score_from_points(points, max(5.0, 0.6 * atr_prev)),
                "mistake_reason": f"expiry_day low follow-through move={fut_move:.2f}",
            }
        if (
            points <= 0.0
            and (
                "range" in regime_tag
                or "chop" in regime_tag
                or "compress" in regime_tag
                or close_reason in {"compression_return", "speed_fade"}
                or (close_reason == "time_exit" and flip_count >= 2)
            )
        ):
            return {
                "mistake_type": "chop_zone",
                "mistake_score": self._score_from_points(points, max(6.0, atr_prev)),
                "mistake_reason": f"range/compression trap reason={close_reason}; flips={flip_count}",
            }
        if (
            points < 0.0
            and expected_sign != 0
            and (expected_sign * fut_move) < -max(0.35 * atr_prev, 6.0)
            and opposite_count >= 3
        ):
            return {
                "mistake_type": "wrong_side_pain",
                "mistake_score": self._score_from_points(points, max(6.0, atr_prev)),
                "mistake_reason": f"opposite drift in first bars; move={fut_move:.2f}",
            }
        if points <= 0.0 and pre_move >= max(1.5 * atr_prev, 10.0):
            return {
                "mistake_type": "late_entry",
                "mistake_score": self._score_from_points(points, max(6.0, atr_prev)),
                "mistake_reason": f"pre-move extended pre={pre_move:.2f} atr={atr_prev:.2f}",
            }
        # Winning trade = no mistake; Losing trade = unclassified loss
        if points > 0.0:
            return {
                "mistake_type": "none",
                "mistake_score": 0.0,
                "mistake_reason": "winning_trade",
            }
        else:
            return {
                "mistake_type": "unclassified_loss",
                "mistake_score": self._score_from_points(points, max(6.0, atr_prev)),
                "mistake_reason": "loss_without_clear_pattern",
            }

    def _fetch_trade_labels(
        self,
        conn: Any,
        *,
        since_ts: int,
        until_ts: int,
        candles: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        table = str(self.settings.paper_trade_mysql_trades_table or "").strip()
        if not table:
            return []
        columns = self._table_columns(conn, table)
        entry_ts_col = self._pick(columns, "entry_ts", "entry_timestamp", "ts")
        if not entry_ts_col:
            return []
        exit_ts_col = self._pick(columns, "exit_ts")
        direction_col = self._pick(columns, "direction")
        points_col = self._pick(columns, "points")
        close_reason_col = self._pick(columns, "close_reason")
        holding_col = self._pick(columns, "holding_seconds")
        signal_col = self._pick(columns, "signal_json")
        status_col = self._pick(columns, "status")
        entry_price_col = self._pick(columns, "entry_price")
        fields = [f"`{entry_ts_col}` AS entry_ts"]
        if exit_ts_col:
            fields.append(f"`{exit_ts_col}` AS exit_ts")
        if direction_col:
            fields.append(f"`{direction_col}` AS direction")
        if points_col:
            fields.append(f"`{points_col}` AS points")
        if close_reason_col:
            fields.append(f"`{close_reason_col}` AS close_reason")
        if holding_col:
            fields.append(f"`{holding_col}` AS holding_seconds")
        if signal_col:
            fields.append(f"`{signal_col}` AS signal_json")
        if entry_price_col:
            fields.append(f"`{entry_price_col}` AS entry_price")
        if status_col:
            fields.append(f"`{status_col}` AS status")
        sql = ", ".join(fields)
        where = [f"`{entry_ts_col}` >= %s", f"`{entry_ts_col}` <= %s"]
        params: List[Any] = [int(since_ts), int(until_ts)]
        if status_col:
            where.append(f"LOWER(COALESCE(`{status_col}`, '')) = 'closed'")
        cur = conn.cursor(dictionary=True)
        try:
            cur.execute(
                f"""
                SELECT {sql}
                FROM `{table}`
                WHERE {" AND ".join(where)}
                ORDER BY `{entry_ts_col}` ASC
                """,
                tuple(params),
            )
            rows = cur.fetchall() or []
        finally:
            cur.close()

        if not rows:
            return []

        candles_sorted = sorted(
            [
                {
                    "timestamp": _to_int(item.get("timestamp"), 0),
                    "open": _to_float(item.get("open")),
                    "high": _to_float(item.get("high")),
                    "low": _to_float(item.get("low")),
                    "close": _to_float(item.get("close")),
                }
                for item in candles
                if _to_int(item.get("timestamp"), 0) > 0
            ],
            key=lambda item: int(item["timestamp"]),
        )
        ts_values = [int(item["timestamp"]) for item in candles_sorted]
        out: List[Dict[str, Any]] = []
        for row in rows:
            signal_raw = row.get("signal_json")
            if isinstance(signal_raw, (bytes, bytearray)):
                signal_raw = signal_raw.decode("utf-8", errors="ignore")
            try:
                signal = json.loads(str(signal_raw or "{}"))
                if not isinstance(signal, dict):
                    signal = {}
            except Exception:
                signal = {}

            mistake = self._classify_trade_mistake(
                row=row,
                signal=signal,
                candles=candles_sorted,
                ts_values=ts_values,
            )
            mistake_type = self._normalize_mistake_type(mistake.get("mistake_type"))
            points = _to_float(row.get("points"), 0.0)
            label_ts = _to_int(signal.get("release_ts"), _to_int(row.get("entry_ts"), 0))
            if label_ts <= 0:
                continue
            entry_trigger = "1" if (points > 0.0 and mistake_type == "none") else "0"
            out.append(
                {
                    "timestamp": label_ts,
                    "pain_phase": str(signal.get("pain_phase") or "comfort"),
                    "dominant_pain_group": str(signal.get("dominant_pain_group") or "none"),
                    "next_likely_pain_group": str(signal.get("next_likely_pain_group") or "none"),
                    "guidance": str(signal.get("guidance") or "observe"),
                    "entry_trigger": entry_trigger,
                    "stop_loss_distance": _to_float(signal.get("stop_loss_distance"), 0.0),
                    "target_level": _to_float(signal.get("target_level"), 0.0),
                    "mistake_type": mistake_type,
                    "mistake_score": max(0.0, min(1.0, _to_float(mistake.get("mistake_score"), 0.0))),
                }
            )
        return out

    @staticmethod
    def _write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        file_path = Path(path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key, "") for key in fieldnames})

    def _deploy_decision(self, baseline: Dict[str, Any], candidate: Dict[str, Any], force: bool) -> Dict[str, Any]:
        candidate_test_rows = _to_int(candidate.get("test_rows"), 0)
        if candidate_test_rows < self.min_test_rows:
            return {
                "accept": False,
                "reason": "candidate_test_rows_too_low",
                "candidate_test_rows": candidate_test_rows,
                "required_test_rows": self.min_test_rows,
            }

        candidate_score = _weighted_score(candidate)
        candidate_phase_macro_f1 = _target_macro_f1(candidate, "pain_phase")
        candidate_dominant_macro_f1 = _target_macro_f1(candidate, "dominant_pain_group")
        candidate_guidance_macro_f1 = _target_macro_f1(candidate, "guidance")
        candidate_next_macro_f1 = _target_macro_f1(candidate, "next_likely_pain_group")
        candidate_mistake_macro_f1 = _target_macro_f1(candidate, "mistake_type")
        decision: Dict[str, Any] = {
            "accept": True,
            "reason": "no_baseline",
            "candidate_score": candidate_score,
            "baseline_score": 0.0,
            "improvement": candidate_score,
            "candidate_phase_macro_f1": candidate_phase_macro_f1,
            "candidate_dominant_macro_f1": candidate_dominant_macro_f1,
            "candidate_guidance_macro_f1": candidate_guidance_macro_f1,
            "candidate_next_macro_f1": candidate_next_macro_f1,
            "candidate_mistake_macro_f1": candidate_mistake_macro_f1,
        }

        recall_guards: List[Dict[str, Any]] = []

        def enforce_recall_guard(
            *,
            target: str,
            class_label: str,
            min_support: int,
            floor: float,
            max_drop: float,
        ) -> bool:
            support = _target_class_support(candidate, target, class_label)
            if support < int(min_support):
                return True
            candidate_recall = _target_class_recall(candidate, target, class_label)
            baseline_recall = _target_class_recall(baseline, target, class_label) if baseline else 0.0
            required = _min_required_recall(
                baseline_recall=baseline_recall,
                floor=floor,
                max_drop=max_drop,
            )
            guard_row = {
                "target": target,
                "class": class_label,
                "support": support,
                "candidate_recall": candidate_recall,
                "baseline_recall": baseline_recall,
                "required_recall": required,
            }
            recall_guards.append(guard_row)
            if candidate_recall + 1e-9 >= required:
                return True
            decision["accept"] = False
            decision["reason"] = f"recall_guard_failed_{target}_{class_label}"
            decision["failed_recall_guard"] = guard_row
            return False

        recall_guard_cfg = [
            ("entry_trigger", "1", 8, 0.12, max(self.max_core_drop * 1.5, 0.06)),
            ("next_likely_pain_group", "bearish", 25, 0.22, max(self.max_core_drop * 2.0, 0.08)),
            ("next_likely_pain_group", "bullish", 25, 0.22, max(self.max_core_drop * 2.0, 0.08)),
            ("mistake_type", "false_entry", 6, 0.08, max(self.max_core_drop * 2.0, 0.06)),
            ("mistake_type", "chop_zone", 6, 0.08, max(self.max_core_drop * 2.0, 0.06)),
        ]
        for target, class_label, min_support, floor, max_drop in recall_guard_cfg:
            if not enforce_recall_guard(
                target=target,
                class_label=class_label,
                min_support=min_support,
                floor=floor,
                max_drop=max_drop,
            ):
                decision["recall_guards"] = recall_guards
                return decision
        decision["recall_guards"] = recall_guards

        if not baseline:
            return decision

        baseline_score = _weighted_score(baseline)
        improvement = candidate_score - baseline_score
        baseline_phase_macro_f1 = _target_macro_f1(baseline, "pain_phase")
        baseline_dominant_macro_f1 = _target_macro_f1(baseline, "dominant_pain_group")
        baseline_guidance_macro_f1 = _target_macro_f1(baseline, "guidance")
        baseline_next_macro_f1 = _target_macro_f1(baseline, "next_likely_pain_group")
        baseline_mistake_macro_f1 = _target_macro_f1(baseline, "mistake_type")
        decision["baseline_score"] = baseline_score
        decision["improvement"] = improvement
        decision["baseline_phase_macro_f1"] = baseline_phase_macro_f1
        decision["baseline_dominant_macro_f1"] = baseline_dominant_macro_f1
        decision["baseline_guidance_macro_f1"] = baseline_guidance_macro_f1
        decision["baseline_next_macro_f1"] = baseline_next_macro_f1
        decision["baseline_mistake_macro_f1"] = baseline_mistake_macro_f1
        if force:
            decision["reason"] = "force_triggered"
            return decision

        phase_drop = _target_accuracy(candidate, "pain_phase") - _target_accuracy(baseline, "pain_phase")
        dominant_drop = _target_accuracy(candidate, "dominant_pain_group") - _target_accuracy(baseline, "dominant_pain_group")
        guidance_drop = _target_accuracy(candidate, "guidance") - _target_accuracy(baseline, "guidance")
        next_delta = _target_accuracy(candidate, "next_likely_pain_group") - _target_accuracy(
            baseline,
            "next_likely_pain_group",
        )
        mistake_delta = _target_accuracy(candidate, "mistake_type") - _target_accuracy(
            baseline,
            "mistake_type",
        )
        phase_macro_drop = candidate_phase_macro_f1 - baseline_phase_macro_f1
        dominant_macro_drop = candidate_dominant_macro_f1 - baseline_dominant_macro_f1
        guidance_macro_drop = candidate_guidance_macro_f1 - baseline_guidance_macro_f1
        next_macro_drop = candidate_next_macro_f1 - baseline_next_macro_f1
        mistake_macro_drop = candidate_mistake_macro_f1 - baseline_mistake_macro_f1
        decision.update(
            {
                "phase_delta": phase_drop,
                "dominant_delta": dominant_drop,
                "guidance_delta": guidance_drop,
                "next_delta": next_delta,
                "mistake_delta": mistake_delta,
                "phase_macro_delta": phase_macro_drop,
                "dominant_macro_delta": dominant_macro_drop,
                "guidance_macro_delta": guidance_macro_drop,
                "next_macro_delta": next_macro_drop,
                "mistake_macro_delta": mistake_macro_drop,
            }
        )
        if improvement < self.min_improvement:
            decision["accept"] = False
            decision["reason"] = "insufficient_improvement"
            return decision
        if phase_drop < -self.max_core_drop or dominant_drop < -self.max_core_drop or guidance_drop < -self.max_core_drop:
            decision["accept"] = False
            decision["reason"] = "core_target_drop_too_high"
            return decision
        if (
            phase_macro_drop < -max(self.max_core_drop, 0.02)
            or dominant_macro_drop < -max(self.max_core_drop, 0.02)
            or guidance_macro_drop < -max(self.max_core_drop, 0.02)
        ):
            decision["accept"] = False
            decision["reason"] = "core_macro_f1_drop_too_high"
            return decision
        if next_delta < -max(self.max_core_drop * 1.5, 0.01):
            decision["accept"] = False
            decision["reason"] = "next_group_regressed"
            return decision
        if next_macro_drop < -max(self.max_core_drop * 2.0, 0.03):
            decision["accept"] = False
            decision["reason"] = "next_group_macro_f1_regressed"
            return decision
        if _target_accuracy(baseline, "mistake_type") > 0.0 and mistake_delta < -max(self.max_core_drop, 0.02):
            decision["accept"] = False
            decision["reason"] = "mistake_type_regressed"
            return decision
        if _target_accuracy(baseline, "mistake_type") > 0.0 and mistake_macro_drop < -max(self.max_core_drop * 1.5, 0.03):
            decision["accept"] = False
            decision["reason"] = "mistake_type_macro_f1_regressed"
            return decision
        decision["reason"] = "safe_improvement"
        return decision

    def _backup_active_model(self) -> Optional[str]:
        active = Path(self.settings.pain_ai_model_path)
        if not active.exists():
            return None
        backup_root = Path(self.backup_dir)
        backup_root.mkdir(parents=True, exist_ok=True)
        stamp = int(time.time())
        backup_path = backup_root / f"{active.stem}_{stamp}{active.suffix}"
        shutil.copy2(active, backup_path)
        return str(backup_path)

    def _run_once_blocking(self, *, reason: str, force: bool) -> Dict[str, Any]:
        now_ts = int(time.time())
        since_ts = now_ts - (self.lookback_days * 86400)
        conn = None
        try:
            conn = self._connect_mysql()
            candles = self._fetch_candles(conn, since_ts=since_ts, until_ts=now_ts)
            options = self._fetch_options(conn, since_ts=since_ts, until_ts=now_ts)
            labels = self._fetch_trade_labels(
                conn,
                since_ts=since_ts,
                until_ts=now_ts,
                candles=candles,
            )
            conn.close()
            conn = None

            if len(candles) < self.min_rows:
                return {
                    "ok": False,
                    "status": "skipped",
                    "reason": "insufficient_training_rows",
                    "candles_rows": len(candles),
                    "required_rows": self.min_rows,
                    "started_at": now_ts,
                    "finished_at": int(time.time()),
                }

            with tempfile.TemporaryDirectory(prefix="pta_retrain_") as temp_dir:
                candles_path = str(Path(temp_dir) / "candles_1m.csv")
                options_path = str(Path(temp_dir) / "options.csv")
                labels_path = str(Path(temp_dir) / "labels.csv")
                self._write_csv(
                    candles_path,
                    candles,
                    ["timestamp", "open", "high", "low", "close", "volume"],
                )
                options_arg = ""
                labels_arg = ""
                if options:
                    self._write_csv(
                        options_path,
                        options,
                        [
                            "timestamp",
                            "atm_ce_ltp",
                            "atm_pe_ltp",
                            "atm_ce_volume",
                            "atm_pe_volume",
                            "atm_ce_oi_change",
                            "atm_pe_oi_change",
                            "band_volume_near_atm",
                            "band_volume_total",
                        ],
                    )
                    options_arg = options_path
                if labels:
                    self._write_csv(
                        labels_path,
                        labels,
                        [
                            "timestamp",
                            "pain_phase",
                            "dominant_pain_group",
                            "next_likely_pain_group",
                            "guidance",
                            "entry_trigger",
                            "stop_loss_distance",
                            "target_level",
                            "mistake_type",
                            "mistake_score",
                        ],
                    )
                    labels_arg = labels_path

                metrics = train_model(
                    candles_1m_path=candles_path,
                    candles_5m_path="",
                    options_path=options_arg,
                    labels_path=labels_arg,
                    model_path=self.candidate_path,
                    window_1m=self.window_1m,
                    window_5m=self.window_5m,
                    next_horizon=self.next_horizon,
                    test_size=self.test_size,
                )

            baseline = _read_artifact_metrics(self.settings.pain_ai_model_path)
            decision = self._deploy_decision(baseline, metrics, force=bool(force))
            backup_path = None
            deployed = False
            if bool(decision.get("accept")):
                backup_path = self._backup_active_model()
                Path(self.settings.pain_ai_model_path).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(self.candidate_path, self.settings.pain_ai_model_path)
                self.runtime.reload_model(self.settings.pain_ai_model_path, self.settings.pain_ai_use_model)
                deployed = True

            return {
                "ok": True,
                "status": "completed",
                "deployed": deployed,
                "reason": str(decision.get("reason") or ""),
                "trigger": str(reason or "manual"),
                "started_at": now_ts,
                "finished_at": int(time.time()),
                "candles_rows": len(candles),
                "options_rows": len(options),
                "labels_rows": len(labels),
                "mistake_rows": sum(1 for item in labels if str(item.get("mistake_type") or "none") != "none"),
                "candidate_model_path": self.candidate_path,
                "active_model_path": self.settings.pain_ai_model_path,
                "backup_model_path": backup_path,
                "candidate_metrics": metrics,
                "baseline_metrics": baseline,
                "decision": decision,
            }
        except Exception as exc:
            return {
                "ok": False,
                "status": "failed",
                "error": str(exc),
                "trigger": str(reason or "manual"),
                "started_at": now_ts,
                "finished_at": int(time.time()),
            }
        finally:
            if conn is not None:
                conn.close()

    async def run_once(self, *, reason: str, force: bool = False) -> Dict[str, Any]:
        if not self.enabled and not force:
            now_ts = int(time.time())
            result = {
                "ok": False,
                "status": "disabled",
                "message": "MODEL_AUTO_RETRAIN_ENABLED is false.",
                "trigger": str(reason or "manual"),
                "started_at": now_ts,
                "finished_at": now_ts,
            }
            await asyncio.to_thread(self._persist_run, result)
            return result
        if self._lock.locked():
            now_ts = int(time.time())
            result = {
                "ok": False,
                "status": "busy",
                "message": "Auto retrain is already running.",
                "trigger": str(reason or "manual"),
                "started_at": now_ts,
                "finished_at": now_ts,
            }
            await asyncio.to_thread(self._persist_run, result)
            return result
        async with self._lock:
            self._running = True
            self._run_count += 1
            self._last_started_ts = int(time.time())
            try:
                result = await asyncio.to_thread(self._run_once_blocking, reason=reason, force=force)
            except Exception as exc:
                result = {
                    "ok": False,
                    "status": "failed",
                    "error": str(exc),
                    "trigger": str(reason or "manual"),
                    "started_at": self._last_started_ts,
                    "finished_at": int(time.time()),
                }
            self._last_result = dict(result)
            self._last_finished_ts = _to_int(result.get("finished_at"), int(time.time()))
            if bool(result.get("ok")):
                self._success_count += 1
                if bool(result.get("deployed")):
                    self._last_success_ts = self._last_finished_ts
            else:
                self._failure_count += 1
            await asyncio.to_thread(self._persist_run, result)
            self._running = False
            return dict(result)

    def get_status(self) -> Dict[str, Any]:
        baseline = _read_artifact_metrics(self.settings.pain_ai_model_path)
        return {
            "enabled": self.enabled,
            "running": self._running,
            "run_store_ready": self._run_store_ready,
            "run_store_error": self._run_store_error,
            "runs_table": self.runs_table,
            "interval_sec": self.interval_sec,
            "daily_time": str(self.settings.model_auto_retrain_daily_time or ""),
            "lookback_days": self.lookback_days,
            "min_rows": self.min_rows,
            "min_test_rows": self.min_test_rows,
            "min_improvement": self.min_improvement,
            "max_core_drop": self.max_core_drop,
            "window_1m": self.window_1m,
            "window_5m": self.window_5m,
            "next_horizon": self.next_horizon,
            "test_size": self.test_size,
            "run_count": self._run_count,
            "success_count": self._success_count,
            "failure_count": self._failure_count,
            "last_started_ts": self._last_started_ts,
            "last_finished_ts": self._last_finished_ts,
            "last_success_ts": self._last_success_ts,
            "active_model_path": self.settings.pain_ai_model_path,
            "candidate_model_path": self.candidate_path,
            "baseline_metrics": baseline,
            "last_result": dict(self._last_result),
        }
