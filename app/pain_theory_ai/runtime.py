from __future__ import annotations

from collections import deque
from typing import Any, Deque, Dict, List, Optional

from ..ml.pain_theory_model import PainTheoryModelBundle, load_pain_theory_model, predict_pain_theory
from .core import (
    FEATURE_COLUMNS,
    build_explanation,
    build_dual_timeframe_features,
    classify_from_rules,
)


class PainTheoryRuntime:
    def __init__(
        self,
        model_path: str = "",
        use_model: bool = True,
        max_candles: int = 1440,
        max_candles_5m: int = 720,
        max_options: int = 3600,
        use_5m_primary: bool = True,
    ) -> None:
        # 1m candles are kept for execution timing context.
        self.candles: Deque[Dict[str, float]] = deque(maxlen=max_candles)
        # 5m candles are used as primary analysis timeframe.
        self.candles_5m: Deque[Dict[str, float]] = deque(maxlen=max_candles_5m)
        self.option_snapshots: Deque[Dict[str, Any]] = deque(maxlen=max_options)
        self.use_5m_primary = bool(use_5m_primary)
        self.model_path = str(model_path or "")
        self.use_model = bool(use_model)
        self.last_phase = "comfort"
        self.model: Optional[PainTheoryModelBundle] = None
        if self.use_model and self.model_path:
            self.model = load_pain_theory_model(self.model_path)

        self.latest_state: Dict[str, Any] = {
            "pain_phase": "comfort",
            "dominant_pain_group": "none",
            "next_likely_pain_group": "none",
            "confidence": 0.0,
            "guidance": "observe",
            "entry_signal": False,
            "entry_direction": "NONE",
            "entry_trigger_confidence": 0.0,
            "mistake_type": "none",
            "mistake_confidence": 0.0,
            "stop_loss_distance": 0.0,
            "target_level": 0.0,
            "recommended_stop_loss": 0.0,
            "recommended_targets": [],
            "explanation": (
                "pain_phase is comfort.\n"
                "dominant_pain_group is none; next_likely_pain_group is none.\n"
                "Balanced movement shows no clear pain transfer edge.\n"
                "guidance is observe; waiting for enough rolling data."
            ),
            "analysis_timeframe": "5m" if self.use_5m_primary else "1m",
            "execution_timeframe": "1m",
            "analysis_context": {
                "enabled": True,
                "available": False,
                "trend": "unknown",
                "momentum": "flat",
                "score": 0.0,
                "alignment": "neutral",
            },
            "one_min_context": {
                "enabled": True,
                "available": False,
                "trend": "unknown",
                "momentum": "flat",
                "score": 0.0,
                "alignment": "neutral",
            },
            "five_min_context": {
                "enabled": True,
                "available": False,
                "trend": "unknown",
                "momentum": "flat",
                "score": 0.0,
                "alignment": "neutral",
            },
        }

    @staticmethod
    def _normalize_candle(candle: Dict[str, Any]) -> Optional[Dict[str, float]]:
        row = {
            "timestamp": int(float(candle.get("timestamp", 0))),
            "open": float(candle.get("open", 0.0)),
            "high": float(candle.get("high", 0.0)),
            "low": float(candle.get("low", 0.0)),
            "close": float(candle.get("close", 0.0)),
            "volume": float(candle.get("volume", 0.0)),
        }
        if row["timestamp"] <= 0:
            return None
        return row

    @staticmethod
    def _append_candle_row(target: Deque[Dict[str, float]], row: Dict[str, float]) -> None:
        row_ts = int(row["timestamp"])
        if target and int(target[-1]["timestamp"]) == row_ts:
            target[-1] = row
        elif not target or int(target[-1]["timestamp"]) < row_ts:
            target.append(row)

    @staticmethod
    def _tail_rows(source: Deque[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
        if limit <= 0:
            return []
        if not source:
            return []
        size = len(source)
        if limit >= size:
            return list(source)
        rows: List[Dict[str, Any]] = []
        rev_iter = reversed(source)
        for _ in range(limit):
            rows.append(next(rev_iter))
        rows.reverse()
        return rows

    @staticmethod
    def _sort_by_timestamp(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if len(rows) <= 1:
            return rows
        return sorted(rows, key=lambda item: int(float(item.get("timestamp", 0))))

    def _append_candle(self, candle: Dict[str, Any]) -> None:
        row = self._normalize_candle(candle)
        if row is None:
            return
        self._append_candle_row(self.candles, row)

    def _append_candle_5m(self, candle: Dict[str, Any]) -> None:
        row = self._normalize_candle(candle)
        if row is None:
            return
        self._append_candle_row(self.candles_5m, row)

    def _append_option(self, snapshot: Dict[str, Any]) -> None:
        item = dict(snapshot)
        try:
            item["timestamp"] = int(float(snapshot.get("timestamp", 0)))
        except (TypeError, ValueError):
            return
        if item["timestamp"] <= 0:
            return
        if self.option_snapshots and int(self.option_snapshots[-1].get("timestamp", 0)) == item["timestamp"]:
            self.option_snapshots[-1] = item
        elif not self.option_snapshots or int(self.option_snapshots[-1].get("timestamp", 0)) < item["timestamp"]:
            self.option_snapshots.append(item)

    def _recent_options_for(self, timestamp: int, limit: int = 30) -> List[Dict[str, Any]]:
        max_rows = max(1, int(limit))
        out: List[Dict[str, Any]] = []
        for row in reversed(self.option_snapshots):
            if int(row.get("timestamp", 0)) <= timestamp:
                out.append(row)
                if len(out) >= max_rows:
                    break
        out.reverse()
        return out

    @staticmethod
    def _direction(value: float) -> int:
        if value > 0.0:
            return 1
        if value < 0.0:
            return -1
        return 0

    def _trend_context(self, candles: Deque[Dict[str, float]], enabled: bool = True) -> Dict[str, Any]:
        if len(candles) < 2:
            return {
                "enabled": bool(enabled),
                "available": False,
                "trend": "unknown",
                "momentum": "flat",
                "score": 0.0,
                "alignment": "neutral",
            }

        latest = candles[-1]
        prev = candles[-2]
        body_direction = self._direction(float(latest["close"]) - float(latest["open"]))
        close_momentum = self._direction(float(latest["close"]) - float(prev["close"]))
        score = float(body_direction) + float(close_momentum)
        trend = "sideways"
        if score > 0.0:
            trend = "up"
        elif score < 0.0:
            trend = "down"
        momentum = "flat"
        if close_momentum > 0:
            momentum = "rising"
        elif close_momentum < 0:
            momentum = "falling"
        return {
            "enabled": bool(enabled),
            "available": True,
            "trend": trend,
            "momentum": momentum,
            "score": score,
            "alignment": "neutral",
        }

    def _analysis_candles(self) -> Deque[Dict[str, float]]:
        if self.use_5m_primary:
            return self.candles_5m
        return self.candles

    def _analysis_step_sec(self) -> int:
        return 300 if self.use_5m_primary else 60

    def _apply_execution_filter(
        self,
        next_group: str,
        guidance: str,
        confidence: float,
        context: Dict[str, Any],
    ) -> tuple[str, float, str]:
        if not bool(context.get("available", False)):
            return guidance, confidence, "neutral"

        score = float(context.get("score", 0.0))
        text = str(next_group or "").strip().lower()
        long_signal = text in {"put_buyers", "call_sellers", "bearish"}
        short_signal = text in {"call_buyers", "put_sellers", "bullish"}
        if not long_signal and not short_signal:
            return guidance, confidence, "neutral"

        alignment = "aligned"
        adjusted_guidance = guidance
        adjusted_confidence = confidence

        if (long_signal and score < 0.0) or (short_signal and score > 0.0):
            alignment = "counter_trend"
            adjusted_confidence = confidence * 0.75
            if guidance == "caution":
                adjusted_guidance = "observe"
        elif abs(score) >= 1.0:
            adjusted_confidence = confidence * 1.05

        adjusted_confidence = max(0.0, min(1.0, adjusted_confidence))
        return adjusted_guidance, adjusted_confidence, alignment

    def _recompute(self) -> None:
        analysis_candles = self._analysis_candles()
        if len(analysis_candles) < 30:
            return
        if len(self.candles) < 30:
            return

        latest_ts = int(analysis_candles[-1]["timestamp"])
        analysis_window = self._tail_rows(analysis_candles, 30)
        execution_window = self._tail_rows(self.candles, 30)
        option_window_analysis = self._recent_options_for(latest_ts, limit=90 if self.use_5m_primary else 30)
        option_window_execution = self._recent_options_for(latest_ts, limit=30)
        features = build_dual_timeframe_features(
            analysis_window,
            execution_window,
            analysis_options=option_window_analysis,
            execution_options=option_window_execution,
        )

        if self.model is not None:
            predicted = predict_pain_theory(self.model, features)
        else:
            predicted = classify_from_rules(features, self.last_phase)

        pain_phase = str(predicted.get("pain_phase", "comfort"))
        dominant = str(predicted.get("dominant_pain_group", "none"))
        next_group = str(predicted.get("next_likely_pain_group", "none"))
        guidance = str(predicted.get("guidance", "observe"))
        confidence = float(predicted.get("confidence", 0.0))
        entry_signal = bool(predicted.get("entry_signal", False))
        entry_direction = str(predicted.get("entry_direction", "NONE")).upper()
        entry_trigger_confidence = float(predicted.get("entry_trigger_confidence", confidence))
        mistake_type = str(predicted.get("mistake_type", "none")).strip().lower() or "none"
        mistake_confidence = max(0.0, min(1.0, float(predicted.get("mistake_confidence", 0.0))))
        stop_loss_distance = max(0.0, float(predicted.get("stop_loss_distance", 0.0)))
        target_level = max(0.0, float(predicted.get("target_level", 0.0)))
        recommended_stop_loss = max(0.0, float(predicted.get("recommended_stop_loss", 0.0)))
        raw_targets = predicted.get("recommended_targets") if isinstance(predicted.get("recommended_targets"), list) else []
        recommended_targets = [max(0.0, float(item)) for item in raw_targets if isinstance(item, (int, float))]
        analysis_context = self._trend_context(analysis_candles, enabled=True)
        execution_context = self._trend_context(self.candles, enabled=True)
        guidance, confidence, alignment = self._apply_execution_filter(
            next_group=next_group,
            guidance=guidance,
            confidence=confidence,
            context=execution_context,
        )
        if guidance in {"wait", "observe"} or confidence < 0.5:
            entry_signal = False
        execution_context["alignment"] = alignment

        explanation = build_explanation(
            pain_phase=pain_phase,
            dominant_pain_group=dominant,
            next_likely_pain_group=next_group,
            guidance=guidance,
            features=features,
            entry_signal=entry_signal,
            stop_loss_distance=stop_loss_distance,
            target_level=target_level,
        )
        if self.use_5m_primary:
            explanation += (
                f"\nPrimary analysis timeframe is 5m "
                f"(trend={analysis_context.get('trend')}, momentum={analysis_context.get('momentum')}, "
                f"score={float(analysis_context.get('score', 0.0)):.2f})."
            )
        if bool(execution_context.get("available", False)):
            explanation += (
                f"\n1m execution filter is {alignment} "
                f"(trend={execution_context.get('trend')}, momentum={execution_context.get('momentum')}, "
                f"score={float(execution_context.get('score', 0.0)):.2f})."
            )

        five_min_context = (
            dict(analysis_context)
            if self.use_5m_primary
            else self._trend_context(self.candles_5m, enabled=True)
        )
        five_min_context["alignment"] = "primary" if self.use_5m_primary else five_min_context.get("alignment", "neutral")

        self.latest_state = {
            "timestamp": latest_ts + self._analysis_step_sec(),
            "pain_phase": pain_phase,
            "dominant_pain_group": dominant,
            "next_likely_pain_group": next_group,
            "confidence": max(0.0, min(1.0, confidence)),
            "guidance": guidance,
            "entry_signal": bool(entry_signal),
            "entry_direction": entry_direction,
            "entry_trigger_confidence": max(0.0, min(1.0, entry_trigger_confidence)),
            "mistake_type": mistake_type,
            "mistake_confidence": mistake_confidence,
            "stop_loss_distance": stop_loss_distance,
            "target_level": target_level,
            "recommended_stop_loss": recommended_stop_loss,
            "recommended_targets": recommended_targets,
            "explanation": explanation,
            "features": {key: float(features.get(key, 0.0)) for key in FEATURE_COLUMNS},
            "analysis_timeframe": "5m" if self.use_5m_primary else "1m",
            "execution_timeframe": "1m",
            "analysis_context": analysis_context,
            "one_min_context": execution_context,
            "five_min_context": five_min_context,
        }
        self.last_phase = pain_phase

    def ingest(
        self,
        candles: List[Dict[str, Any]],
        candles_5m: Optional[List[Dict[str, Any]]] = None,
        option_snapshots: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        for candle in self._sort_by_timestamp(candles):
            self._append_candle(candle)

        if candles_5m:
            for candle in self._sort_by_timestamp(candles_5m):
                self._append_candle_5m(candle)

        if option_snapshots:
            for item in self._sort_by_timestamp(option_snapshots):
                self._append_option(item)

        self._recompute()
        return self.latest_state

    def get_state(self) -> Dict[str, Any]:
        return dict(self.latest_state)

    def get_recent_candles(self, timeframe: str = "1m", limit: int = 500) -> List[Dict[str, float]]:
        max_rows = max(10, min(int(limit), 5000))
        source = self.candles_5m if str(timeframe).strip().lower() == "5m" else self.candles
        return [dict(row) for row in self._tail_rows(source, max_rows)]

    def get_recent_options_for(self, timestamp: int, limit: int = 30) -> List[Dict[str, Any]]:
        max_rows = max(1, min(int(limit), 5000))
        return [dict(row) for row in self._recent_options_for(int(timestamp), limit=max_rows)]

    def reload_model(self, model_path: str | None = None, use_model: Optional[bool] = None) -> bool:
        if use_model is not None:
            self.use_model = bool(use_model)
        if model_path is not None:
            self.model_path = str(model_path or "")
        if not self.use_model:
            self.model = None
            return False
        if not self.model_path:
            self.model = None
            return False
        bundle = load_pain_theory_model(self.model_path)
        self.model = bundle
        return self.model is not None
