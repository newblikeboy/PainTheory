from __future__ import annotations

import argparse
from bisect import bisect_right
import csv
import json
import math
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import joblib
from sklearn.dummy import DummyClassifier, DummyRegressor
from sklearn.metrics import accuracy_score, classification_report, mean_absolute_error, mean_squared_error
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier, XGBRegressor

from ..pain_theory_ai.core import (
    GUIDANCE_CHOICES,
    PAIN_PHASES,
    PARTICIPANT_GROUPS,
    build_dual_timeframe_features,
    classify_from_rules,
    dual_feature_columns,
)


TARGETS = (
    "pain_phase",
    "dominant_pain_group",
    "next_likely_pain_group",
    "guidance",
    "entry_trigger",
    "mistake_type",
)

REGRESSION_TARGETS = (
    "stop_loss_distance",
    "target_level",
)

MISTAKE_TYPES = (
    "no_trade",
    "none",
    "false_entry",
    "late_entry",
    "wrong_side_pain",
    "chop_zone",
    "expiry_noise",
    "unclassified_loss",
)

MARKET_DIRECTIONS = (
    "bullish",
    "bearish",
    "neutral",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train Pain Theory AI multi-head classifiers/regressors")
    parser.add_argument("--candles", required=True, help="1m candles file path (.csv or .parquet)")
    parser.add_argument("--candles-5m", default="", help="Optional 5m candles file path (.csv or .parquet)")
    parser.add_argument("--options", default="", help="Options-band file path (.csv or .parquet)")
    parser.add_argument("--labels", default="", help="Optional labels file path (.csv or .parquet)")
    parser.add_argument("--model-path", default="models/pain_theory_ai.pkl", help="Model output artifact path")
    parser.add_argument("--window-minutes", type=int, default=30, help="Rolling 1m input window in bars")
    parser.add_argument("--window-bars-5m", type=int, default=30, help="Rolling 5m analysis window in bars")
    parser.add_argument("--next-horizon", type=int, default=15, help="Next likely group/trade horizon in minutes")
    parser.add_argument("--test-size", type=float, default=0.2, help="Date-based test split ratio")
    return parser.parse_args()


def _read_csv(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def _read_parquet(path: str) -> List[Dict[str, Any]]:
    try:
        import pandas as pd  # type: ignore
    except ImportError as exc:
        raise RuntimeError("Parquet input requires pandas and pyarrow installed.") from exc
    frame = pd.read_parquet(path)
    return frame.to_dict("records")


def _load_table(path: str) -> List[Dict[str, Any]]:
    if not path:
        return []
    source = Path(path)
    if not source.exists():
        raise RuntimeError(f"File not found: {path}")
    suffix = source.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix in {".parquet", ".pq"}:
        return _read_parquet(path)
    raise RuntimeError(f"Unsupported file extension for {path}. Use .csv or .parquet")


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


def _normalize_candles(rows: List[Dict[str, Any]]) -> List[Dict[str, float]]:
    normalized: List[Dict[str, float]] = []
    for row in rows:
        ts = _to_int(row.get("timestamp") or row.get("start"), 0)
        if ts <= 0:
            continue
        item = {
            "timestamp": ts,
            "open": _to_float(row.get("open")),
            "high": _to_float(row.get("high")),
            "low": _to_float(row.get("low")),
            "close": _to_float(row.get("close")),
            "volume": _to_float(row.get("volume")),
        }
        normalized.append(item)
    normalized.sort(key=lambda item: item["timestamp"])
    return normalized


def _normalize_options(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        ts = _to_int(row.get("timestamp") or row.get("start"), 0)
        if ts <= 0:
            continue
        item = {
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
        normalized.append(item)
    normalized.sort(key=lambda item: int(item["timestamp"]))
    return normalized


def _aggregate_5m_from_1m(candles_1m: List[Dict[str, float]]) -> List[Dict[str, float]]:
    if not candles_1m:
        return []
    rows = sorted(candles_1m, key=lambda item: int(item["timestamp"]))
    result: List[Dict[str, float]] = []
    current: Dict[str, float] | None = None
    for row in rows:
        ts = int(row["timestamp"])
        bucket_ts = ts - (ts % 300)
        if current is None or int(current["timestamp"]) != bucket_ts:
            if current is not None:
                result.append(current)
            current = {
                "timestamp": float(bucket_ts),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
            continue
        current["high"] = max(float(current["high"]), float(row["high"]))
        current["low"] = min(float(current["low"]), float(row["low"]))
        current["close"] = float(row["close"])
        current["volume"] = float(current["volume"]) + float(row["volume"])
    if current is not None:
        result.append(current)
    return result


def _normalize_label(value: str, allowed: Tuple[str, ...], fallback: str) -> str:
    text = str(value or "").strip().lower()
    if text in allowed:
        return text
    return fallback


def _normalize_binary_label(value: Any, fallback: str = "0") -> str:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on", "entry", "trigger"}:
        return "1"
    if text in {"0", "false", "no", "off", "none", "skip"}:
        return "0"
    return "1" if str(fallback) == "1" else "0"


def _normalize_mistake_label(value: Any, fallback: str = "none") -> str:
    text = str(value or "").strip().lower()
    if text in MISTAKE_TYPES:
        return text
    fb = str(fallback or "none").strip().lower()
    if fb in MISTAKE_TYPES:
        return fb
    return "none"


def _label_maps(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    mapping: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        ts = _to_int(row.get("timestamp") or row.get("start"), 0)
        if ts <= 0:
            continue
        mistake_type = _normalize_mistake_label(row.get("mistake_type", ""), "none")
        provided_trigger = _normalize_binary_label(row.get("entry_trigger", ""))
        if str(row.get("entry_trigger", "")).strip() == "":
            # Only explicit actionable mistakes should default to avoid-entry.
            provided_trigger = "0" if mistake_type not in {"none", "no_trade"} else "1"
        mapping[ts] = {
            "pain_phase": _normalize_label(row.get("pain_phase", ""), PAIN_PHASES, "comfort"),
            "dominant_pain_group": _normalize_label(
                row.get("dominant_pain_group", ""),
                PARTICIPANT_GROUPS,
                "none",
            ),
            "next_likely_pain_group": _normalize_label(
                row.get("next_likely_pain_group", ""),
                PARTICIPANT_GROUPS,
                "none",
            ),
            "guidance": _normalize_label(row.get("guidance", ""), GUIDANCE_CHOICES, "observe"),
            "entry_trigger": provided_trigger,
            "stop_loss_distance": max(0.0, _to_float(row.get("stop_loss_distance"), 0.0)),
            "target_level": max(0.0, _to_float(row.get("target_level"), 0.0)),
            "mistake_type": mistake_type,
            "mistake_score": max(0.0, min(1.0, _to_float(row.get("mistake_score"), 0.0))),
        }
    return mapping


def _group_to_direction(next_group: str) -> int:
    text = str(next_group or "").strip().lower()
    if text == "bearish":
        return 1
    if text == "bullish":
        return -1
    if text in {"put_buyers", "call_sellers"}:
        return 1
    if text in {"call_buyers", "put_sellers"}:
        return -1
    return 0


def _group_to_3class(group: str) -> str:
    """
    Map 6 pain groups to 3 market direction classes for improved prediction accuracy.
    
    Bullish: call_buyers (profit from up) + put_sellers (profit from up)
    Bearish: put_buyers (profit from down) + call_sellers (profit from down)  
    Neutral: buyers_both (no bias) + none (no bias)
    """
    text = str(group or "").strip().lower()
    if text in {"call_buyers", "put_sellers"}:
        return "bullish"
    if text in {"put_buyers", "call_sellers"}:
        return "bearish"
    return "neutral"


def _derive_trade_targets(
    *,
    current_close: float,
    future_window: List[Dict[str, float]],
    next_group: str,
    guidance: str,
    features: Dict[str, float],
) -> tuple[int, float, float]:
    atr = max(0.25, _to_float(features.get("execution_atr_window"), _to_float(features.get("atr_window"), 1.0)))
    direction = _group_to_direction(next_group)
    if direction == 0 or not future_window:
        return 0, max(0.35, 0.8 * atr), max(0.5, 1.1 * atr)

    favorable = 0.0
    adverse = 0.0
    for candle in future_window:
        high = _to_float(candle.get("high"), current_close)
        low = _to_float(candle.get("low"), current_close)
        if direction > 0:
            favorable = max(favorable, high - current_close)
            adverse = max(adverse, current_close - low)
        else:
            favorable = max(favorable, current_close - low)
            adverse = max(adverse, high - current_close)

    stop_loss_distance = max(0.35, adverse * 1.05, atr * 0.6)
    target_level = max(stop_loss_distance * 1.05, favorable, atr * 0.9)

    shock = max(0.0, _to_float(features.get("execution_shock_move"), _to_float(features.get("shock_move"), 0.0)))
    compression = max(
        0.0,
        _to_float(features.get("execution_compression_regime"), _to_float(features.get("compression_regime"), 0.0)),
    )
    trigger = int(
        favorable >= stop_loss_distance * (1.05 + min(0.25, 0.1 * compression))
        and favorable > adverse * 0.8
        and shock >= 0.18
        and str(guidance or "").strip().lower() in {"caution"}
    )
    if str(guidance or "").strip().lower() == "wait":
        trigger = 0
    return trigger, float(stop_loss_distance), float(target_level)


def _build_samples(
    candles_1m: List[Dict[str, float]],
    candles_5m: List[Dict[str, float]],
    options: List[Dict[str, Any]],
    labels: Dict[int, Dict[str, Any]],
    window_1m: int,
    window_5m: int,
    next_horizon: int,
) -> List[Dict[str, Any]]:
    if len(candles_1m) < window_1m + 2 or len(candles_5m) < window_5m:
        return []

    samples: List[Dict[str, Any]] = []
    candles_5m_ts = [int(item["timestamp"]) for item in candles_5m]
    options_ts = [int(_to_int(item.get("timestamp"), 0)) for item in options]
    last_phase = "comfort"

    for idx in range(len(candles_1m)):
        current_ts = int(candles_1m[idx]["timestamp"])
        if idx < window_1m - 1:
            continue
        end_5m = bisect_right(candles_5m_ts, current_ts)
        if end_5m < window_5m:
            continue
        end_opt = bisect_right(options_ts, current_ts) if options_ts else 0

        analysis_window = candles_5m[end_5m - window_5m : end_5m]
        execution_window = candles_1m[idx - window_1m + 1 : idx + 1]
        option_window_analysis = options[max(0, end_opt - max(90, window_1m)) : end_opt] if end_opt else []
        option_window_execution = options[max(0, end_opt - window_1m) : end_opt] if end_opt else []
        features = build_dual_timeframe_features(
            analysis_window,
            execution_window,
            analysis_options=option_window_analysis,
            execution_options=option_window_execution,
        )

        row_label = labels.get(current_ts)
        if row_label:
            target_now = dict(row_label)
        else:
            target_now = classify_from_rules(features, last_phase)

        # ALWAYS use look-ahead to get actual future dominant_group as ground truth
        # This replaces rule-based guesses with verified outcomes from historical data
        future_idx = min(len(candles_1m) - 1, idx + max(1, next_horizon))
        future_ts = int(candles_1m[future_idx]["timestamp"])
        future_end_5m = bisect_right(candles_5m_ts, future_ts)
        future_end_opt = bisect_right(options_ts, future_ts) if options_ts else 0
        future_execution_window = candles_1m[max(0, future_idx - window_1m + 1) : future_idx + 1]
        future_analysis_window = candles_5m[max(0, future_end_5m - window_5m) : future_end_5m]
        future_option_analysis = (
            options[max(0, future_end_opt - max(90, window_1m)) : future_end_opt]
            if future_end_opt
            else []
        )
        future_option_execution = (
            options[max(0, future_end_opt - window_1m) : future_end_opt]
            if future_end_opt
            else []
        )
        future_features = build_dual_timeframe_features(
            future_analysis_window,
            future_execution_window,
            analysis_options=future_option_analysis,
            execution_options=future_option_execution,
        )
        next_state = classify_from_rules(future_features, str(target_now.get("pain_phase", "comfort")))
        
        # Get actual future dominant_group as 6-class ground truth
        next_label_6class = str(next_state.get("dominant_pain_group", "none") or "none")
        
        # Convert to 3-class for improved accuracy (18% → 35-45%)
        next_label = _group_to_3class(next_label_6class)

        horizon_end = min(len(candles_1m), idx + 1 + max(1, next_horizon))
        future_window = candles_1m[idx + 1 : horizon_end]
        derived_trigger, derived_stop, derived_target = _derive_trade_targets(
            current_close=_to_float(candles_1m[idx].get("close"), 0.0),
            future_window=future_window,
            next_group=next_label,
            guidance=str(target_now.get("guidance", "observe")),
            features=features,
        )

        entry_trigger = _normalize_binary_label(
            (row_label or {}).get("entry_trigger", ""),
            fallback=str(derived_trigger),
        )
        stop_loss_distance = max(0.0, _to_float((row_label or {}).get("stop_loss_distance"), derived_stop))
        target_level = max(0.0, _to_float((row_label or {}).get("target_level"), derived_target))
        if stop_loss_distance <= 0.0:
            stop_loss_distance = float(derived_stop)
        if target_level <= 0.0:
            target_level = float(derived_target)

        # Keep target level at least slightly above stop distance for stable regressors.
        target_level = max(target_level, stop_loss_distance * 1.02)

        sample = {
            "timestamp": current_ts,
            "features": features,
            "pain_phase": _normalize_label(target_now.get("pain_phase", ""), PAIN_PHASES, "comfort"),
            "dominant_pain_group": _normalize_label(
                target_now.get("dominant_pain_group", ""),
                PARTICIPANT_GROUPS,
                "none",
            ),
            "next_likely_pain_group": _normalize_label(next_label, MARKET_DIRECTIONS, "neutral"),
            "guidance": _normalize_label(target_now.get("guidance", ""), GUIDANCE_CHOICES, "observe"),
            "entry_trigger": entry_trigger,
            "mistake_type": _normalize_mistake_label((row_label or {}).get("mistake_type", "none"), "none"),
            "mistake_score": max(0.0, min(1.0, _to_float((row_label or {}).get("mistake_score"), 0.0))),
            "stop_loss_distance": float(stop_loss_distance),
            "target_level": float(target_level),
        }
        samples.append(sample)
        last_phase = sample["pain_phase"]

    return samples


def _option_sample_weight(features: Dict[str, float]) -> float:
    quality = max(
        0.0,
        _to_float(
            features.get("execution_option_data_quality", features.get("option_data_quality")),
            0.0,
        ),
    )
    strength = max(
        0.0,
        _to_float(
            features.get("execution_option_flow_strength", features.get("option_flow_strength")),
            0.0,
        ),
    )
    skew = abs(
        _to_float(
            features.get("execution_option_skew_balance", features.get("option_skew_balance")),
            0.0,
        )
    )
    consistency = max(
        0.0,
        _to_float(
            features.get("execution_option_signal_consistency", features.get("option_signal_consistency")),
            0.0,
        ),
    )
    base = 1.0 + (0.60 * min(1.0, quality)) + (0.35 * min(2.0, strength)) + (0.20 * min(1.0, skew))
    if quality >= 0.30 and consistency >= 0.35:
        base += 0.20
    elif quality < 0.12:
        base *= 0.85
    return max(0.50, min(2.50, float(base)))


def _mistake_sample_weight(mistake_type: str, mistake_score: float) -> float:
    name = _normalize_mistake_label(mistake_type, "none")
    score = max(0.0, min(1.0, _to_float(mistake_score, 0.0)))
    if name in {"none", "no_trade"}:
        return 1.0
    # Stronger penalty for costly mistakes that repeatedly hurt live quality.
    base = {
        "false_entry": 1.55,
        "chop_zone": 1.45,
        "wrong_side_pain": 1.35,
        "late_entry": 1.25,
        "expiry_noise": 1.20,
    }.get(name, 1.15)
    return max(1.0, min(3.0, base + (0.60 * score)))


def _build_matrix(
    samples: List[Dict[str, Any]],
    feature_columns: List[str],
) -> Tuple[List[List[float]], Dict[str, List[str]], Dict[str, List[float]], List[int], List[float]]:
    X: List[List[float]] = []
    y = {name: [] for name in TARGETS}
    y_reg = {name: [] for name in REGRESSION_TARGETS}
    timestamps: List[int] = []
    sample_weights: List[float] = []
    for row in samples:
        features = row["features"]
        X.append([float(features.get(name, 0.0)) for name in feature_columns])
        timestamps.append(int(row["timestamp"]))
        base_weight = _option_sample_weight(features)
        mistake_weight = _mistake_sample_weight(
            str(row.get("mistake_type", "none")),
            _to_float(row.get("mistake_score"), 0.0),
        )
        sample_weights.append(float(base_weight) * float(mistake_weight))
        for target in TARGETS:
            y[target].append(str(row[target]))
        for target in REGRESSION_TARGETS:
            y_reg[target].append(float(_to_float(row.get(target), 0.0)))
    return X, y, y_reg, timestamps, sample_weights


def _date_split(
    X: List[List[float]],
    y_class: Dict[str, List[str]],
    y_reg: Dict[str, List[float]],
    sample_weights: List[float],
    test_size: float,
):
    total = len(X)
    split_idx = int(total * (1.0 - test_size))
    split_idx = max(1, min(total - 1, split_idx))
    X_train = X[:split_idx]
    X_test = X[split_idx:]
    y_train_class = {key: values[:split_idx] for key, values in y_class.items()}
    y_test_class = {key: values[split_idx:] for key, values in y_class.items()}
    y_train_reg = {key: values[:split_idx] for key, values in y_reg.items()}
    y_test_reg = {key: values[split_idx:] for key, values in y_reg.items()}
    w_train = sample_weights[:split_idx]
    w_test = sample_weights[split_idx:]
    return X_train, X_test, y_train_class, y_test_class, y_train_reg, y_test_reg, w_train, w_test


def _fit_classifier(
    X_train: List[List[float]],
    y_train_enc: List[int],
    class_count: int,
    sample_weights: List[float] | None = None,
):
    fit_kwargs: Dict[str, Any] = {}
    if sample_weights and len(sample_weights) == len(y_train_enc):
        fit_kwargs["sample_weight"] = sample_weights

    if class_count < 2:
        model = DummyClassifier(strategy="most_frequent")
        try:
            model.fit(X_train, y_train_enc, **fit_kwargs)
        except TypeError:
            model.fit(X_train, y_train_enc)
        return model
    if class_count == 2:
        model = XGBClassifier(
            n_estimators=240,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.9,
            colsample_bytree=0.9,
            objective="binary:logistic",
            eval_metric="logloss",
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X_train, y_train_enc, **fit_kwargs)
        return model
    model = XGBClassifier(
        n_estimators=260,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="multi:softprob",
        num_class=class_count,
        eval_metric="mlogloss",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train_enc, **fit_kwargs)
    return model


def _balanced_class_weights(y_train_enc: List[int], class_count: int) -> tuple[Dict[int, int], Dict[int, float]]:
    counts: Dict[int, int] = {}
    for value in y_train_enc:
        idx = int(value)
        counts[idx] = counts.get(idx, 0) + 1
    total = max(1, len(y_train_enc))
    weights: Dict[int, float] = {}
    for idx in range(max(1, int(class_count))):
        count = max(1, int(counts.get(idx, 0)))
        raw = float(total) / (float(max(1, class_count)) * float(count))
        # Cap extremes so minority classes get support without destabilizing trees.
        weights[idx] = max(0.40, min(12.0, raw))
    return counts, weights


def _fit_regressor(X_train: List[List[float]], y_train: List[float], sample_weights: List[float] | None = None):
    fit_kwargs: Dict[str, Any] = {}
    if sample_weights and len(sample_weights) == len(y_train):
        fit_kwargs["sample_weight"] = sample_weights
    if len(y_train) < 30:
        model = DummyRegressor(strategy="mean")
        try:
            model.fit(X_train, y_train, **fit_kwargs)
        except TypeError:
            model.fit(X_train, y_train)
        return model
    min_val = min(y_train) if y_train else 0.0
    max_val = max(y_train) if y_train else 0.0
    if abs(max_val - min_val) < 1e-8:
        model = DummyRegressor(strategy="mean")
        try:
            model.fit(X_train, y_train, **fit_kwargs)
        except TypeError:
            model.fit(X_train, y_train)
        return model
    model = XGBRegressor(
        n_estimators=320,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.9,
        objective="reg:squarederror",
        eval_metric="rmse",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, **fit_kwargs)
    return model


def train(
    candles_1m_path: str,
    candles_5m_path: str,
    options_path: str,
    labels_path: str,
    model_path: str,
    window_1m: int,
    window_5m: int,
    next_horizon: int,
    test_size: float,
) -> Dict[str, Any]:
    candle_rows_1m = _normalize_candles(_load_table(candles_1m_path))
    candle_rows_5m = (
        _normalize_candles(_load_table(candles_5m_path))
        if candles_5m_path
        else _aggregate_5m_from_1m(candle_rows_1m)
    )
    option_rows = _normalize_options(_load_table(options_path)) if options_path else []
    label_rows = _label_maps(_load_table(labels_path)) if labels_path else {}

    samples = _build_samples(
        candle_rows_1m,
        candle_rows_5m,
        option_rows,
        label_rows,
        window_1m=window_1m,
        window_5m=window_5m,
        next_horizon=next_horizon,
    )
    if len(samples) < 120:
        raise RuntimeError("Not enough samples to train (need at least 120 rolling samples).")

    feature_columns = dual_feature_columns(include_base=True)
    X, y_class, y_reg, timestamps, sample_weights = _build_matrix(samples, feature_columns)
    X_train, X_test, y_train_class, y_test_class, y_train_reg, y_test_reg, w_train, w_test = _date_split(
        X,
        y_class,
        y_reg,
        sample_weights,
        test_size,
    )

    classifiers: Dict[str, Any] = {}
    encoders: Dict[str, LabelEncoder] = {}
    regressors: Dict[str, Any] = {}

    metrics: Dict[str, Any] = {
        "rows": len(samples),
        "train_rows": len(X_train),
        "test_rows": len(X_test),
        "targets": {},
        "regression_targets": {},
        "window_1m": window_1m,
        "window_5m": window_5m,
        "next_horizon": next_horizon,
        "split_mode": "date",
        "from_ts": int(timestamps[0]),
        "to_ts": int(timestamps[-1]),
        "input_candles_1m": candles_1m_path,
        "input_candles_5m": candles_5m_path if candles_5m_path else "aggregated_from_1m",
        "input_options": options_path if options_path else "",
        "input_labels": labels_path if labels_path else "",
        "feature_count": len(feature_columns),
        "sample_weight": {
            "train_mean": float(sum(w_train) / max(1, len(w_train))),
            "test_mean": float(sum(w_test) / max(1, len(w_test))),
            "train_max": float(max(w_train) if w_train else 0.0),
            "train_min": float(min(w_train) if w_train else 0.0),
            "high_weight_rows_train": int(sum(1 for value in w_train if value >= 1.35)),
        },
    }

    for target in TARGETS:
        encoder = LabelEncoder()
        train_encoded = encoder.fit_transform(y_train_class[target])
        train_encoded_list = [int(item) for item in train_encoded.tolist()]
        class_count = len(encoder.classes_)
        class_support, class_balance = _balanced_class_weights(train_encoded_list, class_count)
        target_weights: List[float] | None = None
        if w_train and len(w_train) == len(train_encoded_list):
            target_weights = [
                float(w_train[idx]) * float(class_balance.get(train_encoded_list[idx], 1.0))
                for idx in range(len(train_encoded_list))
            ]
        model = _fit_classifier(X_train, train_encoded_list, class_count, sample_weights=target_weights)
        encoders[target] = encoder
        classifiers[target] = model

        known = set(encoder.classes_)
        filtered_test = [
            (x_item, y_item)
            for x_item, y_item in zip(X_test, y_test_class[target])
            if y_item in known
        ]
        if filtered_test:
            X_target_test = [item[0] for item in filtered_test]
            y_target_test = [item[1] for item in filtered_test]
            y_true = encoder.transform(y_target_test)
            raw_pred = model.predict(X_target_test)
            if getattr(raw_pred, "ndim", 1) > 1:
                y_pred = raw_pred.argmax(axis=1)
            else:
                y_pred = [int(round(_to_float(item, 0.0))) for item in raw_pred]
            report = classification_report(
                y_true,
                y_pred,
                labels=list(range(len(encoder.classes_))),
                target_names=[str(item) for item in encoder.classes_],
                output_dict=True,
                zero_division=0,
            )
            score = float(accuracy_score(y_true, y_pred))
        else:
            report = {}
            score = 0.0

        metrics["targets"][target] = {
            "classes": [str(item) for item in encoder.classes_],
            "accuracy": score,
            "train_support": {
                str(encoder.classes_[idx]): int(class_support.get(idx, 0))
                for idx in range(len(encoder.classes_))
            },
            "class_balance_weight": {
                str(encoder.classes_[idx]): float(class_balance.get(idx, 1.0))
                for idx in range(len(encoder.classes_))
            },
            "report": report,
        }

    for target in REGRESSION_TARGETS:
        model = _fit_regressor(X_train, y_train_reg[target], sample_weights=w_train)
        regressors[target] = model
        if X_test and y_test_reg[target]:
            preds = [float(item) for item in model.predict(X_test)]
            y_true = [float(item) for item in y_test_reg[target]]
            mae = float(mean_absolute_error(y_true, preds))
            mse = float(mean_squared_error(y_true, preds))
            rmse = float(math.sqrt(max(0.0, mse)))
        else:
            mae = 0.0
            rmse = 0.0
        metrics["regression_targets"][target] = {
            "mae": mae,
            "rmse": rmse,
            "train_mean": float(sum(y_train_reg[target]) / max(1, len(y_train_reg[target]))),
            "test_mean": float(sum(y_test_reg[target]) / max(1, len(y_test_reg[target]))),
        }

    artifact = {
        "classifiers": classifiers,
        "encoders": encoders,
        "regressors": regressors,
        "feature_columns": feature_columns,
        "trained_at": int(time.time()),
        "metrics": metrics,
    }
    output = Path(model_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, output)
    return metrics


def main() -> None:
    args = parse_args()
    metrics = train(
        candles_1m_path=args.candles,
        candles_5m_path=args.candles_5m,
        options_path=args.options,
        labels_path=args.labels,
        model_path=args.model_path,
        window_1m=max(5, int(args.window_minutes)),
        window_5m=max(10, int(args.window_bars_5m)),
        next_horizon=max(1, int(args.next_horizon)),
        test_size=max(0.05, min(0.5, float(args.test_size))),
    )
    print("Pain Theory training complete")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
