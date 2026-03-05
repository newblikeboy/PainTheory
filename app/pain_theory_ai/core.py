from __future__ import annotations

from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Dict, Iterable, List, Tuple


PAIN_PHASES = (
    "comfort",
    "late_entry",
    "discomfort",
    "exit_pain",
    "exhaustion_pain",
    "digestion",
    "transfer",
    "expiry_pain",
)

PARTICIPANT_GROUPS = (
    "call_buyers",
    "put_buyers",
    "call_sellers",
    "put_sellers",
    "none",
    "buyers_both",
)

MARKET_DIRECTIONS = (
    "bullish",
    "bearish",
    "neutral",
)

GUIDANCE_CHOICES = ("wait", "observe", "caution", "no_edge")

FEATURE_COLUMNS = [
    "spot_close",
    "candle_speed",
    "range_expansion",
    "overlap_ratio",
    "wick_dominance",
    "time_without_progress",
    "direction_persistence",
    "current_direction",
    "direction_flip",
    "atr_window",
    "compression_regime",
    "shock_move",
    "liquidity_sweep",
    "trap_event",
    "premium_velocity_ce",
    "premium_velocity_pe",
    "premium_decay_rate_ce",
    "premium_decay_rate_pe",
    "volume_burst_ce",
    "volume_burst_pe",
    "oi_change_pressure_ce",
    "oi_change_pressure_pe",
    "band_concentration",
    "magnet_distance",
    "oi_concentration",
    "option_skew_balance",
    "atm_ltp_skew",
    "atm_volume_skew",
    "atm_oi_skew",
    "option_flow_strength",
    "option_signal_consistency",
    "option_data_quality",
    "regime_trend_score",
    "expiry_proximity",
    "gamma_risk",
]


def dual_feature_columns(include_base: bool = True) -> List[str]:
    columns: List[str] = []
    if include_base:
        columns.extend(FEATURE_COLUMNS)
    for key in FEATURE_COLUMNS:
        columns.append(f"analysis_{key}")
        columns.append(f"execution_{key}")
        columns.append(f"delta_{key}")
    columns.extend(
        [
            "tf_direction_alignment",
            "tf_speed_ratio",
            "tf_range_ratio",
            "tf_overlap_ratio",
            "tf_premium_balance",
            "tf_atr_ratio",
            "tf_regime_shift",
            "tf_gamma_ratio",
        ]
    )
    return columns


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0.0:
        return 0.0
    return numerator / denominator


def _value(row: Dict[str, Any], key: str, default: float = 0.0) -> float:
    raw = row.get(key, default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _median(values: Iterable[float], default: float = 0.0) -> float:
    items = [float(item) for item in values]
    if not items:
        return default
    return float(median(items))


def _clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    if value > high:
        return high
    return value


def _sign(value: float, eps: float = 1e-9) -> int:
    val = float(value)
    if val > eps:
        return 1
    if val < -eps:
        return -1
    return 0


def _strike_step(values: List[float], default: float = 50.0) -> float:
    if len(values) < 2:
        return float(default)
    ordered = sorted(set(float(item) for item in values))
    diffs = [ordered[idx + 1] - ordered[idx] for idx in range(len(ordered) - 1)]
    positive = [item for item in diffs if item > 0]
    if not positive:
        return float(default)
    return float(min(positive))


def _expiry_features(timestamp: int) -> tuple[float, float]:
    if int(timestamp) <= 0:
        return 0.0, 0.0
    ist = timezone(timedelta(hours=5, minutes=30))
    dt_obj = datetime.fromtimestamp(int(timestamp), tz=timezone.utc).astimezone(ist)
    # Indian weekly index expiry is typically Thursday.
    days_to_thursday = (3 - dt_obj.weekday()) % 7
    proximity = _clamp(1.0 - (float(days_to_thursday) / 6.0), 0.0, 1.0)
    intraday_boost = 1.15 if dt_obj.weekday() == 3 else 1.0
    gamma = _clamp(proximity * intraday_boost, 0.0, 1.5)
    return proximity, gamma


def _strike_magnet_features(snapshot: Dict[str, Any], spot_price: float) -> tuple[float, float]:
    rows = snapshot.get("strikes")
    if not isinstance(rows, list) or not rows:
        return 0.0, 0.0

    strike_values: List[float] = []
    weighted: List[tuple[float, float]] = []
    total_weight = 0.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        strike = _value(row, "strike")
        if strike <= 0.0:
            continue
        oi = max(0.0, _value(row, "oi", _value(row, "open_interest")))
        oi_change = abs(_value(row, "oi_change", _value(row, "oich")))
        volume = max(0.0, _value(row, "volume"))
        weight = oi + oi_change + (0.25 * volume)
        if weight <= 0.0:
            continue
        strike_values.append(strike)
        weighted.append((strike, weight))
        total_weight += weight

    if not weighted or total_weight <= 0.0:
        return 0.0, 0.0

    weighted.sort(key=lambda item: item[1], reverse=True)
    top_strike = float(weighted[0][0])
    top2_weight = float(weighted[0][1])
    if len(weighted) > 1:
        top2_weight += float(weighted[1][1])

    step = _strike_step(strike_values, default=50.0)
    magnet_distance = _clamp(abs(float(spot_price) - top_strike) / max(25.0, step), 0.0, 10.0)
    concentration = _clamp(top2_weight / max(1e-9, total_weight), 0.0, 1.0)
    return magnet_distance, concentration


def _direction(candle: Dict[str, Any]) -> int:
    close = _value(candle, "close")
    open_ = _value(candle, "open")
    if close > open_:
        return 1
    if close < open_:
        return -1
    return 0


def _option_snapshot(raw: Dict[str, Any], spot_price: float) -> Dict[str, float]:
    snapshot = {
        "timestamp": _value(raw, "timestamp"),
        "atm_ce_ltp": 0.0,
        "atm_pe_ltp": 0.0,
        "atm_ce_volume": 0.0,
        "atm_pe_volume": 0.0,
        "atm_ce_oi_change": 0.0,
        "atm_pe_oi_change": 0.0,
        "band_volume_near_atm": _value(raw, "band_volume_near_atm"),
        "band_volume_total": _value(raw, "band_volume_total"),
    }

    # Direct aggregated payload format.
    for key in (
        "atm_ce_ltp",
        "atm_pe_ltp",
        "atm_ce_volume",
        "atm_pe_volume",
        "atm_ce_oi_change",
        "atm_pe_oi_change",
    ):
        if key in raw:
            snapshot[key] = _value(raw, key)

    strikes = raw.get("strikes") or []
    if not isinstance(strikes, list) or not strikes:
        return snapshot

    atm = raw.get("atm_strike")
    if atm is None:
        strike_values = []
        for row in strikes:
            try:
                strike_values.append(float(row.get("strike", 0.0)))
            except (TypeError, ValueError):
                continue
        if strike_values:
            atm = min(strike_values, key=lambda item: abs(item - spot_price))

    atm_ce = None
    atm_pe = None
    total_volume = 0.0
    near_volume = 0.0
    for row in strikes:
        strike = _value(row, "strike")
        option_type = str(row.get("option_type", "")).upper()
        vol = _value(row, "volume")
        total_volume += vol
        if atm is not None and abs(strike - float(atm)) <= 100.0:
            near_volume += vol
        if atm is None or abs(strike - float(atm)) > 1e-9:
            continue
        if option_type == "CE":
            atm_ce = row
        elif option_type == "PE":
            atm_pe = row

    if atm_ce is not None:
        snapshot["atm_ce_ltp"] = _value(atm_ce, "ltp")
        snapshot["atm_ce_volume"] = _value(atm_ce, "volume")
        snapshot["atm_ce_oi_change"] = _value(atm_ce, "oi_change", _value(atm_ce, "oich"))
    if atm_pe is not None:
        snapshot["atm_pe_ltp"] = _value(atm_pe, "ltp")
        snapshot["atm_pe_volume"] = _value(atm_pe, "volume")
        snapshot["atm_pe_oi_change"] = _value(atm_pe, "oi_change", _value(atm_pe, "oich"))

    if snapshot["band_volume_total"] <= 0.0:
        snapshot["band_volume_total"] = total_volume
    if snapshot["band_volume_near_atm"] <= 0.0:
        snapshot["band_volume_near_atm"] = near_volume
    return snapshot


def build_features(
    candles: List[Dict[str, Any]],
    options: List[Dict[str, Any]] | None = None,
) -> Dict[str, float]:
    if len(candles) < 2:
        out = {key: 0.0 for key in FEATURE_COLUMNS}
        if candles:
            current = candles[-1]
            out["spot_close"] = _value(current, "close")
            expiry_prox, gamma = _expiry_features(int(_value(current, "timestamp")))
            out["expiry_proximity"] = expiry_prox
            out["gamma_risk"] = gamma
        return out

    window = candles[-30:] if len(candles) >= 30 else candles
    current = window[-1]
    previous = window[-2]

    ranges = [max(0.0, _value(item, "high") - _value(item, "low")) for item in window]
    body_sizes = [abs(_value(item, "close") - _value(item, "open")) for item in window]
    median_range = max(1e-9, _median(ranges, 1.0))
    median_body = max(1e-9, _median(body_sizes, 1.0))

    current_range = max(0.0, _value(current, "high") - _value(current, "low"))
    current_body = abs(_value(current, "close") - _value(current, "open"))
    upper_wick = _value(current, "high") - max(_value(current, "close"), _value(current, "open"))
    lower_wick = min(_value(current, "close"), _value(current, "open")) - _value(current, "low")

    overlap_values: List[float] = []
    overlap_window = window[-10:] if len(window) >= 10 else window
    for idx in range(1, len(overlap_window)):
        left = overlap_window[idx - 1]
        right = overlap_window[idx]
        left_high = _value(left, "high")
        left_low = _value(left, "low")
        right_high = _value(right, "high")
        right_low = _value(right, "low")
        overlap = max(0.0, min(left_high, right_high) - max(left_low, right_low))
        denom = max(1e-9, min(left_high - left_low, right_high - right_low))
        overlap_values.append(_safe_div(overlap, denom))

    direction_sequence: List[int] = []
    for idx in range(1, len(window)):
        close_now = _value(window[idx], "close")
        close_prev = _value(window[idx - 1], "close")
        if close_now > close_prev:
            direction_sequence.append(1)
        elif close_now < close_prev:
            direction_sequence.append(-1)
        else:
            direction_sequence.append(0)

    consecutive = 0
    if direction_sequence:
        tail = direction_sequence[-1]
        if tail != 0:
            consecutive = 1
            for item in reversed(direction_sequence[:-1]):
                if item == tail:
                    consecutive += 1
                else:
                    break
            consecutive *= tail

    highs = [_value(item, "high") for item in window]
    lows = [_value(item, "low") for item in window]
    running_high = max(highs)
    running_low = min(lows)
    since_extension = len(window) - 1
    for idx in range(len(window) - 1, -1, -1):
        candle = window[idx]
        if _value(candle, "high") >= running_high or _value(candle, "low") <= running_low:
            since_extension = len(window) - 1 - idx
            break

    current_direction = _direction(current)
    prev_direction = _direction(previous)

    overlap_ratio = _median(overlap_values)
    range_expansion = _safe_div(current_range, median_range)
    candle_speed = _safe_div(current_body, median_body)
    wick_dominance = _safe_div(max(0.0, upper_wick) + max(0.0, lower_wick), max(1e-9, current_range))
    compression_regime = _clamp(overlap_ratio * _safe_div(1.0, max(0.25, range_expansion)), 0.0, 2.5)
    shock_move = _clamp((max(candle_speed, range_expansion) - 1.0) / 1.8, 0.0, 2.5)

    prior_high = max(highs[:-1]) if len(highs) > 1 else highs[-1]
    prior_low = min(lows[:-1]) if len(lows) > 1 else lows[-1]
    sweep_high = _value(current, "high") > prior_high and _value(current, "close") < prior_high
    sweep_low = _value(current, "low") < prior_low and _value(current, "close") > prior_low
    liquidity_sweep = 1.0 if (sweep_high or sweep_low) else 0.0
    trap_event = 1.0 if (liquidity_sweep > 0.0 and current_direction != 0 and current_direction != prev_direction) else 0.0

    trend_score = (
        (_safe_div(float(consecutive), 6.0))
        + (0.35 * float(current_direction))
        + (0.2 * max(-1.0, min(2.0, range_expansion - 1.0)))
        - (0.45 * overlap_ratio)
    )
    trend_score = _clamp(trend_score, -2.0, 2.0)

    expiry_proximity, gamma_risk = _expiry_features(int(_value(current, "timestamp")))
    spot_close = _value(current, "close")

    features = {
        "spot_close": spot_close,
        "candle_speed": candle_speed,
        "range_expansion": range_expansion,
        "overlap_ratio": overlap_ratio,
        "wick_dominance": wick_dominance,
        "time_without_progress": float(since_extension),
        "direction_persistence": _safe_div(float(consecutive), 10.0),
        "current_direction": float(current_direction),
        "direction_flip": 1.0 if (current_direction != 0 and prev_direction != 0 and current_direction != prev_direction) else 0.0,
        "atr_window": float(median_range),
        "compression_regime": compression_regime,
        "shock_move": shock_move,
        "liquidity_sweep": liquidity_sweep,
        "trap_event": trap_event,
        "premium_velocity_ce": 0.0,
        "premium_velocity_pe": 0.0,
        "premium_decay_rate_ce": 0.0,
        "premium_decay_rate_pe": 0.0,
        "volume_burst_ce": 0.0,
        "volume_burst_pe": 0.0,
        "oi_change_pressure_ce": 0.0,
        "oi_change_pressure_pe": 0.0,
        "band_concentration": 0.0,
        "magnet_distance": 0.0,
        "oi_concentration": 0.0,
        "option_skew_balance": 0.0,
        "atm_ltp_skew": 0.0,
        "atm_volume_skew": 0.0,
        "atm_oi_skew": 0.0,
        "option_flow_strength": 0.0,
        "option_signal_consistency": 0.0,
        "option_data_quality": 0.0,
        "regime_trend_score": trend_score,
        "expiry_proximity": expiry_proximity,
        "gamma_risk": gamma_risk,
    }

    if options:
        normalized = [_option_snapshot(item, spot_close) for item in options]
        latest = normalized[-1]
        history = normalized[:-1]
        prev = history[-1] if history else latest
        history_tail = history[-10:] if len(history) >= 10 else history

        median_ce_volume = _median([_value(item, "atm_ce_volume") for item in history_tail], 1.0)
        median_pe_volume = _median([_value(item, "atm_pe_volume") for item in history_tail], 1.0)
        median_ce_oich = _median([abs(_value(item, "atm_ce_oi_change")) for item in history_tail], 1.0)
        median_pe_oich = _median([abs(_value(item, "atm_pe_oi_change")) for item in history_tail], 1.0)

        features["premium_velocity_ce"] = _safe_div(
            _value(latest, "atm_ce_ltp") - _value(prev, "atm_ce_ltp"),
            max(1.0, abs(_value(prev, "atm_ce_ltp"))),
        )
        features["premium_velocity_pe"] = _safe_div(
            _value(latest, "atm_pe_ltp") - _value(prev, "atm_pe_ltp"),
            max(1.0, abs(_value(prev, "atm_pe_ltp"))),
        )
        features["premium_decay_rate_ce"] = max(
            0.0,
            _safe_div(
                _value(prev, "atm_ce_ltp") - _value(latest, "atm_ce_ltp"),
                max(1.0, _value(prev, "atm_ce_ltp")),
            ),
        )
        features["premium_decay_rate_pe"] = max(
            0.0,
            _safe_div(
                _value(prev, "atm_pe_ltp") - _value(latest, "atm_pe_ltp"),
                max(1.0, _value(prev, "atm_pe_ltp")),
            ),
        )
        features["volume_burst_ce"] = _safe_div(_value(latest, "atm_ce_volume"), max(1.0, median_ce_volume))
        features["volume_burst_pe"] = _safe_div(_value(latest, "atm_pe_volume"), max(1.0, median_pe_volume))
        features["oi_change_pressure_ce"] = _safe_div(_value(latest, "atm_ce_oi_change"), max(1.0, median_ce_oich))
        features["oi_change_pressure_pe"] = _safe_div(_value(latest, "atm_pe_oi_change"), max(1.0, median_pe_oich))
        features["band_concentration"] = _safe_div(
            _value(latest, "band_volume_near_atm"),
            max(1.0, _value(latest, "band_volume_total")),
        )

        raw_latest = options[-1] if isinstance(options[-1], dict) else {}
        magnet_distance, oi_concentration = _strike_magnet_features(raw_latest, spot_close)
        features["magnet_distance"] = magnet_distance
        features["oi_concentration"] = oi_concentration
        features["option_skew_balance"] = (
            (features["premium_velocity_pe"] - features["premium_velocity_ce"])
            + 0.5 * (features["oi_change_pressure_pe"] - features["oi_change_pressure_ce"])
        )
        ce_ltp = max(0.0, _value(latest, "atm_ce_ltp"))
        pe_ltp = max(0.0, _value(latest, "atm_pe_ltp"))
        ce_vol = max(0.0, _value(latest, "atm_ce_volume"))
        pe_vol = max(0.0, _value(latest, "atm_pe_volume"))
        ce_oich = _value(latest, "atm_ce_oi_change")
        pe_oich = _value(latest, "atm_pe_oi_change")
        features["atm_ltp_skew"] = _safe_div(pe_ltp - ce_ltp, max(1.0, pe_ltp + ce_ltp))
        features["atm_volume_skew"] = _safe_div(pe_vol - ce_vol, max(1.0, pe_vol + ce_vol))
        features["atm_oi_skew"] = _safe_div(pe_oich - ce_oich, max(1.0, abs(pe_oich) + abs(ce_oich)))

        signal_votes = [
            _sign(features["option_skew_balance"]),
            _sign(features["atm_ltp_skew"]),
            _sign(features["atm_volume_skew"]),
            _sign(features["atm_oi_skew"]),
        ]
        non_zero_votes = [vote for vote in signal_votes if vote != 0]
        if non_zero_votes:
            features["option_signal_consistency"] = _clamp(
                abs(float(sum(non_zero_votes))) / float(len(non_zero_votes)),
                0.0,
                1.0,
            )

        history_ratio = _clamp(float(len(normalized)) / 10.0, 0.0, 1.0)
        populated_fields = 0
        for value in (ce_ltp, pe_ltp, ce_vol, pe_vol, abs(ce_oich), abs(pe_oich)):
            if float(value) > 0.0:
                populated_fields += 1
        field_ratio = float(populated_fields) / 6.0
        features["option_data_quality"] = _clamp((0.60 * history_ratio) + (0.40 * field_ratio), 0.0, 1.0)

        flow_raw = (
            (0.45 * abs(features["atm_ltp_skew"]))
            + (0.30 * abs(features["atm_volume_skew"]))
            + (0.35 * abs(features["atm_oi_skew"]))
            + (0.30 * abs(features["option_skew_balance"]))
            + (0.25 * features["band_concentration"])
        )
        features["option_flow_strength"] = _clamp(
            flow_raw * (0.65 + (0.35 * features["option_data_quality"])),
            0.0,
            2.5,
        )
        features["gamma_risk"] = _clamp(
            features["gamma_risk"]
            * (0.35 + (0.65 * features["band_concentration"]))
            * (1.0 + (0.5 * min(1.5, abs(features["option_skew_balance"]))))
            * (1.0 + (0.35 * min(1.5, features["option_flow_strength"]))),
            0.0,
            2.5,
        )

    return features


def build_dual_timeframe_features(
    analysis_candles: List[Dict[str, Any]],
    execution_candles: List[Dict[str, Any]],
    *,
    analysis_options: List[Dict[str, Any]] | None = None,
    execution_options: List[Dict[str, Any]] | None = None,
) -> Dict[str, float]:
    analysis = build_features(analysis_candles, analysis_options)
    execution = build_features(execution_candles, execution_options)
    merged: Dict[str, float] = {key: float(analysis.get(key, 0.0)) for key in FEATURE_COLUMNS}

    for key in FEATURE_COLUMNS:
        analysis_value = float(analysis.get(key, 0.0))
        execution_value = float(execution.get(key, 0.0))
        merged[f"analysis_{key}"] = analysis_value
        merged[f"execution_{key}"] = execution_value
        merged[f"delta_{key}"] = execution_value - analysis_value

    direction_analysis = int(round(float(analysis.get("current_direction", 0.0))))
    direction_execution = int(round(float(execution.get("current_direction", 0.0))))
    if direction_analysis != 0 and direction_execution != 0:
        direction_alignment = 1.0 if direction_analysis == direction_execution else -1.0
    else:
        direction_alignment = 0.0

    merged["tf_direction_alignment"] = direction_alignment
    merged["tf_speed_ratio"] = _safe_div(
        float(execution.get("candle_speed", 0.0)),
        max(1e-9, abs(float(analysis.get("candle_speed", 0.0)))),
    )
    merged["tf_range_ratio"] = _safe_div(
        float(execution.get("range_expansion", 0.0)),
        max(1e-9, abs(float(analysis.get("range_expansion", 0.0)))),
    )
    merged["tf_overlap_ratio"] = _safe_div(
        float(execution.get("overlap_ratio", 0.0)),
        max(1e-9, abs(float(analysis.get("overlap_ratio", 0.0)))),
    )
    merged["tf_premium_balance"] = (
        (float(execution.get("premium_velocity_pe", 0.0)) - float(execution.get("premium_velocity_ce", 0.0)))
        - (float(analysis.get("premium_velocity_pe", 0.0)) - float(analysis.get("premium_velocity_ce", 0.0)))
    )
    merged["tf_atr_ratio"] = _safe_div(
        float(execution.get("atr_window", 0.0)),
        max(1e-9, float(analysis.get("atr_window", 0.0))),
    )
    merged["tf_regime_shift"] = float(execution.get("regime_trend_score", 0.0)) - float(
        analysis.get("regime_trend_score", 0.0)
    )
    merged["tf_gamma_ratio"] = _safe_div(
        float(execution.get("gamma_risk", 0.0)),
        max(1e-9, float(analysis.get("gamma_risk", 0.0))),
    )
    return merged


def _signal_direction(next_group: str) -> str:
    text = str(next_group or "").strip().lower()
    if text == "bearish":
        return "LONG"
    if text == "bullish":
        return "SHORT"
    if text in {"put_buyers", "call_sellers"}:
        return "LONG"
    if text in {"call_buyers", "put_sellers"}:
        return "SHORT"
    return "NONE"


def derive_trade_plan(
    *,
    features: Dict[str, float],
    pain_phase: str,
    next_group: str,
    guidance: str,
    confidence: float,
) -> Dict[str, Any]:
    direction = _signal_direction(next_group)
    atr = max(0.25, float(features.get("atr_window", 0.0)))
    compression = max(0.0, float(features.get("compression_regime", 0.0)))
    shock = max(0.0, float(features.get("shock_move", 0.0)))
    trend = abs(float(features.get("regime_trend_score", 0.0)))
    gamma = max(0.0, float(features.get("gamma_risk", 0.0)))

    stop_loss_distance = max(0.35, atr * (0.55 + (0.28 * compression) + (0.12 * gamma)))
    target_multiple = 1.20 + min(0.9, (0.7 * shock) + (0.12 * trend) - (0.18 * compression))
    target_level = max(stop_loss_distance * 1.05, stop_loss_distance * target_multiple)

    entry_signal = bool(
        direction != "NONE"
        and guidance in {"caution"}
        and float(confidence) >= 0.53
        and not (pain_phase in {"digestion", "exhaustion_pain", "expiry_pain"} and guidance == "wait")
    )
    if compression >= 1.6 and shock <= 0.2:
        entry_signal = False

    return {
        "entry_signal": bool(entry_signal),
        "entry_direction": direction,
        "stop_loss_distance": float(stop_loss_distance),
        "target_level": float(target_level),
        "target_level_2": float(max(target_level * 1.5, target_level + (0.8 * atr))),
    }


def classify_from_rules(
    features: Dict[str, float],
    last_phase: str = "comfort",
) -> Dict[str, Any]:
    direction = int(round(features.get("current_direction", 0.0)))
    speed = float(features.get("candle_speed", 0.0))
    expansion = float(features.get("range_expansion", 0.0))
    overlap = float(features.get("overlap_ratio", 0.0))
    wick = float(features.get("wick_dominance", 0.0))
    idle = float(features.get("time_without_progress", 0.0))
    flip = float(features.get("direction_flip", 0.0)) >= 0.5
    decay_ce = float(features.get("premium_decay_rate_ce", 0.0))
    decay_pe = float(features.get("premium_decay_rate_pe", 0.0))
    burst_ce = float(features.get("volume_burst_ce", 0.0))
    burst_pe = float(features.get("volume_burst_pe", 0.0))
    compression = float(features.get("compression_regime", 0.0))
    shock = float(features.get("shock_move", 0.0))
    sweep = float(features.get("liquidity_sweep", 0.0))
    trap = float(features.get("trap_event", 0.0))
    option_skew = float(features.get("option_skew_balance", 0.0))
    atm_ltp_skew = float(features.get("atm_ltp_skew", 0.0))
    atm_volume_skew = float(features.get("atm_volume_skew", 0.0))
    atm_oi_skew = float(features.get("atm_oi_skew", 0.0))
    option_flow_strength = float(features.get("option_flow_strength", 0.0))
    option_signal_consistency = float(features.get("option_signal_consistency", 0.0))
    option_data_quality = float(features.get("option_data_quality", 0.0))
    expiry_proximity = float(features.get("expiry_proximity", 0.0))
    band_concentration = float(features.get("band_concentration", 0.0))
    option_bias = (
        (0.60 * option_skew)
        + (0.20 * atm_ltp_skew)
        + (0.12 * atm_oi_skew)
        + (0.08 * atm_volume_skew)
    )
    option_usable = bool(option_data_quality >= 0.28 and option_flow_strength >= 0.20)

    phase = "comfort"
    if expiry_proximity >= 0.84 and band_concentration >= 0.50 and abs(option_skew) >= 0.08:
        phase = "expiry_pain"
    elif shock >= 0.55 and expansion >= 1.35 and compression <= 0.95:
        phase = "exit_pain"
    elif trap >= 0.5 or (flip and sweep >= 0.5):
        phase = "transfer"
    elif compression >= 1.25 and overlap >= 0.60 and idle >= 8:
        phase = "digestion"
    elif overlap >= 0.62 and idle >= 10 and (decay_ce >= 0.01 or decay_pe >= 0.01):
        phase = "exhaustion_pain"
    elif expansion >= 1.15 and speed >= 1.05 and max(burst_ce, burst_pe) >= 1.2:
        phase = "late_entry"
    elif idle >= 4 or wick >= 0.92:
        phase = "discomfort"

    if phase == "comfort" and last_phase in {"exit_pain", "transfer"} and overlap >= 0.52:
        phase = "digestion"
    if phase == "comfort" and option_usable and abs(option_bias) >= 0.065 and option_signal_consistency >= 0.35:
        phase = "transfer"

    dominant = "none"
    next_group = "none"
    guidance = "no_edge"

    if phase == "exit_pain":
        if direction > 0:
            dominant = "call_sellers"
            next_group = "put_buyers"
        elif direction < 0:
            dominant = "put_sellers"
            next_group = "call_buyers"
        guidance = "caution"
    elif phase == "transfer":
        if direction > 0:
            dominant = "put_buyers"
            next_group = "call_buyers"
        elif direction < 0:
            dominant = "call_buyers"
            next_group = "put_buyers"
        guidance = "caution"
    elif phase in {"exhaustion_pain", "expiry_pain"}:
        dominant = "buyers_both"
        next_group = "none"
        guidance = "wait"
    elif phase == "digestion":
        dominant = "buyers_both"
        if direction > 0:
            next_group = "put_buyers"
        elif direction < 0:
            next_group = "call_buyers"
        guidance = "wait"
    elif phase == "late_entry":
        if direction > 0:
            dominant = "call_buyers"
            next_group = "call_buyers"
        elif direction < 0:
            dominant = "put_buyers"
            next_group = "put_buyers"
        guidance = "observe"
    elif phase == "discomfort":
        if direction > 0:
            dominant = "put_buyers"
            next_group = "call_buyers"
        elif direction < 0:
            dominant = "call_buyers"
            next_group = "put_buyers"
        guidance = "observe"

    option_next = "none"
    if option_usable and option_signal_consistency >= 0.34 and abs(option_bias) >= 0.05:
        option_next = "call_buyers" if option_bias > 0.0 else "put_buyers"
        if next_group == "none":
            next_group = option_next
            guidance = "caution"
        elif next_group != option_next:
            if guidance == "caution":
                guidance = "observe"
        elif guidance in {"no_edge", "observe"}:
            guidance = "caution"

        if next_group == "call_buyers":
            dominant = "put_sellers"
        elif next_group == "put_buyers":
            dominant = "call_sellers"

    confidence = 0.43
    if phase == "exit_pain":
        confidence = min(0.95, 0.5 + (0.25 * shock) + 0.07 * max(0.0, expansion - 1.1))
    elif phase == "transfer":
        confidence = min(0.9, 0.48 + (0.15 * trap) + (0.10 * sweep) + (0.06 * wick))
    elif phase in {"exhaustion_pain", "digestion"}:
        confidence = min(0.9, 0.45 + (0.18 * overlap) + (0.05 * compression))
    elif phase == "expiry_pain":
        confidence = min(0.92, 0.5 + (0.25 * expiry_proximity) + (0.08 * band_concentration))
    elif phase in {"late_entry", "discomfort"}:
        confidence = min(0.82, 0.45 + (0.12 * expansion) + (0.05 * wick))

    if option_usable:
        confidence += 0.05 * min(1.0, option_data_quality)
        confidence += 0.06 * min(1.5, option_flow_strength)
        if option_next != "none" and option_next == next_group:
            confidence += 0.05 * min(1.0, option_signal_consistency)
        elif option_next != "none" and option_next != next_group:
            confidence -= 0.08 * min(1.0, option_signal_consistency)

    confidence = _clamp(confidence, 0.0, 1.0)
    trade_plan = derive_trade_plan(
        features=features,
        pain_phase=phase,
        next_group=next_group,
        guidance=guidance,
        confidence=confidence,
    )

    return {
        "pain_phase": phase,
        "dominant_pain_group": dominant,
        "next_likely_pain_group": next_group,
        "confidence": confidence,
        "guidance": guidance,
        "entry_signal": bool(trade_plan["entry_signal"]),
        "entry_direction": str(trade_plan["entry_direction"]),
        "stop_loss_distance": float(trade_plan["stop_loss_distance"]),
        "target_level": float(trade_plan["target_level"]),
        "target_level_2": float(trade_plan["target_level_2"]),
    }


def build_explanation(
    pain_phase: str,
    dominant_pain_group: str,
    next_likely_pain_group: str,
    guidance: str,
    features: Dict[str, float],
    *,
    entry_signal: bool | None = None,
    stop_loss_distance: float | None = None,
    target_level: float | None = None,
) -> str:
    direction = int(round(features.get("current_direction", 0.0)))
    speed = features.get("candle_speed", 0.0)
    overlap = features.get("overlap_ratio", 0.0)
    decay_ce = features.get("premium_decay_rate_ce", 0.0)
    decay_pe = features.get("premium_decay_rate_pe", 0.0)
    option_strength = float(features.get("option_flow_strength", 0.0))
    option_consistency = float(features.get("option_signal_consistency", 0.0))
    option_quality = float(features.get("option_data_quality", 0.0))
    option_skew = float(features.get("option_skew_balance", 0.0))

    lines = [
        f"pain_phase is {pain_phase}.",
        f"dominant_pain_group is {dominant_pain_group}; next_likely_pain_group is {next_likely_pain_group}.",
    ]

    if pain_phase == "exit_pain":
        if direction > 0:
            lines.append("Fast upward movement is forcing exit pain mainly on call_sellers.")
        elif direction < 0:
            lines.append("Fast downward movement is forcing exit pain mainly on put_sellers.")
        else:
            lines.append("Fast movement is creating exit pain among crowded participants.")
    elif pain_phase in {"digestion", "exhaustion_pain"}:
        lines.append(
            "Sideways time pressure is creating premium bleed; buyers_both feel time pain while sellers stay in comfort."
        )
    elif pain_phase == "transfer":
        lines.append("A quick trap and reversal is shifting pain from one buyer group to the other.")
    elif pain_phase == "expiry_pain":
        lines.append("Expiry proximity and strike magnet pressure are creating rapid two-way pain.")
    elif pain_phase == "late_entry":
        lines.append("Fresh late entries are crowding one side and pain risk is increasing.")
    else:
        if direction > 0:
            lines.append("Upward pressure keeps put_buyers under stress and tests call_sellers.")
        elif direction < 0:
            lines.append("Downward pressure keeps call_buyers under stress and tests put_sellers.")
        else:
            lines.append("Balanced movement shows no clear pain transfer edge.")

    lines.append(
        f"guidance is {guidance}; candle_speed={speed:.2f}, overlap_ratio={overlap:.2f}, ce_decay={decay_ce:.3f}, pe_decay={decay_pe:.3f}."
    )
    if option_quality > 0.0 or option_strength > 0.0:
        lines.append(
            f"option_flow_strength={option_strength:.2f}, option_consistency={option_consistency:.2f}, "
            f"option_quality={option_quality:.2f}, option_skew={option_skew:.3f}."
        )
    if entry_signal is not None and stop_loss_distance is not None and target_level is not None:
        lines.append(
            f"entry_signal is {'on' if bool(entry_signal) else 'off'}; "
            f"stop_loss_distance={float(stop_loss_distance):.2f}; target_level={float(target_level):.2f}."
        )
    return "\n".join(lines[:7])
