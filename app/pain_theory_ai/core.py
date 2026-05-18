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

PAIN_SIDE_LABELS = (
    "bullish_pain",
    "bearish_pain",
    "neutral_pain",
)

LEGACY_PAIN_SIDE_LABELS = (
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
    "market_structure_trend",
    "market_structure_strength",
    "swing_high_state",
    "swing_low_state",
    "accepted_high_break",
    "accepted_low_break",
    "failed_high_break",
    "failed_low_break",
    "swing_range_position",
    "demand_zone_low",
    "demand_zone_high",
    "supply_zone_low",
    "supply_zone_high",
    "demand_zone_age",
    "supply_zone_age",
    "demand_zone_touches",
    "supply_zone_touches",
    "price_in_demand_zone",
    "price_in_supply_zone",
    "breakout_from_demand",
    "breakdown_from_supply",
    "failed_break_above_supply",
    "failed_break_below_demand",
    "retest_acceptance",
    "distance_to_demand_zone",
    "distance_to_supply_zone",
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

OPTION_SNAPSHOT_FEATURE_COLUMNS = {
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
}


def ai_feature_columns() -> List[str]:
    return [key for key in FEATURE_COLUMNS if key not in OPTION_SNAPSHOT_FEATURE_COLUMNS]


def dual_feature_columns(include_base: bool = True, include_options: bool = True) -> List[str]:
    base_columns = FEATURE_COLUMNS if include_options else ai_feature_columns()
    columns: List[str] = []
    if include_base:
        columns.extend(base_columns)
    for key in base_columns:
        columns.append(f"analysis_{key}")
        columns.append(f"execution_{key}")
        columns.append(f"delta_{key}")
    columns.extend(
        [
            "tf_direction_alignment",
            "tf_speed_ratio",
            "tf_range_ratio",
            "tf_overlap_ratio",
            "tf_atr_ratio",
            "tf_regime_shift",
            "tf_gamma_ratio",
        ]
    )
    if include_options:
        columns.append("tf_premium_balance")
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


def _confirmed_swings(
    candles: List[Dict[str, Any]],
    *,
    left: int = 2,
    right: int = 2,
) -> tuple[List[tuple[int, float]], List[tuple[int, float]]]:
    highs: List[tuple[int, float]] = []
    lows: List[tuple[int, float]] = []
    if len(candles) < left + right + 1:
        return highs, lows
    last_confirmed = len(candles) - right - 1
    for idx in range(left, last_confirmed + 1):
        row = candles[idx]
        high = _value(row, "high")
        low = _value(row, "low")
        left_rows = candles[idx - left : idx]
        right_rows = candles[idx + 1 : idx + 1 + right]
        left_highs = [_value(item, "high") for item in left_rows]
        right_highs = [_value(item, "high") for item in right_rows]
        left_lows = [_value(item, "low") for item in left_rows]
        right_lows = [_value(item, "low") for item in right_rows]
        if high >= max(left_highs + right_highs) and high > min(left_highs + right_highs):
            highs.append((idx, high))
        if low <= min(left_lows + right_lows) and low < max(left_lows + right_lows):
            lows.append((idx, low))
    return highs, lows


def _market_structure_features(window: List[Dict[str, Any]], current: Dict[str, Any]) -> Dict[str, float]:
    highs, lows = _confirmed_swings(window)
    close = _value(current, "close")
    current_high = _value(current, "high")
    current_low = _value(current, "low")

    swing_high_state = 0.0
    swing_low_state = 0.0
    if len(highs) >= 2:
        swing_high_state = float(_sign(highs[-1][1] - highs[-2][1], eps=1e-6))
    if len(lows) >= 2:
        swing_low_state = float(_sign(lows[-1][1] - lows[-2][1], eps=1e-6))

    last_high = highs[-1][1] if highs else 0.0
    last_low = lows[-1][1] if lows else 0.0
    accepted_high_break = 1.0 if last_high > 0.0 and current_high > last_high and close > last_high else 0.0
    accepted_low_break = 1.0 if last_low > 0.0 and current_low < last_low and close < last_low else 0.0
    failed_high_break = 1.0 if last_high > 0.0 and current_high > last_high and close <= last_high else 0.0
    failed_low_break = 1.0 if last_low > 0.0 and current_low < last_low and close >= last_low else 0.0

    trend = 0.0
    if swing_high_state > 0.0 and swing_low_state > 0.0:
        trend = 1.0
    elif swing_high_state < 0.0 and swing_low_state < 0.0:
        trend = -1.0
    elif accepted_high_break > 0.0 and swing_low_state >= 0.0:
        trend = 1.0
    elif accepted_low_break > 0.0 and swing_high_state <= 0.0:
        trend = -1.0

    strength = 0.0
    if trend != 0.0:
        matching_parts = 0
        if (trend > 0.0 and swing_high_state > 0.0) or (trend < 0.0 and swing_high_state < 0.0):
            matching_parts += 1
        if (trend > 0.0 and swing_low_state > 0.0) or (trend < 0.0 and swing_low_state < 0.0):
            matching_parts += 1
        if (trend > 0.0 and accepted_high_break > 0.0) or (trend < 0.0 and accepted_low_break > 0.0):
            matching_parts += 1
        strength = _clamp(0.35 + (0.22 * matching_parts), 0.0, 1.0)

    range_position = 0.5
    if last_high > last_low > 0.0:
        range_position = _clamp(_safe_div(close - last_low, last_high - last_low), 0.0, 1.0)

    return {
        "market_structure_trend": trend,
        "market_structure_strength": strength,
        "swing_high_state": swing_high_state,
        "swing_low_state": swing_low_state,
        "accepted_high_break": accepted_high_break,
        "accepted_low_break": accepted_low_break,
        "failed_high_break": failed_high_break,
        "failed_low_break": failed_low_break,
        "swing_range_position": range_position,
    }


def _zone_bounds(candle: Dict[str, Any], kind: str, atr: float) -> tuple[float, float]:
    open_ = _value(candle, "open")
    close = _value(candle, "close")
    high = _value(candle, "high")
    low = _value(candle, "low")
    body_low = min(open_, close)
    body_high = max(open_, close)
    zone_pad = max(atr * 0.15, 1e-6)
    zone_cap = max(atr * 1.10, zone_pad)

    if kind == "demand":
        zone_low = low
        zone_high = max(body_high, low + zone_pad)
        zone_high = min(zone_high, low + zone_cap)
    else:
        zone_high = high
        zone_low = min(body_low, high - zone_pad)
        zone_low = max(zone_low, high - zone_cap)

    if zone_high <= zone_low:
        midpoint = (zone_high + zone_low) / 2.0
        zone_low = midpoint - (zone_pad / 2.0)
        zone_high = midpoint + (zone_pad / 2.0)
    return float(zone_low), float(zone_high)


def _zone_touched(candle: Dict[str, Any], zone_low: float, zone_high: float) -> bool:
    return _value(candle, "low") <= zone_high and _value(candle, "high") >= zone_low


def _select_active_zone(
    window: List[Dict[str, Any]],
    swings: List[tuple[int, float]],
    *,
    kind: str,
    atr: float,
) -> Dict[str, float] | None:
    invalidation_pad = max(atr * 0.15, 1e-6)
    for swing_idx, _price in reversed(swings):
        if swing_idx < 0 or swing_idx >= len(window):
            continue
        zone_low, zone_high = _zone_bounds(window[swing_idx], kind, atr)
        later = window[swing_idx + 1 :]
        if kind == "demand":
            invalidated = any(_value(row, "close") < zone_low - invalidation_pad for row in later)
        else:
            invalidated = any(_value(row, "close") > zone_high + invalidation_pad for row in later)
        if invalidated:
            continue
        touches = sum(1 for row in later if _zone_touched(row, zone_low, zone_high))
        return {
            "low": zone_low,
            "high": zone_high,
            "age": float(len(window) - 1 - swing_idx),
            "touches": float(touches),
        }
    return None


def _distance_to_zone(close: float, zone_low: float, zone_high: float, atr: float, *, kind: str) -> float:
    if zone_low <= close <= zone_high:
        return 0.0
    denom = max(atr, 1e-9)
    if kind == "demand":
        if close > zone_high:
            return _clamp((close - zone_high) / denom, -10.0, 10.0)
        return _clamp((close - zone_low) / denom, -10.0, 10.0)
    if close < zone_low:
        return _clamp((zone_low - close) / denom, -10.0, 10.0)
    return _clamp((zone_high - close) / denom, -10.0, 10.0)


def _demand_supply_zone_features(
    window: List[Dict[str, Any]],
    current: Dict[str, Any],
    *,
    atr: float,
    structure: Dict[str, float],
) -> Dict[str, float]:
    highs, lows = _confirmed_swings(window)
    demand = _select_active_zone(window, lows, kind="demand", atr=atr)
    supply = _select_active_zone(window, highs, kind="supply", atr=atr)

    close = _value(current, "close")
    current_direction = float(_direction(current))
    expansion = _safe_div(max(0.0, _value(current, "high") - _value(current, "low")), max(atr, 1e-9))
    accepted_high_break = float(structure.get("accepted_high_break", 0.0)) >= 0.5
    accepted_low_break = float(structure.get("accepted_low_break", 0.0)) >= 0.5
    structure_trend = float(structure.get("market_structure_trend", 0.0))

    features = {
        "demand_zone_low": 0.0,
        "demand_zone_high": 0.0,
        "supply_zone_low": 0.0,
        "supply_zone_high": 0.0,
        "demand_zone_age": 0.0,
        "supply_zone_age": 0.0,
        "demand_zone_touches": 0.0,
        "supply_zone_touches": 0.0,
        "price_in_demand_zone": 0.0,
        "price_in_supply_zone": 0.0,
        "breakout_from_demand": 0.0,
        "breakdown_from_supply": 0.0,
        "failed_break_above_supply": 0.0,
        "failed_break_below_demand": 0.0,
        "retest_acceptance": 0.0,
        "distance_to_demand_zone": 0.0,
        "distance_to_supply_zone": 0.0,
    }

    demand_recent = False
    if demand is not None:
        demand_low = demand["low"]
        demand_high = demand["high"]
        features["demand_zone_low"] = demand_low
        features["demand_zone_high"] = demand_high
        features["demand_zone_age"] = demand["age"]
        features["demand_zone_touches"] = demand["touches"]
        features["price_in_demand_zone"] = 1.0 if _zone_touched(current, demand_low, demand_high) else 0.0
        features["distance_to_demand_zone"] = _distance_to_zone(close, demand_low, demand_high, atr, kind="demand")
        demand_recent = any(_zone_touched(row, demand_low, demand_high) for row in window[-5:])
        failed_below = _value(current, "low") < demand_low and close >= demand_low
        features["failed_break_below_demand"] = 1.0 if failed_below else 0.0
        breakout = (
            demand_recent
            and close > demand_high
            and current_direction >= 0.0
            and (accepted_high_break or structure_trend >= 0.0)
            and expansion >= 0.75
        )
        features["breakout_from_demand"] = 1.0 if breakout else 0.0
        if features["price_in_demand_zone"] > 0.0 and close > demand_high and current_direction >= 0.0:
            features["retest_acceptance"] = 1.0

    supply_recent = False
    if supply is not None:
        supply_low = supply["low"]
        supply_high = supply["high"]
        features["supply_zone_low"] = supply_low
        features["supply_zone_high"] = supply_high
        features["supply_zone_age"] = supply["age"]
        features["supply_zone_touches"] = supply["touches"]
        features["price_in_supply_zone"] = 1.0 if _zone_touched(current, supply_low, supply_high) else 0.0
        features["distance_to_supply_zone"] = _distance_to_zone(close, supply_low, supply_high, atr, kind="supply")
        supply_recent = any(_zone_touched(row, supply_low, supply_high) for row in window[-5:])
        failed_above = _value(current, "high") > supply_high and close <= supply_high
        features["failed_break_above_supply"] = 1.0 if failed_above else 0.0
        breakdown = (
            supply_recent
            and close < supply_low
            and current_direction <= 0.0
            and (accepted_low_break or structure_trend <= 0.0)
            and expansion >= 0.75
        )
        features["breakdown_from_supply"] = 1.0 if breakdown else 0.0
        if features["price_in_supply_zone"] > 0.0 and close < supply_low and current_direction <= 0.0:
            features["retest_acceptance"] = -1.0

    if demand_recent and supply_recent:
        features["retest_acceptance"] = 0.0
    return features


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
    structure = _market_structure_features(window, current)
    zones = _demand_supply_zone_features(window, current, atr=median_range, structure=structure)

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
        "market_structure_trend": structure["market_structure_trend"],
        "market_structure_strength": structure["market_structure_strength"],
        "swing_high_state": structure["swing_high_state"],
        "swing_low_state": structure["swing_low_state"],
        "accepted_high_break": structure["accepted_high_break"],
        "accepted_low_break": structure["accepted_low_break"],
        "failed_high_break": structure["failed_high_break"],
        "failed_low_break": structure["failed_low_break"],
        "swing_range_position": structure["swing_range_position"],
        "demand_zone_low": zones["demand_zone_low"],
        "demand_zone_high": zones["demand_zone_high"],
        "supply_zone_low": zones["supply_zone_low"],
        "supply_zone_high": zones["supply_zone_high"],
        "demand_zone_age": zones["demand_zone_age"],
        "supply_zone_age": zones["supply_zone_age"],
        "demand_zone_touches": zones["demand_zone_touches"],
        "supply_zone_touches": zones["supply_zone_touches"],
        "price_in_demand_zone": zones["price_in_demand_zone"],
        "price_in_supply_zone": zones["price_in_supply_zone"],
        "breakout_from_demand": zones["breakout_from_demand"],
        "breakdown_from_supply": zones["breakdown_from_supply"],
        "failed_break_above_supply": zones["failed_break_above_supply"],
        "failed_break_below_demand": zones["failed_break_below_demand"],
        "retest_acceptance": zones["retest_acceptance"],
        "distance_to_demand_zone": zones["distance_to_demand_zone"],
        "distance_to_supply_zone": zones["distance_to_supply_zone"],
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
    include_options: bool = True,
) -> Dict[str, float]:
    analysis = build_features(analysis_candles, analysis_options if include_options else None)
    execution = build_features(execution_candles, execution_options if include_options else None)
    base_columns = FEATURE_COLUMNS if include_options else ai_feature_columns()
    merged: Dict[str, float] = {key: float(analysis.get(key, 0.0)) for key in base_columns}

    for key in base_columns:
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
    if include_options:
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
    if text in {"bearish_pain", "bearish"}:
        return "LONG"
    if text in {"bullish_pain", "bullish"}:
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
    structure_trend = float(features.get("market_structure_trend", 0.0))
    structure_strength = max(0.0, float(features.get("market_structure_strength", 0.0)))
    failed_high_break = float(features.get("failed_high_break", 0.0)) >= 0.5
    failed_low_break = float(features.get("failed_low_break", 0.0)) >= 0.5
    zone_rejection_context = bool(
        float(features.get("trap_event", 0.0)) >= 0.5
        or float(features.get("direction_flip", 0.0)) >= 0.5
        or float(features.get("wick_dominance", 0.0)) >= 0.55
    )
    failed_above_supply = bool(
        float(features.get("failed_break_above_supply", 0.0)) >= 0.5
        and (failed_high_break or zone_rejection_context)
    )
    failed_below_demand = bool(
        float(features.get("failed_break_below_demand", 0.0)) >= 0.5
        and (failed_low_break or zone_rejection_context)
    )
    gamma = max(0.0, float(features.get("gamma_risk", 0.0)))

    stop_loss_distance = max(0.35, atr * (0.55 + (0.28 * compression) + (0.12 * gamma)))
    target_multiple = 1.20 + min(0.9, (0.7 * shock) + (0.12 * trend) + (0.16 * structure_strength) - (0.18 * compression))
    target_level = max(stop_loss_distance * 1.05, stop_loss_distance * target_multiple)

    trend_counter = bool(
        (direction == "LONG" and structure_trend < 0.0 and not failed_low_break and not failed_below_demand)
        or (direction == "SHORT" and structure_trend > 0.0 and not failed_high_break and not failed_above_supply)
    )

    entry_signal = bool(
        direction != "NONE"
        and guidance in {"caution"}
        and float(confidence) >= 0.53
        and not trend_counter
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
    structure_trend = float(features.get("market_structure_trend", 0.0))
    structure_strength = float(features.get("market_structure_strength", 0.0))
    swing_high_state = float(features.get("swing_high_state", 0.0))
    swing_low_state = float(features.get("swing_low_state", 0.0))
    accepted_high_break = float(features.get("accepted_high_break", 0.0)) >= 0.5
    accepted_low_break = float(features.get("accepted_low_break", 0.0)) >= 0.5
    failed_high_break = float(features.get("failed_high_break", 0.0)) >= 0.5
    failed_low_break = float(features.get("failed_low_break", 0.0)) >= 0.5
    breakout_from_demand = float(features.get("breakout_from_demand", 0.0)) >= 0.5
    breakdown_from_supply = float(features.get("breakdown_from_supply", 0.0)) >= 0.5
    failed_above_supply = float(features.get("failed_break_above_supply", 0.0)) >= 0.5
    failed_below_demand = float(features.get("failed_break_below_demand", 0.0)) >= 0.5
    retest_acceptance = float(features.get("retest_acceptance", 0.0))
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
    zone_failed_rejection = bool(
        (failed_above_supply or failed_below_demand)
        and (failed_high_break or failed_low_break or trap >= 0.5 or flip or wick >= 0.55 or expansion >= 1.10)
    )
    zone_breakout_acceptance = bool(
        (breakout_from_demand or breakdown_from_supply or abs(retest_acceptance) > 0.0)
        and (expansion >= 1.00 or speed >= 0.80 or abs(structure_trend) > 0.0)
    )

    phase = "comfort"
    market_expiry_pressure = bool(expiry_proximity >= 0.95 and (compression >= 1.20 or wick >= 0.82))
    market_exhaustion = bool(overlap >= 0.62 and idle >= 10 and (compression >= 1.15 or shock <= 0.12))
    market_late_entry = bool(expansion >= 1.15 and speed >= 1.05 and (abs(float(features.get("direction_persistence", 0.0))) >= 0.20 or shock >= 0.18))

    if failed_high_break or failed_low_break or zone_failed_rejection:
        phase = "transfer"
    elif (expiry_proximity >= 0.84 and band_concentration >= 0.50 and abs(option_skew) >= 0.08) or market_expiry_pressure:
        phase = "expiry_pain"
    elif zone_breakout_acceptance:
        phase = "exit_pain"
    elif (accepted_high_break or accepted_low_break) and expansion >= 1.05 and speed >= 0.80 and compression <= 1.15:
        phase = "exit_pain"
    elif shock >= 0.55 and expansion >= 1.35 and compression <= 0.95:
        phase = "exit_pain"
    elif trap >= 0.5 or (flip and sweep >= 0.5):
        phase = "transfer"
    elif compression >= 1.25 and overlap >= 0.60 and idle >= 8:
        phase = "digestion"
    elif overlap >= 0.62 and idle >= 10 and ((decay_ce >= 0.01 or decay_pe >= 0.01) or market_exhaustion):
        phase = "exhaustion_pain"
    elif expansion >= 1.15 and speed >= 1.05 and (max(burst_ce, burst_pe) >= 1.2 or market_late_entry):
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

    if failed_high_break or (failed_above_supply and zone_failed_rejection):
        dominant = "call_buyers"
        next_group = "call_buyers"
        guidance = "caution"
    elif failed_low_break or (failed_below_demand and zone_failed_rejection):
        dominant = "put_buyers"
        next_group = "put_buyers"
        guidance = "caution"
    elif (breakout_from_demand or retest_acceptance > 0.0) and zone_breakout_acceptance:
        dominant = "call_sellers"
        next_group = "put_buyers"
        guidance = "caution"
    elif (breakdown_from_supply or retest_acceptance < 0.0) and zone_breakout_acceptance:
        dominant = "put_sellers"
        next_group = "call_buyers"
        guidance = "caution"
    elif phase == "exit_pain":
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

    long_signal = next_group in {"put_buyers", "call_sellers"}
    short_signal = next_group in {"call_buyers", "put_sellers"}
    if (long_signal and structure_trend > 0.0) or (short_signal and structure_trend < 0.0):
        confidence += 0.08 * min(1.0, structure_strength)
    elif (long_signal and structure_trend < 0.0 and not failed_low_break) or (
        short_signal and structure_trend > 0.0 and not failed_high_break
    ):
        confidence -= 0.12 * min(1.0, structure_strength)

    if zone_failed_rejection:
        confidence += 0.09
    elif failed_high_break or failed_low_break:
        confidence += 0.08
    elif zone_breakout_acceptance:
        confidence += 0.06
    elif (retest_acceptance > 0.0 and next_group == "put_buyers") or (
        retest_acceptance < 0.0 and next_group == "call_buyers"
    ):
        confidence += 0.05
    elif (accepted_high_break and next_group == "put_buyers") or (accepted_low_break and next_group == "call_buyers"):
        confidence += 0.05

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
    breakout_from_demand = float(features.get("breakout_from_demand", 0.0)) >= 0.5
    breakdown_from_supply = float(features.get("breakdown_from_supply", 0.0)) >= 0.5
    failed_above_supply = float(features.get("failed_break_above_supply", 0.0)) >= 0.5
    failed_below_demand = float(features.get("failed_break_below_demand", 0.0)) >= 0.5
    price_in_demand = float(features.get("price_in_demand_zone", 0.0)) >= 0.5
    price_in_supply = float(features.get("price_in_supply_zone", 0.0)) >= 0.5

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
        if decay_ce > 0.0 or decay_pe > 0.0:
            lines.append(
                "Sideways time pressure is creating premium bleed; buyers_both feel time pain while sellers stay in comfort."
            )
        else:
            lines.append("Sideways time pressure and overlap are showing exhaustion without a clean release edge.")
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

    if failed_above_supply:
        lines.append("Price rejected above supply, so trapped call_buyers are the main pain group.")
    elif failed_below_demand:
        lines.append("Price rejected below demand, so trapped put_buyers are the main pain group.")
    elif breakout_from_demand:
        lines.append("Price accepted away from demand, supporting upward continuation pressure.")
    elif breakdown_from_supply:
        lines.append("Price accepted away from supply, supporting downward continuation pressure.")
    elif price_in_demand or price_in_supply:
        zone_name = "demand" if price_in_demand else "supply"
        lines.append(f"Price is testing the active {zone_name} zone.")

    if decay_ce > 0.0 or decay_pe > 0.0:
        lines.append(
            f"guidance is {guidance}; candle_speed={speed:.2f}, overlap_ratio={overlap:.2f}, ce_decay={decay_ce:.3f}, pe_decay={decay_pe:.3f}."
        )
    else:
        lines.append(f"guidance is {guidance}; candle_speed={speed:.2f}, overlap_ratio={overlap:.2f}.")
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
