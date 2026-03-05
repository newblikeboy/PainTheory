from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Dict, Optional

import joblib

from ..pain_theory_ai.core import (
    GUIDANCE_CHOICES,
    MARKET_DIRECTIONS,
    PAIN_PHASES,
    PARTICIPANT_GROUPS,
    derive_trade_plan,
)

MISTAKE_TYPES = {
    "no_trade",
    "none",
    "false_entry",
    "late_entry",
    "wrong_side_pain",
    "chop_zone",
    "expiry_noise",
    "unclassified_loss",
}
_NON_BLOCKING_MISTAKE_TYPES = {"none", "no_trade"}

_LEGACY_NEXT_GROUPS = {
    "call_buyers",
    "put_buyers",
    "call_sellers",
    "put_sellers",
    "buyers_both",
    "none",
}
_DIRECTIONAL_NEXT_GROUPS = {
    "call_buyers",
    "put_buyers",
    "call_sellers",
    "put_sellers",
    "bullish",
    "bearish",
}
_SUPPORTED_NEXT_GROUPS = set(MARKET_DIRECTIONS) | _LEGACY_NEXT_GROUPS


@dataclass
class PainTheoryModelBundle:
    classifiers: Dict[str, Any]
    encoders: Dict[str, Any]
    regressors: Dict[str, Any]
    feature_columns: list[str]


def _clamp(value: float, low: float, high: float) -> float:
    if value < low:
        return low
    if value > high:
        return high
    return value


def load_pain_theory_model(path: str) -> Optional[PainTheoryModelBundle]:
    if not path or not os.path.exists(path):
        return None
    artifact = joblib.load(path)
    try:
        return PainTheoryModelBundle(
            classifiers=artifact["classifiers"],
            encoders=artifact["encoders"],
            regressors=artifact.get("regressors") if isinstance(artifact.get("regressors"), dict) else {},
            feature_columns=artifact["feature_columns"],
        )
    except KeyError as exc:
        raise RuntimeError(f"Invalid pain theory model artifact: missing {exc}") from exc


def _predict_target(model: Any, vector: list[float]) -> tuple[int, float]:
    raw_pred = model.predict([vector])[0]
    if hasattr(raw_pred, "__len__") and not isinstance(raw_pred, (str, bytes)):
        index = int(max(range(len(raw_pred)), key=lambda idx: raw_pred[idx]))
    else:
        index = int(round(float(raw_pred)))

    confidence = 0.5
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba([vector])[0]
        try:
            confidence = float(max(proba))
        except Exception:
            confidence = 0.5
    return index, confidence


def _predict_regressor(model: Any, vector: list[float], default: float) -> float:
    try:
        raw = model.predict([vector])[0]
    except Exception:
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _entry_label_to_bool(value: str) -> bool:
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on", "entry", "trigger"}


def _feature_value(features: Dict[str, float], *keys: str, default: float = 0.0) -> float:
    for key in keys:
        raw = features.get(key)
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return float(default)


def _is_supported_next_group(value: str) -> bool:
    return str(value or "").strip().lower() in _SUPPORTED_NEXT_GROUPS


def _is_directional_next_group(value: str) -> bool:
    return str(value or "").strip().lower() in _DIRECTIONAL_NEXT_GROUPS


def _dominant_from_next_group(next_group: str) -> str:
    text = str(next_group or "").strip().lower()
    if text == "put_buyers":
        return "call_sellers"
    if text == "call_buyers":
        return "put_sellers"
    return "none"


def _apply_option_overlay(
    *,
    features: Dict[str, float],
    dominant: str,
    next_group: str,
    guidance: str,
    confidence: float,
) -> Dict[str, Any]:
    skew = _feature_value(features, "execution_option_skew_balance", "option_skew_balance")
    ltp_skew = _feature_value(features, "execution_atm_ltp_skew", "atm_ltp_skew")
    oi_skew = _feature_value(features, "execution_atm_oi_skew", "atm_oi_skew")
    vol_skew = _feature_value(features, "execution_atm_volume_skew", "atm_volume_skew")
    strength = _clamp(
        max(
            _feature_value(features, "execution_option_flow_strength", "option_flow_strength"),
            abs(skew),
        ),
        0.0,
        2.5,
    )
    consistency = _clamp(
        max(
            _feature_value(features, "execution_option_signal_consistency", "option_signal_consistency"),
            0.0,
        ),
        0.0,
        1.0,
    )
    quality = _clamp(
        max(
            _feature_value(features, "execution_option_data_quality", "option_data_quality"),
            0.0,
        ),
        0.0,
        1.0,
    )
    bias = (0.60 * skew) + (0.20 * ltp_skew) + (0.12 * oi_skew) + (0.08 * vol_skew)
    usable = bool(quality >= 0.28 and strength >= 0.20 and abs(bias) >= 0.05)
    out = {
        "dominant": str(dominant or "none"),
        "next_group": str(next_group or "none"),
        "guidance": str(guidance or "observe"),
        "confidence": float(confidence),
        "overlay": {
            "applied": False,
            "usable": usable,
            "bias": float(bias),
            "strength": float(strength),
            "consistency": float(consistency),
            "quality": float(quality),
            "reason": "not_applicable",
        },
    }
    if not usable:
        return out

    option_next = "call_buyers" if bias > 0.0 else "put_buyers"
    current_next = out["next_group"]
    if current_next == "none" or float(out["confidence"]) < 0.62:
        out["next_group"] = option_next
        out["dominant"] = _dominant_from_next_group(option_next)
        out["guidance"] = "caution"
        out["confidence"] = max(float(out["confidence"]), 0.60 + (0.10 * min(1.0, quality)))
        out["overlay"]["applied"] = True
        out["overlay"]["reason"] = "seeded_direction_from_option_flow"
    elif current_next == option_next:
        out["confidence"] = float(out["confidence"]) + (0.08 * min(1.0, consistency)) + (0.05 * min(1.0, quality))
        if str(out["guidance"]) in {"no_edge", "observe"}:
            out["guidance"] = "caution"
        out["overlay"]["applied"] = True
        out["overlay"]["reason"] = "reinforced_existing_direction"
    else:
        out["confidence"] = float(out["confidence"]) - (0.10 * min(1.0, consistency))
        if str(out["guidance"]) == "caution":
            out["guidance"] = "observe"
        out["overlay"]["applied"] = True
        out["overlay"]["reason"] = "direction_conflict_penalty"

    out["confidence"] = _clamp(float(out["confidence"]), 0.0, 1.0)
    return out


def predict_pain_theory(
    model: PainTheoryModelBundle,
    features: Dict[str, float],
) -> Dict[str, Any]:
    vector = [float(features.get(col, 0.0)) for col in model.feature_columns]

    phase_idx, phase_conf = _predict_target(model.classifiers["pain_phase"], vector)
    dom_idx, dom_conf = _predict_target(model.classifiers["dominant_pain_group"], vector)
    next_idx, next_conf = _predict_target(model.classifiers["next_likely_pain_group"], vector)
    guide_idx, guide_conf = _predict_target(model.classifiers["guidance"], vector)

    pain_phase = str(model.encoders["pain_phase"].inverse_transform([phase_idx])[0])
    dominant = str(model.encoders["dominant_pain_group"].inverse_transform([dom_idx])[0])
    next_group = str(model.encoders["next_likely_pain_group"].inverse_transform([next_idx])[0])
    guidance = str(model.encoders["guidance"].inverse_transform([guide_idx])[0])

    if pain_phase not in PAIN_PHASES:
        pain_phase = "comfort"
    if dominant not in PARTICIPANT_GROUPS:
        dominant = "none"
    if not _is_supported_next_group(next_group):
        next_group = "neutral"
    if guidance not in GUIDANCE_CHOICES:
        guidance = "observe"

    confidence = _clamp((phase_conf + dom_conf + next_conf + guide_conf) / 4.0, 0.0, 1.0)
    overlay_out = _apply_option_overlay(
        features=features,
        dominant=dominant,
        next_group=next_group,
        guidance=guidance,
        confidence=confidence,
    )
    dominant = str(overlay_out.get("dominant", dominant))
    next_group = str(overlay_out.get("next_group", next_group))
    guidance = str(overlay_out.get("guidance", guidance))
    confidence = _clamp(float(overlay_out.get("confidence", confidence)), 0.0, 1.0)

    trade_plan = derive_trade_plan(
        features=features,
        pain_phase=pain_phase,
        next_group=next_group,
        guidance=guidance,
        confidence=confidence,
    )

    derived_entry_signal = bool(trade_plan["entry_signal"])
    entry_signal = bool(derived_entry_signal)
    entry_trigger_conf = 0.5
    if "entry_trigger" in model.classifiers and "entry_trigger" in model.encoders:
        trigger_idx, trigger_conf = _predict_target(model.classifiers["entry_trigger"], vector)
        trigger_label = str(model.encoders["entry_trigger"].inverse_transform([trigger_idx])[0])
        predicted_entry = _entry_label_to_bool(trigger_label)
        if predicted_entry:
            entry_signal = True
        elif not derived_entry_signal:
            entry_signal = False
        else:
            # Treat low-confidence negatives as advisory, not hard veto.
            entry_signal = bool(trigger_conf < 0.80)
        entry_trigger_conf = float(trigger_conf)
        overlay = overlay_out.get("overlay") if isinstance(overlay_out.get("overlay"), dict) else {}
        if (
            not entry_signal
            and bool(overlay.get("usable", False))
            and confidence >= 0.60
            and guidance in {"caution"}
            and _is_directional_next_group(str(next_group))
            and float(overlay.get("strength", 0.0)) >= 0.35
        ):
            entry_signal = True
    entry_trigger_conf = _clamp(entry_trigger_conf, 0.0, 1.0)

    mistake_type = "none"
    mistake_conf = 0.0
    if "mistake_type" in model.classifiers and "mistake_type" in model.encoders:
        try:
            mistake_idx, mistake_conf = _predict_target(model.classifiers["mistake_type"], vector)
            mistake_label = str(model.encoders["mistake_type"].inverse_transform([mistake_idx])[0])
            text = str(mistake_label or "").strip().lower()
            if text in MISTAKE_TYPES:
                mistake_type = text
        except Exception:
            mistake_type = "none"
            mistake_conf = 0.0
    mistake_conf = _clamp(float(mistake_conf), 0.0, 1.0)

    if mistake_type not in _NON_BLOCKING_MISTAKE_TYPES and mistake_conf >= 0.55:
        confidence = _clamp(confidence * max(0.35, 1.0 - (0.55 * mistake_conf)), 0.0, 1.0)
        if guidance == "caution":
            guidance = "observe"
        if mistake_conf >= 0.72:
            entry_signal = False

    # User policy: observe guidance is strictly non-tradable.
    if guidance == "observe":
        entry_signal = False

    stop_default = float(trade_plan["stop_loss_distance"])
    target_default = float(trade_plan["target_level"])
    stop_loss_distance = stop_default
    target_level = target_default
    if "stop_loss_distance" in model.regressors:
        stop_loss_distance = _predict_regressor(model.regressors["stop_loss_distance"], vector, stop_default)
    if "target_level" in model.regressors:
        target_level = _predict_regressor(model.regressors["target_level"], vector, target_default)

    stop_loss_distance = max(0.05, float(stop_loss_distance))
    target_level = max(stop_loss_distance * 1.02, float(target_level))

    direction = str(trade_plan.get("entry_direction", "NONE")).upper()
    spot_close = float(features.get("execution_spot_close", features.get("spot_close", 0.0)))
    recommended_stop_loss = 0.0
    recommended_targets: list[float] = []
    if spot_close > 0.0 and direction in {"LONG", "SHORT"} and entry_signal:
        if direction == "LONG":
            recommended_stop_loss = max(0.0, spot_close - stop_loss_distance)
            recommended_targets = [spot_close + target_level, spot_close + (target_level * 1.5)]
        else:
            recommended_stop_loss = max(0.0, spot_close + stop_loss_distance)
            recommended_targets = [spot_close - target_level, spot_close - (target_level * 1.5)]

    return {
        "pain_phase": pain_phase,
        "dominant_pain_group": dominant,
        "next_likely_pain_group": next_group,
        "confidence": confidence,
        "guidance": guidance,
        "entry_signal": bool(entry_signal),
        "entry_direction": direction,
        "entry_trigger_confidence": entry_trigger_conf,
        "mistake_type": mistake_type,
        "mistake_confidence": mistake_conf,
        "stop_loss_distance": float(stop_loss_distance),
        "target_level": float(target_level),
        "recommended_stop_loss": float(recommended_stop_loss),
        "recommended_targets": [float(item) for item in recommended_targets],
        "option_overlay": overlay_out.get("overlay", {}),
    }
