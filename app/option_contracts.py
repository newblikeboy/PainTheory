from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    _IST = ZoneInfo("Asia/Kolkata")
except ZoneInfoNotFoundError:
    _IST = timezone(timedelta(hours=5, minutes=30))

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
    "O": "OCT",
    "N": "NOV",
    "D": "DEC",
    "01": "JAN",
    "02": "FEB",
    "03": "MAR",
    "04": "APR",
    "05": "MAY",
    "06": "JUN",
    "07": "JUL",
    "08": "AUG",
    "09": "SEP",
    "10": "OCT",
    "11": "NOV",
    "12": "DEC",
}
_MONTH_TO_NUM = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_EXPLICIT_EXPIRY_KEYS = (
    "expiry_date",
    "expiryDate",
    "expiry",
    "expiry_dt",
    "expiryDt",
    "expiry_ts",
    "expiryTs",
    "expd",
    "exd",
    "expiration",
    "expirationDate",
    "expireDate",
)


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


def normalize_option_symbol(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    text = text.replace(" ", "")
    text = text.replace("-", "")
    text = text.replace("_", "")
    text = text.replace("INDEX", "")
    text = text.replace("NIFTY50", "NIFTY")
    text = text.replace("NIFTY 50", "NIFTY")
    return text


def _normalize_underlying(value: Any) -> str:
    text = str(value or "").strip().upper()
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    return re.sub(r"[^A-Z]", "", text)


def _to_ist_date(dt: date) -> Tuple[str, int]:
    text = dt.isoformat()
    ts = int(datetime(dt.year, dt.month, dt.day, tzinfo=_IST).timestamp())
    return text, ts


def _parse_expiry_value(raw: Any) -> Tuple[str, int, str]:
    if raw is None:
        return "", 0, ""
    if isinstance(raw, datetime):
        dt = raw.astimezone(_IST) if raw.tzinfo else raw.replace(tzinfo=_IST)
        return _to_ist_date(dt.date()) + ("field_datetime",)
    if isinstance(raw, date):
        return _to_ist_date(raw) + ("field_date",)

    text = str(raw).strip()
    if not text:
        return "", 0, ""

    digits = re.sub(r"\D", "", text)
    if digits:
        if len(digits) == 8 and digits.startswith(("19", "20")):
            try:
                dt = datetime.strptime(digits, "%Y%m%d").date()
                return _to_ist_date(dt) + ("field_yyyymmdd",)
            except ValueError:
                pass
        maybe_num = _to_int(digits, 0)
        if maybe_num > 10**11:
            dt = datetime.fromtimestamp(maybe_num / 1000.0, _IST).date()
            return _to_ist_date(dt) + ("field_epoch_ms",)
        if maybe_num > 10**8:
            dt = datetime.fromtimestamp(maybe_num, _IST).date()
            return _to_ist_date(dt) + ("field_epoch_sec",)

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=_IST)
        else:
            dt = dt.astimezone(_IST)
        return _to_ist_date(dt.date()) + ("field_iso",)
    except ValueError:
        pass

    for fmt in (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y/%m/%d",
        "%d-%b-%Y",
        "%d-%b-%y",
        "%d%b%Y",
        "%d%b%y",
        "%d %b %Y",
        "%d %b %y",
    ):
        try:
            dt = datetime.strptime(text.upper(), fmt).date()
            return _to_ist_date(dt) + ("field_text",)
        except ValueError:
            continue
    return "", 0, ""


def _last_tuesday_of_month(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    current = date(year, month, last_day)
    while current.weekday() != 1:
        current -= timedelta(days=1)
    return current


def _candidate_score(candidate: Dict[str, Any], *, explicit_option_type: str, explicit_strike: float) -> Tuple[int, int, int, int]:
    side = str(candidate.get("option_type") or "").upper()
    strike = _to_float(candidate.get("strike"), 0.0)
    strike_diff = abs(strike - explicit_strike) if explicit_strike > 0.0 else 0.0
    strike_score = 0
    if explicit_strike > 0.0:
        if strike_diff <= 1e-6:
            strike_score = 1000
        elif strike_diff <= 1.0:
            strike_score = 500
        else:
            strike_score = max(-500, -int(round(strike_diff)))
    elif strike > 0.0:
        strike_score = 100
    side_score = 100 if not explicit_option_type or side == explicit_option_type else -100
    exact_score = 10 if candidate.get("expiry_kind") == "weekly" else 0
    plausible_score = 10 if strike > 0.0 else -1000
    return (strike_score, side_score, exact_score, plausible_score)


def _parse_symbol_contract(symbol: str, *, explicit_option_type: str = "", explicit_strike: float = 0.0) -> Dict[str, Any]:
    text = normalize_option_symbol(symbol)
    if not text:
        return {}

    candidates: List[Dict[str, Any]] = []

    monthly = re.fullmatch(r"([A-Z]+)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+)(CE|PE)", text)
    if monthly:
        candidates.append(
            {
                "underlying": monthly.group(1),
                "expiry_kind": "monthly",
                "expiry_year_short": monthly.group(2),
                "expiry_month_name": monthly.group(3),
                "expiry_day": 0,
                "strike": _to_float(monthly.group(4), 0.0),
                "option_type": monthly.group(5),
                "symbol": text,
                "expiry_source": "symbol_monthly",
            }
        )

    weekly_angel = re.fullmatch(r"([A-Z]+)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})(\d+)(CE|PE)", text)
    if weekly_angel:
        candidates.append(
            {
                "underlying": weekly_angel.group(1),
                "expiry_kind": "weekly",
                "expiry_day": _to_int(weekly_angel.group(2), 0),
                "expiry_month_name": weekly_angel.group(3),
                "expiry_year_short": weekly_angel.group(4),
                "strike": _to_float(weekly_angel.group(5), 0.0),
                "option_type": weekly_angel.group(6),
                "symbol": text,
                "expiry_source": "symbol_ddmonyy",
            }
        )

    weekly_single = re.fullmatch(r"([A-Z]+)(\d{2})([1-9OND])(\d{2})(\d+)(CE|PE)", text)
    if weekly_single:
        month_name = _MONTH_CODE_MAP.get(weekly_single.group(3), "")
        if month_name:
            candidates.append(
                {
                    "underlying": weekly_single.group(1),
                    "expiry_kind": "weekly",
                    "expiry_year_short": weekly_single.group(2),
                    "expiry_month_name": month_name,
                    "expiry_day": _to_int(weekly_single.group(4), 0),
                    "strike": _to_float(weekly_single.group(5), 0.0),
                    "option_type": weekly_single.group(6),
                    "symbol": text,
                    "expiry_source": "symbol_yymdd",
                }
            )

    weekly_double = re.fullmatch(r"([A-Z]+)(\d{2})(0[1-9]|1[0-2])(\d{2})(\d+)(CE|PE)", text)
    if weekly_double:
        month_name = _MONTH_CODE_MAP.get(str(int(weekly_double.group(3))), "")
        if month_name:
            candidates.append(
                {
                    "underlying": weekly_double.group(1),
                    "expiry_kind": "weekly",
                    "expiry_year_short": weekly_double.group(2),
                    "expiry_month_name": month_name,
                    "expiry_day": _to_int(weekly_double.group(4), 0),
                    "strike": _to_float(weekly_double.group(5), 0.0),
                    "option_type": weekly_double.group(6),
                    "symbol": text,
                    "expiry_source": "symbol_yymmdd",
                }
            )

    if not candidates:
        return {}

    best = max(
        candidates,
        key=lambda item: _candidate_score(
            item,
            explicit_option_type=str(explicit_option_type or "").upper(),
            explicit_strike=_to_float(explicit_strike, 0.0),
        ),
    )

    expiry_year_short = str(best.get("expiry_year_short") or "").strip()
    expiry_month_name = str(best.get("expiry_month_name") or "").strip().upper()
    expiry_day = _to_int(best.get("expiry_day"), 0)
    expiry_year = 2000 + _to_int(expiry_year_short, 0) if expiry_year_short else 0
    expiry_month = _MONTH_TO_NUM.get(expiry_month_name, 0)
    expiry_date = ""
    expiry_ts = 0
    if expiry_year > 0 and expiry_month > 0:
        try:
            if str(best.get("expiry_kind")) == "monthly":
                dt = _last_tuesday_of_month(expiry_year, expiry_month)
                expiry_date, expiry_ts = _to_ist_date(dt)
                best["expiry_source"] = "symbol_monthly_derived"
            elif expiry_day > 0:
                dt = date(expiry_year, expiry_month, expiry_day)
                expiry_date, expiry_ts = _to_ist_date(dt)
        except ValueError:
            expiry_date = ""
            expiry_ts = 0

    best["expiry_year"] = expiry_year
    best["expiry_month"] = expiry_month
    best["expiry_date"] = expiry_date
    best["expiry_ts"] = expiry_ts
    return best


def build_contract_key(contract: Dict[str, Any]) -> str:
    symbol = normalize_option_symbol(contract.get("symbol"))
    underlying = _normalize_underlying(contract.get("underlying"))
    expiry_date = str(contract.get("expiry_date") or "").strip()
    option_type = str(contract.get("option_type") or "").strip().upper()
    strike = _to_float(contract.get("strike"), 0.0)
    strike_text = str(int(round(strike))) if strike > 0.0 and abs(strike - round(strike)) < 1e-6 else str(strike or "")
    if underlying and expiry_date and option_type and strike_text:
        return f"{underlying}|{expiry_date}|{strike_text}|{option_type}"
    if symbol:
        return symbol
    return f"{underlying}|{strike_text}|{option_type}".strip("|")


def extract_option_contract(
    source: Any,
    *,
    snapshot_ts: int = 0,
    explicit_option_type: str = "",
    explicit_strike: float = 0.0,
    explicit_expiry: Any = None,
) -> Dict[str, Any]:
    row = source if isinstance(source, dict) else {}
    symbol_raw = ""
    if isinstance(source, dict):
        symbol_raw = (
            row.get("symbol")
            or row.get("symbol_ticker")
            or row.get("symbol_name")
            or row.get("ticker")
            or row.get("option_symbol")
            or row.get("tradingsymbol")
            or row.get("name")
            or ""
        )
    else:
        symbol_raw = source
    symbol = normalize_option_symbol(symbol_raw)
    option_type = str(
        explicit_option_type
        or (row.get("option_type") if isinstance(source, dict) else "")
        or (row.get("side") if isinstance(source, dict) else "")
        or ""
    ).strip().upper()
    strike = _to_float(
        explicit_strike
        if explicit_strike > 0.0
        else (row.get("strike") if isinstance(source, dict) else 0.0),
        0.0,
    )
    exchange = str(
        (row.get("exchange") if isinstance(source, dict) else "")
        or ""
    ).strip().upper()
    symbol_token = str(
        (row.get("symbol_token") if isinstance(source, dict) else "")
        or (row.get("symboltoken") if isinstance(source, dict) else "")
        or (row.get("token") if isinstance(source, dict) else "")
        or (row.get("fy_token") if isinstance(source, dict) else "")
        or ""
    ).strip()

    expiry_date = ""
    expiry_ts = 0
    expiry_source = ""
    expiry_value = explicit_expiry
    if expiry_value is None and isinstance(source, dict):
        for key in _EXPLICIT_EXPIRY_KEYS:
            if row.get(key) not in (None, ""):
                expiry_value = row.get(key)
                break
    if expiry_value not in (None, ""):
        expiry_date, expiry_ts, expiry_source = _parse_expiry_value(expiry_value)

    parsed = _parse_symbol_contract(
        symbol,
        explicit_option_type=option_type,
        explicit_strike=strike,
    )

    if not option_type:
        option_type = str(parsed.get("option_type") or "").strip().upper()
    if strike <= 0.0:
        strike = _to_float(parsed.get("strike"), 0.0)

    underlying = _normalize_underlying(parsed.get("underlying") or symbol)
    expiry_kind = str(parsed.get("expiry_kind") or "").strip().lower()
    expiry_year = _to_int(parsed.get("expiry_year"), 0)
    expiry_month = _to_int(parsed.get("expiry_month"), 0)
    expiry_day = _to_int(parsed.get("expiry_day"), 0)
    if not expiry_date:
        expiry_date = str(parsed.get("expiry_date") or "").strip()
        expiry_ts = _to_int(parsed.get("expiry_ts"), 0)
        expiry_source = str(parsed.get("expiry_source") or "").strip()

    trade_ts = _to_int(snapshot_ts or (row.get("timestamp") if isinstance(source, dict) else 0), 0)
    contract = {
        "symbol": symbol,
        "symbol_token": symbol_token,
        "exchange": exchange,
        "underlying": underlying,
        "option_type": option_type,
        "strike": float(strike),
        "expiry_date": expiry_date,
        "expiry_ts": int(expiry_ts),
        "expiry_kind": expiry_kind,
        "expiry_year": int(expiry_year),
        "expiry_month": int(expiry_month),
        "expiry_day": int(expiry_day),
        "expiry_month_name": calendar.month_abbr[expiry_month].upper() if 0 < expiry_month < 13 else "",
        "expiry_year_short": str(expiry_year)[-2:] if expiry_year > 0 else "",
        "expiry_source": expiry_source,
        "snapshot_ts": int(trade_ts),
    }
    contract["contract_key"] = build_contract_key(contract)
    return contract


def contract_tradeability(contract: Dict[str, Any], *, reference_ts: int, allow_expiry_day: bool = True) -> Tuple[bool, str]:
    symbol = normalize_option_symbol(contract.get("symbol"))
    if not symbol:
        return False, "missing_symbol"
    expiry_ts = _to_int(contract.get("expiry_ts"), 0)
    expiry_date = str(contract.get("expiry_date") or "").strip()
    if expiry_ts <= 0 or not expiry_date:
        return True, ""
    reference_day = datetime.fromtimestamp(max(0, int(reference_ts or 0)), _IST).date()
    expiry_day = datetime.fromtimestamp(expiry_ts, _IST).date()
    if expiry_day < reference_day:
        return False, f"expired:{expiry_date}"
    if expiry_day == reference_day and not allow_expiry_day:
        return False, f"expiry_day_blocked:{expiry_date}"
    return True, ""


def contract_days_to_expiry(contract: Dict[str, Any], *, reference_ts: int) -> int:
    expiry_ts = _to_int(contract.get("expiry_ts"), 0)
    if expiry_ts <= 0:
        return 999999
    reference_day = datetime.fromtimestamp(max(0, int(reference_ts or 0)), _IST).date()
    expiry_day = datetime.fromtimestamp(expiry_ts, _IST).date()
    return (expiry_day - reference_day).days


def build_symbol_search_candidates(contract: Dict[str, Any]) -> List[str]:
    symbol = normalize_option_symbol(contract.get("symbol"))
    underlying = _normalize_underlying(contract.get("underlying"))
    option_type = str(contract.get("option_type") or "").strip().upper()
    strike = _to_float(contract.get("strike"), 0.0)
    expiry_ts = _to_int(contract.get("expiry_ts"), 0)
    expiry_kind = str(contract.get("expiry_kind") or "").strip().lower()
    out: List[str] = []

    def _add(candidate: str) -> None:
        text = normalize_option_symbol(candidate)
        if text and text not in out:
            out.append(text)

    if expiry_ts > 0 and underlying and option_type in {"CE", "PE"} and strike > 0.0:
        expiry_dt = datetime.fromtimestamp(expiry_ts, _IST)
        strike_text = str(int(round(strike))) if abs(strike - round(strike)) < 1e-6 else str(strike)
        _add(f"{underlying}{expiry_dt.strftime('%d%b%y').upper()}{strike_text}{option_type}")
    if expiry_kind == "monthly" and underlying and option_type in {"CE", "PE"} and strike > 0.0:
        expiry_year = _to_int(contract.get("expiry_year"), 0)
        expiry_month = _to_int(contract.get("expiry_month"), 0)
        if expiry_year > 0 and expiry_month > 0:
            strike_text = str(int(round(strike))) if abs(strike - round(strike)) < 1e-6 else str(strike)
            _add(f"{underlying}{str(expiry_year)[-2:]}{calendar.month_abbr[expiry_month].upper()}{strike_text}{option_type}")
    _add(symbol)
    return out
