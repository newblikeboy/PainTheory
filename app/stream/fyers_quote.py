from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fyers_apiv3 import fyersModel

from ..option_contracts import extract_option_contract

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


_FYERS_OPTION_CHAIN_UNDERLYINGS = {
    "NIFTY": "NSE:NIFTY50-INDEX",
    "BANKNIFTY": "NSE:NIFTYBANK-INDEX",
    "FINNIFTY": "NSE:FINNIFTY-INDEX",
    "MIDCPNIFTY": "NSE:MIDCPNIFTY-INDEX",
    "SENSEX": "BSE:SENSEX-INDEX",
    "BANKEX": "BSE:BANKEX-INDEX",
}


class FyersQuoteClient:
    def __init__(self, *, client_id: str, access_token: str, log_path: str = "") -> None:
        if not client_id or not access_token:
            raise RuntimeError("FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN are required")
        self.client_id = str(client_id)
        self.access_token = str(access_token)
        self.log_path = str(log_path or "").strip()
        if self.log_path:
            os.makedirs(self.log_path, exist_ok=True)
        self._client = self._build_client(self.access_token)

    def _build_client(self, token: str):
        return fyersModel.FyersModel(
            client_id=self.client_id,
            token=str(token or ""),
            is_async=False,
            log_path=self.log_path,
        )

    def set_access_token(self, token: str) -> None:
        fresh = str(token or "").strip()
        if not fresh or fresh == self.access_token:
            return
        self.access_token = fresh
        self._client = self._build_client(self.access_token)

    def _parse_quote_payload(self, payload: Any, target_symbol: str) -> Optional[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return None
        if _to_int(payload.get("code"), 0) != 200:
            return None
        rows = payload.get("d") or payload.get("data") or []
        if not isinstance(rows, list) or not rows:
            return None

        target = str(target_symbol or "").strip().upper()
        best = None
        for item in rows:
            if not isinstance(item, dict):
                continue
            name = str(item.get("n") or item.get("symbol") or "").strip()
            if target and name and name.upper() != target:
                continue
            best = item
            break
        if best is None:
            best = rows[0] if isinstance(rows[0], dict) else None
        if not isinstance(best, dict):
            return None

        values = best.get("v") if isinstance(best.get("v"), dict) else {}
        ltp = _to_float(
            values.get("lp"),
            _to_float(best.get("lp"), 0.0),
        )
        if ltp <= 0.0:
            return None
        ts = _to_int(values.get("tt"), _to_int(best.get("tt"), 0))
        if ts > 10_000_000_000:
            ts = int(ts / 1000)
        return {
            "symbol": name or target,
            "ltp": float(ltp),
            "timestamp": ts,
            "raw": payload,
        }

    @staticmethod
    def _quote_candidates(symbol: str) -> List[str]:
        text = str(symbol or "").strip()
        if not text:
            return []
        candidates: List[str] = []
        if ":" in text:
            candidates.append(text)
        else:
            candidates.extend([text, f"NSE:{text}", f"NFO:{text}"])
        out: List[str] = []
        seen = set()
        for item in candidates:
            key = item.upper()
            if key in seen:
                continue
            seen.add(key)
            out.append(item)
        return out

    def _fetch_ltp_exact(self, symbol: str) -> Optional[Dict[str, Any]]:
        payload = self._client.quotes(data={"symbols": str(symbol or "").strip()})
        return self._parse_quote_payload(payload, str(symbol or "").strip())

    @staticmethod
    def _extract_option_chain_rows(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        rows = payload.get("data")
        if isinstance(rows, list):
            return [dict(item) for item in rows if isinstance(item, dict)]
        if isinstance(rows, dict):
            nested = rows.get("optionsChain")
            if isinstance(nested, list):
                return [dict(item) for item in nested if isinstance(item, dict)]
        return []

    def _resolve_option_symbol(self, symbol: str) -> str:
        contract = extract_option_contract(symbol)
        underlying = str(contract.get("underlying") or "").strip().upper()
        option_type = str(contract.get("option_type") or "").strip().upper()
        strike = _to_float(contract.get("strike"), 0.0)
        expiry_ts = _to_int(contract.get("expiry_ts"), 0)
        if not underlying or option_type not in {"CE", "PE"} or strike <= 0.0:
            return ""
        chain_symbol = _FYERS_OPTION_CHAIN_UNDERLYINGS.get(underlying, "")
        if not chain_symbol:
            return ""

        payload = self._client.optionchain(
            data={
                "symbol": chain_symbol,
                "strikecount": 50,
                "timestamp": "",
            }
        )
        rows = self._extract_option_chain_rows(payload)
        if not rows:
            return ""

        best_symbol = ""
        best_score: Optional[tuple] = None
        for row in rows:
            row_option_type = str(row.get("option_type") or "").strip().upper()
            row_strike = _to_float(row.get("strike_price"), 0.0)
            if row_option_type != option_type or abs(row_strike - strike) > 1e-6:
                continue
            parsed = extract_option_contract(
                row,
                explicit_option_type=row_option_type,
                explicit_strike=row_strike,
            )
            row_symbol = str(row.get("symbol") or parsed.get("symbol") or "").strip()
            if not row_symbol:
                continue
            row_expiry_ts = _to_int(parsed.get("expiry_ts"), 0)
            expiry_diff = abs(row_expiry_ts - expiry_ts) if expiry_ts > 0 and row_expiry_ts > 0 else 0
            score = (
                0 if row_expiry_ts == expiry_ts and expiry_ts > 0 else 1,
                expiry_diff,
                0 if ":" in row_symbol else 1,
                len(row_symbol),
            )
            if best_score is None or score < best_score:
                best_score = score
                best_symbol = row_symbol
        return best_symbol

    def fetch_ltp(self, symbol: str) -> Optional[Dict[str, Any]]:
        requested_symbol = str(symbol or "").strip()
        if not requested_symbol:
            return None

        attempted = self._quote_candidates(requested_symbol)
        for candidate in attempted:
            result = self._fetch_ltp_exact(candidate)
            if result is not None:
                if result.get("symbol") != requested_symbol:
                    result["requested_symbol"] = requested_symbol
                    result["resolved_symbol"] = str(result.get("symbol") or candidate)
                return result

        resolved_symbol = self._resolve_option_symbol(requested_symbol)
        if not resolved_symbol:
            return None
        for candidate in self._quote_candidates(resolved_symbol):
            if candidate.upper() in {item.upper() for item in attempted}:
                continue
            result = self._fetch_ltp_exact(candidate)
            if result is not None:
                result["requested_symbol"] = requested_symbol
                result["resolved_symbol"] = str(result.get("symbol") or candidate)
                return result
        return None
