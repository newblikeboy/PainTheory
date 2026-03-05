from __future__ import annotations

import os
from typing import Any, Dict, Optional

from fyers_apiv3 import fyersModel


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

    def fetch_ltp(self, symbol: str) -> Optional[Dict[str, Any]]:
        payload = self._client.quotes(data={"symbols": str(symbol or "").strip()})
        if not isinstance(payload, dict):
            return None
        if _to_int(payload.get("code"), 0) != 200:
            return None
        rows = payload.get("d") or payload.get("data") or []
        if not isinstance(rows, list) or not rows:
            return None

        target = str(symbol or "").strip().upper()
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
