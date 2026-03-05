import asyncio
import os
from typing import Any, Awaitable, Callable, Dict, Optional

from fyers_apiv3 import fyersModel


class OptionChainPoller:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        symbol: str,
        strikecount: int,
        poll_interval_sec: int,
        log_path: str = "",
    ) -> None:
        if not client_id or not access_token:
            raise RuntimeError("FYERS_CLIENT_ID and FYERS_ACCESS_TOKEN are required")
        self.client_id = client_id
        self.access_token = access_token
        self.symbol = symbol
        self.log_path = str(log_path or "").strip()
        # Ensure payload has enough strikes so backend can enforce ATM +/- 5 band.
        self.strikecount = max(11, int(strikecount))
        self.poll_interval_sec = max(5, poll_interval_sec)
        if self.log_path:
            os.makedirs(self.log_path, exist_ok=True)

        self.fyers = fyersModel.FyersModel(
            client_id=self.client_id,
            token=self.access_token,
            is_async=False,
            log_path=self.log_path,
        )

    async def run(
        self,
        handler: Callable[[Dict[str, Any]], Awaitable[None]],
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> None:
        while True:
            if should_stop and should_stop():
                return
            payload = await asyncio.to_thread(self._fetch)
            await handler(payload)
            if should_stop and should_stop():
                return
            await asyncio.sleep(self.poll_interval_sec)

    def _fetch(self) -> Dict[str, Any]:
        data = {
            "symbol": self.symbol,
            "strikecount": self.strikecount,
            "timestamp": "",
        }
        response = self.fyers.optionchain(data=data)
        return response
