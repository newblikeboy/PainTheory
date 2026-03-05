import asyncio
import logging
import threading
import time
from typing import Any, Awaitable, Callable, Dict, Optional

try:
    from fyers_apiv3.FyersWebsocket import data_ws
except ImportError:  # pragma: no cover - optional dependency
    data_ws = None

logger = logging.getLogger(__name__)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_ts(value: Any) -> int:
    if value is None:
        return int(time.time())
    try:
        ts = int(float(value))
    except (TypeError, ValueError):
        return int(time.time())
    if ts > 1_000_000_000_000:
        return int(ts / 1000)
    return ts


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _message_text(message: Any) -> str:
    if isinstance(message, dict):
        return str(
            message.get("message")
            or message.get("error")
            or message.get("s")
            or message
        )
    return str(message)


def _is_auth_error(message: Any) -> bool:
    if isinstance(message, dict):
        code = _to_int(message.get("code"), 0)
        if code in {-300, -99, -15, 401, 403}:
            return True
    text = _message_text(message).lower()
    if "token" in text and ("expired" in text or "invalid" in text or "valid token" in text):
        return True
    return False


class FyersTickStream:
    def __init__(
        self,
        access_token: str,
        symbol: str,
        data_type: str = "symbolData",
        log_path: str = "",
        lite_mode: bool = False,
        reconnect: bool = True,
        volume_mode: str = "delta",
        debug: bool = False,
    ) -> None:
        if data_ws is None:
            raise RuntimeError("fyers-apiv3 is required for STREAM_MODE=fyers")
        if not access_token:
            raise RuntimeError("FYERS_ACCESS_TOKEN is required for STREAM_MODE=fyers")

        self.access_token = access_token
        self.symbol = symbol
        self.data_type = self._normalize_data_type(data_type)
        self.log_path = log_path
        self.lite_mode = lite_mode
        self.reconnect = reconnect
        self.volume_mode = volume_mode
        self.debug = debug

        self.queue: asyncio.Queue[Dict[str, float]] = asyncio.Queue()
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._socket: Optional[data_ws.FyersDataSocket] = None
        self._last_volume: Optional[float] = None
        self._fatal_error: str = ""
        self._fatal_lock = threading.Lock()

    @staticmethod
    def _normalize_data_type(value: str) -> str:
        text = str(value or "").strip().lower()
        if text in {"depthupdate", "depth_update", "depthdata"}:
            return "DepthUpdate"
        return "SymbolUpdate"

    async def run(
        self,
        handler: Callable[[Dict[str, float]], Awaitable[None]],
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.loop = asyncio.get_running_loop()
        self._thread = threading.Thread(target=self._connect, daemon=True)
        self._thread.start()
        while True:
            if should_stop and should_stop():
                self._close_socket()
                return
            fatal_error = self._get_fatal_error()
            if fatal_error:
                self._close_socket()
                raise RuntimeError(fatal_error)
            try:
                tick = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await handler(tick)

    def _connect(self) -> None:
        self._socket = data_ws.FyersDataSocket(
            access_token=self.access_token,
            log_path=self.log_path,
            litemode=self.lite_mode,
            write_to_file=False,
            reconnect=self.reconnect,
            on_connect=self._onopen,
            on_message=self._onmessage,
            on_error=self._onerror,
            on_close=self._onclose,
        )
        self._socket.connect()

    def _onopen(self) -> None:
        if self._socket is None:
            return
        self._socket.subscribe(symbols=[self.symbol], data_type=self.data_type)
        self._socket.keep_running()

    def _onmessage(self, message: Any) -> None:
        ticks = message if isinstance(message, list) else [message]
        for item in ticks:
            tick = self._parse_message(item)
            if tick and self.loop and not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.queue.put_nowait, tick)

    def _onerror(self, message: Any) -> None:
        logger.error("Fyers socket error: %s", message)
        if _is_auth_error(message):
            self._set_fatal_error(f"FYERS auth error: {_message_text(message)}")

    def _onclose(self, message: Any) -> None:
        logger.warning("Fyers socket closed: %s", message)
        if _is_auth_error(message):
            self._set_fatal_error(f"FYERS auth close: {_message_text(message)}")

    def _set_fatal_error(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        with self._fatal_lock:
            if not self._fatal_error:
                self._fatal_error = text

    def _get_fatal_error(self) -> str:
        with self._fatal_lock:
            return self._fatal_error

    def _close_socket(self) -> None:
        if self._socket is None:
            return
        try:
            self._socket.close_connection()
        except Exception:
            return

    def _parse_message(self, message: Any) -> Optional[Dict[str, float]]:
        if not isinstance(message, dict):
            return None

        symbol = message.get("symbol") or message.get("sym")
        if symbol and symbol != self.symbol:
            return None

        price = _to_float(
            message.get("ltp")
            or message.get("last_price")
            or message.get("price")
            or message.get("p")
        )
        if price is None:
            return None

        raw_volume = _to_float(
            message.get("vol_traded_today")
            or message.get("volume")
            or message.get("v")
            or message.get("ttv")
        )
        if raw_volume is None:
            raw_volume = 0.0

        volume = raw_volume
        if self.volume_mode == "delta":
            if self._last_volume is None:
                volume = 0.0
            else:
                volume = raw_volume - self._last_volume
                if volume < 0:
                    volume = raw_volume
            self._last_volume = raw_volume

        timestamp = _normalize_ts(
            message.get("timestamp")
            or message.get("last_traded_time")
            or message.get("tt")
            or message.get("time")
        )

        tick = {"timestamp": timestamp, "price": price, "volume": volume}
        if self.debug:
            tick["_raw"] = message
        return tick
