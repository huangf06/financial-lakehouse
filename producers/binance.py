"""Binance WS trade producer."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import websockets

logger = logging.getLogger(__name__)

_FIELD_MAP = {
    "e": "event_type",
    "E": "event_time",
    "s": "symbol",
    "t": "trade_id",
    "p": "price",
    "q": "quantity",
    "T": "trade_time",
    "m": "buyer_is_maker",
}


def normalize_trade_message(raw: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in raw.items():
        new_key = _FIELD_MAP.get(key, key)
        if new_key in {"event_time", "trade_time"} and isinstance(value, int):
            result[new_key] = datetime.fromtimestamp(value / 1000, tz=UTC).isoformat()
        else:
            result[new_key] = value
    return result


class BinanceTradeProducer:
    def __init__(self, symbols: list[str], landing_root: Path) -> None:
        from producers.base import AtomicJsonlWriter

        self._symbols = [s.lower() for s in symbols]
        self._writer = AtomicJsonlWriter(landing_root=landing_root, source="binance")
        self._stop = asyncio.Event()

    @property
    def ws_url(self) -> str:
        streams = "/".join(f"{s}@trade" for s in self._symbols)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    async def run(self) -> None:
        backoff = 1.0
        async with self._writer:
            while not self._stop.is_set():
                try:
                    async with websockets.connect(
                        self.ws_url, ping_interval=20, ping_timeout=10
                    ) as ws:
                        backoff = 1.0
                        async for message in ws:
                            payload = json.loads(message)
                            data = payload.get("data") if "data" in payload else payload
                            if isinstance(data, dict) and data.get("e") == "trade":
                                await self._writer.write(normalize_trade_message(data))
                except Exception as e:  # noqa: BLE001
                    logger.warning("Binance WS error: %s; reconnecting in %.1fs", e, backoff)
                    with suppress(TimeoutError):
                        await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                    backoff = min(backoff * 2, 60.0)

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    producer = BinanceTradeProducer(
        symbols=os.environ.get("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(","),
        landing_root=Path(os.environ.get("LANDING_ROOT", "/tmp/landing")),
    )
    loop = asyncio.new_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, producer.stop)
    loop.run_until_complete(producer.run())


if __name__ == "__main__":
    main()
