"""Alpaca Market Data REST polling producer."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp

from producers.base import JsonlWriter, landing_writer_from_env

logger = logging.getLogger(__name__)

_FIELD_MAP = {
    "t": "bar_open_ts",
    "S": "symbol",
    "o": "open",
    "h": "high",
    "l": "low",
    "c": "close",
    "v": "volume",
    "vw": "vwap",
    "n": "trade_count",
}


def normalize_bar(raw: dict[str, Any], timeframe: str) -> dict[str, Any]:
    result: dict[str, Any] = {"timeframe": timeframe}
    for key, value in raw.items():
        new_key = _FIELD_MAP.get(key, key)
        if new_key == "bar_open_ts" and isinstance(value, str):
            result[new_key] = (
                datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC).isoformat()
            )
        else:
            result[new_key] = value
    return result


class AlpacaBarProducer:
    def __init__(
        self,
        symbols: list[str],
        api_key: str,
        api_secret: str,
        writer: JsonlWriter | None = None,
        timeframe: str = "1Min",
        poll_interval_seconds: float = 60.0,
        feed: str = "iex",
    ) -> None:
        self._symbols = symbols
        self._headers = {"APCA-API-KEY-ID": api_key, "APCA-API-SECRET-KEY": api_secret}
        self._writer = writer if writer is not None else landing_writer_from_env("alpaca")
        self._timeframe = timeframe
        self._poll_interval = poll_interval_seconds
        self._feed = feed
        self._stop = asyncio.Event()
        self._seen: set[str] = set()

    async def _fetch_bars(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        end = datetime.now(tz=UTC)
        start = end - timedelta(minutes=2)
        params = {
            "symbols": ",".join(self._symbols),
            "timeframe": self._timeframe,
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": end.isoformat().replace("+00:00", "Z"),
            "feed": self._feed,
            "limit": "1000",
        }
        async with session.get(
            "https://data.alpaca.markets/v2/stocks/bars", headers=self._headers, params=params
        ) as resp:
            if resp.status != 200:
                logger.warning("Alpaca returned %s: %s", resp.status, await resp.text())
                return []
            data = await resp.json()
        rows: list[dict[str, Any]] = []
        for symbol, items in data.get("bars", {}).items():
            for raw in items:
                raw["S"] = symbol
                key = f"{symbol}|{raw.get('t')}"
                if key not in self._seen:
                    self._seen.add(key)
                    rows.append(raw)
        if len(self._seen) > 10_000:
            self._seen = set(list(self._seen)[-5000:])
        return rows

    async def run(self) -> None:
        async with self._writer, aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                for raw in await self._fetch_bars(session):
                    await self._writer.write(normalize_bar(raw, self._timeframe))
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=self._poll_interval)

    def stop(self) -> None:
        self._stop.set()


def main() -> None:
    producer = AlpacaBarProducer(
        symbols=os.environ.get("ALPACA_SYMBOLS", "AAPL,MSFT,SPY").split(","),
        api_key=os.environ["ALPACA_API_KEY"],
        api_secret=os.environ["ALPACA_API_SECRET"],
    )
    asyncio.run(producer.run())


if __name__ == "__main__":
    main()
