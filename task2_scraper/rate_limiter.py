from __future__ import annotations

import asyncio
from collections import deque


class RateLimiter:
    def __init__(self, rate: int, period: float = 1.0) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        self._rate = rate
        self._period = period
        self._timestamps: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            async with self._lock:
                now = loop.time()
                while self._timestamps and now - self._timestamps[0] >= self._period:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._rate:
                    self._timestamps.append(now)
                    return

                sleep_for = self._period - (now - self._timestamps[0])

            await asyncio.sleep(max(sleep_for, 0.0))
