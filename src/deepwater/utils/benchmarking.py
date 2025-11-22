# feeds/metrics_core.py
# Lightweight, lockless metrics for Deepwater engine
import time
import random
from collections import deque
from typing import Deque, Dict, Tuple

class _SecondBuckets:
    """Ring of (sec, count, bytes). Snapshots sum the last N seconds.
    Keeps <= cap entries; cap=16 covers 10s windows with slack.
    """
    __slots__ = ("_q", "_cap")
    def __init__(self, cap: int = 16) -> None:
        self._q: Deque[Tuple[int, int, int]] = deque()
        self._cap = cap
    def mark(self, t_sec: int, count: int = 1, nbytes: int = 0) -> None:
        if self._q and self._q[-1][0] == t_sec:
            s, c, b = self._q[-1]; self._q[-1] = (s, c + count, b + nbytes)
        else:
            self._q.append((t_sec, count, nbytes))
            if len(self._q) > self._cap:
                self._q.popleft()
    def sum_window(self, now: int, window_s: int) -> Tuple[int, int]:
        c = b = 0
        cutoff = now - window_s
        for s, cc, bb in self._q:
            if s >= cutoff:
                c += cc; b += bb
        return c, b

class _PidBuckets:
    __slots__ = ("_q", "_cap")
    def __init__(self, cap: int = 16) -> None:
        self._q: Deque[Tuple[int, int]] = deque()
        self._cap = cap
    def mark(self, t_sec: int, inc: int = 1) -> None:
        if self._q and self._q[-1][0] == t_sec:
            s, c = self._q[-1]; self._q[-1] = (s, c + inc)
        else:
            self._q.append((t_sec, inc))
            if len(self._q) > self._cap:
                self._q.popleft()
    def sum_window(self, now: int, window_s: int) -> int:
        c = 0
        cutoff = now - window_s
        for s, cc in self._q:
            if s >= cutoff:
                c += cc
        return c


class _LatencyWindow:
    """Reservoir-based percentile tracker over a rolling time window."""
    __slots__ = ("W","cap","_secs","_last_sec")
    def __init__(self, window_seconds: int = 10, cap: int = 128) -> None:
        self.W = max(1, int(window_seconds))
        self.cap = int(cap)
        self._secs: Dict[int, Dict[str, object]] = {}
        self._last_sec: int | None = None

    def _prune(self, now_sec: int) -> None:
        cutoff = now_sec - self.W + 1
        for s in list(self._secs.keys()):
            if s < cutoff:
                del self._secs[s]

    def add(self, value: float) -> None:
        now_sec = int(time.time())
        if self._last_sec != now_sec:
            self._prune(now_sec)
            self._last_sec = now_sec
        bucket = self._secs.get(now_sec)
        if bucket is None:
            bucket = self._secs[now_sec] = {"seen": 0, "buf": []}
        bucket["seen"] += 1
        buf: list[float] = bucket["buf"]  # type: ignore[assignment]
        if len(buf) < self.cap:
            buf.append(float(value))
        else:
            seen = bucket["seen"]
            if random.randrange(seen) < self.cap:
                buf[random.randrange(self.cap)] = float(value)

    def p99(self) -> float:
        if not self._secs:
            return 0.0
        now_sec = int(time.time())
        self._prune(now_sec)
        pool: list[float] = []
        for bucket in self._secs.values():
            pool.extend(bucket["buf"])  # type: ignore[arg-type]
        if not pool:
            return 0.0
        pool.sort()
        idx = int(0.99 * (len(pool) - 1))
        return float(pool[idx])

    def reset(self) -> None:
        self._secs.clear()
        self._last_sec = None


class _LatencyTracker:
    """Expose 'fast' and 'slow' p99s plus last-sample latency."""
    __slots__ = ("fast","slow","_last")
    def __init__(self, fast_window: int = 3, slow_window: int = 15, cap: int = 256) -> None:
        self.fast = _LatencyWindow(fast_window, cap)
        self.slow = _LatencyWindow(slow_window, cap)
        self._last = 0.0

    def add_us(self, value_us: float) -> None:
        self._last = float(value_us)
        self.fast.add(value_us)
        self.slow.add(value_us)

    def snapshot(self) -> Dict[str, float]:
        return {
            "last": self._last,
            "p99_fast": self.fast.p99(),
            "p99_slow": self.slow.p99(),
        }

    def reset(self) -> None:
        self.fast.reset()
        self.slow.reset()
        self._last = 0.0

class Metrics:
    """Public, engine-agnostic metrics surface."""
    __slots__ = ("_total", "_tr_rates", "_l2_rates", "_tr_lat", "_l2_lat")
    def __init__(self) -> None:
        self._total = _SecondBuckets()
        self._tr_rates: Dict[str, _PidBuckets] = {}
        self._l2_rates: Dict[str, _PidBuckets] = {}
        self._tr_lat: Dict[str, _LatencyTracker] = {}
        self._l2_lat: Dict[str, _LatencyTracker] = {}

    # marks
    def ingress(self, nbytes: int, count: int = 1) -> None:
        self._total.mark(int(time.time()), count, nbytes)

    def trade(self, pid: str, n: int, latency_us: float) -> None:
        now = int(time.time())
        r = self._tr_rates.get(pid)
        if r is None:
            r = self._tr_rates[pid] = _PidBuckets()
            self._tr_lat[pid] = _LatencyTracker()
        r.mark(now, n)
        self._tr_lat[pid].add_us(latency_us)

    def l2(self, pid: str, n: int, latency_us: float) -> None:
        now = int(time.time())
        r = self._l2_rates.get(pid)
        if r is None:
            r = self._l2_rates[pid] = _PidBuckets()
            self._l2_lat[pid] = _LatencyTracker()
        r.mark(now, n)
        self._l2_lat[pid].add_us(latency_us)

    # snapshots
    def snapshot(self) -> dict:
        now = int(time.time())
        c1, b1   = self._total.sum_window(now, 1)
        c10, b10 = self._total.sum_window(now, 10)
        out = {
            "total": {
                "rps_1s": float(c1),
                "rps_10s": float(c10) / 10.0,
                "MBps_1s":  (b1  / 1_000_000.0),
                "MBps_10s": (b10 / 1_000_000.0) / 10.0,
            },
            "trades": {},
            "l2": {},
        }
        for pid, r in self._tr_rates.items():
            c1 = r.sum_window(now, 1)
            c10 = r.sum_window(now, 10)
            out["trades"][pid] = {
                "rates": {"rps_1s": float(c1), "rps_10s": float(c10) / 10.0},
                "latency": self._tr_lat[pid].snapshot(),
            }
        for pid, r in self._l2_rates.items():
            c1 = r.sum_window(now, 1)
            c10 = r.sum_window(now, 10)
            out["l2"][pid] = {
                "rates": {"rps_1s": float(c1), "rps_10s": float(c10) / 10.0},
                "latency": self._l2_lat[pid].snapshot(),
            }
        return out

    def reset(self) -> None:
        self.__init__()
