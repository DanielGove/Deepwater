# feeds/websocket_client.py
import threading, socket, orjson, queue, time, struct, sys, traceback
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
from calendar import timegm
from typing import Dict, Iterable, Optional, Tuple, Any, List

import numba as _nb
from fastnumbers import fast_float as _ff

from core.platform import Platform  # unchanged platform substrate

# ======= tiny, allocation-aware helpers =======

def _now_s() -> int: return int(time.time())
def _now_ns() -> int: return time.time_ns()
def _perf_ns() -> int: return time.perf_counter_ns()

@_nb.njit(cache=True, fastmath=True)
def _days_from_civil(y: int, m: int, d:int):
    y -= m <=  2
    era = (y if y >= 0 else 400) // 400
    yoe = y - era * 400
    doy = (153 * (m + (-3 if m > 2 else 9)) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + yoe // 400 + doy
    return era * 146097 + doe - 719468

@_nb.njit(cache=True, fastmath=True)
def parse_ns_timestamp(ts):
    # digits to ints without slicing
    def d2(i): return (ts[i]-48)*10+(ts[i+1]-48)
    def d4(i): return (ts[i]-48)*1000+(ts[i+1]-48)*100+(ts[i+2]-48)*10+(ts[i+3]-48)
    year = d4(0); month = d2(5); day = d2(8)
    hour = d2(11); minute = d2(14); sec = d2(17)
    days = _days_from_civil(year, month, day)
    secs = days*86400+hour*3600+minute*60+sec
    nanos = 10000*d2(20)+100*d2(22)#+d2(24)
    return secs*1_000_000_000 + nanos

class SlidingRate:
    '''Fixed 1s buckets over a short window (default 10s). Non-destructive snapshots.'''
    __slots__ = ("window","_b","_lock")
    def __init__(self, window: int = 10):
        self.window = max(1, int(window))
        self._b: Dict[int, Tuple[int,int]] = {}
        self._lock = threading.Lock()
    def incr(self, n: int, by: int, sec: Optional[int] = None) -> None:
        s = _now_s() if sec is None else sec
        with self._lock:
            c,b = self._b.get(s, (0,0))
            self._b[s] = (c+n, b+by)
            cutoff = s - self.window + 1
            for k in list(self._b.keys()):
                if k < cutoff: self._b.pop(k, None)
    def snapshot(self) -> dict:
        now = _now_s()
        def span(sp: int) -> Tuple[float,float]:
            start = now - sp + 1
            c=b=0
            with self._lock:
                for ts,(cc,bb) in self._b.items():
                    if start <= ts <= now:
                        c += cc; b += bb
            sp = _ff(max(1, sp))
            return c/sp, (b/(1024*1024))/sp
        r1,m1 = span(1)
        r10,m10 = span(min(self.window, 10))
        return {"rps_1s": r1, "MBps_1s": m1, "rps_10s": r10, "MBps_10s": m10}

class RollingP99:
    '''Ring sampler in µs; p99 via partial sort on snapshot (cap defaults to 4096).'''
    __slots__=("buf","cap","n","i","_lock")
    def __init__(self, cap: int = 256):
        self.cap = max(256, int(cap)); self.buf=[0]*self.cap; self.n=0; self.i=0
        self._lock = threading.Lock()
    def add_us(self, v_us: int) -> None:
        if v_us < 0: v_us = 0
        with self._lock:
            self.buf[self.i] = v_us
            self.i = (self.i + 1) % self.cap
            self.n = self.cap if self.n == self.cap else self.n + 1
    def p99(self) -> float:
        with self._lock:
            n = self.n
            if n == 0: return 0.0
            tmp = self.buf[:n]  # copy only used portion
        tmp.sort()
        k = max(0, int(0.99 * (n - 1)))
        return _ff(tmp[k])

# ======= record formats (unchanged) =======
_PACK_TRADE = struct.Struct("<cc14xQQQQdd")   # 'T',side,trade_id,ev_ns,ws_ts_ns,proc_ns,price,size
_PACK_L2    = struct.Struct("<cc6xQQQdd16x")  # 'U',side,ws_ts,ev_ns,proc_ts,price,qty

# ======= Engine =======

class MarketDataEngine:
    '''
    Blazing-fast WS ingest using bounded queue and zero-ish copy parsing.
    - Platform writers are UNCHANGED (data substrate stays the same).
    - Channels: 'market_trades' and 'level2' (advanced trade WS).
    - No stdout spam; control plane decides rendering.
    '''
    def __init__(self, uri: str = "wss://advanced-trade-ws.coinbase.com",
                 max_queue: int = 16384,
                 sample_size: int = 16) -> None:
        # config
        self.uri = uri
        self.channels = ["market_trades", "level2"]
        self.sample_size = int(sample_size)
        # state
        self._should_run = False
        self._ws = None
        self._pid_lock = threading.Lock()
        self.product_ids: set[str] = set()
        # threads
        self.recv_thread: Optional[threading.Thread] = None
        self.proc_thread: Optional[threading.Thread] = None
        # bounded queue (drop-oldest)
        self.msg_queue: "queue.Queue[tuple[int,str]]" = queue.Queue(maxsize=max_queue)
        self.queue_hwm = 0
        # data buffers (minimize alloc)
        self._rec_trade = _PACK_TRADE.size
        self._rec_l2 = _PACK_L2.size
        self._buf_trade = bytearray(self._rec_trade*64)
        self._buf_l2 = bytearray(self._rec_l2*64)
        # platform
        self.platform = Platform()
        self.trade_writers: Dict[str, Any] = {}
        self.book_writers:  Dict[str, Any] = {}
        # metrics
        self._total_rate = SlidingRate(10)          # packet-level
        self._trade_rate: Dict[str, SlidingRate] = {}
        self._l2_rate:    Dict[str, SlidingRate] = {}
        self._lat_trade:  Dict[str, RollingP99] = {}
        self._lat_l2:     Dict[str, RollingP99] = {}
        self._seq_gap_trades = 0
        self._last_seq: int = -1

    # ---- lifecycle ----

    def start(self) -> None:
        if self._should_run: return
        self._should_run = True
        self.recv_thread = threading.Thread(target=self._recv_loop, name="ws", daemon=True)
        self.proc_thread = threading.Thread(target=self._proc_loop, name="proc", daemon=True)
        self.recv_thread.start(); self.proc_thread.start()

    def stop(self) -> None:
        self._should_run = False
        try:
            if self._ws is not None: self._ws.close()
        except Exception:
            pass
        for t in (self.recv_thread, self.proc_thread):
            if t and t.is_alive():
                t.join(timeout=3.0)
        # close writers predictably
        for w in list(self.trade_writers.values()):
            try: w.close()
            except Exception: pass
        for w in list(self.book_writers.values()):
            try: w.close()
            except Exception: pass

    # ---- control ----

    def subscribe_many(self, product_ids: Iterable[str]) -> None:
        with self._pid_lock:
            for p in product_ids:
                if p: self.product_ids.add(str(p).upper())
            snapshot = tuple(sorted(self.product_ids))
        self._send_subscribe(snapshot)

    def subscribe(self, product_id: str) -> None:
        if not product_id: return
        with self._pid_lock:
            self.product_ids.add(product_id.upper())
            snapshot = tuple(sorted(self.product_ids))
        self._send_subscribe(snapshot)

    def unsubscribe(self, product_id: str) -> None:
        if not product_id: return
        pid = product_id.upper()
        with self._pid_lock:
            self.product_ids.discard(pid)
        self._send_unsubscribe([pid])

    def list_products(self) -> list[str]:
        with self._pid_lock:
            return sorted(self.product_ids)

    def is_connected(self) -> bool:
        return self._ws is not None

    # ---- metrics (stable) ----

    def metrics_snapshot(self) -> dict:
        # non-destructive snapshot for smooth "status"
        trades = {}
        for pid, rate in list(self._trade_rate.items()):
            trades[pid] = {"rates": rate.snapshot(), "p99_us": self._lat_trade[pid].p99()}
        l2 = {}
        for pid, rate in list(self._l2_rate.items()):
            l2[pid] = {"rates": rate.snapshot(), "p99_us": self._lat_l2[pid].p99()}
        return {
            "queue": {"size": self.msg_queue.qsize(), "hwm": self.queue_hwm, "cap": self.msg_queue.maxsize},
            "rates_total": self._total_rate.snapshot(),
            "seq_gaps_trades": self._seq_gap_trades,
            "trades": trades,
            "l2": l2,
            "connected": self.is_connected(),
        }

    # ---- internals ----

    def _connect(self):
        # lowest-latency socket options
        self._ws = create_connection(
            self.uri,
            timeout=5,
            enable_multithread=True,
            sockopt=[
                (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),
                (socket.SOL_SOCKET,   socket.SO_KEEPALIVE, 1),
                (socket.SOL_SOCKET,   socket.SO_RCVBUF, 1<<20),
            ],
            skip_utf8_validation=True,
        )
        self._ws.settimeout(2.0)

    def _send_subscribe(self, snapshot: Tuple[str, ...]) -> None:
        if not snapshot or self._ws is None: return
        try:
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"heartbeats"}))
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"market_trades","product_ids":list(snapshot)}))
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"level2","product_ids":list(snapshot)}))
        except Exception:
            pass

    def _send_unsubscribe(self, targets: List[str]) -> None:
        if not targets or self._ws is None: return
        try:
            self._ws.send(orjson.dumps({"type":"unsubscribe","channel":"market_trades","product_ids":targets}))
            self._ws.send(orjson.dumps({"type":"unsubscribe","channel":"level2","product_ids":targets}))
        except Exception:
            pass

    def _recv_loop(self) -> None:
        put_nowait = self.msg_queue.put_nowait
        get_nowait = self.msg_queue.get_nowait
        while self._should_run:
            try:
                self._connect()
                # resubscribe
                with self._pid_lock:
                    snapshot = tuple(sorted(self.product_ids))
                if snapshot: self._send_subscribe(snapshot)

                while self._should_run:
                    try:
                        msg = self._ws.recv()
                    except WebSocketTimeoutException:
                        continue
                    except Exception:
                        break
                    ts_ns = _now_ns()
                    self._total_rate.incr(1, len(msg))
                    try:
                        put_nowait((ts_ns, msg))
                    except queue.Full:
                        # drop oldest to keep tail latency bounded
                        try: get_nowait()
                        except Exception: pass
                        try: put_nowait((ts_ns, msg))
                        except Exception: pass
                    qs = self.msg_queue.qsize()
                    if qs > self.queue_hwm: self.queue_hwm = qs
            except WebSocketConnectionClosedException:
                pass
            except Exception:
                # suppress stack prints in hot path
                pass
            finally:
                try:
                    if self._ws: self._ws.close()
                except Exception:
                    pass
                self._ws = None
                if self._should_run:
                    time.sleep(0.5)  # small backoff

    def _ensure_trade_writer(self, pid: str):
        wr = self.trade_writers.get(pid)
        if wr is None:
            wr = self.platform.create_feed(f"CB-TRADES-{pid}")
            self.trade_writers[pid] = wr
            self._trade_rate[pid] = SlidingRate(10)
            self._lat_trade[pid] = RollingP99(self.sample_size)
        return wr

    def _ensure_l2_writer(self, pid: str):
        wr = self.book_writers.get(pid)
        if wr is None:
            wr = self.platform.create_feed(f"CB-L2-{pid}")
            self.book_writers[pid] = wr
            self._l2_rate[pid] = SlidingRate(10)
            self._lat_l2[pid] = RollingP99(self.sample_size)
        return wr

    def _proc_loop(self) -> None:        
        loads = orjson.loads
        get = self.msg_queue.get
        done = self.msg_queue.task_done
        mv_trade = memoryview(self._buf_trade)
        mv_l2 = memoryview(self._buf_l2)
        while self._should_run or not self.msg_queue.empty():
            try:
                t_in_ns, raw = get(timeout=0.0001)
            except queue.Empty:
                continue

            base_proc = _perf_ns()
            try:
                obj = loads(raw)
            except Exception:
                done(); continue
            
            seq = obj.get("sequence_num")
            if seq is not None:
                if int(seq) != self._last_seq+1:
                    self._seq_gap_trades += 1
                self._last_seq = int(seq)

            ch = obj.get("channel")
            if ch == "market_trades":
                events = obj.get("events")
                for ev in events:
                    trades = ev.get("trades")
                    for tr in trades:
                        pid = tr.get("product_id")
                        wr = self._ensure_trade_writer(pid)
                        ev_ns = parse_ns_timestamp(tr.get("time").encode('ascii'))
                        side  = b'B' if tr.get("side") == "BUY" else b'S'
                        price = _ff(tr.get("price")); size = _ff(tr.get("size"))
                        tid   = int(tr.get("trade_id") or 0)
                        _PACK_TRADE.pack_into(self._buf_trade, 0, b'T', side, tid, ev_ns, t_in_ns, base_proc, price, size)
                        wr.write(ev_ns, mv_trade[:self._rec_trade])

                self._trade_rate[pid].incr(1, len(obj))
                self._lat_trade[pid].add_us(int((_perf_ns() - base_proc)//1000))

            elif ch == "l2_data":
                events = obj.get("events")
                for ev in events:
                    pid = ev.get("product_id")
                    wr = self._ensure_l2_writer(pid)
                    updates = ev.get("updates")
                    n_updates = len(updates)
                    batch = 0
                    while batch < n_updates:
                        u0=0
                        for u in updates[batch:batch+64]:
                            ev_ns = parse_ns_timestamp(u.get("event_time").encode('ascii'))
                            side  = b'B' if (u.get("side") == "bid") else b'A'
                            price = _ff(u.get("price_level")); qty = _ff(u.get("new_quantity"))
                            _PACK_L2.pack_into(self._buf_l2, u0<<6, b'U', side, t_in_ns, ev_ns, base_proc, price, qty)
                            u0+=1
                        # Batch write
                        wr.write(_perf_ns(), mv_l2[:u0<<6])
                        batch += 64

                self._l2_rate[pid].incr(1, len(obj))
                self._lat_l2[pid].add_us(int((_perf_ns() - base_proc)//1000))
            
            # else: heartbeats/acks ignored
            done()