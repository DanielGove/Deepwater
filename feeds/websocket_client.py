# feeds/websocket_client.py
import threading, socket, orjson, time, struct, signal
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
from typing import Dict, Iterable, Optional, Tuple, Any, List

import numba as _nb
from fastnumbers import fast_float as _ff

from core.platform import Platform  # unchanged platform substrate
from utils.benchmarking import Metrics

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

# ======= record formats (unchanged) =======
_PACK_TRADE = struct.Struct("<cc14xQQdd16x")
_PACK_L2    = struct.Struct("<cc6xQdd")

def trades_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name":"type",       "type":"char",   "desc":"record type 'T'"},
            {"name":"side",       "type":"char",   "desc":"B=buy,S=sell"},
            {"name":"_",          "type":"_14",    "desc":"padding"},
            {"name":"trade_id",   "type":"uint64", "desc":"exchange trade id"},
            {"name":"ev_ns",      "type":"uint64", "desc":"event timestamp (ns)"},
            {"name":"price",      "type":"float64","desc":"trade price"},
            {"name":"size",       "type":"float64","desc":"trade size"},
            {"name":"_",          "type":"_16",     "desc":"padding"},
        ],
        "ts_col": "ev_ns",
        "chunk_size_mb": 16,
        "retention_hours": 2,
        "persist": True
    }

def l2_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-L2-{pid}",
        "mode": "UF",
        "fields": [
            {"name":"type",       "type":"char",   "desc":"record type 'U'"},
            {"name":"side",       "type":"char",   "desc":"B=bid,A=ask"},
            {"name":"_",          "type":"_6",     "desc":"padding"},
            {"name":"ev_ns",      "type":"uint64", "desc":"event timestamp (ns)"},
            {"name":"price",      "type":"float64","desc":"price level"},
            {"name":"qty",        "type":"float64","desc":"new quantity at level"},
        ],
        "ts_col": "ev_ns",
        "chunk_size_mb": 16,
        "retention_hours": 2,
        "persist": True,
        "index_playback": True
    }

# ======= Engine =======

class MarketDataEngine:
    """
    Blazing-fast WS ingest with single-thread recv+process.
    - Platform writers are UNCHANGED (data substrate stays the same).
    - Channels: 'market_trades' and 'level2' (advanced trade WS).
    """
    def __init__(self, uri: str = "wss://advanced-trade-ws.coinbase.com",
                 sample_size: int = 16) -> None:
        # config
        self.uri = uri
        self.channels = ["market_trades", "level2"]
        self.sample_size = int(sample_size)

        # Engine State
        self._should_run = False
        self._ws = None
        self._pid_lock = threading.Lock()
        self.product_ids: set[str] = set()

        # single IO thread
        self.io_thread: Optional[threading.Thread] = None

        # platform
        self.platform = Platform(base_path="./platform_data")
        self.trade_writers: Dict[str, Any] = {}
        self.book_writers:  Dict[str, Any] = {}

        # data buffers (minimize alloc)
        self._rec_trade = _PACK_TRADE.size
        self._rec_l2 = _PACK_L2.size
        self._buf_size_trade = 64
        self._buf_size_l2 = 512
        self._buf_trade = bytearray(self._rec_trade*self._buf_size_trade)
        self._buf_l2 = bytearray(self._rec_l2*self._buf_size_l2)

        # metrics
        self._seq_gap_trades = 0
        self._last_seq: int = -1
        self._metrics = Metrics()

        # Control flow
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    # ---- lifecycle ----

    def start(self) -> None:
        if self._should_run: return
        self._should_run = True
        self.io_thread = threading.Thread(target=self._io_loop, name="ws-io", daemon=True)
        self.io_thread.start()

    def stop(self) -> None:
        self._should_run = False
        if self.io_thread and self.io_thread.is_alive():
            self.io_thread.join()
        if self._ws is not None:
            try: self._ws.close()
            except Exception: pass
        self.platform.close()
        self.book_writers = dict()
        self.trade_writers = dict()

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
                (socket.SOL_SOCKET,   socket.SO_RCVBUF, 1<<21), # 2 MB Buffer
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

    def _ensure_trade_writer(self, pid: str):
        wr = self.trade_writers.get(pid)
        if wr is None:
            feed_spec = trades_spec(pid)
            self.platform.create_feed(feed_spec)
            self.trade_writers[pid] = self.platform.create_writer(feed_spec["feed_name"])
            wr = self.trade_writers[pid]
        return wr

    def _ensure_l2_writer(self, pid: str):
        wr = self.book_writers.get(pid)
        if wr is None:
            feed_spec = l2_spec(pid)
            wr = self.platform.create_feed(feed_spec)
            self.book_writers[pid] = self.platform.create_writer(feed_spec["feed_name"])
            wr = self.book_writers[pid]
        return wr

    # ---- unified IO loop: recv + process ----

    def _io_loop(self) -> None:
        loads = orjson.loads
        mv_trade = memoryview(self._buf_trade)
        mv_l2 = memoryview(self._buf_l2)

        while self._should_run:
            try:
                self._connect()
                # resubscribe
                with self._pid_lock:
                    snapshot = tuple(sorted(self.product_ids))
                if snapshot: self._send_subscribe(snapshot)

                # main recv→process loop
                while self._should_run:
                    try:
                        raw = self._ws.recv()
                    except WebSocketTimeoutException:
                        continue
                    except Exception:
                        break

                    t_in_ns = _perf_ns()
                    self._metrics.ingress(len(raw), 1)

                    try:
                        obj = loads(raw)
                    except Exception:
                        continue

                    # sequence gaps (unchanged)
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
                                side  = tr.get("side")[0].encode('ascii') # 'B' or 'S'
                                price = _ff(tr.get("price")); size = _ff(tr.get("size"))
                                tid   = int(tr.get("trade_id") or 0)
                                #_PACK_TRADE.pack_into(self._buf_trade, 0, b'T', side, tid, ev_ns, price, size)
                                wr.write_values(b'T',side,tid,ev_ns,price,size)
                                self._metrics.trade(pid, n=1, latency_us=_perf_ns()-t_in_ns)

                    elif ch == "l2_data":
                        continue
                        events = obj.get("events")
                        for ev in events:
                            pid = ev.get("product_id")
                            l2_type = ev.get("type")[0].encode('ascii')  # 'U' or 'S'
                            wr = self._ensure_l2_writer(pid)
                            updates = ev.get("updates")
                            n_updates = len(updates)
                            batch = 0
                            while batch < n_updates:
                                u0=0
                                for u in updates[batch:batch+self._buf_size_l2]:
                                    ev_ns = parse_ns_timestamp(u.get("event_time").encode('ascii'))
                                    side  = u.get("side")[0].encode('ascii')  # 'B' or 'A'
                                    price = _ff(u.get("price_level")); qty = _ff(u.get("new_quantity"))
                                    _PACK_L2.pack_into(self._buf_l2, u0*self._rec_l2, l2_type, side, ev_ns, price, qty)
                                    u0+=1
                                # Batch write
                                wr.write(t_in_ns, mv_l2[:u0*self._rec_l2])
                                batch += self._buf_size_l2
                            self._metrics.l2(pid, n=n_updates, latency_us=_perf_ns()-t_in_ns)

                    elif ch == "subscriptions":
                        pass
                    elif ch == "heartbeats":
                        pass
                    else:
                        # Unknown channel; keep loop going
                        raise Exception("Unknown Channel:", ch)

            except WebSocketConnectionClosedException:
                pass
            except Exception as e:
                raise Exception(e)
            finally:
                if self._should_run:
                    try:
                        if self._ws is not None:
                            self._ws.close()
                    except Exception:
                        pass
                    time.sleep(0.5)  # small backoff

    def _handle_signal(self, signum, _frame):
        try:
            self.stop()
        finally:
            raise SystemExit(0)
        
    def metrics_snapshot(self) -> dict: return self._metrics.snapshot()
    def metrics_reset(self) -> None: self._metrics.reset()