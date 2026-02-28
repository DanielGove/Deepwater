# feeds/websocket_client.py
import threading, socket, time, struct, signal
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
from typing import Dict, Iterable, Optional, Tuple, Any, List
from fastnumbers import fast_float as _ff, fast_int as _fi

from simdjson import Parser as _JSONParser
import orjson
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.ws")
if not log.handlers:
    log.addHandler(logging.NullHandler())

from deepwater.platform import Platform  # unchanged platform substrate
from deepwater.utils.benchmarking import Metrics
from deepwater.utils.timestamps import parse_us_timestamp

# ======= feed configuration =======
def trades_spec(pid: str) -> dict:
    return {
        "feed_name": f"CB-TRADES-{pid}",
        "mode": "UF",
        "fields": [
            {"name":"type",       "type":"char",   "desc":"record type 'T'"},
            {"name":"side",       "type":"char",   "desc":"B=buy,S=sell"},
            {"name":"_",          "type":"_6",    "desc":"padding"},
            {"name":"trade_id",   "type":"uint64", "desc":"exchange trade id"},
            {"name":"packet_us",  "type":"uint64", "desc":"time packet was sent (us)"},
        {"name":"recv_us",    "type":"uint64", "desc":"time packet was received (us)"},
        {"name":"proc_us",    "type":"uint64", "desc":"time packet was ingested (us)"},
        {"name":"ev_us",      "type":"uint64", "desc":"event timestamp (us)"},
        {"name":"price",      "type":"float64","desc":"trade price"},
        {"name":"size",       "type":"float64","desc":"trade size"},
    ],
    "clock_level": 3,  # three leading timestamp axes
    "chunk_size_bytes": 0.0625 * 1024 * 1024,
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
            {"name":"packet_us",  "type":"uint64", "desc":"time packet was sent (us)"},
        {"name":"recv_us",    "type":"uint64", "desc":"time packet was received (us)"},
        {"name":"proc_us",    "type":"uint64", "desc":"time packet was ingested (us)"},
        {"name":"ev_us",      "type":"uint64", "desc":"event timestamp (us)"},
        {"name":"price",      "type":"float64","desc":"price level"},
        {"name":"qty",        "type":"float64","desc":"new quantity at level"},
        {"name":"_",          "type":"_8",     "desc":"padding"},
    ],
    "clock_level": 3,
    "chunk_size_bytes": 0.0625 * 1024 * 1024,
    "persist": True,
    "index_playback": True
}

# ======= tiny allocation-aware helpers =======
def _now_us() -> int: return time.time_ns() // 1_000
def _perf_ns() -> int: return time.perf_counter_ns()

def _backoff():
    b = 0.5
    while True:
        yield b
        b = min(b*2.0, 15.0)

def _ensure_bytes(val) -> Optional[bytes]:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray, memoryview)):
        return bytes(val)
    return str(val).encode("ascii")

def _parse_ts(val) -> int:
    data = _ensure_bytes(val)
    if not data:
        return 0
    return parse_us_timestamp(data)

def _parse_frame(raw):
    parser = _JSONParser()
    return parser.parse(raw).as_dict()

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
        self.product_ids: set[str] = set()

        # single IO thread
        self.io_thread: Optional[threading.Thread] = None

        # platform
        self.platform = Platform(base_path="data/coinbase-test")
        self.trade_writers: Dict[str, Any] = {}
        self.book_writers:  Dict[str, Any] = {}

        # metrics
        self._seq_gap_trades = 0
        self._last_seq: int = -1
        self._metrics = Metrics()
        self._parser = _JSONParser()

        # Control flow
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Dead socket detection
        now = time.monotonic()
        self._hb_last = now
        self._msg_last = now
        self._hb_timeout = 12.0     # seconds with no heartbeats -> reconnect
        self._msg_timeout = 12.0    # seconds with no messages  -> reconnect

    # ---- lifecycle ----

    def start(self) -> None:
        if self._should_run: return
        self._should_run = True
        self.io_thread = threading.Thread(target=self._io_loop, name="ws-io", daemon=True)
        self.io_thread.start()
        for pid in self.product_ids:
            self.subscribe(pid)

    def stop(self) -> None:
        self._should_run = False
        if self._ws is not None:
            try: self._ws.close()
            except Exception as e: log.warning("WS close error: %s", e)
            self._ws = None
        if self.io_thread and self.io_thread.is_alive():
            self.io_thread.join(timeout=2.0)
        self._close_all_writers()
        self.platform.close()
        self.book_writers.clear()
        self.trade_writers.clear()

    # ---- control ----

    def subscribe(self, product_id: str) -> None:
        if not product_id: return
        product_id = product_id.upper()
        self._open_writers_for_product(product_id)

        self.product_ids.add(product_id)
        self._send_subscribe((product_id,))

    def unsubscribe(self, product_id: str) -> None:
        if not product_id: return
        pid = product_id.upper()
        self.product_ids.discard(pid)
        self._close_writers_for_product(pid)
        self._send_unsubscribe([pid])

    def list_products(self) -> list[str]:
        return sorted(self.product_ids)

    def is_connected(self) -> bool:
        return self._ws is not None

    def _trade_feed_name(self, product_id: str) -> str:
        return f"CB-TRADES-{product_id}"

    def _book_feed_name(self, product_id: str) -> str:
        return f"CB-L2-{product_id}"

    def _open_writers_for_product(self, product_id: str) -> None:
        """Ensure feed specs exist and writers are open for one product."""
        pid = product_id.upper()
        trade_spec = trades_spec(pid)
        self.platform.create_feed(trade_spec)
        self.trade_writers[pid] = self.platform.create_writer(trade_spec["feed_name"])

        book_spec = l2_spec(pid)
        self.platform.create_feed(book_spec)
        self.book_writers[pid] = self.platform.create_writer(book_spec["feed_name"])

    def _ensure_writers_for_subscriptions(self) -> None:
        """Re-open writers for current subscriptions (used on reconnect)."""
        for pid in tuple(self.product_ids):
            if pid not in self.trade_writers or pid not in self.book_writers:
                self._open_writers_for_product(pid)

    def _close_writers_for_product(self, product_id: str) -> None:
        """Close product writers via Platform cache-aware API."""
        pid = product_id.upper()
        try:
            self.platform.close_writer(self._trade_feed_name(pid))
        except Exception as e:
            log.warning("Error closing trade writer for %s: %s", pid, e)
        self.trade_writers.pop(pid, None)

        try:
            self.platform.close_writer(self._book_feed_name(pid))
        except Exception as e:
            log.warning("Error closing book writer for %s: %s", pid, e)
        self.book_writers.pop(pid, None)

    def _close_all_writers(self) -> None:
        """Close all active writers (finalizes segments for current session)."""
        pids = set(self.product_ids)
        pids.update(self.trade_writers.keys())
        pids.update(self.book_writers.keys())
        for pid in tuple(pids):
            self._close_writers_for_product(pid)
    
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

    def _send_subscribe(self, product_ids) -> None:
        if self._ws is None: return
        try:
            pids = tuple(product_ids or ())
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"heartbeats"}))
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"market_trades","product_ids":pids}))
            self._ws.send(orjson.dumps({"type":"subscribe","channel":"level2","product_ids":pids}))
        except Exception as e:
            log.warning("subscribe error: %s", e, exc_info=True)

    def _send_unsubscribe(self, targets) -> None:
        if self._ws is None: return
        try:
            pids = tuple(targets or ())
            self._ws.send(orjson.dumps({"type":"unsubscribe","channel":"market_trades","product_ids":pids}))
            self._ws.send(orjson.dumps({"type":"unsubscribe","channel":"level2","product_ids":pids}))
        except Exception as e:
            log.warning("unsubscribe error: %s", e, exc_info=True)

    # ---- unified IO loop: recv + process ----

    def _io_loop(self) -> None:
        parse = self._parser.parse
        now_us = _now_us
        fast_float = _ff
        fast_int = _fi

        metrics = self._metrics
        ingress = metrics.ingress
        trade_metric = metrics.trade
        l2_metric = metrics.l2
        trade_writers = self.trade_writers
        book_writers = self.book_writers
        bad_frame_dir = self.platform.base_path / "bad_frames"
        try:
            bad_frame_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            bad_frame_dir = None

        for delay in _backoff():
            if not self._should_run:
                return
            try:
                self._connect()
                log.info("WS connected %s", self.uri)
                # Writers are session-scoped for segmentation; reopen on each connect.
                self._ensure_writers_for_subscriptions()
                self._send_subscribe(self.product_ids)

                while self._should_run:
                    try:
                        raw = self._ws.recv()
                        if not raw:
                            raise WebSocketConnectionClosedException("recv returned None/empty")
                        recv_us = now_us()
                        ingress(len(raw), 1)
                        self._msg_last = time.monotonic()
                    except WebSocketTimeoutException:
                        # treat as liveness issue; fall through to timeout checks below
                        raw = None
                    except WebSocketConnectionClosedException:
                        raise
                    except Exception as e:
                        raise WebSocketConnectionClosedException(f"WS recv error: {e!s}")

                    if raw is None:
                        continue

                    try:
                        doc = parse(raw)
                    except Exception as e:
                        msg = f"JSON decode error: {e!s}"
                        if bad_frame_dir is not None:
                            try:
                                path = bad_frame_dir / f"badframe_{now_us()}.json"
                                data = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8", "replace")
                                path.write_bytes(data)
                                msg = f"JSON decode error (frame saved to {path})"
                            except Exception as ex:
                                log.debug("bad frame dump failed: %s", ex, exc_info=True)
                        log.warning(msg)
                        continue

                    if doc is None:
                        log.error("WS received null document: %r", doc)
                        continue

                    if doc.get("type") == "error":
                        log.error("WS error message: %r", doc)
                        del doc
                        continue

                    if doc["sequence_num"] != self._last_seq + 1:
                        self._seq_gap_trades += 1
                    self._last_seq = _fi(doc["sequence_num"])

                    if doc["channel"] == "heartbeats":
                        self._hb_last = time.monotonic()
                        del doc
                        continue

                    if doc["channel"] == "market_trades":
                        packet_us = _parse_ts(doc.get("timestamp"))
                        for ev in doc["events"]:
                            proc_us = now_us()
                            counts: Dict[str, int] = {}
                            # Reverse trades to get chronological order (oldest first)
                            for tr in reversed(ev["trades"]):
                                writer = trade_writers.get(tr["product_id"])
                                writer.write_values(b'T',
                                                    tr["side"][0].encode("ascii"),
                                                    fast_int(tr["trade_id"]),
                                                    packet_us, recv_us, proc_us,
                                                    _parse_ts(tr["time"]),
                                                    fast_float(tr["price"]),
                                                    fast_float(tr["size"]))
                                counts[tr["product_id"]] = counts.get(tr["product_id"], 0) + 1
                            if counts:
                                latency_us = max(0, now_us() - recv_us)
                                for pid, count in counts.items():
                                    trade_metric(pid, n=count, latency_us=latency_us)
                        del ev; del tr

                    elif doc["channel"] == "l2_data":
                        packet_us = _parse_ts(doc.get("timestamp"))
                        for ev in doc["events"]:
                            writer = book_writers.get(ev["product_id"])
                            if writer is None:
                                log.warning("no writer for product %s", ev.get("product_id"))
                                continue
                            l2_type = ev["type"][0].encode('ascii')
                            idx = True if l2_type == b's' else False
                            proc_us = now_us()

                            # Reverse updates to get chronological order (oldest first)
                            for u in reversed(ev["updates"]):
                                writer.write_values(
                                    l2_type,
                                    u["side"][0].encode("ascii"),
                                    packet_us, recv_us, proc_us,
                                    _parse_ts(u["event_time"]),
                                    fast_float(u["price_level"]),
                                    fast_float(u["new_quantity"]),
                                    create_index=idx)
                                idx = False
                            if ev["updates"]:
                                latency_us = max(0, now_us() - recv_us)
                                l2_metric(pid, n=len(ev["updates"]), latency_us=latency_us)
                        del ev; del u

                    elif doc["channel"] == "subscriptions":
                        pass
                    else:
                        log.debug("unknown channel: %r", doc["channel"])
                    
                    del doc
                    
                # should_run flipped false -> exit thread cleanly
                return

            except WebSocketConnectionClosedException as e:
                log.warning("WS closed: %s", e)
            except Exception as e:
                log.error("WS ERROR: %s", e, exc_info=True)
                del doc
            finally:
                # Clean up socket only; do NOT recursively restart here
                if self._ws is not None:
                    try: self._ws.close()
                    except Exception: pass
                    self._ws = None
                # Segment boundary should follow socket session boundaries.
                self._close_all_writers()

            if not self._should_run:
                return
            log.info("Reconnecting in %.2fs", delay)
            time.sleep(delay)

    def _handle_signal(self, signum, _frame):
        try:
            self.stop()
        finally:
            raise SystemExit(0)
        
    def metrics_snapshot(self) -> dict: return self._metrics.snapshot()
    def metrics_reset(self) -> None: self._metrics.reset()

    def status_snapshot(self) -> dict:
        now = time.monotonic()
        return {
            "running": self._should_run,
            "connected": self._ws is not None,
            "subs": sorted(self.product_ids),
            "seq_gap_trades": self._seq_gap_trades,
            "hb_age": max(0.0, now - self._hb_last) if self._hb_last else None,
            "msg_age": max(0.0, now - self._msg_last) if self._msg_last else None,
        }
