# feeds/websocket_client.py
import threading, socket, time, struct, signal
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
from typing import Dict, Iterable, Optional, Tuple, Any, List
from fastnumbers import fast_float as _ff

import orjson
from simdjson import Parser as _JSONParser
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
log = logging.getLogger("dw.ws")
if not log.handlers:
    log.addHandler(logging.NullHandler())

from deepwater.platform import Platform  # unchanged platform substrate
from deepwater.utils.benchmarking import Metrics
from deepwater.utils.timestamps import parse_ns_timestamp

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
            {"name":"packet_ns",  "type":"uint64", "desc":"time packet was sent (ns)"},
            {"name":"recv_ns",    "type":"uint64", "desc":"time packet was received (ns)"},
            {"name":"proc_ns",    "type":"uint64", "desc":"time packet was ingested (ns)"},
            {"name":"ev_ns",      "type":"uint64", "desc":"event timestamp (ns)"},
            {"name":"price",      "type":"float64","desc":"trade price"},
            {"name":"size",       "type":"float64","desc":"trade size"},
        ],
        "ts_col": "proc_ns",
        "chunk_size_mb": 0.0625,
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
            {"name":"packet_ns",  "type":"uint64", "desc":"time packet was sent (ns)"},
            {"name":"recv_ns",    "type":"uint64", "desc":"time packet was received (ns)"},
            {"name":"proc_ns",    "type":"uint64", "desc":"time packet was ingested (ns)"},
            {"name":"ev_ns",      "type":"uint64", "desc":"event timestamp (ns)"},
            {"name":"price",      "type":"float64","desc":"price level"},
            {"name":"qty",        "type":"float64","desc":"new quantity at level"},
            {"name":"_",          "type":"_8",     "desc":"padding"},
        ],
        "ts_col": "proc_ns",
        "chunk_size_mb": 0.0625,
        "retention_hours": 2,
        "persist": True,
        "index_playback": True
    }

# ======= tiny allocation-aware helpers =======
def _now_ns() -> int: return time.time_ns()
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
    return parse_ns_timestamp(data)

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
        self.platform = Platform(base_path="/deepwater/data/coinbase-test")
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
        self.platform.close()
        self.book_writers.clear()
        self.trade_writers.clear()

    # ---- control ----

    def subscribe(self, product_id: str) -> None:
        if not product_id: return
        product_id = product_id.upper()

        feed_spec = trades_spec(product_id)
        self.platform.create_feed(feed_spec)
        self.trade_writers[product_id] = self.platform.create_writer(feed_spec["feed_name"])

        feed_spec = l2_spec(product_id)
        self.platform.create_feed(feed_spec)
        self.book_writers[product_id] = self.platform.create_writer(feed_spec["feed_name"])

        self.product_ids.add(product_id)
        self._send_subscribe((product_id,))

    def unsubscribe(self, product_id: str) -> None:
        if not product_id: return
        pid = product_id.upper()
        self.product_ids.discard(pid)
        # Close writers to release feed registry locks
        if pid in self.trade_writers:
            try:
                self.trade_writers[pid].close()
            except Exception as e:
                log.warning(f"Error closing trade writer for {pid}: {e}")
            del self.trade_writers[pid]
        if pid in self.book_writers:
            try:
                self.book_writers[pid].close()
            except Exception as e:
                log.warning(f"Error closing book writer for {pid}: {e}")
            del self.book_writers[pid]
        self._send_unsubscribe([pid])

    def list_products(self) -> list[str]:
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
        now_ns = _now_ns
        fast_float = _ff

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
                self._send_subscribe(self.product_ids)

                while self._should_run:
                    try:
                        raw = self._ws.recv()
                        if not raw:
                            raise WebSocketConnectionClosedException("recv returned None/empty")
                        recv_ns = now_ns()
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
                        obj = doc.as_dict()
                    except Exception:
                        msg = "JSON decode error (frame skipped)"
                        if bad_frame_dir is not None:
                            try:
                                path = bad_frame_dir / f"badframe_{now_ns()}.json"
                                data = raw if isinstance(raw, (bytes, bytearray)) else str(raw).encode("utf-8", "replace")
                                path.write_bytes(data)
                                msg = f"JSON decode error (frame saved to {path})"
                            except Exception as ex:
                                log.debug("bad frame dump failed: %s", ex, exc_info=True)
                        log.warning(msg)
                        continue
                    finally:
                        doc = None

                    seq = obj.get("sequence_num")
                    if seq is not None:
                        if int(seq) != self._last_seq+1:
                            self._seq_gap_trades += 1
                        self._last_seq = int(seq)

                    ch = obj.get("channel")
                    if ch == "heartbeats":
                        self._hb_last = time.monotonic()
                        continue

                    if ch == "market_trades":
                        packet_ns = _parse_ts(obj.get("timestamp"))
                        events = obj.get("events") or ()
                        for ev in events:
                            trades = ev.get("trades") or ()
                            if not trades:
                                continue
                            proc_ns = now_ns()
                            written_counts: Dict[str, int] = {}
                            for tr in trades:
                                pid = tr.get("product_id")
                                if not pid:
                                    continue
                                writer = trade_writers.get(pid)
                                if writer is None:
                                    continue
                                write_values = writer.write_values
                                ev_ns = _parse_ts(tr.get("time"))
                                side_val = tr.get("side")
                                side = side_val[0].encode("ascii") if side_val else b'?'
                                price = fast_float(tr.get("price"))
                                size = fast_float(tr.get("size"))
                                tid = int(tr.get("trade_id") or 0)
                                write_values(b'T', side, tid, packet_ns, recv_ns, proc_ns, ev_ns, price, size)
                                written_counts[pid] = written_counts.get(pid, 0) + 1
                            if written_counts:
                                latency_us = max(0, now_ns() - recv_ns) / 1_000.0
                                for pid, count in written_counts.items():
                                    trade_metric(pid, n=count, latency_us=latency_us)

                    elif ch == "l2_data":
                        packet_ns = _parse_ts(obj.get("timestamp"))
                        events = obj.get("events") or ()
                        for ev in events:
                            pid = ev.get("product_id")
                            if not pid:
                                continue
                            writer = book_writers.get(pid)
                            if writer is None:
                                continue
                            updates = ev.get("updates") or ()
                            if not updates:
                                continue
                            l2_type_val = ev.get("type")
                            l2_type = l2_type_val[0].encode('ascii') if l2_type_val else b'U'
                            idx = True if l2_type == b's' else False
                            proc_ns = now_ns()
                            write_values = writer.write_values
                            for u in updates:
                                ev_ns = _parse_ts(u.get("event_time"))
                                side_val = u.get("side")
                                side = side_val[0].encode("ascii") if side_val else b'?'
                                price = fast_float(u.get("price_level"))
                                qty = fast_float(u.get("new_quantity"))
                                write_values(l2_type, side, packet_ns, recv_ns, proc_ns, ev_ns, price, qty, create_index=idx)
                                idx = False
                            count = len(updates)
                            if count:
                                latency_us = max(0, now_ns() - recv_ns) / 1_000.0
                                l2_metric(pid, n=count, latency_us=latency_us)

                    elif ch == "subscriptions":
                        # fine to ignore
                        pass
                    else:
                        # Unknown channel? log & continue (don’t crash)
                        log.debug("unknown channel: %r", ch)

                # should_run flipped false -> exit thread cleanly
                return

            except WebSocketConnectionClosedException as e:
                log.warning("WS closed: %s", e)
            except Exception as e:
                log.error("WS ERROR: %s", e, exc_info=True)
            finally:
                # Clean up socket only; do NOT recursively restart here
                if self._ws is not None:
                    try: self._ws.close()
                    except Exception: pass
                    self._ws = None

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
