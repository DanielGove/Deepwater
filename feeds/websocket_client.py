import threading
import orjson
import queue
import time
import struct
from io import BytesIO
from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException

from core.platform import Platform
from calendar import timegm

def parse_ns_timestamp(ts: str) -> int:
    seconds = timegm((
        int(ts[0:4]), int(ts[5:7]), int(ts[8:10]),
        int(ts[11:13]), int(ts[14:16]), int(ts[17:19])
    ))
    nanos = int(ts[20:-1].ljust(9, '0'))
    return seconds * 1_000_000_000 + nanos

class MarketDataEngine:
    def __init__(self):
        """ Basic Configuration """
        self.uri = "wss://advanced-trade-ws.coinbase.com"
        self.product_ids = set()
        self.channels = ["market_trades", "level2"]

        """ Concurrency Handling """
        self._is_running = False
        self._should_run = False
        self._ws = None
        self.recv_thread = None
        self.proc_thread = None
        self.msg_queue = queue.Queue()

        """ Data Management """
        self.last_seq_num = None
        self.platform = Platform()
        self.trade_writers = {}
        self.book_writers = {}

    def start(self):
        """ Start the Websocket connection """
        if not self._is_running:
            self._should_run = True
            self.recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self.proc_thread = threading.Thread(target=self._process_loop, daemon=True)
            self.recv_thread.start()
            self.proc_thread.start()
            self._is_running = True
            print("▶️  Engine Started")

    def stop(self):
        """ Stop the websocket and finish processing """
        self._should_run = False
        if self.recv_thread and self._recv_thread.is_alive():
            print("⏳ Waiting for receive thread to finish...")
            self.recv_thread.join()
        if self.proc_thread and self._processing_thread.is_alive():
            print("⏳ Waiting for processing thread to finish...")
            self.proc_thread.join()
        self._is_running = False
        print("⏸️  Engine Stopped")

    def _recv_loop(self):
        """ Listen to the websocket and write messages to queue 
            Handles reconnects and heartbeat.               """
        while self._should_run:
            try:
                print(f"🌐 Connecting to {self.uri}")
                self._ws = create_connection(self.uri, max_size=None)
                self._ws.settimeout(2)  # Set a timeout for the connection ❤️
                self._ws.send(orjson.dumps({"type": "subscribe", "channel": "heartbeats"}))
                print("❤️  Established Heartbeat")

                if self.product_ids:
                    self.subscribe()

                while self._should_run:
                    try:
                        message = self._ws.recv()
                        self.msg_queue.put(message)
                    except WebSocketTimeoutException:
                        print("⏳ No message received, waiting...")
            
            except WebSocketConnectionClosedException as e:
                print(f"🔌 Disconnected: {e}")
            except Exception as e:
                print(f"⚠️ WebSocket error: {e}")
            finally:
                self.sequence_num = None
                if self._ws:
                    self._ws.close()
                    self._ws = None
                    print("🔌 WebSocket closed.")
                if self._should_run:
                    print("🔁 Reconnecting in 5 seconds...")
                    time.sleep(5)

    def _process_loop(self):
        """ Process messages from the queue """
        while self._should_run or not self.msg_queue.empty():
            try:
                message = self.msg_queue.get(timeout=1)
                self.msg_queue.task_done()
                data = orjson.loads(message)

                seq_num = data.get("sequence_num")
                if self.last_seq_num is None:
                    self.last_seq_num = seq_num
                elif seq_num != self.last_seq_num + 1:
                    print(f"⚠️ Sequence gap: {self.last_seq_num} -> {seq_num}")
                self.last_seq_num = seq_num

                channel = data.get("channel")
                timestamp = parse_ns_timestamp(data.get("timestamp"))
                events = data.get("events")
                
                for event in events:
                    if channel == "l2_data":
                        pid = event["product_id"]
                        if pid not in self.book_writers:
                            self.book_writers[pid] = self.platform.create_feed(f"CB-L2-{pid}")
                        for update in event.get("updates", []):
                            ev_ns = parse_ns_timestamp(update.get("event_time"))
                            side = b'B' if update.get("side") == "bid" else b'A'
                            price = float(update["price_level"])
                            qty = float(update["new_quantity"])
                            packed = struct.pack("<cc6xQQQdd16x", b'U', side, timestamp, ev_ns, time.time_ns(), price, qty)
                            self.book_writers[pid].write(ev_ns, packed)
                    elif channel == "market_trades":
                        trades = event["trades"]
                        for trade in trades:
                            pid = trade["product_id"]
                            if pid not in self.trade_writers:
                                self.trade_writers[pid] = self.platform.create_feed(f"CB-TRADES-{pid}")
                            ev_ns = parse_ns_timestamp(trade["time"])
                            trade_id = int(trade["trade_id"])
                            side = b'B' if trade["side"] == "BUY" else b'S'
                            price = float(trade["price"])
                            size = float(trade["size"])
                            packed = struct.pack("<cc6xQQQQdd8x", b'T', side, trade_id, ev_ns, timestamp, time.time_ns(), price, size)
                            self.trade_writers[pid].write(ev_ns, packed)

            except queue.Empty:
                continue  # No messages to process, continue waiting
            except Exception as e:
                print(f"🚫 Processing error: {e}")

    def subscribe(self, product_id=None):
        """ Start receiving data for a product """
        if product_id:
            self.product_ids.add(product_id)
        if self._ws is not None:
            targets = [product_id] if product_id else list(self.product_ids)
            for channel in self.channels:
                message = {
                    "type": "subscribe",
                    "product_ids": targets,
                    "channel": channel
                }
                self._ws_send_message(message)

    def unsubscribe(self, product_id=None):
        """ Stop receiving data for a product """
        if product_id:
            self.product_ids.discard(product_id)
        if self._ws is not None:
            targets = [product_id] if product_id else list(self.product_ids)
            for channel in self.channels:
                message = {
                    "type": "unsubscribe",
                    "product_ids": targets,
                    "channel": channel
                }
                self._ws_send_message(message)

    def _ws_send_message(self, message):
        """ Send a message through the socket """
        if self._ws is not None:
            try:
                self._ws.send(orjson.dumps(message))
            except Exception as e:
                print(f"🚫 Error sending message: {e}")
        else:
            print("🚫 WebSocket is not connected.")

    def is_connected(self):
        return self._ws is not None

    def status(self):
        run = "▶️ Running" if self._should_run else "⏸️ Stopped"
        connected = "🟢 Yes" if self.is_connected() else "🔴 No"
        products = ", ".join(self.product_ids) or "🚫 None"
        channels = ", ".join(self.channels) or "🚫 None"
        queue_size = self.msg_queue.qsize()

        print(
            f"\n🧠 Engine Status:\n"
            f"   • ⚙️  State: {run}\n"
            f"   • 🌐 Connected: {connected}\n"
            f"   • 📦 Products: [{products}]\n"
            f"   • 📡 Channels: [{channels}]\n"
            f"   • 📬 Queue Size: {queue_size}\n"
        )