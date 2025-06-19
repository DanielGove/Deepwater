from websocket import create_connection, WebSocketTimeoutException, WebSocketConnectionClosedException
import threading
import signal
import orjson
import queue
import time

from .order_book import OrderBookIngestor, TradeIngestor, parse_ns_timestamp

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
        self._recv_thread = None
        self._processing_thread = None
        self.message_queue = queue.Queue()

        """ Data Management """
        self.order_books = dict()
        self.last_sequence_number = None

    def start(self):
        """ Start the Websocket connection """
        if not self._is_running:
            self._should_run = True
            self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
            self._processing_thread = threading.Thread(target=self._process_loop, daemon=True)
            self._recv_thread.start()
            self._processing_thread.start()
            print("▶️  Engine Started")

    def stop(self):
        """ Stop the websocket and finish processing """
        print("⏸️  Engine Stopping...")
        self._should_run = False
        if self._recv_thread and self._recv_thread.is_alive():
            print("⏳ Waiting for receive thread to finish...")
            self._recv_thread.join()
        if self._processing_thread and self._processing_thread.is_alive():
            print("⏳ Waiting for processing thread to finish...")
            self._processing_thread.join()
        print("⏸️  Engine Stopped...")

    def _recv_loop(self):
        """ Listen to the websocket and write messages to queue 
            Handles reconnects and heartbeat.               """
        while self._should_run:
            try:
                print(f"🌐 Connecting to {self.uri}")
                self._ws = create_connection(self.uri, max_size=None)
                self._ws.settimeout(2)  # Set a timeout for the connection ❤️ 
                print("🧩 Connected.")

                self._ws.send(orjson.dumps({
                    "type": "subscribe",
                    "channel": "heartbeats"
                }))
                print("❤️  Established Heartbeat")

                if self.product_ids:
                    self.subscribe()

                while self._should_run:
                    try:
                        message = self._ws.recv()
                        self.message_queue.put(message)
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
        while self._should_run or not self.message_queue.empty():
            try:
                message = self.message_queue.get(timeout=1)
                self.message_queue.task_done()
                data = orjson.loads(message)

                seq_num = data.get("sequence_num")
                if self.last_sequence_number is None:
                    self.last_sequence_number = seq_num
                elif seq_num != self.last_sequence_number + 1:
                    print(f"⚠️ Sequence gap: {self.last_sequence_number} -> {seq_num}")
                self.last_sequence_number = seq_num

                channel = data.get("channel")
                timestamp = data.get("timestamp")
                events = data.get("events")
                
                for event in events:
                    if channel == "l2_data":
                        product_id = event["product_id"]
                        if product_id not in self.order_books:
                            self.order_books[product_id] = OrderBookIngestor(product_id)
                        self.order_books[product_id](timestamp, event)

                    elif channel == "market_trades":
                        trades = event["trades"]
                        for trade in trades:
                            product_id = trade["product_id"]
                            if product_id not in self.order_books:
                                self.order_books[product_id] = TradeIngestor(product_id)
                            self.order_books[product_id](timestamp, trade)
                            print((time.time_ns()-parse_ns_timestamp(timestamp))/1000000)

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
        queue_size = self.message_queue.qsize()

        print(
            f"\n🧠 Engine Status:\n"
            f"   • ⚙️  State: {run}\n"
            f"   • 🌐 Connected: {connected}\n"
            f"   • 📦 Products: [{products}]\n"
            f"   • 📡 Channels: [{channels}]\n"
            f"   • 📬 Queue Size: {queue_size}\n"
        )