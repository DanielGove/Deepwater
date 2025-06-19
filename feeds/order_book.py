import struct
from io import BytesIO
from bisect import bisect_left, insort
from typing import List, Tuple
import signal
import time

from core.platform import Platform

## Fast parsing
from calendar import timegm
def parse_ns_timestamp(ts: str) -> int:
    # Extract components by fixed positions (assuming correct format)
    seconds = timegm((
        int(ts[0:4]),   # year
        int(ts[5:7]),   # month
        int(ts[8:10]),  # day
        int(ts[11:13]), # hour
        int(ts[14:16]), # minute
        int(ts[17:19])  # second
    ))

    # Get nanoseconds: up to 9 digits after the dot
    nanos = int(ts[20:-1].ljust(9, '0'))  # No . or Z included here

    return seconds * 1_000_000_000 + nanos

class OrderBookIngestor:
    def __init__(self, product_id):
        self.product_id = product_id
        self.snapshot_interval = 3000
        self.update_count = 0
        self.last_snapshot_time = None

        self.bids = {}         # price -> size
        self.asks = {}         # price -> size
        self.bid_prices = []   # Sorted descending via negative values
        self.ask_prices = []   # Sorted ascending

        self.platform = Platform()
        self.writer = self.platform.create_feed("CB-L2-" + product_id)
        
    def __call__(self, sent_time, event):
        try:
            event_type = event.get("type")
            updates = event.get("updates", [])

            # Handle initial snapshot from websocket
            if event_type == "snapshot":
                self._process_initial_snapshot(sent_time, updates)
                return

            # Process regular updates
            for update in updates:
                event_time = parse_ns_timestamp(update.get("event_time"))

                # Apply update to live book
                self._apply_update_to_book(update)

                # Write the delta to platform
                update_bytes = self.encode_update(sent_time, update)
                self.writer(sent_time, update_bytes)
                self.update_count += 1

                # Check if we need to create a snapshot
                if self._should_create_snapshot():
                    self.writer(sent_time, self.encode_snapshot(sent_time), force_index=True)
                    self.update_count = 0
                    self.last_snapshot_time = sent_time

        except Exception as e:
            print(f"🚫 Order Book Error: {e}")

    def _process_initial_snapshot(self, sent_time, updates):
        """Process the initial snapshot from websocket"""
        print(f"📦 Processing initial snapshot for {self.product_id}")

        # Clear existing state
        self.bids.clear()
        self.asks.clear()
        self.bid_prices.clear()
        self.ask_prices.clear()

        # Apply all snapshot updates
        for update in updates:
            self._apply_update_to_book(update)
        
        # Write snapshot to platform
        sent_time = parse_ns_timestamp(sent_time)
        self.writer(sent_time, self.encode_snapshot(sent_time), force_index=True)
        self.last_snapshot_time = sent_time
        self.update_count = 0

    def _apply_update_to_book(self, update):
        """Apply a single update to the live order book"""
        side = update["side"]
        price = float(update["price_level"])
        size = float(update["new_quantity"])

        book = self.bids if side == 'bid' else self.asks
        price_list = self.bid_prices if side == 'bid' else self.ask_prices
        pkey = -price if side == 'bid' else price

        if size == 0.0:
            # Remove level
            if price in book:
                del book[price]
                idx = bisect_left(price_list, pkey)
                if 0 <= idx < len(price_list) and price_list[idx] == pkey:
                    price_list.pop(idx)
        else:
            # Add/Update level
            if price not in book:
                insort(price_list, pkey)
            book[price] = size

    def _should_create_snapshot(self):
        """Check if we should create a new snapshot"""
        return self.update_count >= self.snapshot_interval

    def encode_snapshot(self, timestamp):
        buffer = BytesIO()
        buffer.write(struct.pack("<cQH",b'S',timestamp,len(self.product_id)))
        buffer.write(self.product_id.encode('utf-8'))

        # ⬇️ Align header to next 32-byte boundary
        current = buffer.tell()
        pad = (32 - (current % 32)) % 32
        buffer.write(b'\x00' * pad)

        buffer.write(struct.pack("<II",len(self.bid_prices),len(self.ask_prices)))

        for price in self.bid_prices:
            size = self.bids.get(price,0.0)
            buffer.write(struct.pack("<dd",price,size))
        for price in self.ask_prices:
            size = self.asks.get(price,0.0)
            buffer.write(struct.pack("<dd",price,size))

        # ⬇️ Align snapshot to next 32-byte boundary
        # Aligned to 16 up to last level. So VVV returns 0 or 16
        padding_needed = buffer.tell() % 32
        buffer.write(b'\x00' * padding_needed)

        return buffer.getvalue()
    
    def encode_update(self, sent_time, update):
        buffer = BytesIO()     #b'U' for "Update" vs "Snapshot"

        # b'U', side, 6x, event, sent, written, price, quantity, 16x
        b_struct = struct.Struct("<cc6xQQQdd16x") # 64 bytes
        sent_time = parse_ns_timestamp(sent_time)
        event_time = parse_ns_timestamp(update.get("event_time"))
        side = b'B' if update.get("side") == 'bid' else b'A'
        price = float(update.get("price_level"))
        quantity = float(update.get("new_quantity"))

        buffer.write(b_struct.pack(b'U', side, 
                                   sent_time, event_time,
                                   time.time_ns(),
                                   price, quantity))
        return buffer.getvalue()

    def get_current_snapshot_info(self):
        """Get info about current orderbook state"""
        return {
            "product_id": self.product_id,
            "bid_levels": len(self.bid_prices),
            "ask_levels": len(self.ask_prices),
            "updates_since_snapshot": self.update_count,
            "last_snapshot_time": self.last_snapshot_time,
            "top_bid": -self.bid_prices[0] if self.bid_prices else None,
            "top_ask": self.ask_prices[0] if self.ask_prices else None
        }

    def get_snapshot(self) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        depth_limit = getattr(self, 'depth_limit', 100)  # Default depth
        bids = [(-p, self.bids[-p]) for p in self.bid_prices[:depth_limit]]
        asks = [(p, self.asks[p]) for p in self.ask_prices[:depth_limit]]
        return bids, asks

class TradeIngestor:
    
    def __init__(self, product_id):
        self.product_id = product_id
        self.num_messages = 0
        self.index_every_n = 5000

        self.platform = Platform()
        self.writer = self.platform.create_feed("CB-TRADES-" + product_id)
    
    def __call__(self, sent_time, trade):
        try:
            sent_time = parse_ns_timestamp(sent_time)
            trade_bytes = self.encode_trade(sent_time, trade)
            force_index = (self.num_messages % self.index_every_n == 0)
            self.writer(sent_time, trade_bytes, force_index=force_index)
        except Exception as e:
            print(f"🚫 TradeIngestor error: {e}")

    def encode_trade(self, sent_time: int, trade_event: dict) -> bytes:
        buffer = BytesIO()

        # Create the struct: 64 bytes total
        # Layout: T, Side, Padding(6), Trade ID, Event, Sent, Written, Price, Size, Padding(16)
        trade_struct = struct.Struct("<cc6xQQQQdd8x")

        event_time = parse_ns_timestamp(trade_event["time"])
        trade_id = int(trade_event["trade_id"])
        side = b'B' if trade_event["side"] == "BUY" else b'S'
        price = float(trade_event["price"])
        size = float(trade_event["size"])

        buffer.write(trade_struct.pack(b'T', side, trade_id,
                                       event_time, sent_time,
                                       time.time_ns(),
                                       price, size))
        return buffer.getvalue()