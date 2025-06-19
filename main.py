#!/usr/bin/env python3
"""
Feed Control Loop - Test/Debug Interface

Simple control interface to run your market data feeds for testing.
Doesn't alter your existing objects - just provides a control interface.
"""

import time
import threading
import signal
import sys
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

from feeds.websocket_client import MarketDataEngine
from feeds.order_book import OrderBookIngestor, TradeIngestor

class FeedController:
    """
    Control interface for your market data feeds
    """
    
    def __init__(self):
        self.engine = MarketDataEngine()
        self.running = False
        self.monitoring_thread = None
        self.start_time = None
        
        # Statistics tracking
        self.stats = {
            'total_messages': 0,
            'l2_updates': 0,
            'trades': 0,
            'products': set(),
            'errors': 0,
            'uptime': 0
        }
        
        print("🎛️  Feed Controller initialized")
        print("📊 Your feeds will write to:")
        print("   • OrderBook: CB-L2-{product_id}")
        print("   • Trades: CB-TRADES-{product_id}")
    
    def start_feeds(self):
        """Start the market data engine"""
        if self.running:
            print("⚠️  Feeds already running")
            return
        
        try:
            self.engine.start()
            self.running = True
            self.start_time = time.time()
            
            # Start monitoring thread
            self.monitoring_thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.monitoring_thread.start()
            
            print("✅ Market data feeds started")
            
        except Exception as e:
            print(f"❌ Failed to start feeds: {e}")
    
    def stop_feeds(self):
        """Stop the market data engine"""
        if not self.running:
            print("⚠️  Feeds not running")
            return
        
        try:
            self.engine.stop()
            self.running = False
            
            print("🛑 Market data feeds stopped")
            
        except Exception as e:
            print(f"❌ Error stopping feeds: {e}")
    
    def subscribe_product(self, product_id: str):
        """Subscribe to a product"""
        try:
            self.engine.subscribe(product_id)
            self.stats['products'].add(product_id)
            print(f"📡 Subscribed to {product_id}")
            
        except Exception as e:
            print(f"❌ Failed to subscribe to {product_id}: {e}")
    
    def unsubscribe_product(self, product_id: str):
        """Unsubscribe from a product"""
        try:
            self.engine.unsubscribe(product_id)
            self.stats['products'].discard(product_id)
            print(f"📡 Unsubscribed from {product_id}")
            
        except Exception as e:
            print(f"❌ Failed to unsubscribe from {product_id}: {e}")
    
    def show_status(self):
        """Show current status"""
        print("\n📊 FEED STATUS")
        print("=" * 40)
        
        # Engine status
        self.engine.status()
        
        # Statistics
        if self.start_time:
            self.stats['uptime'] = int(time.time() - self.start_time)
        
        print(f"\n📈 Statistics:")
        print(f"   • Uptime: {self.stats['uptime']}s")
        print(f"   • Products: {list(self.stats['products'])}")
        print(f"   • Total Messages: {self.stats['total_messages']}")
        print(f"   • L2 Updates: {self.stats['l2_updates']}")
        print(f"   • Trades: {self.stats['trades']}")
        print(f"   • Errors: {self.stats['errors']}")
        
        # OrderBook status
        print(f"\n📚 OrderBooks:")
        for product_id, orderbook in self.engine.order_books.items():
            if hasattr(orderbook, 'get_current_snapshot_info'):
                info = orderbook.get_current_snapshot_info()
                print(f"   • {product_id}:")
                print(f"     - Bid levels: {info.get('bid_levels', 0)}")
                print(f"     - Ask levels: {info.get('ask_levels', 0)}")
                print(f"     - Updates since snapshot: {info.get('updates_since_snapshot', 0)}")
                print(f"     - Top bid: {info.get('top_bid', 'N/A')}")
                print(f"     - Top ask: {info.get('top_ask', 'N/A')}")
        
        print()
    
    def show_feed_data(self, product_id: str, lines: int = 10):
        """Show recent feed data for debugging"""
        print(f"\n📖 Recent data for {product_id} (last {lines} entries):")
        print("-" * 50)
        
        if product_id in self.engine.order_books:
            orderbook = self.engine.order_books[product_id]
            if hasattr(orderbook, 'get_snapshot'):
                try:
                    bids, asks = orderbook.get_snapshot()
                    
                    print("📉 Top Bids:")
                    for i, (price, size) in enumerate(bids[:min(5, len(bids))]):
                        print(f"   {i+1}. ${price:.2f} @ {size:.6f}")
                    
                    print("📈 Top Asks:")
                    for i, (price, size) in enumerate(asks[:min(5, len(asks))]):
                        print(f"   {i+1}. ${price:.2f} @ {size:.6f}")
                    
                    if bids and asks:
                        spread = asks[0][0] - bids[0][0]
                        mid_price = (bids[0][0] + asks[0][0]) / 2
                        print(f"\n💰 Spread: ${spread:.4f}")
                        print(f"💰 Mid Price: ${mid_price:.2f}")
                    
                except Exception as e:
                    print(f"❌ Error reading orderbook: {e}")
            else:
                print("⚠️  OrderBook doesn't support snapshot reading")
        else:
            print(f"⚠️  No orderbook found for {product_id}")
        
        print()
    
    def test_crash_recovery(self):
        """Test crash recovery by stopping and restarting"""
        print("\n🧪 TESTING CRASH RECOVERY")
        print("=" * 35)
        
        if not self.running:
            print("⚠️  Feeds not running - start them first")
            return
        
        products_before = list(self.stats['products'])
        
        print("1️⃣ Stopping feeds (simulating crash)...")
        self.stop_feeds()
        
        print("2️⃣ Waiting 2 seconds...")
        time.sleep(2)
        
        print("3️⃣ Restarting feeds...")
        self.start_feeds()
        
        print("4️⃣ Re-subscribing to products...")
        for product_id in products_before:
            self.subscribe_product(product_id)
        
        print("✅ Crash recovery test complete!")
        print("   Your platform should have resumed exactly where it left off")
    
    def _monitor_loop(self):
        """Background monitoring loop"""
        last_stats_time = time.time()
        
        while self.running:
            try:
                # Update statistics every 10 seconds
                if time.time() - last_stats_time > 10:
                    self._update_stats()
                    last_stats_time = time.time()
                
                time.sleep(1)
                
            except Exception as e:
                print(f"⚠️ Monitoring error: {e}")
                self.stats['errors'] += 1
    
    def _update_stats(self):
        """Update statistics from engine"""
        try:
            # Count orderbooks as a proxy for activity
            if hasattr(self.engine, 'order_books'):
                for product_id, orderbook in self.engine.order_books.items():
                    if hasattr(orderbook, 'update_count'):
                        self.stats['l2_updates'] += orderbook.update_count
                        orderbook.update_count = 0  # Reset counter
            
            self.stats['total_messages'] = self.engine.message_queue.qsize()
            
        except Exception as e:
            print(f"⚠️ Stats update error: {e}")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print(f"\n🛑 Received signal {signum}, shutting down...")
        self.stop_feeds()
        sys.exit(0)

def control_loop():
    """Main control loop with interactive commands"""
    controller = FeedController()
    
    print("\n🎛️  FEED CONTROLLER")
    print("=" * 25)
    print("Type 'help' for available commands")
    print("Your feeds will have crash recovery enabled!")
    
    with patch_stdout():
        try:
            while True:
                try:
                    cmd = prompt("feed> ").strip().split()
                    
                    if not cmd:
                        continue
                    
                    command = cmd[0].lower()
                    
                    if command == "help":
                        print("""
📋 Available Commands:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🚀 Feed Control:
   start                    Start market data feeds
   stop                     Stop market data feeds  
   status                   Show current status
   
📡 Product Management:
   subscribe <product>      Subscribe to product (e.g. BTC-USD)
   unsubscribe <product>    Unsubscribe from product
   products                 List subscribed products
   
🔍 Debugging:
   data <product> [lines]   Show recent data for product
   crash-test               Test crash recovery
   monitor                  Show live monitoring for 30s
   
❓ Other:
   clear                    Clear screen
   quit / exit              Exit controller
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
""")
                    
                    elif command == "start":
                        controller.start_feeds()
                    
                    elif command == "stop":
                        controller.stop_feeds()
                    
                    elif command == "status":
                        controller.show_status()
                    
                    elif command == "subscribe" and len(cmd) > 1:
                        controller.subscribe_product(cmd[1])
                    
                    elif command == "unsubscribe" and len(cmd) > 1:
                        controller.unsubscribe_product(cmd[1])
                    
                    elif command == "products":
                        products = list(controller.stats['products'])
                        if products:
                            print(f"📡 Subscribed products: {', '.join(products)}")
                        else:
                            print("📡 No products subscribed")
                    
                    elif command == "data" and len(cmd) > 1:
                        lines = int(cmd[2]) if len(cmd) > 2 else 10
                        controller.show_feed_data(cmd[1], lines)
                    
                    elif command == "crash-test":
                        controller.test_crash_recovery()
                    
                    elif command == "monitor":
                        print("📊 Live monitoring for 30 seconds...")
                        for i in range(30):
                            print(f"\r⏱️  {30-i}s remaining - "
                                  f"Queue: {controller.engine.message_queue.qsize()}, "
                                  f"Products: {len(controller.stats['products'])}, "
                                  f"Running: {controller.running}     ", end='')
                            time.sleep(1)
                        print("\n✅ Monitoring complete")
                    
                    elif command == "clear":
                        import os
                        os.system('clear' if os.name == 'posix' else 'cls')
                    
                    elif command in ["quit", "exit", "q"]:
                        controller.stop_feeds()
                        break
                    
                    else:
                        print(f"❓ Unknown command: '{command}' - type 'help'")
                
                except KeyboardInterrupt:
                    print("\n⚠️  Use 'quit' to exit properly")
                except Exception as e:
                    print(f"❌ Command error: {e}")
        
        except KeyboardInterrupt:
            print("\n🛑 Interrupted")
        finally:
            controller.stop_feeds()
            print("👋 Goodbye!")

def quick_test():
    """Quick test mode - start feeds and subscribe to BTC-USD"""
    print("🚀 QUICK TEST MODE")
    print("=" * 20)
    
    controller = FeedController()
    
    try:
        print("1️⃣ Starting feeds...")
        controller.start_feeds()
        
        print("2️⃣ Subscribing to BTC-USD...")
        controller.subscribe_product("BTC-USD")
        
        print("3️⃣ Running for 30 seconds...")
        for i in range(30):
            time.sleep(1)
            if i % 5 == 0:
                print(f"   ⏱️  {30-i}s remaining...")
        
        print("4️⃣ Showing final status...")
        controller.show_status()
        
        print("5️⃣ Showing recent data...")
        controller.show_feed_data("BTC-USD")
        
    except KeyboardInterrupt:
        print("\n🛑 Test interrupted")
    finally:
        controller.stop_feeds()
        print("✅ Quick test complete!")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        quick_test()
    else:
        control_loop()
