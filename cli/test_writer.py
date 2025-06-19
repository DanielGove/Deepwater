from core.platform import Platform
import time

def btc_orderbook_indexer(data: memoryview, timestamp: int) -> bool:
    """Index large snapshots"""
    return len(data) > 10000  # Index snapshots larger than 10KB

def main():
    platform = Platform("./data/test_hft_data")

    try:
        writer = platform.create_feed(
            "btc_orderbook",
            chunk_size_mb=64,
            index_callback=btc_orderbook_indexer
        )

        for i in range(1000):
            timestamp = time.time_ns()
            data = f"orderbook_update_{i}".encode() * 100
            writer.write(timestamp, data)

            if i % 100 == 0:
                print(f"📝 Wrote record {i}")

        print("✅ Write test completed successfully")

    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        platform.close()

if __name__ == "__main__":
    main()