# Coinbase DW App

Headless runners that use Deepwater to ingest Coinbase data and build orderbook snapshots.

## Setup
```bash
cd ~/coinbase-dw
python -m venv venv
source venv/bin/activate
pip install -e /home/dan/Deepwater
```

## Commands
- Ingest trades/L2:
  ```bash
  python -m app.ingest --base-path data/coinbase-test --products BTC-USD ETH-USD
  ```
- Build snapshots:
  ```bash
  python -m app.snapshots --base-path data/coinbase-test --products BTC-USD ETH-USD --depth 500 --interval 1.0
  ```
- Read trades window:
  ```bash
  python -m app.read_trades --base-path data/coinbase-test --feed CB-TRADES-BTC-USD --start 10:00 --end 10:05
  ```
