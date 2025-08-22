#!/usr/bin/env python3
# main.py — Mark 1 terminal control (clean, satisfying, no spam)
import os, sys, time, threading, shutil
from typing import List
from feeds.websocket_client import MarketDataEngine

DEFAULTS = [p.strip().upper() for p in (os.getenv("DEEPWATER_DEFAULT_PRODUCTS") or "BTC-USD,ETH-USD,SOL-USD").split(",") if p.strip()]

def parse_products(arg: str) -> List[str]:
    if not arg: return []
    return [p.strip().upper() for p in (arg.replace(",", " ").split()) if p.strip()]

class Control:
    def __init__(self) -> None:
        self.engine = MarketDataEngine()
        self.started = False

    # ---- ops ----
    def start(self) -> None:
        if self.started: print("already started"); return
        self.engine.start()
        if DEFAULTS: self.engine.subscribe_many(DEFAULTS)
        self.started = True
        print("started")

    def stop(self) -> None:
        if not self.started: print("not running"); return
        self.engine.stop()
        self.started = False
        print("stopped")

    def sub(self, prods: List[str]) -> None:
        if not prods: return
        self.engine.subscribe_many(prods)
        print("ok")

    def unsub(self, prods: List[str]) -> None:
        for p in prods: self.engine.unsubscribe(p)
        print("ok")

    # ---- views ----
    def status(self) -> None:
        s = self.engine.metrics_snapshot()
        print(f"state={'running' if self.started else 'stopped'} connected={'yes' if s['connected'] else 'no'}")
        q = s['queue']; rt = s['rates_total']
        print(f"queue={q['size']}/{q['cap']} hwm={q['hwm']}  rps_1s={rt['rps_1s']:.1f} rps_10s={rt['rps_10s']:.1f}  MBps_1s={rt['MBps_1s']:.3f} MBps_10s={rt['MBps_10s']:.3f} gaps={s['seq_gaps_trades']}")
        # top N by 10s rate
        def tops(dct: dict, n=8):
            items = sorted(dct.items(), key=lambda kv: kv[1]['rates']['rps_10s'], reverse=True)[:n]
            return items
        T = tops(s['trades']); L2 = tops(s['l2'])
        if T:
            print("trades.top:")
            for pid, m in T:
                r=m['rates']; print(f"  {pid:10s} rps_10s={r['rps_10s']:.1f} p99_us={m['p99_us']:.1f}")
        if L2:
            print("l2.top:")
            for pid, m in L2:
                r=m['rates']; print(f"  {pid:10s} rps_10s={r['rps_10s']:.1f} p99_us={m['p99_us']:.1f}")

    def ui(self, hz: float = 2.0) -> None:
        """Full-screen status that redraws in place. Ctrl-C to exit."""
        try:
            while True:
                s = self.engine.metrics_snapshot()
                cols = shutil.get_terminal_size((100, 30)).columns
                print("\\x1b[2J\\x1b[H", end="")  # clear + home
                print("Deepwater — Mark 1 (Ctrl-C to quit)".ljust(cols))
                line2 = f"state={'running' if self.started else 'stopped'} | connected={'yes' if s['connected'] else 'no'} | subs={len(self.engine.list_products())}"
                print(line2.ljust(cols))
                q = s['queue']; rt = s['rates_total']
                print(f"queue {q['size']}/{q['cap']} (hwm {q['hwm']})    rps_1s {rt['rps_1s']:.1f}   rps_10s {rt['rps_10s']:.1f}    MBps_10s {rt['MBps_10s']:.3f}    gaps {s['seq_gaps_trades']}".ljust(cols))
                # tables
                def table(title, dct, n=10):
                    items = sorted(dct.items(), key=lambda kv: kv[1]['rates']['rps_10s'], reverse=True)[:n]
                    if not items: return [f"{title}: <none>"]
                    rows = [title + " (top by rps_10s)"]
                    for pid, m in items:
                        r=m['rates']; rows.append(f"  {pid:10s} rps_10s={r['rps_10s']:.1f}  p99_us={m['p99_us']:.1f}")
                    return rows
                for row in table("TRADES", s['trades'])[:12]: print(row.ljust(cols))
                for row in table("L2    ", s['l2'])[:12]: print(row.ljust(cols))
                time.sleep(max(0.05, 1.0/float(hz)))
        except KeyboardInterrupt:
            print("\\x1b[2J\\x1b[H", end="")  # clear on exit

def repl():
    ctl = Control()
    print("type 'help' for commands")
    while True:
        try:
            line = input("deepwater> ").strip()
        except (EOFError, KeyboardInterrupt):
            print(); break
        if not line: continue
        op, *rest = line.split(' ', 1)
        arg = rest[0] if rest else ""
        if op in ("quit","exit","q"): ctl.stop(); break
        elif op == "help":
            print("commands: start | stop | sub <pid...> | unsub <pid...> | status | ui | products | quit")
        elif op == "start": ctl.start()
        elif op == "stop": ctl.stop()
        elif op == "sub": ctl.sub(parse_products(arg))
        elif op == "unsub": ctl.unsub(parse_products(arg))
        elif op == "products": print(", ".join(ctl.engine.list_products()) or "<none>")
        elif op == "status": ctl.status()
        elif op == "ui": ctl.ui()
        else: print("unknown")

if __name__ == "__main__":
    repl()