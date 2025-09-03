#!/usr/bin/env python3
"""
Deepwater Control — dashboard + live command bar (v8)
----------------------------------------------------
Fixes/changes vs v7:
• No visual artifacts: every rendered line is padded to window width AND ends with '\n'.
• One coherent frame: snapshot is cached per refresh tick so both panes use the same data.
• Logs view still captures stdout/stderr without echoing to the terminal (no UI conflicts).
"""
import os, sys, time
from collections import deque
from typing import List, Tuple, Dict, Any
from prompt_toolkit.application import Application
from prompt_toolkit.styles import Style
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.layout import Layout, HSplit, VSplit, Window
from prompt_toolkit.layout.controls import FormattedTextControl
from prompt_toolkit.widgets import TextArea
from prompt_toolkit.filters import has_focus
from prompt_toolkit.layout.margins import ScrollbarMargin
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.application.current import get_app

# Engine
from websocket_client import MarketDataEngine

# ────────────────────────── stdout capture ──────────────────────────
LOGFILE = os.environ.get("DW_UI_LOGFILE")  # optional path

class _RingWriter:
    """Capture stdout/stderr into a ring buffer. Do not echo to terminal.
    Also write to LOGFILE if set.
    """
    def __init__(self, ring: deque):
        self._ring = ring
        self._buf = ""
        self._fh = open(LOGFILE, "a", buffering=1) if LOGFILE else None
    def write(self, s: str) -> int:
        self._buf += s
        while True:
            i = self._buf.find("\n")
            if i == -1:
                break
            line = self._buf[:i+1]
            self._ring.append(line.rstrip("\n"))
            if self._fh:
                try: self._fh.write(line)
                except Exception: pass
            self._buf = self._buf[i+1:]
        return len(s)
    def flush(self) -> None:
        if self._fh:
            try: self._fh.flush()
            except Exception: pass
    def close(self) -> None:
        if self._fh:
            try: self._fh.close()
            except Exception: pass

# ────────────────────────── helpers ──────────────────────────
def _term_width(default: int = 120) -> int:
    try:
        return get_app().output.get_size().columns
    except Exception:
        return default

def _padline(s: str) -> Tuple[str, str]:
    """Right-pad to terminal width and add a newline, so old chars are cleared."""
    w = _term_width()
    if len(s) < w:
        s = s + (" " * (w - len(s)))
    return ('', s + "\n")

def _bar(val: float, vmax: float, width: int = 24) -> str:
    if vmax <= 0: return " " * width
    filled = int(max(0.0, min(1.0, val / vmax)) * width + 0.5)
    return "█" * filled + " " * (width - filled)

def _fmt_latency(ns: float) -> str:
    if ns is None: return "—"
    if ns >= 1_000_000_000: return f"{ns/1_000_000_000:.2f}s"
    if ns >= 1_000_000:     return f"{ns/1_000_000:.2f}ms"
    if ns >= 1_000:       return f"{ns/1_000:.1f}µs"
    return f"{ns}ns"

def _sum_rates(block: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for _pid, d in (block or {}).items():
        r = d.get("rates", {})
        total += float(r.get("rps_10s", 0.0))
    return total

# ────────────────────────── UI ──────────────────────────
class DashboardApp:
    def __init__(self, engine: MarketDataEngine, refresh_hz: float = 6.0) -> None:
        self.engine = engine
        self.refresh = max(1.0/60.0, 1.0/refresh_hz)
        self.last_msg: str = ""
        self._running = False
        self._last_cmd = ("", 0.0)
        self._mode = "dash"  # or "logs"
        self._snap_cache: Dict[str, Any] = {}
        self._snap_ts = 0.0

        # capture ring
        self._ring = deque(maxlen=5000)

        # DASHBOARD widgets
        self.header = FormattedTextControl(self._render_header)
        self.totals = FormattedTextControl(self._render_totals)
        self.trades = FormattedTextControl(lambda: self._render_table("trades"))
        self.l2     = FormattedTextControl(lambda: self._render_table("l2"))
        self.status = FormattedTextControl(self._render_status)

        self.cmd = TextArea(
            height=1, prompt="cmd> ", style="class:cmd",
            multiline=False, wrap_lines=False, accept_handler=self._on_command
        )
        self.logstrip = TextArea(height=3, read_only=True, focusable=False, scrollbar=False, style="class:log", text="")

        dash_root = HSplit([
            Window(height=1, content=self.header, always_hide_cursor=True),
            Window(height=3, content=self.totals, always_hide_cursor=True),
            VSplit([
                Window(content=self.trades, right_margins=[ScrollbarMargin()], always_hide_cursor=True),
                Window(width=1, char='│', always_hide_cursor=True),
                Window(content=self.l2, right_margins=[ScrollbarMargin()], always_hide_cursor=True),
            ], padding=1),
            Window(height=1, content=self.status, always_hide_cursor=True),
            self.logstrip,
            self.cmd,
        ])

        # LOGS view
        self.logs_header = FormattedTextControl(lambda: [ _padline(" Logs — press ESC to return") ])
        self.logs_view = TextArea(read_only=True, scrollbar=True, wrap_lines=False, style="class:logs_view")
        logs_root = HSplit([Window(height=1, content=self.logs_header, always_hide_cursor=True), self.logs_view])

        # swap roots
        self._dash_root = dash_root
        self._logs_root = logs_root

        # Keys
        kb = KeyBindings()
        @kb.add('c-c')
        def _(e): e.app.exit()
        @kb.add('q', filter=~has_focus(self.cmd))
        def _(e):
            if self._mode == "logs": self._show_dash()
            else: e.app.exit()
        @kb.add('escape')
        def _(e):
            if self._mode == "logs": self._show_dash()
        @kb.add('f2')
        def _(e):
            if self._mode == "dash": self._show_logs()
            else: self._show_dash()
        @kb.add('r', filter=~has_focus(self.cmd))
        def _(e):
            try:
                if hasattr(self.engine, "metrics_reset"):
                    self.engine.metrics_reset()
                    self._notice("metrics reset")
            except Exception as ex:
                self._notice(f"reset error: {ex!s}")

        self.style = Style.from_dict({
            'header': 'bold #00ffff',
            'subtle': '#888888',
            'key': 'bold #ffaa00',
            'tot.label': '#aaaaaa',
            'tot.value': 'bold',
            'table.header': 'bold #00aaff',
            'table.line': '#cccccc',
            'status': 'bold #00ff88',
            'warn': 'bold #ff4444',
            'cmd': 'bg:#202020 #ffffff',
            'log': 'bg:#101010 #dddddd',
            'logs_view': 'bg:#101010 #dddddd',
        })

        self.app = Application(
            layout=Layout(self._dash_root, focused_element=self.cmd),
            key_bindings=kb,
            style=self.style,
            full_screen=True,
            refresh_interval=self.refresh,
        )

    # ───────────── layout switching ─────────────
    def _show_logs(self):
        self._mode = "logs"
        self._refresh_logs()
        self.app.layout.container = self._logs_root
        self.app.invalidate()

    def _show_dash(self):
        self._mode = "dash"
        self.app.layout.container = self._dash_root
        self.app.layout.focus(self.cmd)
        self.app.invalidate()

    def _refresh_logs(self):
        # Called on every frame while in logs mode
        self.logs_view.text = "\n".join(self._ring)

    # ───────────── snapshots ─────────────
    def _snapshot(self) -> dict:
        now = time.monotonic()
        # cohere the frame: reuse the same snapshot across all renderers within ~refresh window
        if self._snap_cache and (now - self._snap_ts) < max(0.01, self.refresh * 0.6):
            return self._snap_cache
        try:
            s = self.engine.metrics_snapshot()
            if isinstance(s, dict) and "total" in s:
                self._snap_cache = s
                self._snap_ts = now
                return s
        except Exception as e:
            self._notice(f"metrics: {e!s}")
        return {"total": {"rps_1s":0.0,"rps_10s":0.0,"MBps_1s":0.0,"MBps_10s":0.0}, "trades": {}, "l2": {}}

    # ───────────── renderers ─────────────
    def _render_header(self) -> List[Tuple[str,str]]:
        if self._mode == "logs":
            self._refresh_logs()
        now = time.strftime('%Y-%m-%d %H:%M:%S')
        state = "RUNNING" if self._running else "IDLE"
        return [ _padline(f"Deepwater • Dashboard   {now}   [{state}]  (Ctrl-C/q quit, r reset, F2 logs)") ]

    def _render_totals(self) -> List[Tuple[str,str]]:
        s = self._snapshot(); t = s["total"]
        if not any(t.values()):
            return [ _padline(' No metrics yet — try `start`, then `sub BTC-USD ETH-USD`.'),
                     _padline(''),
                     _padline('') ]

        ws_rps  = float(t.get("rps_10s", 0.0))   # ingress messages/s
        ws_mbps = float(t.get("MBps_10s", 0.0))  # ingress MB/s
        trades_sum = _sum_rates(s.get("trades"))
        l2_sum = _sum_rates(s.get("l2"))
        rec_sum = trades_sum + l2_sum

        vmax_ws = max(50.0, ws_rps * 1.5)
        vmax_rec = max(100.0, rec_sum * 1.25)
        vmax_mb = max(1.0, ws_mbps * 1.5)

        return [
            _padline(f"WS msgs/s {_bar(ws_rps, vmax_ws)}  {ws_rps:7.1f} rps   Ingress MB/s {_bar(ws_mbps, vmax_mb)}  {ws_mbps:5.2f} "),
            _padline(f"Processed recs/s {_bar(rec_sum, vmax_rec)}  {rec_sum:7.1f} rps  (trades {trades_sum:.1f} + l2 {l2_sum:.1f})"),
            _padline(''),
        ]

    def _render_table(self, kind: str) -> List[Tuple[str,str]]:
        s = self._snapshot(); data = s.get(kind, {})
        title = "Trades" if kind == 'trades' else 'L2'
        lines: List[Tuple[str,str]] = [ ('class:table.header', f"{title} (top by rps)\n") ]
        if not data:
            lines.append(('class:table.line', _padline('  <no data>')[1]))
            lines.append(('', _padline('')[1]))
            return lines
        items = sorted(data.items(), key=lambda kv: kv[1]['rates']['rps_10s'], reverse=True)[:16]
        vmax = max(1.0, max(v['rates']['rps_10s'] for _, v in items) * 1.2)
        for pid, m in items:
            r = m['rates']; p99 = m.get('p99_us', 0.0)
            bar = _bar(r['rps_10s'], vmax)
            lines.append(('class:table.line', _padline(f"  {pid:12s} {bar}  {r['rps_10s']:7.2f} rps  p99={_fmt_latency(p99)}")[1]))
        # a couple of blanks to clear leftovers when the list shrinks
        lines.append(('', _padline('')[1])); lines.append(('', _padline('')[1]))
        return lines

    def _render_status(self) -> List[Tuple[str,str]]:
        msg = self.last_msg or "Type a command below. Try: start · sub BTC-USD ETH-USD · logs"
        return [ _padline(" " + msg) ]

    # ───────────── commands ─────────────
    def _notice(self, text: str) -> None:
        self.last_msg = text
        self._ring.append(text)
        self.logstrip.text = "\n".join(list(self._ring)[-3:])
        try: self.app.invalidate()
        except Exception: pass

    def _on_command(self, buff) -> None:
        line = (buff.text or "").strip(); buff.text = ""
        if not line: return
        now = time.monotonic()
        if line == self._last_cmd[0] and (now - self._last_cmd[1]) < 0.25:
            return
        self._last_cmd = (line, now)
        try:
            self._execute(line)
        except Exception as e:
            self._notice(f"cmd error: {e!s}")

    def _execute(self, line: str) -> None:
        parts = line.strip().split()
        op = (parts[0].lower() if parts else "")
        args = parts[1:]

        if op in ("q","quit","exit"):
            self.app.exit(); return
        if op == "help":
            self._notice("start | stop | sub <Product ID> | unsub <Product ID> | products | status | reset | logs | quit")
            return
        if op == "logs":
            self._show_logs(); return

        if op == "start":
            self.engine.start(); self._running = True; self._notice("engine started")
        elif op == "stop":
            self.engine.stop(); self._running = False; self._notice("engine stopped")
        elif op == "status":
            s = self._snapshot(); t = s["total"]
            recs = _sum_rates(s.get('trades')) + _sum_rates(s.get('l2'))
            self._notice(f"WS={t['rps_10s']:.1f} rps / {t['MBps_10s']:.2f} MB/s; recs≈{recs:.1f} rps")
        elif op == "sub":
            pids = args[-1]
            if not pids: self._notice("usage: sub <Product ID>")
            else: self.engine.subscribe(pids); self._notice(f"subscribed: {pids}")
        elif op == "unsub":
            pids = args[-1]
            if not pids: self._notice("usage: unsub <Product ID>")
            else: self.engine.unsubscribe(pids); self._notice(f"unsubscribed: {pids}")
        elif op == "products":
            try:
                items = self.engine.list_products()
                self._notice(", ".join(items) if items else "<none>")
            except Exception:
                self._notice("<engine does not expose list_products()>")
        elif op == "reset":
            try:
                if hasattr(self.engine, "metrics_reset"):
                    self.engine.metrics_reset(); self._notice("metrics reset")
                else:
                    self._notice("no metrics_reset() on engine")
            except Exception as e:
                self._notice(f"reset error: {e!s}")
        else:
            self._notice("unknown command — try: help")

    # ───────────── run ─────────────
    def run(self) -> None:
        # Replace stdout/stderr with ring writer (no echo -> no UI conflicts)
        with patch_stdout():
            old_out, old_err = sys.stdout, sys.stderr
            rw = _RingWriter(self._ring)
            try:
                sys.stdout = rw
                sys.stderr = rw
                self.app.run()
            finally:
                sys.stdout = old_out
                sys.stderr = old_err
                rw.close()

# ────────────────────────── entry ──────────────────────────
if __name__ == "__main__":
    DashboardApp(MarketDataEngine(), refresh_hz=2.0).run()