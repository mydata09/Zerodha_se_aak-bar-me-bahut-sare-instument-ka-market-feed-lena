#!/usr/bin/env python3
"""
mcx_36conn_health_test.py
-------------------------
mcx_depth5_streamer.py jaisi hi setting (MCX tokens, 450 tokens/connection,
36 connections, "full" mode = 5-level depth) se connections kholti hai, par kaam
sirf TEST karna hai. Ye chalne ke baad terminal par dikhati hai:

  * 36 me se kitne connections sahi chal rahe hai (aur kaun fail / drop / dead hai)
  * har connection ke kitne instruments ko tick mila aur kitno ko nahi
  * saare connections ke total instruments me se:
        - kitno ka tick aa raha hai (LIVE: haal hi me tick aaya)
        - kitno ka tick aaya par ab purana hai (STALE)
        - kitno ka bilkul tick nahi aaya (NEVER)
  * FUT / INDEX / OPTIONS ka alag breakdown

Setup:
    pip install websocket-client requests python-dateutil
    # enctoken 3 tareeke se de sakte ho (priority isi order me):
    #   1) python mcx_36conn_health_test.py --enctoken "..."
    #   2) is file me neeche ENCTOKEN = "..." (config section me)
    #   3) export KITE_ENCTOKEN="..."
    python mcx_36conn_health_test.py

Chhota test pehle (recommended):
    python mcx_36conn_health_test.py --connections 5 --duration 60

Zaroori:
  * kite_trade.py isi folder me honi chahiye (wahi jo streamer ke saath use hoti hai).
  * Ye enctoken (Kite web session) use karti hai, jo unofficial tareeka hai.
    36 connections ek saath kholna Zerodha ki reasonable-use policy ke khilaf ho
    sakta hai aur account flag/block ho sakta hai. Apne risk pe chalao.
  * MCX market hours (subah 9:00 se raat 11:30/11:55) me chalao. Market band
    hone par bahut se instruments sirf ek snapshot tick dete hai, to "LIVE" kam
    dikhega aur "STALE" zyada.
"""

import argparse
import csv
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict

import kite_trade
from kite_trade import KiteApp, KiteTicker20


# ---------------------------------------------------------------- config
DEFAULTS = dict(
    csv_path="instruments.csv",
    per_conn=450,          # streamer ke TOKENS_PER_CONNECTION jaisa
    connections=36,        # streamer ke MAX_CONNECTIONS jaisa
    stagger=3.0,           # naya connection kholne se pehle wait (sec)
    connect_timeout=10.0,  # connect hone ka max wait (sec)
    duration=90.0,         # aakhri connection khulne ke baad kitni der measure karna (sec)
    interval=10.0,         # progress line interval (sec)
    live_window=30.0,      # itne sec ke andar tick aaya to LIVE
    ok_ratio=0.90,         # connection HEALTHY tab jab itne fraction tokens ko tick mile
)
EXCHANGES = {"MCX"}
INCLUDE_INDICES = True

# >>> Apna enctoken yaha paste karo (quotes ke andar). Is file ko kisi ke saath share /
# >>> git pe push mat karna, warna tumhara Zerodha session leak ho jayega.
ENCTOKEN = "S54L9qlB2KeJk0n62vRggwWJiqbU2t/11o1/JzaHCFwZ/inycviWlr17JLIHKpAIEVxoeJx+3ZS0qbD7fWkUo0uVWd+y1/E4k+0bIGKRJ/6u55o6otbDhQ=="


def clean_enctoken(raw):
    """Space, quotes, 'enctoken=' prefix aur URL-encoding hata ke sirf token deta hai."""
    from urllib.parse import unquote
    t = (raw or "").strip().strip("\"'").strip()
    if t.lower().startswith("enctoken"):
        t = t.split("=", 1)[-1].strip() if "=" in t else t[8:].strip()
    if "%" in t:
        t = unquote(t)
    return t


# ---------------------------------------------------------------- tokens
def _row_kind(itype, segment):
    if segment == "INDICES":
        return "INDEX"
    if itype == "FUT":
        return "FUT"
    if itype in ("CE", "PE"):
        return "OPT"
    return "OTHER"


def load_tokens(csv_path, kite):
    """MCX tokens, streamer ke jaisa hi order: FUT -> INDEX -> options.
    CSV na mile to KiteApp.instruments('MCX') se laata hai."""
    rows_raw = []
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows_raw = list(csv.DictReader(f))
        src = csv_path
    else:
        rows_raw = kite.instruments("MCX")
        src = "KiteApp.instruments('MCX')"

    rows = []
    for row in rows_raw:
        try:
            if str(row["exchange"]).strip().upper() not in EXCHANGES:
                continue
            itype = str(row["instrument_type"]).strip().upper()
            segment = str(row.get("segment", "")).strip().upper()
            kind = _row_kind(itype, segment)
            if kind == "INDEX" and not INCLUDE_INDICES:
                continue
            token = int(row["instrument_token"])
        except (KeyError, ValueError, TypeError, AttributeError):
            continue
        priority = 0 if kind == "FUT" else (1 if kind == "INDEX" else 2)
        try:
            strike = float(row.get("strike") or 0)
        except (ValueError, TypeError):
            strike = 0.0
        rows.append((priority, str(row.get("name", "")), str(row.get("expiry") or ""),
                     strike, token, kind))
    rows.sort()
    seen, tokens, kinds = set(), [], {}
    for *_, token, kind in rows:
        if token not in seen:
            seen.add(token)
            tokens.append(token)
            kinds[token] = kind
    return tokens, kinds, src


# ---------------------------------------------------------------- connection
class Conn:
    def __init__(self, idx, tokens, enctoken, user_id, cfg):
        self.label = f"conn-{idx:02d}"
        self.tokens = tokens
        self.tokset = set(tokens)
        self.enctoken = enctoken
        self.user_id = user_id
        self.cfg = cfg

        self.state = "PENDING"      # PENDING/CONNECTING/CONNECTED/FAILED/DROPPED
        self.stopping = False
        self.kws = None
        self.lock = threading.Lock()

        self.last_tick = {}         # token -> time.time() of last tick
        self.depth_tokens = set()   # tokens jinke tick me 'depth' aaya
        self.tick_msgs = 0
        self.stray = 0              # subscribed list ke bahar ke tokens ke ticks

        self.t_start = None
        self.t_connected = None
        self.t_first_tick = None
        self.last_error = None
        self.close_info = None

        self.thread = threading.Thread(target=self._run, daemon=True)

    # ---- lifecycle
    def start(self):
        self.thread.start()

    def stop(self):
        self.stopping = True
        if self.kws:
            try:
                self.kws.close()
            except Exception:
                pass

    def _run(self):
        self.state = "CONNECTING"
        self.t_start = time.time()
        kws = KiteTicker20(enctoken=self.enctoken, user_id=self.user_id, depth20=True)
        self.kws = kws
        mode = kws.MODE_FULL  # 5-level depth (streamer ka TICK_MODE="full", DEPTH_LEVEL="5")

        def on_connect(ws):
            self.t_connected = time.time()
            self.state = "CONNECTED"
            kws.subscribe(self.tokens)
            kws.set_mode(mode, self.tokens)

        def on_close(ws, code, msg):
            if self.stopping:
                return
            self.close_info = (code, msg)
            if self.state == "CONNECTED":
                self.state = "DROPPED"

        def on_error(ws, error):
            self.last_error = repr(error)

        def on_ticks(ws, ticks):
            now = time.time()
            with self.lock:
                self.tick_msgs += len(ticks)
                if self.t_first_tick is None:
                    self.t_first_tick = now
                for t in ticks:
                    tok = t.get("instrument_token")
                    if tok in self.tokset:
                        self.last_tick[tok] = now
                        if t.get("depth") is not None:
                            self.depth_tokens.add(tok)
                    else:
                        self.stray += 1

        kws.on_connect = on_connect
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_ticks = on_ticks
        try:
            kws.connect(threaded=True)
        except Exception as e:  # pragma: no cover
            self.last_error = repr(e)
            self.state = "FAILED"
            return

        deadline = time.time() + self.cfg.connect_timeout
        while time.time() < deadline and not self.stopping:
            if self.state == "CONNECTED" or kws.is_connected():
                return
            time.sleep(0.1)
        if self.state != "CONNECTED" and not self.stopping:
            self.state = "FAILED"
            try:
                kws.close()  # original script ki tarah adhura connection leak na ho
            except Exception:
                pass

    # ---- stats
    def stats(self, now, live_window):
        with self.lock:
            n = len(self.tokens)
            got = len(self.last_tick)
            live = sum(1 for ts in self.last_tick.values() if now - ts <= live_window)
            return dict(
                label=self.label, state=self.state, tokens=n, got=got,
                live=live, stale=got - live, never=n - got,
                depth=len(self.depth_tokens), ticks=self.tick_msgs,
                first_tick_s=(None if self.t_first_tick is None or self.t_connected is None
                              else round(self.t_first_tick - self.t_connected, 2)),
                error=self.last_error, close=self.close_info,
            )

    def verdict(self, st, ok_ratio):
        if st["state"] == "FAILED" or st["state"] in ("PENDING", "CONNECTING"):
            return "FAILED"
        if st["state"] == "DROPPED":
            return "DROPPED"
        if st["got"] == 0:
            return "DEAD"
        if st["got"] / st["tokens"] < ok_ratio:
            return "PARTIAL"
        return "HEALTHY"


# ---------------------------------------------------------------- reporting
def snapshot(conns, kinds, cfg):
    now = time.time()
    rows = []
    for c in conns:
        st = c.stats(now, cfg.live_window)
        st["verdict"] = c.verdict(st, cfg.ok_ratio)
        rows.append(st)

    by_kind = defaultdict(lambda: dict(total=0, got=0, live=0))
    for c in conns:
        with c.lock:
            lt = dict(c.last_tick)
        for tok in c.tokens:
            k = kinds.get(tok, "OTHER")
            by_kind[k]["total"] += 1
            ts = lt.get(tok)
            if ts is not None:
                by_kind[k]["got"] += 1
                if now - ts <= cfg.live_window:
                    by_kind[k]["live"] += 1
    return rows, by_kind


def pct(a, b):
    return f"{(100.0 * a / b):5.1f}%" if b else "  n/a "


def print_progress(conns, kinds, cfg, t0, last):
    rows, _ = snapshot(conns, kinds, cfg)
    up = sum(1 for r in rows if r["state"] == "CONNECTED")
    total = sum(r["tokens"] for r in rows)
    got = sum(r["got"] for r in rows)
    live = sum(r["live"] for r in rows)
    ticks = sum(r["ticks"] for r in rows)
    now = time.time()
    rate = (ticks - last["ticks"]) / max(now - last["t"], 1e-6)
    last["ticks"], last["t"] = ticks, now
    print(f"[+{int(now - t0):>4}s] connections up {up}/{len(rows)} | "
          f"ticks mila {got}/{total} | LIVE(<= {int(cfg.live_window)}s) {live} | "
          f"{rate:,.0f} tick-msgs/sec", flush=True)


def print_final(conns, kinds, cfg, src, user_id, elapsed):
    rows, by_kind = snapshot(conns, kinds, cfg)

    print("\n" + "=" * 104)
    print("PER-CONNECTION RESULT")
    print("=" * 104)
    hdr = (f"{'conn':<8}{'status':<9}{'tokens':>7}{'tick mila':>11}{'LIVE':>7}"
           f"{'STALE':>7}{'NEVER':>7}{'depth':>7}{'tick-msgs':>11}{'1st tick':>10}  note")
    print(hdr)
    print("-" * 104)
    for r in rows:
        note = ""
        if r["verdict"] in ("FAILED", "DROPPED", "DEAD") and (r["error"] or r["close"]):
            note = (r["error"] or str(r["close"]))[:34]
        ft = "-" if r["first_tick_s"] is None else f"{r['first_tick_s']}s"
        print(f"{r['label']:<8}{r['verdict']:<9}{r['tokens']:>7}{r['got']:>11}{r['live']:>7}"
              f"{r['stale']:>7}{r['never']:>7}{r['depth']:>7}{r['ticks']:>11}{ft:>10}  {note}")

    n = len(rows)
    cnt = defaultdict(int)
    for r in rows:
        cnt[r["verdict"]] += 1
    total = sum(r["tokens"] for r in rows)
    got = sum(r["got"] for r in rows)
    live = sum(r["live"] for r in rows)
    stale = sum(r["stale"] for r in rows)
    never_all = sum(r["never"] for r in rows)
    never_conn_bad = sum(r["tokens"] for r in rows if r["verdict"] == "FAILED")
    never_conn_ok = never_all - never_conn_bad
    depth = sum(r["depth"] for r in rows)
    ticks = sum(r["ticks"] for r in rows)
    stray = sum(c.stray for c in conns)

    print("\n" + "=" * 104)
    print("OVERALL SUMMARY")
    print("=" * 104)
    print(f"User / data source        : {user_id} / {src}")
    print(f"Test chala                : {elapsed:.0f} sec  (LIVE = pichhle {int(cfg.live_window)} sec me tick aaya)")
    print()
    print(f"CONNECTIONS (total {n})")
    print(f"  HEALTHY (>= {cfg.ok_ratio*100:.0f}% tokens ko tick)  : {cnt['HEALTHY']:>3}   <- sahi chal rahe")
    print(f"  PARTIAL (connected, kam tokens ko tick) : {cnt['PARTIAL']:>3}")
    print(f"  DEAD    (connected, 0 tick)             : {cnt['DEAD']:>3}")
    print(f"  DROPPED (connect hoke beech me tuta)    : {cnt['DROPPED']:>3}")
    print(f"  FAILED  (connect hi nahi hua)           : {cnt['FAILED']:>3}")
    print()
    print(f"INSTRUMENTS (total subscribe-list: {total})")
    print(f"  Tick aa raha hai (kam se kam 1 tick)    : {got:>6}  {pct(got, total)}")
    print(f"      LIVE  (haal ka tick)                : {live:>6}  {pct(live, total)}")
    print(f"      STALE (tick aaya par ab purana)     : {stale:>6}  {pct(stale, total)}")
    print(f"  Bilkul tick nahi aaya (NEVER)           : {never_all:>6}  {pct(never_all, total)}")
    print(f"      connection fail hone ki wajah se    : {never_conn_bad:>6}")
    print(f"      connected tha par tick nahi aaya    : {never_conn_ok:>6}")
    print(f"  5-level depth wala full packet mila     : {depth:>6}  {pct(depth, total)}")
    print()
    print("TYPE-WISE (FUT / INDEX / OPT)")
    for k in ("FUT", "INDEX", "OPT", "OTHER"):
        if k in by_kind:
            d = by_kind[k]
            print(f"  {k:<6} total {d['total']:>6} | tick mila {d['got']:>6} {pct(d['got'], d['total'])} "
                  f"| LIVE {d['live']:>6} {pct(d['live'], d['total'])}")
    print()
    print(f"Total tick messages       : {ticks:,}  (avg {ticks / max(elapsed, 1):,.0f}/sec)")
    if stray:
        print(f"Subscribe list ke bahar ke ticks: {stray}")
    print()
    print("Kaise padhe:")
    print("  - Index tokens me depth nahi hoti, aur illiquid options bahut kam tick dete hai,")
    print("    isliye NEVER/STALE me unka hona normal ho sakta hai (OPT line dekho).")
    print("  - Market band ho to LIVE kam aur STALE zyada dikhega (sirf snapshot milta hai).")
    print("  - FAILED/DROPPED zyada ho to connection limit ya throttling ka sanket hai.")
    print("=" * 104)

    return dict(
        user_id=user_id, source=src, elapsed_sec=round(elapsed, 1),
        config=dict(per_conn=cfg.per_conn, connections=cfg.connections,
                    live_window=cfg.live_window, ok_ratio=cfg.ok_ratio),
        connections=rows, verdict_counts=dict(cnt),
        instruments=dict(total=total, got=got, live=live, stale=stale, never=never_all,
                         never_due_to_failed_conn=never_conn_bad,
                         never_connected_ok=never_conn_ok, depth=depth),
        by_kind={k: v for k, v in by_kind.items()},
        total_tick_msgs=ticks,
    )


# ---------------------------------------------------------------- main
def parse_args():
    p = argparse.ArgumentParser(description="MCX multi-connection feed health test")
    p.add_argument("--csv", dest="csv_path", default=DEFAULTS["csv_path"])
    p.add_argument("--per-conn", type=int, default=DEFAULTS["per_conn"])
    p.add_argument("--connections", type=int, default=DEFAULTS["connections"])
    p.add_argument("--stagger", type=float, default=DEFAULTS["stagger"])
    p.add_argument("--connect-timeout", type=float, default=DEFAULTS["connect_timeout"])
    p.add_argument("--duration", type=float, default=DEFAULTS["duration"])
    p.add_argument("--interval", type=float, default=DEFAULTS["interval"])
    p.add_argument("--live-window", type=float, default=DEFAULTS["live_window"])
    p.add_argument("--ok-ratio", type=float, default=DEFAULTS["ok_ratio"])
    p.add_argument("--enctoken", default=None, help="Kite enctoken (nahi diya to file ka ENCTOKEN / env KITE_ENCTOKEN)")
    p.add_argument("--report", default="mcx_test_report.json", help="JSON report ka path ('' = na likho)")
    p.add_argument("--verbose-lib", action="store_true", help="kite_trade ke apne print bhi dikhao")
    return p.parse_args()


def main():
    cfg = parse_args()

    enctoken = clean_enctoken(cfg.enctoken or ENCTOKEN or os.environ.get("KITE_ENCTOKEN"))
    if not enctoken:
        print("enctoken nahi mila. File me ENCTOKEN = \"...\" daalo, ya --enctoken ya env KITE_ENCTOKEN use karo.")
        sys.exit(1)

    if not cfg.verbose_lib:
        kite_trade.print = lambda *a, **k: None  # 36 connections ka shor kam karne ke liye

    kite = KiteApp(enctoken=enctoken)
    try:
        user_id = kite.profile()["user_id"]
    except Exception as e:
        print(f"Login fail (enctoken galat ya expire?): {e!r}")
        sys.exit(1)
    print(f"Login OK, user_id={user_id}")

    tokens, kinds, src = load_tokens(cfg.csv_path, kite)
    if not tokens:
        print("MCX ka koi token nahi mila.")
        sys.exit(1)
    chunks = [tokens[i:i + cfg.per_conn] for i in range(0, len(tokens), cfg.per_conn)]
    chunks = chunks[:cfg.connections]
    covered = sum(len(c) for c in chunks)
    print(f"MCX tokens: {len(tokens)} ({src}) | connections: {len(chunks)} x {cfg.per_conn} "
          f"| cover honge: {covered}\n")

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())

    conns = []
    t0 = time.time()
    last = dict(ticks=0, t=t0)
    next_print = t0 + cfg.interval

    def wait_with_progress(seconds):
        nonlocal next_print
        end = time.time() + seconds
        while time.time() < end and not stop.is_set():
            time.sleep(0.2)
            if time.time() >= next_print:
                print_progress(conns, kinds, cfg, t0, last)
                next_print += cfg.interval

    print("Connections khul rahi hai (stagger ke saath) ...")
    for i, chunk in enumerate(chunks, start=1):
        if stop.is_set():
            break
        c = Conn(i, chunk, enctoken, user_id, cfg)
        conns.append(c)
        c.start()
        wait_with_progress(cfg.stagger)

    if not stop.is_set():
        print(f"\nSab connections start ho gayi. Ab {cfg.duration:.0f} sec measure kar rahe hai "
              f"(Ctrl+C se jaldi rok sakte ho) ...")
        wait_with_progress(cfg.duration)

    elapsed = time.time() - t0
    report = print_final(conns, kinds, cfg, src, user_id, elapsed)

    for c in conns:
        c.stop()
    if cfg.report:
        try:
            with open(cfg.report, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            print(f"JSON report: {cfg.report}")
        except Exception as e:
            print(f"Report likhne me error: {e!r}")


if __name__ == "__main__":
    main()