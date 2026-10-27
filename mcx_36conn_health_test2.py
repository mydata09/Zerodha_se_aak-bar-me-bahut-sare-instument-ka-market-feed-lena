#!/usr/bin/env python3
"""
mcx_36conn_health_test.py
-------------------------
mcx_depth5_streamer.py jaisi hi setting (MCX tokens, 450 tokens/connection,
36 connections, "full" mode = 5-level depth) se connections kholti hai, par kaam
sirf TEST karna hai. Ye chalne ke baad terminal par dikhati hai:

  * 36 me se kitne connections sahi chal rahe hai (aur kaun fail / drop / dead hai)
  * har connection ke kitne instruments ko tick mila aur kitno ko nahi
  * saare instruments me se: LIVE / STALE / NEVER, aur FUT / INDEX / OPT breakdown
  * NAYA: jinka tick kam / purana / nahi aaya, unka KARAN (reason) aur aakhir me
    un sab instruments ki list symbol ke saath:
        EXPIRED       contract expire ho chuka (CSV purani)
        NO_DATA       ek bhi tick nahi aaya
        INVALID_TOKEN REST quote me bhi nahi mila  -> instrument galat / delisted
        NO_TRADE_REST REST me valid hai par last_price 0 / volume 0
        WS_MISSING    REST me price hai par websocket tick nahi aaya
        TOKEN_MISMATCH symbol ka token CSV se alag hai
        NEVER_TRADED  ltp 0, abhi tak koi trade nahi
        NO_QUOTES     na trade na bid/ask (depth poori khali)
        DEPTH_EMPTY   price hai par order book me koi bid/ask nahi
        SNAPSHOT_ONLY sirf subscribe wala ek snapshot tick, uske baad kuch nahi
        FROZEN        price aur depth dono ek hi jagah atke
        PRICE_STUCK   price ek jagah atka, depth badal rahi hai
        DEPTH_STUCK   price chal raha hai, depth ek jagah atki
        WENT_QUIET    pehle active tha, ab chup
        LOW_ACTIVITY  tick bahut kam
        CONN_FAILED   connection hi fail/drop hua

Setup:
    pip install websocket-client requests python-dateutil
    # enctoken 3 tareeke se de sakte ho (priority isi order me):
    #   1) python mcx_36conn_health_test.py --enctoken "..."
    #   2) is file me neeche ENCTOKEN = "..." (config section me)
    #   3) export KITE_ENCTOKEN="..."
    python mcx_36conn_health_test.py

Chhota test pehle (recommended):
    python mcx_36conn_health_test.py --connections 5 --duration 60

Output options:
    --list-limit 50      terminal me har reason ke sirf 50 instruments dikhao
                         (poori list hamesha mcx_instrument_diagnosis.csv me jaati hai)
    --no-verify          NO_DATA wale instruments ka REST quote check band karo
    --low-ticks 3        itne se kam total tick wale instruments bhi list me aayenge

Zaroori:
  * kite_trade.py isi folder me honi chahiye (wahi jo streamer ke saath use hoti hai).
  * Ye enctoken (Kite web session) use karti hai, jo unofficial tareeka hai.
    36 connections ek saath kholna Zerodha ki reasonable-use policy ke khilaf ho
    sakta hai aur account flag/block ho sakta hai. Apne risk pe chalao.
  * MCX market hours (subah 9:00 se raat 11:30/11:55) me chalao. Market band
    hone par bahut se instruments sirf ek snapshot tick dete hai.
"""

import argparse
import csv
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, Counter
from datetime import date, datetime

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
    low_ticks=3,           # total tick isse kam ho to diagnosis list me aayega (LIVE ho tab bhi)
    verify_max=5000,       # REST se zyada se zyada itne NO_DATA instruments verify honge
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


def _parse_date(v):
    if not v:
        return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return date.fromisoformat(str(v).strip()[:10])
    except ValueError:
        return None


def load_tokens(csv_path, kite):
    """MCX tokens, streamer ke jaisa hi order: FUT -> INDEX -> options.
    CSV na mile to KiteApp.instruments('MCX') se laata hai.
    Return: tokens, kinds, meta (token -> symbol/expiry/strike...), src"""
    rows_raw = []
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows_raw = list(csv.DictReader(f))
        src = csv_path
    else:
        rows_raw = kite.instruments("MCX")
        src = "KiteApp.instruments('MCX')"

    rows = []
    meta = {}
    for row in rows_raw:
        try:
            exch = str(row["exchange"]).strip().upper()
            if exch not in EXCHANGES:
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
        expiry_raw = row.get("expiry")
        rows.append((priority, str(row.get("name", "")), str(expiry_raw or ""),
                     strike, token, kind))
        if token not in meta:
            exp_date = _parse_date(expiry_raw)
            meta[token] = dict(
                symbol=str(row.get("tradingsymbol") or row.get("name") or token),
                name=str(row.get("name", "")),
                exchange=exch,
                itype=itype,
                expiry=str(exp_date) if exp_date else "",
                expiry_date=exp_date,
                strike=strike,
            )
    rows.sort()
    seen, tokens, kinds = set(), [], {}
    for *_, token, kind in rows:
        if token not in seen:
            seen.add(token)
            tokens.append(token)
            kinds[token] = kind
    return tokens, kinds, meta, src


# ---------------------------------------------------------------- per-token state
class TokStat:
    """Ek instrument ke ticks ka hisaab: price / depth / volume kitni baar badla."""
    __slots__ = ("n", "first_ts", "last_ts", "first_price", "last_price", "price_changes",
                 "first_vol", "last_vol", "vol_changes", "depth_sig", "depth_changes",
                 "depth_any", "has_depth_field", "last_oi", "last_trade_time")

    def __init__(self):
        self.n = 0
        self.first_ts = None
        self.last_ts = None
        self.first_price = None
        self.last_price = None
        self.price_changes = 0
        self.first_vol = None
        self.last_vol = None
        self.vol_changes = 0
        self.depth_sig = None
        self.depth_changes = 0
        self.depth_any = False          # kabhi bhi koi bid/ask order dikha?
        self.has_depth_field = False    # tick me 'depth' key aayi?
        self.last_oi = None
        self.last_trade_time = None


def _depth_info(depth):
    """(signature, koi_order_hai) -- depth = {'buy': [{price, quantity, orders}], 'sell': [...]}"""
    try:
        if isinstance(depth, dict):
            levels = []
            any_order = False
            for side in ("buy", "sell"):
                for lv in depth.get(side) or []:
                    p = lv.get("price")
                    q = lv.get("quantity")
                    levels.append((p, q))
                    if (q or 0) > 0 or (p or 0) > 0:
                        any_order = True
            return hash(tuple(levels)), any_order
    except Exception:
        pass
    return hash(repr(depth)), True


def _update_tok(s, t, now):
    s.n += 1
    if s.first_ts is None:
        s.first_ts = now
    s.last_ts = now

    p = t.get("last_price")
    if p is not None:
        if s.first_price is None:
            s.first_price = p
        elif p != s.last_price:
            s.price_changes += 1
        s.last_price = p

    v = t.get("volume_traded", t.get("volume"))
    if v is not None:
        if s.first_vol is None:
            s.first_vol = v
        elif v != s.last_vol:
            s.vol_changes += 1
        s.last_vol = v

    oi = t.get("oi")
    if oi is not None:
        s.last_oi = oi
    ltt = t.get("last_trade_time") or t.get("exchange_timestamp")
    if ltt:
        s.last_trade_time = ltt

    d = t.get("depth")
    if d is not None:
        s.has_depth_field = True
        sig, any_order = _depth_info(d)
        if s.depth_sig is not None and sig != s.depth_sig:
            s.depth_changes += 1
        s.depth_sig = sig
        if any_order:
            s.depth_any = True


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

        self.tok = {}               # token -> TokStat (sirf jinko tick mila)
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
                        s = self.tok.get(tok)
                        if s is None:
                            s = self.tok[tok] = TokStat()
                        _update_tok(s, t, now)
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
                kws.close()  # adhura connection leak na ho
            except Exception:
                pass

    # ---- stats
    def stats(self, now, live_window):
        with self.lock:
            n = len(self.tokens)
            got = len(self.tok)
            live = sum(1 for s in self.tok.values() if now - s.last_ts <= live_window)
            depth = sum(1 for s in self.tok.values() if s.has_depth_field)
            return dict(
                label=self.label, state=self.state, tokens=n, got=got,
                live=live, stale=got - live, never=n - got,
                depth=depth, ticks=self.tick_msgs,
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


# ---------------------------------------------------------------- diagnosis
REASON_TEXT = {
    "EXPIRED":       "Contract expire ho chuka hai -> instruments.csv purani hai, nayi download karo",
    "CONN_FAILED":   "Connection hi fail / drop hua, isliye tick nahi aaya",
    "NO_DATA":       "Subscribe ke baad ek bhi tick (snapshot bhi) nahi aaya, REST check nahi hua",
    "INVALID_TOKEN": "Ek bhi tick nahi aaya aur REST quote me bhi nahi mila -> instrument galat / delisted",
    "TOKEN_MISMATCH": "REST me symbol ka token CSV wale se alag hai -> CSV ka token galat",
    "NO_TRADE_REST": "Instrument valid hai (REST me hai) par last_price 0 / volume 0 -> trading hi nahi hoti",
    "WS_MISSING":    "REST me price hai par websocket se tick nahi aaya -> feed / websocket problem",
    "NEVER_TRADED":  "Tick aaye par LTP 0 aur volume 0 -> abhi tak ek bhi trade nahi hua",
    "NO_QUOTES":     "LTP 0 aur depth bhi poori khali -> na trade na bid/ask (practically dead)",
    "DEPTH_EMPTY":   "Price hai par order book me koi bid/ask nahi (market depth khali)",
    "SNAPSHOT_ONLY": "Sirf subscribe wala ek snapshot tick aaya, uske baad koi update nahi",
    "FROZEN":        "Price AUR depth dono ek hi jagah atke hai",
    "PRICE_STUCK":   "Price (LTP) ek hi jagah atka hai, depth/quotes badal rahe hai (trade nahi ho raha)",
    "DEPTH_STUCK":   "Price chal raha hai par market depth ek hi jagah atki hai",
    "WENT_QUIET":    "Pehle active tha (price/depth badle), ab live-window se zyada se koi tick nahi",
    "LOW_ACTIVITY":  "Tick bahut kam aaye (price/depth badle par bahut kam)",
}
REASON_ORDER = list(REASON_TEXT.keys())


def diagnose(m, s, cverdict, now, cfg, today):
    """Ek instrument ka (reason, detail). s = TokStat ya None (tick kabhi nahi aaya)."""
    exp = m.get("expiry_date")
    if exp is not None and exp < today:
        return "EXPIRED", f"expiry {exp} nikal chuki hai"
    if s is None or s.n == 0:
        if cverdict in ("FAILED", "DROPPED"):
            return "CONN_FAILED", f"connection {cverdict}"
        return "NO_DATA", "subscribe ke baad ek bhi tick nahi aaya"

    ltp = s.last_price if s.last_price is not None else 0
    vol = s.last_vol or 0
    age = now - s.last_ts
    base = f"ticks={s.n}, ltp={ltp}, vol={vol}, aakhri tick {age:.0f}s pehle"

    if ltp <= 0 and vol <= 0:
        if s.has_depth_field and not s.depth_any:
            return "NO_QUOTES", base + ", depth khali"
        return "NEVER_TRADED", base + ", koi trade nahi"
    if s.has_depth_field and not s.depth_any:
        return "DEPTH_EMPTY", base + ", order book khali"
    if s.n == 1:
        return "SNAPSHOT_ONLY", base + ", sirf ek snapshot"
    if s.price_changes == 0:
        if s.has_depth_field and s.depth_changes == 0:
            return "FROZEN", base + f", price aur depth dono ek jagah (ltp={ltp})"
        extra = "aaj koi trade nahi" if vol <= 0 else "pichhle trade ke baad naya trade nahi"
        return "PRICE_STUCK", base + f", price_chg=0, depth_chg={s.depth_changes}, {extra}"
    if s.has_depth_field and s.depth_changes == 0:
        return "DEPTH_STUCK", base + f", price_chg={s.price_changes}, depth_chg=0"
    if age > cfg.live_window:
        return "WENT_QUIET", base + f", price_chg={s.price_changes}, depth_chg={s.depth_changes}"
    return "LOW_ACTIVITY", base + f", price_chg={s.price_changes}, depth_chg={s.depth_changes}"


def build_diagnosis(conns, kinds, meta, cfg, now, today):
    entries = []
    for c in conns:
        st = c.stats(now, cfg.live_window)
        cverdict = c.verdict(st, cfg.ok_ratio)
        with c.lock:
            tok = dict(c.tok)
        for token in c.tokens:
            s = tok.get(token)
            live = s is not None and (now - s.last_ts) <= cfg.live_window
            if live and s.n >= cfg.low_ticks:
                continue  # theek chal raha hai
            m = meta.get(token, {})
            reason, detail = diagnose(m, s, cverdict, now, cfg, today)
            entries.append(dict(
                conn=c.label, token=token, symbol=m.get("symbol", str(token)),
                name=m.get("name", ""), kind=kinds.get(token, "OTHER"),
                exchange=m.get("exchange", "MCX"),
                expiry=m.get("expiry", ""), strike=m.get("strike", 0.0),
                reason=reason, detail=detail,
                ticks=(s.n if s else 0),
                ltp=(s.last_price if s else None),
                volume=(s.last_vol if s else None),
                oi=(s.last_oi if s else None),
                price_changes=(s.price_changes if s else 0),
                depth_changes=(s.depth_changes if s else 0),
                last_tick_age_s=(round(now - s.last_ts, 1) if s else None),
            ))
    return entries


def verify_no_data(kite, entries, cfg):
    """NO_DATA wale instruments ko REST quote se check karo: instrument sach me galat hai
    ya valid hai par trade nahi hota ya websocket ki dikkat hai."""
    cand = [e for e in entries if e["reason"] == "NO_DATA"][:cfg.verify_max]
    if not cand:
        return
    if not hasattr(kite, "quote"):
        print("\n(kite_trade me quote() nahi mila, NO_DATA ka REST verify skip kiya)")
        return
    print(f"\nREST quote se {len(cand)} NO_DATA instruments verify ho rahe hai ...", flush=True)
    done = 0
    for i in range(0, len(cand), 400):
        chunk = cand[i:i + 400]
        keys = [f"{e['exchange']}:{e['symbol']}" for e in chunk]
        try:
            res = kite.quote(keys)
        except Exception as ex:
            print(f"  quote batch fail ({ex!r}), ye batch NO_DATA hi rahega")
            time.sleep(1.1)
            continue
        if not isinstance(res, dict) or not res:
            print("  quote ne khali jawab diya (market/permission?), ye batch verify nahi hua")
            time.sleep(1.1)
            continue
        for e, key in zip(chunk, keys):
            q = res.get(key)
            if not q:
                e["reason"] = "INVALID_TOKEN"
                e["detail"] = "REST quote me bhi nahi mila -> instrument galat / delisted"
                continue
            qt = q.get("instrument_token")
            try:
                mismatch = qt is not None and int(qt) != e["token"]
            except (ValueError, TypeError):
                mismatch = False
            ltp = q.get("last_price") or 0
            vol = q.get("volume", q.get("volume_traded")) or 0
            if mismatch:
                e["reason"] = "TOKEN_MISMATCH"
                e["detail"] = f"REST token={qt}, CSV token={e['token']}"
            elif ltp <= 0 and vol <= 0:
                e["reason"] = "NO_TRADE_REST"
                e["detail"] = "REST: last_price=0, volume=0"
            else:
                e["reason"] = "WS_MISSING"
                e["detail"] = f"REST: ltp={ltp}, vol={vol} par websocket tick nahi aaya"
        done += len(chunk)
        time.sleep(1.1)  # REST rate limit
    print(f"  verify hue: {done}/{len(cand)}")


def print_diagnosis(entries, cfg):
    print("\n" + "=" * 104)
    print("KARAN-WISE DIAGNOSIS: jinka tick kam / purana / nahi aaya")
    print("=" * 104)
    if not entries:
        print("Sab instruments theek chal rahe hai (koi problem wala nahi mila).")
        return
    cnt = Counter(e["reason"] for e in entries)
    total = len(entries)
    print(f"Total problem wale instruments: {total}\n")
    print(f"{'REASON':<15}{'count':>7}   meaning")
    print("-" * 104)
    for r in REASON_ORDER:
        if cnt.get(r):
            print(f"{r:<15}{cnt[r]:>7}   {REASON_TEXT[r]}")

    grouped = defaultdict(list)
    for e in entries:
        grouped[e["reason"]].append(e)

    print("\n" + "=" * 104)
    print("INSTRUMENT LIST (reason ke hisaab se, symbol ke saath)")
    print("=" * 104)
    for r in REASON_ORDER:
        items = grouped.get(r)
        if not items:
            continue
        print(f"\n--- {r} ({len(items)}) : {REASON_TEXT[r]}")
        shown = items if cfg.list_limit <= 0 else items[:cfg.list_limit]
        for e in shown:
            strike = f"{e['strike']:g}" if e["strike"] else "-"
            print(f"  {e['conn']} | {e['symbol']:<26} | {e['kind']:<5} | exp {e['expiry'] or '-':<10} "
                  f"| strike {strike:<8} | {e['detail']}")
        if len(shown) < len(items):
            print(f"  ... aur {len(items) - len(shown)} (poori list CSV me)")
    print("=" * 104)


def write_diag_csv(entries, path):
    cols = ["reason", "symbol", "token", "conn", "kind", "exchange", "name", "expiry", "strike",
            "ticks", "ltp", "volume", "oi", "price_changes", "depth_changes",
            "last_tick_age_s", "detail"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(entries)


# ---------------------------------------------------------------- reporting
def snapshot(conns, kinds, cfg, now=None):
    now = now or time.time()
    rows = []
    for c in conns:
        st = c.stats(now, cfg.live_window)
        st["verdict"] = c.verdict(st, cfg.ok_ratio)
        rows.append(st)

    by_kind = defaultdict(lambda: dict(total=0, got=0, live=0))
    for c in conns:
        with c.lock:
            lt = {k: s.last_ts for k, s in c.tok.items()}
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


def print_final(conns, kinds, cfg, src, user_id, elapsed, now=None):
    rows, by_kind = snapshot(conns, kinds, cfg, now)

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
    print(f"  HEALTHY (>= {cfg.ok_ratio*100:.0f}% tokens ko tick)  : {cnt['HEALTHY']:>3}   <- connect + snapshot mila (LIVE alag dekho)")
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
    p.add_argument("--low-ticks", type=int, default=DEFAULTS["low_ticks"],
                   help="itne se kam total tick wale instruments bhi diagnosis list me aayenge")
    p.add_argument("--list-limit", type=int, default=0,
                   help="terminal me har reason ke max itne instruments (0 = sab). CSV me hamesha sab.")
    p.add_argument("--no-verify", action="store_true",
                   help="NO_DATA instruments ka REST quote check na karo")
    p.add_argument("--verify-max", type=int, default=DEFAULTS["verify_max"])
    p.add_argument("--diag-csv", default="mcx_instrument_diagnosis.csv",
                   help="diagnosis list ki CSV ('' = na likho)")
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

    tokens, kinds, meta, src = load_tokens(cfg.csv_path, kite)
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

    t_end = time.time()          # LIVE/STALE isi time ke hisaab se
    elapsed = t_end - t0
    for c in conns:              # data freeze, ab naye ticks nahi
        c.stop()

    report = print_final(conns, kinds, cfg, src, user_id, elapsed, t_end)

    entries = build_diagnosis(conns, kinds, meta, cfg, t_end, date.today())
    if entries and not cfg.no_verify:
        verify_no_data(kite, entries, cfg)
    print_diagnosis(entries, cfg)

    report["diagnosis_counts"] = dict(Counter(e["reason"] for e in entries))
    if cfg.diag_csv and entries:
        try:
            write_diag_csv(entries, cfg.diag_csv)
            print(f"\nPoori instrument list (symbol ke saath): {cfg.diag_csv}")
        except Exception as e:
            print(f"Diagnosis CSV likhne me error: {e!r}")
    if cfg.report:
        try:
            with open(cfg.report, "w", encoding="utf-8") as f:
                json.dump(report, f, indent=2, default=str)
            print(f"JSON report: {cfg.report}")
        except Exception as e:
            print(f"Report likhne me error: {e!r}")


if __name__ == "__main__":
    main()
