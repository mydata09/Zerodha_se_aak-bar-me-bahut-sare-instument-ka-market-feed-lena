#!/usr/bin/env python3
"""
mcx_depth_gui.py
----------------
mcx_36conn_health_test2.py ka PyQt5 GUI version.

Kya karta hai:
  * Wahi MCX multi-connection feed (450 tokens/connection, 36 connections, "full" mode)
    GUI se Start / Stop hota hai.
  * Market Watch tab: saare MCX instruments live (LTP, Chg%, Bid, Ask, Volume, OI, Status).
        - kisi symbol par CLICK  -> right side me uski MARKET DEPTH (5-level) live dikhti hai
        - kisi symbol par DOUBLE-CLICK -> alag floating depth window (kai symbols ek saath dekh sako)
        - search box + FUT/INDEX/OPT + LIVE/STALE/NEVER filter, header click se sort
  * Dashboard tab: connections HEALTHY/PARTIAL/DEAD/DROPPED/FAILED, LIVE/STALE/NEVER count,
    type-wise (FUT/INDEX/OPT) table, per-connection table, ticks/sec.
  * Diagnosis tab: jinka tick nahi aaya / purana hai unka KARAN (EXPIRED, NO_DATA, ... ), CSV export.
  * Log tab.

Setup:
    pip install PyQt5 websocket-client requests python-dateutil
    kite_trade.py isi folder me honi chahiye.
    python mcx_depth_gui.py
    python mcx_depth_gui.py --enctoken "..."      # ya env KITE_ENCTOKEN, ya GUI me paste karo
    python mcx_depth_gui.py --demo                # nakli (simulated) ticks se GUI test karo, login nahi chahiye

Dhyan do:
  * enctoken code me hardcode NAHI hai (GUI field / --enctoken / env se aata hai).
  * Ye enctoken (Kite web session) use karta hai -- unofficial tareeka. Bahut saare connections
    ek saath kholna Zerodha ki reasonable-use policy ke khilaf ho sakta hai. Apne risk pe chalao.
"""

import argparse
import csv
import os
import random
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import unquote

from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import Qt

import kite_trade
from kite_trade import KiteApp, KiteTicker20

EXCHANGES = {"MCX"}
INCLUDE_INDICES = True

GREEN = QtGui.QColor("#26c281")
RED = QtGui.QColor("#ef5350")
AMBER = QtGui.QColor("#ffb74d")
GREY = QtGui.QColor("#8a93a3")
BLUE = QtGui.QColor("#4fc3f7")


# =====================================================================================
#  CORE  (health-test script ka logic, GUI ke liye thoda adapt kiya)
# =====================================================================================
def clean_enctoken(raw):
    """Space, quotes, 'enctoken=' prefix aur URL-encoding hata ke sirf token deta hai."""
    t = (raw or "").strip().strip("\"'").strip()
    if t.lower().startswith("enctoken"):
        t = t.split("=", 1)[-1].strip() if "=" in t else t[8:].strip()
    if "%" in t:
        t = unquote(t)
    return t


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
    """MCX tokens, FUT -> INDEX -> options order me. CSV na mile to kite.instruments('MCX').
    Return: tokens, kinds, meta, src"""
    if csv_path and os.path.exists(csv_path):
        with open(csv_path, newline="", encoding="utf-8") as f:
            rows_raw = list(csv.DictReader(f))
        src = csv_path
    elif kite is not None:
        rows_raw = kite.instruments("MCX")
        src = "KiteApp.instruments('MCX')"
    else:
        raise FileNotFoundError(csv_path)

    rows, meta = [], {}
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
        rows.append((priority, str(row.get("name", "")), str(expiry_raw or ""), strike, token, kind))
        if token not in meta:
            exp_date = _parse_date(expiry_raw)
            meta[token] = dict(
                symbol=str(row.get("tradingsymbol") or row.get("name") or token),
                name=str(row.get("name", "")), exchange=exch, itype=itype,
                expiry=str(exp_date) if exp_date else "", expiry_date=exp_date, strike=strike,
                lot_size=row.get("lot_size", ""), tick_size=row.get("tick_size", ""),
            )
    rows.sort()
    seen, tokens, kinds = set(), [], {}
    for *_, token, kind in rows:
        if token not in seen:
            seen.add(token)
            tokens.append(token)
            kinds[token] = kind
    return tokens, kinds, meta, src


class TokStat:
    """Ek instrument ke ticks ka hisaab + aakhri poora tick (depth ke liye)."""
    __slots__ = ("n", "first_ts", "last_ts", "first_price", "last_price", "price_changes",
                 "first_vol", "last_vol", "vol_changes", "depth_sig", "depth_changes",
                 "depth_any", "has_depth_field", "last_oi", "last_trade_time", "last_tick")

    def __init__(self):
        self.n = 0
        self.first_ts = self.last_ts = None
        self.first_price = self.last_price = None
        self.price_changes = 0
        self.first_vol = self.last_vol = None
        self.vol_changes = 0
        self.depth_sig = None
        self.depth_changes = 0
        self.depth_any = False
        self.has_depth_field = False
        self.last_oi = None
        self.last_trade_time = None
        self.last_tick = None


def _depth_info(depth):
    try:
        if isinstance(depth, dict):
            levels, any_order = [], False
            for side in ("buy", "sell"):
                for lv in depth.get(side) or []:
                    p, q = lv.get("price"), lv.get("quantity")
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
    s.last_tick = t

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

    d = t.get("depth", t.get("depth20"))
    if d is not None:
        s.has_depth_field = True
        sig, any_order = _depth_info(d)
        if s.depth_sig is not None and sig != s.depth_sig:
            s.depth_changes += 1
        s.depth_sig = sig
        if any_order:
            s.depth_any = True


class Conn:
    def __init__(self, idx, tokens, enctoken, user_id, cfg, log=None):
        self.label = f"conn-{idx:02d}"
        self.tokens = tokens
        self.tokset = set(tokens)
        self.enctoken = enctoken
        self.user_id = user_id
        self.cfg = cfg
        self.log = log or (lambda m: None)

        self.state = "PENDING"      # PENDING/CONNECTING/CONNECTED/FAILED/DROPPED
        self.stopping = False
        self.kws = None
        self.lock = threading.Lock()

        self.tok = {}               # token -> TokStat
        self.tick_msgs = 0
        self.stray = 0

        self.t_start = self.t_connected = self.t_first_tick = None
        self.last_error = None
        self.close_info = None
        self.thread = threading.Thread(target=self._run, daemon=True)

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
        mode = kws.MODE_FULL  # 5-level depth

        def on_connect(ws):
            self.t_connected = time.time()
            self.state = "CONNECTED"
            kws.subscribe(self.tokens)
            kws.set_mode(mode, self.tokens)
            self.log(f"{self.label}: connected ({len(self.tokens)} tokens)")

        def on_close(ws, code, msg):
            if self.stopping:
                return
            self.close_info = (code, msg)
            if self.state == "CONNECTED":
                self.state = "DROPPED"
                self.log(f"{self.label}: DROPPED code={code} msg={msg}")

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
        except Exception as e:
            self.last_error = repr(e)
            self.state = "FAILED"
            self.log(f"{self.label}: FAILED {e!r}")
            return

        deadline = time.time() + self.cfg.connect_timeout
        while time.time() < deadline and not self.stopping:
            if self.state == "CONNECTED" or kws.is_connected():
                return
            time.sleep(0.1)
        if self.state != "CONNECTED" and not self.stopping:
            self.state = "FAILED"
            self.log(f"{self.label}: FAILED (connect timeout) {self.last_error or ''}")
            try:
                kws.close()
            except Exception:
                pass

    # ---- GUI ke liye views
    def raw_view(self):
        with self.lock:
            return [(tok, s.last_price, s.last_vol, s.last_oi, s.last_ts, s.n, s.last_tick)
                    for tok, s in self.tok.items()]

    def get_state(self, token):
        with self.lock:
            s = self.tok.get(token)
            if s is None:
                return None
            return dict(tick=s.last_tick, n=s.n, last_ts=s.last_ts, price_changes=s.price_changes,
                        depth_changes=s.depth_changes, oi=s.last_oi, vol=s.last_vol,
                        last_trade_time=s.last_trade_time)

    def stats(self, now, live_window):
        with self.lock:
            n, got = len(self.tokens), len(self.tok)
            live = sum(1 for s in self.tok.values() if now - s.last_ts <= live_window)
            depth = sum(1 for s in self.tok.values() if s.has_depth_field)
            return dict(
                label=self.label, state=self.state, tokens=n, got=got, live=live,
                stale=got - live, never=n - got, depth=depth, ticks=self.tick_msgs,
                first_tick_s=(None if self.t_first_tick is None or self.t_connected is None
                              else round(self.t_first_tick - self.t_connected, 2)),
                error=self.last_error, close=self.close_info)

    def verdict(self, st, ok_ratio):
        if st["state"] in ("FAILED", "PENDING", "CONNECTING"):
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
    "EXPIRED": "Contract expire ho chuka hai -> instruments.csv purani hai, nayi download karo",
    "CONN_FAILED": "Connection hi fail / drop hua, isliye tick nahi aaya",
    "NO_DATA": "Subscribe ke baad ek bhi tick (snapshot bhi) nahi aaya, REST check nahi hua",
    "INVALID_TOKEN": "Ek bhi tick nahi aaya aur REST quote me bhi nahi mila -> instrument galat / delisted",
    "TOKEN_MISMATCH": "REST me symbol ka token CSV wale se alag hai -> CSV ka token galat",
    "NO_TRADE_REST": "Instrument valid hai (REST me hai) par last_price 0 / volume 0 -> trading hi nahi hoti",
    "WS_MISSING": "REST me price hai par websocket se tick nahi aaya -> feed / websocket problem",
    "NEVER_TRADED": "Tick aaye par LTP 0 aur volume 0 -> abhi tak ek bhi trade nahi hua",
    "NO_QUOTES": "LTP 0 aur depth bhi poori khali -> na trade na bid/ask (practically dead)",
    "DEPTH_EMPTY": "Price hai par order book me koi bid/ask nahi (market depth khali)",
    "SNAPSHOT_ONLY": "Sirf subscribe wala ek snapshot tick aaya, uske baad koi update nahi",
    "FROZEN": "Price AUR depth dono ek hi jagah atke hai",
    "PRICE_STUCK": "Price (LTP) ek hi jagah atka hai, depth/quotes badal rahe hai (trade nahi ho raha)",
    "DEPTH_STUCK": "Price chal raha hai par market depth ek hi jagah atki hai",
    "WENT_QUIET": "Pehle active tha (price/depth badle), ab live-window se zyada se koi tick nahi",
    "LOW_ACTIVITY": "Tick bahut kam aaye (price/depth badle par bahut kam)",
}
REASON_ORDER = list(REASON_TEXT.keys())


def diagnose(m, s, cverdict, now, cfg, today):
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
                continue
            m = meta.get(token, {})
            reason, detail = diagnose(m, s, cverdict, now, cfg, today)
            entries.append(dict(
                conn=c.label, token=token, symbol=m.get("symbol", str(token)),
                name=m.get("name", ""), kind=kinds.get(token, "OTHER"),
                exchange=m.get("exchange", "MCX"), expiry=m.get("expiry", ""),
                strike=m.get("strike", 0.0), reason=reason, detail=detail,
                ticks=(s.n if s else 0), ltp=(s.last_price if s else None),
                volume=(s.last_vol if s else None), oi=(s.last_oi if s else None),
                price_changes=(s.price_changes if s else 0),
                depth_changes=(s.depth_changes if s else 0),
                last_tick_age_s=(round(now - s.last_ts, 1) if s else None)))
    return entries


def _rest_quote(kite, keys):
    """kite.quote() ho to wahi, warna Kite web ka /oms/quote endpoint (best-effort)."""
    if hasattr(kite, "quote"):
        return kite.quote(keys)
    r = kite.session.get("https://kite.zerodha.com/oms/quote", params=[("i", k) for k in keys],
                         headers=kite.headers, timeout=15)
    return (r.json() or {}).get("data", {})


def verify_no_data(kite, entries, cfg, log):
    """NO_DATA wale instruments ko REST quote se check karo (best-effort)."""
    cand = [e for e in entries if e["reason"] == "NO_DATA"][:cfg.verify_max]
    if not cand:
        return
    log(f"REST quote se {len(cand)} NO_DATA instruments verify ho rahe hai ...")
    done = 0
    for i in range(0, len(cand), 150):
        chunk = cand[i:i + 150]
        keys = [f"{e['exchange']}:{e['symbol']}" for e in chunk]
        try:
            res = _rest_quote(kite, keys)
        except Exception as ex:
            log(f"  quote batch fail ({ex!r}), ye batch NO_DATA hi rahega")
            time.sleep(1.1)
            continue
        if not isinstance(res, dict) or not res:
            log("  quote ne khali jawab diya, ye batch verify nahi hua")
            time.sleep(1.1)
            continue
        for e, key in zip(chunk, keys):
            q = res.get(key)
            if not q:
                e["reason"], e["detail"] = "INVALID_TOKEN", "REST quote me bhi nahi mila -> galat / delisted"
                continue
            qt = q.get("instrument_token")
            try:
                mismatch = qt is not None and int(qt) != e["token"]
            except (ValueError, TypeError):
                mismatch = False
            ltp = q.get("last_price") or 0
            vol = q.get("volume", q.get("volume_traded")) or 0
            if mismatch:
                e["reason"], e["detail"] = "TOKEN_MISMATCH", f"REST token={qt}, CSV token={e['token']}"
            elif ltp <= 0 and vol <= 0:
                e["reason"], e["detail"] = "NO_TRADE_REST", "REST: last_price=0, volume=0"
            else:
                e["reason"], e["detail"] = "WS_MISSING", f"REST: ltp={ltp}, vol={vol} par websocket tick nahi aaya"
        done += len(chunk)
        time.sleep(1.1)
    log(f"  verify hue: {done}/{len(cand)}")


def write_diag_csv(entries, path):
    cols = ["reason", "symbol", "token", "conn", "kind", "exchange", "name", "expiry", "strike",
            "ticks", "ltp", "volume", "oi", "price_changes", "depth_changes", "last_tick_age_s", "detail"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(entries)


def snapshot(conns, kinds, cfg, now):
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


def build_dyn(conns, live_window, now):
    """token -> (ltp, chg, pct, bid, ask, vol, oi, age, status) -- Market Watch ke liye."""
    dyn = {}
    for c in conns:
        for tok, ltp, vol, oi, last_ts, n, tick in c.raw_view():
            chg = pct = bid = ask = None
            if tick:
                close = (tick.get("ohlc") or {}).get("close")
                if ltp is not None and close:
                    chg = ltp - close
                    pct = chg / close * 100.0
                d = tick.get("depth") or tick.get("depth20")
                if d:
                    b, a = d.get("buy") or [], d.get("sell") or []
                    if b and (b[0].get("price") or 0) > 0:
                        bid = b[0]["price"]
                    if a and (a[0].get("price") or 0) > 0:
                        ask = a[0]["price"]
            age = now - last_ts
            dyn[tok] = (ltp, chg, pct, bid, ask, vol, oi, age,
                        "LIVE" if age <= live_window else "STALE")
    return dyn


# =====================================================================================
#  DEMO MODE  (nakli ticks -- bina login ke GUI test / preview ke liye)
# =====================================================================================
class FakeKite:
    def profile(self):
        return {"user_id": "DEMO01"}

    def instruments(self, exchange=None):
        rows, tok, today = [], 100000, date.today()
        names = [("CRUDEOIL", 6500), ("GOLD", 72000), ("SILVER", 88000), ("COPPER", 850),
                 ("NATURALGAS", 250), ("ZINC", 270), ("LEAD", 185), ("ALUMINIUM", 240)]
        for name, base in names:
            for m in range(-1, 3):
                exp = today + timedelta(days=30 * m + 5)
                tok += 1
                rows.append(dict(instrument_token=tok, tradingsymbol=f"{name}{exp:%y%b}FUT".upper(),
                                 name=name, expiry=exp, strike=0.0, instrument_type="FUT",
                                 segment="MCX-FUT", exchange="MCX", lot_size=100, tick_size=0.05))
            for k in range(-6, 7):
                strike = float(round(base * (1 + k * 0.01)))
                for typ in ("CE", "PE"):
                    tok += 1
                    exp = today + timedelta(days=20)
                    rows.append(dict(instrument_token=tok,
                                     tradingsymbol=f"{name}{exp:%y%b}{int(strike)}{typ}".upper(),
                                     name=name, expiry=exp, strike=strike, instrument_type=typ,
                                     segment="MCX-OPT", exchange="MCX", lot_size=100, tick_size=0.05))
        return rows


class FakeTicker:
    MODE_LTP, MODE_QUOTE, MODE_FULL = "ltp", "quote", "full"

    def __init__(self, enctoken=None, user_id=None, depth20=True, **kw):
        self.on_ticks = self.on_connect = self.on_close = self.on_error = None
        self._conn, self._stop = threading.Event(), threading.Event()
        self._toks, self._st = set(), {}

    def connect(self, threaded=True, debug=False):
        threading.Thread(target=self._loop, daemon=True).start()

    def is_connected(self):
        return self._conn.is_set()

    def subscribe(self, tokens):
        self._toks.update(tokens)

    def set_mode(self, mode, tokens):
        pass

    def close(self):
        self._stop.set()
        self._conn.clear()

    def _make(self, tok):
        s = self._st.get(tok)
        if s is None:
            base = 100 + (tok * 37) % 9000
            s = self._st[tok] = dict(p=float(base), close=float(base) * random.uniform(0.98, 1.02),
                                     vol=random.randint(0, 5000), oi=random.randint(100, 90000),
                                     hi=float(base), lo=float(base))
        s["p"] = max(0.05, s["p"] + random.choice((-0.05, 0, 0, 0.05)) * random.randint(1, 4))
        s["p"] = round(s["p"] / 0.05) * 0.05
        s["hi"], s["lo"] = max(s["hi"], s["p"]), min(s["lo"], s["p"])
        s["vol"] += random.randint(0, 40)
        buy, sell, tb, ts = [], [], 0, 0
        for i in range(5):
            q1, q2 = random.randint(1, 60) * 10, random.randint(1, 60) * 10
            buy.append(dict(quantity=q1, price=round(s["p"] - 0.05 * (i + 1), 2), orders=random.randint(1, 12)))
            sell.append(dict(quantity=q2, price=round(s["p"] + 0.05 * (i + 1), 2), orders=random.randint(1, 12)))
            tb += q1
            ts += q2
        return dict(instrument_token=tok, last_price=round(s["p"], 2), last_traded_quantity=random.randint(1, 20) * 10,
                    average_traded_price=round(s["p"], 2), volume_traded=s["vol"],
                    total_buy_quantity=tb * 7, total_sell_quantity=ts * 6, mode="full",
                    ohlc=dict(open=round(s["close"], 2), high=round(s["hi"], 2), low=round(s["lo"], 2),
                              close=round(s["close"], 2)),
                    oi=s["oi"], depth=dict(buy=buy, sell=sell))

    def _loop(self):
        time.sleep(random.uniform(0.2, 0.6))
        self._conn.set()
        if self.on_connect:
            self.on_connect(self)
        first = True
        while not self._stop.wait(0.4):
            ticks = []
            for tok in list(self._toks):
                if tok % 17 == 0:
                    continue                       # kabhi tick nahi (NO_DATA demo)
                if tok % 13 == 0 and not first:
                    continue                       # sirf snapshot (SNAPSHOT_ONLY demo)
                ticks.append(self._make(tok))
            first = False
            if ticks and self.on_ticks:
                self.on_ticks(self, ticks)


# =====================================================================================
#  ENGINE  (connections ko GUI se alag threads me chalata hai)
# =====================================================================================
class Engine(QtCore.QObject):
    log = QtCore.pyqtSignal(str)
    error = QtCore.pyqtSignal(str)
    instruments_ready = QtCore.pyqtSignal()
    chunks_ready = QtCore.pyqtSignal(object)
    running_changed = QtCore.pyqtSignal(bool)

    def __init__(self, demo=False):
        super().__init__()
        self.demo = demo
        self.conns = []
        self.tokens, self.kinds, self.meta, self.src = [], {}, {}, ""
        self.token_conn = {}
        self.kite = None
        self.user_id = ""
        self.cfg = None
        self.stop_evt = threading.Event()
        self.running = False
        self.t_start = self.t_stop = None

    def now_ref(self):
        return self.t_stop or time.time()

    def set_instruments(self, tokens, kinds, meta, src):
        self.tokens, self.kinds, self.meta, self.src = tokens, kinds, meta, src

    def get_state(self, token):
        c = self.token_conn.get(token)
        return c.get_state(token) if c else None

    def start(self, cfg, enctoken, csv_path):
        if self.running:
            return
        self.cfg = cfg
        self.running = True
        self.stop_evt.clear()
        self.conns, self.token_conn = [], {}
        self.t_start, self.t_stop = time.time(), None
        self.running_changed.emit(True)
        threading.Thread(target=self._boot, args=(cfg, enctoken, csv_path), daemon=True).start()

    def _fail(self, msg):
        self.error.emit(msg)
        self.running = False
        self.running_changed.emit(False)

    def _boot(self, cfg, enctoken, csv_path):
        try:
            kite = FakeKite() if self.demo else KiteApp(enctoken=enctoken)
            self.kite = kite
            self.user_id = kite.profile()["user_id"]
        except Exception as e:
            self._fail(f"Login fail (enctoken galat ya expire?): {e!r}")
            return
        self.log.emit(f"Login OK, user_id={self.user_id}")
        try:
            tokens, kinds, meta, src = load_tokens(None if self.demo else csv_path, kite)
        except Exception as e:
            self._fail(f"Instruments load nahi hue: {e!r}")
            return
        if not tokens:
            self._fail("MCX ka koi token nahi mila.")
            return
        self.set_instruments(tokens, kinds, meta, src)
        chunks = [tokens[i:i + cfg.per_conn] for i in range(0, len(tokens), cfg.per_conn)][:cfg.connections]
        covered = sum(len(c) for c in chunks)
        self.log.emit(f"MCX tokens: {len(tokens)} ({src}) | connections: {len(chunks)} x {cfg.per_conn} | cover: {covered}")
        labels = {}
        for i, chunk in enumerate(chunks, start=1):
            for t in chunk:
                labels[t] = f"conn-{i:02d}"
        self.instruments_ready.emit()
        self.chunks_ready.emit(labels)

        for i, chunk in enumerate(chunks, start=1):
            if self.stop_evt.is_set():
                break
            c = Conn(i, chunk, enctoken, self.user_id, cfg, log=self.log.emit)
            self.conns.append(c)
            for t in chunk:
                self.token_conn[t] = c
            c.start()
            if self.stop_evt.is_set():
                c.stop()
                break
            self.stop_evt.wait(cfg.stagger)
        if not self.stop_evt.is_set():
            self.log.emit("Sab connections start ho gayi.")

    def stop(self):
        if not self.running:
            return
        self.stop_evt.set()
        self.t_stop = time.time()
        threading.Thread(target=self._shutdown, daemon=True).start()

    def _shutdown(self):
        for c in list(self.conns):
            c.stop()
        self.running = False
        self.log.emit("Stop ho gaya. Data freeze hai -- Dashboard / Diagnosis dekh sakte ho.")
        self.running_changed.emit(False)


# =====================================================================================
#  TABLE MODELS
# =====================================================================================
def fnum(v, d=2):
    return "-" if v is None else f"{v:,.{d}f}"


def fint(v):
    return "-" if v is None else f"{int(v):,}"


class InstrumentModel(QtCore.QAbstractTableModel):
    HEADERS = ["Symbol", "Type", "Name", "Expiry", "Strike", "LTP", "Chg", "Chg %", "Bid", "Ask",
               "Volume", "OI", "Age (s)", "Status", "Conn", "Token"]
    NUMERIC = {4, 5, 6, 7, 8, 9, 10, 11, 12, 15}

    def __init__(self):
        super().__init__()
        self.static = []     # (token, symbol, kind, name, expiry, strike)
        self.dyn = {}
        self.conn_of = {}
        self.row_of = {}

    def set_instruments(self, tokens, kinds, meta):
        self.beginResetModel()
        self.static = []
        for t in tokens:
            m = meta.get(t, {})
            self.static.append((t, m.get("symbol", str(t)), kinds.get(t, "OTHER"), m.get("name", ""),
                                m.get("expiry", ""), m.get("strike", 0.0)))
        self.row_of = {s[0]: i for i, s in enumerate(self.static)}
        self.dyn = {}
        self.conn_of = {}
        self.endResetModel()

    def set_conn_labels(self, labels):
        self.conn_of = labels
        if self.static:
            self.dataChanged.emit(self.index(0, 14), self.index(len(self.static) - 1, 14))

    def set_dyn(self, dyn):
        self.dyn = dyn
        if self.static:
            self.dataChanged.emit(self.index(0, 5), self.index(len(self.static) - 1, 13))

    def rowCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.static)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.HEADERS)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.HEADERS[section]
        return None

    def token_at(self, row):
        return self.static[row][0]

    def _value(self, r, c):
        s = self.static[r]
        if c == 0: return s[1]
        if c == 1: return s[2]
        if c == 2: return s[3]
        if c == 3: return s[4]
        if c == 4: return s[5] or None
        if c == 14: return self.conn_of.get(s[0], "-")
        if c == 15: return s[0]
        d = self.dyn.get(s[0])
        if c == 13 and d is None:
            return "NEVER"
        return None if d is None else d[c - 5]

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        r, c = index.row(), index.column()
        if role == Qt.DisplayRole:
            v = self._value(r, c)
            if v is None or v == "":
                return "-"
            if c in (4, 5, 8, 9): return fnum(v)
            if c == 6: return f"{v:+,.2f}"
            if c == 7: return f"{v:+.2f}%"
            if c in (10, 11): return fint(v)
            if c == 12: return f"{v:.1f}"
            return str(v)
        if role == Qt.UserRole:
            v = self._value(r, c)
            if c in self.NUMERIC:
                return -1e18 if v is None else float(v)
            return "" if v is None else str(v)
        if role == Qt.TextAlignmentRole:
            return int(Qt.AlignRight | Qt.AlignVCenter) if c in self.NUMERIC else int(Qt.AlignLeft | Qt.AlignVCenter)
        if role == Qt.ForegroundRole:
            if c in (5, 6, 7):
                d = self.dyn.get(self.static[r][0])
                if d and d[1] is not None and d[1] != 0:
                    return QtGui.QBrush(GREEN if d[1] > 0 else RED)
            elif c == 13:
                v = self._value(r, c)
                return QtGui.QBrush(GREEN if v == "LIVE" else (AMBER if v == "STALE" else GREY))
        return None


class InstrumentProxy(QtCore.QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self.text, self.kind, self.status = [], "All", "All"
        self.setSortRole(Qt.UserRole)
        self.setDynamicSortFilter(False)   # har second re-sort na ho (header click par hi sort)

    def set_filters(self, text, kind, status):
        self.text = text.lower().split()
        self.kind, self.status = kind, status
        self.invalidateFilter()

    def filterAcceptsRow(self, row, parent):
        m = self.sourceModel()
        s = m.static[row]
        if self.kind != "All" and s[2] != self.kind:
            return False
        if self.text:
            hay = s[1].lower() + " " + s[3].lower()
            if not all(w in hay for w in self.text):
                return False
        if self.status != "All":
            d = m.dyn.get(s[0])
            if (d[8] if d else "NEVER") != self.status:
                return False
        return True


class ListTableModel(QtCore.QAbstractTableModel):
    """Simple read-only table (diagnosis ke liye)."""

    def __init__(self, headers, numeric=(), color_fn=None):
        super().__init__()
        self.headers, self.numeric, self.color_fn = headers, set(numeric), color_fn
        self.rows = []

    def set_rows(self, rows):
        self.beginResetModel()
        self.rows = rows
        self.endResetModel()

    def rowCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.rows)

    def columnCount(self, parent=QtCore.QModelIndex()):
        return 0 if parent.isValid() else len(self.headers)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.headers[section]
        return None

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        c = index.column()
        v = self.rows[index.row()][c]
        if role == Qt.DisplayRole:
            if v is None or v == "":
                return "-"
            return f"{v:,.2f}" if isinstance(v, float) else str(v)
        if role == Qt.UserRole:
            if c in self.numeric:
                return -1e18 if v is None else float(v)
            return "" if v is None else str(v)
        if role == Qt.TextAlignmentRole:
            return int(Qt.AlignRight | Qt.AlignVCenter) if c in self.numeric else int(Qt.AlignLeft | Qt.AlignVCenter)
        if role == Qt.ForegroundRole and self.color_fn:
            return self.color_fn(c, v)
        return None


# =====================================================================================
#  MARKET DEPTH PANEL
# =====================================================================================
class QtyBarDelegate(QtWidgets.QStyledItemDelegate):
    """Quantity cell ke peeche ratio ke hisaab se rang ki bar."""

    def paint(self, painter, option, index):
        ratio = index.data(Qt.UserRole)
        color = index.data(Qt.UserRole + 1)
        if ratio and color is not None:
            r = option.rect.adjusted(1, 2, -1, -2)
            w = int(r.width() * max(0.0, min(1.0, float(ratio))))
            c = QtGui.QColor(color)
            c.setAlpha(95)
            if index.data(Qt.UserRole + 2) == "R":
                bar = QtCore.QRect(r.right() - w + 1, r.top(), w, r.height())
            else:
                bar = QtCore.QRect(r.left(), r.top(), w, r.height())
            painter.fillRect(bar, c)
        super().paint(painter, option, index)


class DepthPanel(QtWidgets.QFrame):
    def __init__(self, engine, parent=None):
        super().__init__(parent)
        self.engine = engine
        self.token = None
        self.setFrameShape(QtWidgets.QFrame.StyledPanel)

        self.title = QtWidgets.QLabel("Koi symbol select nahi")
        self.title.setStyleSheet("font-size:18px; font-weight:700;")
        self.sub = QtWidgets.QLabel("Market Watch me kisi symbol par click karo")
        self.sub.setStyleSheet("color:#8a93a3;")
        self.sub.setWordWrap(True)
        self.ltp = QtWidgets.QLabel("--")
        self.ltp.setStyleSheet("font-size:34px; font-weight:700;")
        self.chg = QtWidgets.QLabel("")
        self.chg.setStyleSheet("font-size:14px;")

        head = QtWidgets.QHBoxLayout()
        left = QtWidgets.QVBoxLayout()
        left.addWidget(self.title)
        left.addWidget(self.sub)
        head.addLayout(left, 1)
        right = QtWidgets.QVBoxLayout()
        right.addWidget(self.ltp, alignment=Qt.AlignRight)
        right.addWidget(self.chg, alignment=Qt.AlignRight)
        head.addLayout(right)

        # info grid
        self.info = {}
        grid = QtWidgets.QGridLayout()
        grid.setHorizontalSpacing(18)
        keys = ["Open", "High", "Low", "Prev Close", "Volume", "ATP", "OI", "Last Qty"]
        for i, k in enumerate(keys):
            cap = QtWidgets.QLabel(k)
            cap.setStyleSheet("color:#8a93a3; font-size:11px;")
            val = QtWidgets.QLabel("-")
            val.setStyleSheet("font-weight:600;")
            grid.addWidget(cap, (i // 4) * 2, i % 4)
            grid.addWidget(val, (i // 4) * 2 + 1, i % 4)
            self.info[k] = val

        # depth table
        self.table = QtWidgets.QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels(["Orders", "Bid Qty", "Bid", "Ask", "Ask Qty", "Orders"])
        self.table.verticalHeader().setVisible(False)
        self.table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.table.setSelectionMode(QtWidgets.QAbstractItemView.NoSelection)
        self.table.setFocusPolicy(Qt.NoFocus)
        self.table.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        self.table.setItemDelegateForColumn(1, QtyBarDelegate(self.table))
        self.table.setItemDelegateForColumn(4, QtyBarDelegate(self.table))

        self.tot_buy = QtWidgets.QLabel("Total Buy: -")
        self.tot_buy.setStyleSheet(f"color:{GREEN.name()}; font-weight:700;")
        self.tot_sell = QtWidgets.QLabel("Total Sell: -")
        self.tot_sell.setStyleSheet(f"color:{RED.name()}; font-weight:700;")
        self.bar = QtWidgets.QProgressBar()
        self.bar.setRange(0, 1000)
        self.bar.setTextVisible(False)
        self.bar.setFixedHeight(10)
        self.bar.setStyleSheet(f"QProgressBar{{background:{RED.name()}; border:none; border-radius:5px;}}"
                               f"QProgressBar::chunk{{background:{GREEN.name()}; border-radius:5px;}}")
        self.spread = QtWidgets.QLabel("")
        self.spread.setStyleSheet("color:#8a93a3;")
        self.status = QtWidgets.QLabel("")
        self.status.setWordWrap(True)

        tots = QtWidgets.QHBoxLayout()
        tots.addWidget(self.tot_buy)
        tots.addStretch(1)
        tots.addWidget(self.spread)
        tots.addStretch(1)
        tots.addWidget(self.tot_sell)

        lay = QtWidgets.QVBoxLayout(self)
        lay.addLayout(head)
        lay.addSpacing(6)
        lay.addLayout(grid)
        lay.addSpacing(6)
        lay.addWidget(QtWidgets.QLabel("Market Depth"))
        lay.addWidget(self.table, 1)
        lay.addLayout(tots)
        lay.addWidget(self.bar)
        lay.addWidget(self.status)

    # ---- helpers
    def set_token(self, token):
        self.token = token
        m = self.engine.meta.get(token, {})
        self.title.setText(m.get("symbol", str(token)))
        bits = [m.get("name", ""), m.get("itype", "")]
        if m.get("expiry"):
            bits.append(f"exp {m['expiry']}")
        if m.get("strike"):
            bits.append(f"strike {m['strike']:g}")
        if m.get("lot_size") not in ("", None):
            bits.append(f"lot {m['lot_size']}")
        bits.append(f"token {token}")
        self.sub.setText("  •  ".join(b for b in bits if b))
        self.refresh()

    def _clear(self, msg):
        self.ltp.setText("--")
        self.ltp.setStyleSheet("font-size:34px; font-weight:700;")
        self.chg.setText("")
        for v in self.info.values():
            v.setText("-")
        self.table.setRowCount(0)
        self.tot_buy.setText("Total Buy: -")
        self.tot_sell.setText("Total Sell: -")
        self.spread.setText("")
        self.bar.setValue(500)
        self.status.setText(msg)
        self.status.setStyleSheet(f"color:{AMBER.name()};")

    def refresh(self):
        if self.token is None:
            return
        st = self.engine.get_state(self.token)
        if not st or not st["tick"]:
            self._clear("Abhi is symbol ka tick nahi aaya (Start nahi hua / market band / ye instrument "
                        "inactive hai / connection abhi khul raha hai).")
            return
        t = st["tick"]
        ltp = t.get("last_price")
        ohlc = t.get("ohlc") or {}
        close = ohlc.get("close")
        color = "#e6e9ef"
        if ltp is not None and close:
            diff = ltp - close
            color = GREEN.name() if diff > 0 else (RED.name() if diff < 0 else "#e6e9ef")
            self.chg.setText(f"{diff:+,.2f}  ({diff / close * 100:+.2f}%)")
            self.chg.setStyleSheet(f"font-size:14px; color:{color};")
        else:
            self.chg.setText("")
        self.ltp.setText(fnum(ltp))
        self.ltp.setStyleSheet(f"font-size:34px; font-weight:700; color:{color};")

        self.info["Open"].setText(fnum(ohlc.get("open")))
        self.info["High"].setText(fnum(ohlc.get("high")))
        self.info["Low"].setText(fnum(ohlc.get("low")))
        self.info["Prev Close"].setText(fnum(close))
        self.info["Volume"].setText(fint(t.get("volume_traded")))
        self.info["ATP"].setText(fnum(t.get("average_traded_price")))
        self.info["OI"].setText(fint(t.get("oi")))
        self.info["Last Qty"].setText(fint(t.get("last_traded_quantity")))

        depth = t.get("depth") or t.get("depth20")
        buy = (depth or {}).get("buy") or []
        sell = (depth or {}).get("sell") or []
        n = max(len(buy), len(sell))
        if self.table.rowCount() != n:
            self.table.setRowCount(n)
        mx = max([lv.get("quantity", 0) for lv in buy + sell] + [1])

        def put(r, c, text, align, ratio=None, col=None, side=None, bold=False, fg=None):
            it = self.table.item(r, c)
            if it is None:
                it = QtWidgets.QTableWidgetItem()
                self.table.setItem(r, c, it)
            it.setText(text)
            it.setTextAlignment(int(align | Qt.VCenter))
            if ratio is not None:
                it.setData(Qt.UserRole, ratio)
                it.setData(Qt.UserRole + 1, col)
                it.setData(Qt.UserRole + 2, side)
            f = it.font()
            f.setBold(bold)
            it.setFont(f)
            it.setForeground(QtGui.QBrush(fg if fg is not None else QtGui.QColor("#e6e9ef")))

        for i in range(n):
            b = buy[i] if i < len(buy) else None
            s = sell[i] if i < len(sell) else None
            first = i == 0
            if b and (b.get("quantity") or b.get("price")):
                put(i, 0, fint(b["orders"]), Qt.AlignCenter)
                put(i, 1, fint(b["quantity"]), Qt.AlignRight, b["quantity"] / mx, GREEN, "R", first)
                put(i, 2, fnum(b["price"]), Qt.AlignCenter, bold=True, fg=GREEN)
            else:
                for c in range(3):
                    put(i, c, "-", Qt.AlignCenter, 0, GREEN, "R")
            if s and (s.get("quantity") or s.get("price")):
                put(i, 3, fnum(s["price"]), Qt.AlignCenter, bold=True, fg=RED)
                put(i, 4, fint(s["quantity"]), Qt.AlignLeft, s["quantity"] / mx, RED, "L", first)
                put(i, 5, fint(s["orders"]), Qt.AlignCenter)
            else:
                for c in range(3, 6):
                    put(i, c, "-", Qt.AlignCenter, 0, RED, "L")

        tb, ts = t.get("total_buy_quantity"), t.get("total_sell_quantity")
        self.tot_buy.setText(f"Total Buy: {fint(tb)}")
        self.tot_sell.setText(f"Total Sell: {fint(ts)}")
        if tb is not None and ts is not None and (tb + ts) > 0:
            self.bar.setValue(int(tb / (tb + ts) * 1000))
        else:
            self.bar.setValue(500)
        bb = buy[0]["price"] if buy and buy[0].get("price") else None
        ba = sell[0]["price"] if sell and sell[0].get("price") else None
        self.spread.setText(f"Spread: {ba - bb:,.2f}" if bb and ba else "")

        age = time.time() - st["last_ts"] if not self.engine.t_stop else self.engine.t_stop - st["last_ts"]
        if not depth:
            msg, col = "Is instrument ke tick me depth nahi hoti (INDEX).", GREY
        elif age <= (self.engine.cfg.live_window if self.engine.cfg else 30):
            msg, col = f"LIVE  •  aakhri tick {age:.1f}s pehle  •  ticks: {st['n']}", GREEN
        else:
            msg, col = f"STALE  •  aakhri tick {age:.0f}s pehle  •  ticks: {st['n']}", AMBER
        if not self.engine.running:
            msg += "  •  (feed band hai, last data)"
        self.status.setText(msg)
        self.status.setStyleSheet(f"color:{col.name()};")


class DepthWindow(QtWidgets.QDialog):
    def __init__(self, engine, token, parent=None):
        super().__init__(parent)
        self.setAttribute(Qt.WA_DeleteOnClose)
        self.setWindowFlag(Qt.WindowMaximizeButtonHint, True)
        self.panel = DepthPanel(engine)
        self.panel.set_token(token)
        lay = QtWidgets.QVBoxLayout(self)
        lay.setContentsMargins(4, 4, 4, 4)
        lay.addWidget(self.panel)
        self.setWindowTitle(f"Market Depth - {engine.meta.get(token, {}).get('symbol', token)}")
        self.resize(480, 560)


# =====================================================================================
#  MAIN WINDOW
# =====================================================================================
def apply_dark(app):
    app.setStyle("Fusion")
    p = QtGui.QPalette()
    p.setColor(QtGui.QPalette.Window, QtGui.QColor("#11151c"))
    p.setColor(QtGui.QPalette.WindowText, QtGui.QColor("#e6e9ef"))
    p.setColor(QtGui.QPalette.Base, QtGui.QColor("#161b24"))
    p.setColor(QtGui.QPalette.AlternateBase, QtGui.QColor("#1a2029"))
    p.setColor(QtGui.QPalette.Text, QtGui.QColor("#e6e9ef"))
    p.setColor(QtGui.QPalette.Button, QtGui.QColor("#222a36"))
    p.setColor(QtGui.QPalette.ButtonText, QtGui.QColor("#e6e9ef"))
    p.setColor(QtGui.QPalette.ToolTipBase, QtGui.QColor("#222a36"))
    p.setColor(QtGui.QPalette.ToolTipText, QtGui.QColor("#e6e9ef"))
    p.setColor(QtGui.QPalette.Highlight, QtGui.QColor("#2b6cb0"))
    p.setColor(QtGui.QPalette.HighlightedText, QtGui.QColor("#ffffff"))
    p.setColor(QtGui.QPalette.Disabled, QtGui.QPalette.Text, QtGui.QColor("#6b7280"))
    p.setColor(QtGui.QPalette.Disabled, QtGui.QPalette.ButtonText, QtGui.QColor("#6b7280"))
    app.setPalette(p)
    app.setStyleSheet("""
        QGroupBox { border:1px solid #2a3342; border-radius:6px; margin-top:8px; padding-top:8px; }
        QGroupBox::title { subcontrol-origin: margin; left:10px; padding:0 4px; color:#8a93a3; }
        QHeaderView::section { background:#1e2531; color:#c9d1de; border:0; border-right:1px solid #2a3342; padding:4px 6px; }
        QTableView, QTableWidget { gridline-color:#232b38; alternate-background-color:#1a2029; }
        QPushButton { padding:5px 14px; border:1px solid #2f3a4c; border-radius:4px; }
        QPushButton:hover { background:#2b3546; }
        QPushButton#start { background:#1f7a52; border-color:#26c281; font-weight:700; }
        QPushButton#stop { background:#8b2c2c; border-color:#ef5350; font-weight:700; }
        QPushButton:disabled { background:#1a2029; color:#6b7280; border-color:#232b38; }
        QTabBar::tab { padding:7px 16px; }
        QFrame#card { background:#1a2029; border:1px solid #2a3342; border-radius:8px; }
    """)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, engine, enctoken="", csv_path="instruments.csv"):
        super().__init__()
        self.engine = engine
        self.setWindowTitle("MCX Live Feed + Market Depth" + ("  [DEMO - nakli ticks]" if engine.demo else ""))
        self.resize(1560, 900)
        self.depth_windows = []
        self.diag_entries = []
        self._last_ticks, self._last_t = 0, time.time()
        self.settings = QtCore.QSettings("mcx_depth_gui", "settings")

        self._build_ui(enctoken, csv_path)
        self._load_settings()

        engine.log.connect(self.append_log)
        engine.error.connect(self.on_error)
        engine.instruments_ready.connect(self.on_instruments_ready)
        engine.chunks_ready.connect(self.imodel.set_conn_labels)
        engine.running_changed.connect(self.on_running_changed)

        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.refresh_all)
        self.timer.start(1000)
        self.depth_timer = QtCore.QTimer(self)
        self.depth_timer.timeout.connect(self.refresh_depths)
        self.depth_timer.start(400)

        QtCore.QTimer.singleShot(150, self.try_load_instruments)

    # ------------------------------------------------------------ UI build
    def _build_ui(self, enctoken, csv_path):
        # ---- settings bar
        box = QtWidgets.QGroupBox("Connection settings")
        g = QtWidgets.QGridLayout(box)
        self.ed_token = QtWidgets.QLineEdit(enctoken)
        self.ed_token.setEchoMode(QtWidgets.QLineEdit.Password)
        self.ed_token.setPlaceholderText("Kite enctoken yaha paste karo")
        self.cb_show = QtWidgets.QCheckBox("show")
        self.cb_show.toggled.connect(lambda on: self.ed_token.setEchoMode(
            QtWidgets.QLineEdit.Normal if on else QtWidgets.QLineEdit.Password))
        self.ed_csv = QtWidgets.QLineEdit(csv_path)
        btn_csv = QtWidgets.QPushButton("Browse")
        btn_csv.clicked.connect(self.browse_csv)
        btn_reload = QtWidgets.QPushButton("Reload CSV")
        btn_reload.clicked.connect(self.try_load_instruments)

        def spin(lo, hi, val, dec=0, step=1):
            if dec:
                s = QtWidgets.QDoubleSpinBox()
                s.setDecimals(dec)
            else:
                s = QtWidgets.QSpinBox()
            s.setRange(lo, hi)
            s.setValue(val)
            s.setSingleStep(step)
            return s

        self.sp_conn = spin(1, 60, 36)
        self.sp_per = spin(1, 3000, 450)
        self.sp_stagger = spin(0, 30, 3.0, 1, 0.5)
        self.sp_live = spin(1, 600, 30)
        self.sp_low = spin(0, 100, 3)

        g.addWidget(QtWidgets.QLabel("enctoken"), 0, 0)
        g.addWidget(self.ed_token, 0, 1, 1, 5)
        g.addWidget(self.cb_show, 0, 6)
        g.addWidget(QtWidgets.QLabel("instruments CSV"), 1, 0)
        g.addWidget(self.ed_csv, 1, 1, 1, 5)
        g.addWidget(btn_csv, 1, 6)
        g.addWidget(btn_reload, 1, 7)
        row = QtWidgets.QHBoxLayout()
        for label, w in (("Connections", self.sp_conn), ("Tokens/conn", self.sp_per),
                         ("Stagger (s)", self.sp_stagger), ("LIVE window (s)", self.sp_live),
                         ("Low ticks", self.sp_low)):
            row.addWidget(QtWidgets.QLabel(label))
            row.addWidget(w)
            row.addSpacing(10)
        row.addStretch(1)
        self.btn_start = QtWidgets.QPushButton("▶  Start")
        self.btn_start.setObjectName("start")
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop = QtWidgets.QPushButton("■  Stop")
        self.btn_stop.setObjectName("stop")
        self.btn_stop.setEnabled(False)
        self.btn_stop.clicked.connect(self.engine.stop)
        row.addWidget(self.btn_start)
        row.addWidget(self.btn_stop)
        g.addLayout(row, 2, 0, 1, 8)
        g.setColumnStretch(1, 1)

        # ---- tabs
        self.tabs = QtWidgets.QTabWidget()
        self.tabs.addTab(self._build_watch_tab(), "Market Watch")
        self.tabs.addTab(self._build_dashboard_tab(), "Dashboard")
        self.tabs.addTab(self._build_diag_tab(), "Diagnosis")
        self.tabs.addTab(self._build_log_tab(), "Log")

        central = QtWidgets.QWidget()
        lay = QtWidgets.QVBoxLayout(central)
        lay.addWidget(box)
        lay.addWidget(self.tabs, 1)
        self.setCentralWidget(central)

        self.sb_state = QtWidgets.QLabel("Ready")
        self.sb_conn = QtWidgets.QLabel("")
        self.sb_rate = QtWidgets.QLabel("")
        self.statusBar().addWidget(self.sb_state, 1)
        self.statusBar().addPermanentWidget(self.sb_conn)
        self.statusBar().addPermanentWidget(self.sb_rate)

    def _build_watch_tab(self):
        w = QtWidgets.QWidget()
        lay = QtWidgets.QVBoxLayout(w)
        lay.setContentsMargins(0, 6, 0, 0)

        fl = QtWidgets.QHBoxLayout()
        self.ed_search = QtWidgets.QLineEdit()
        self.ed_search.setPlaceholderText("Symbol / name search (jaise: crudeoil 6500 ce)")
        self.ed_search.setClearButtonEnabled(True)
        self.cmb_kind = QtWidgets.QComboBox()
        self.cmb_kind.addItems(["All", "FUT", "INDEX", "OPT", "OTHER"])
        self.cmb_status = QtWidgets.QComboBox()
        self.cmb_status.addItems(["All", "LIVE", "STALE", "NEVER"])
        self.lbl_count = QtWidgets.QLabel("")
        for wdg in (self.ed_search,):
            fl.addWidget(wdg, 1)
        fl.addWidget(QtWidgets.QLabel("Type"))
        fl.addWidget(self.cmb_kind)
        fl.addWidget(QtWidgets.QLabel("Status"))
        fl.addWidget(self.cmb_status)
        fl.addWidget(self.lbl_count)
        lay.addLayout(fl)

        self.imodel = InstrumentModel()
        self.iproxy = InstrumentProxy()
        self.iproxy.setSourceModel(self.imodel)
        self.view = QtWidgets.QTableView()
        self.view.setModel(self.iproxy)
        self.view.setSortingEnabled(True)
        self.view.setAlternatingRowColors(True)
        self.view.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.view.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self.view.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.view.verticalHeader().setVisible(False)
        self.view.verticalHeader().setDefaultSectionSize(22)
        self.view.horizontalHeader().setStretchLastSection(True)
        for i, wd in enumerate([190, 55, 110, 85, 70, 85, 75, 70, 80, 80, 90, 90, 60, 65, 65, 90]):
            self.view.setColumnWidth(i, wd)
        self.view.selectionModel().currentRowChanged.connect(self.on_row_selected)
        self.view.doubleClicked.connect(self.on_row_double_clicked)

        self.ed_search.textChanged.connect(self.apply_filters)
        self.cmb_kind.currentTextChanged.connect(self.apply_filters)
        self.cmb_status.currentTextChanged.connect(self.apply_filters)

        left = QtWidgets.QWidget()
        ll = QtWidgets.QVBoxLayout(left)
        ll.setContentsMargins(0, 0, 0, 0)
        ll.addLayout(fl)
        ll.addWidget(self.view, 1)
        hint = QtWidgets.QLabel("Tip: symbol par click = market depth (right side)  •  double-click = alag depth window")
        hint.setStyleSheet("color:#8a93a3;")
        ll.addWidget(hint)

        self.depth = DepthPanel(self.engine)
        self.depth.setMinimumWidth(440)

        split = QtWidgets.QSplitter(Qt.Horizontal)
        split.addWidget(left)
        split.addWidget(self.depth)
        split.setStretchFactor(0, 3)
        split.setStretchFactor(1, 2)
        split.setSizes([950, 520])
        lay.addWidget(split, 1)
        return w

    def _card(self, title):
        f = QtWidgets.QFrame()
        f.setObjectName("card")
        v = QtWidgets.QVBoxLayout(f)
        v.setContentsMargins(12, 8, 12, 8)
        t = QtWidgets.QLabel(title)
        t.setStyleSheet("color:#8a93a3; font-size:11px;")
        val = QtWidgets.QLabel("-")
        val.setStyleSheet("font-size:22px; font-weight:700;")
        sub = QtWidgets.QLabel("")
        sub.setStyleSheet("color:#8a93a3; font-size:11px;")
        v.addWidget(t)
        v.addWidget(val)
        v.addWidget(sub)
        return f, val, sub

    def _build_dashboard_tab(self):
        w = QtWidgets.QWidget()
        lay = QtWidgets.QVBoxLayout(w)
        grid = QtWidgets.QGridLayout()
        self.cards = {}
        names = ["Connections", "HEALTHY", "PARTIAL / DEAD", "DROPPED / FAILED",
                 "Total instruments", "Tick aa raha hai", "LIVE", "STALE",
                 "NEVER (tick nahi)", "5-level depth mila", "Tick msgs/sec", "Total tick msgs"]
        for i, n in enumerate(names):
            f, val, sub = self._card(n)
            self.cards[n] = (val, sub)
            grid.addWidget(f, i // 4, i % 4)
        lay.addLayout(grid)

        lay.addWidget(QtWidgets.QLabel("Type-wise (FUT / INDEX / OPT)"))
        self.tbl_kind = QtWidgets.QTableWidget(0, 6)
        self.tbl_kind.setHorizontalHeaderLabels(["Type", "Total", "Tick mila", "%", "LIVE", "LIVE %"])
        self.tbl_kind.setMaximumHeight(150)
        lay.addWidget(self.tbl_kind)

        lay.addWidget(QtWidgets.QLabel("Per-connection result"))
        self.tbl_conn = QtWidgets.QTableWidget(0, 10)
        self.tbl_conn.setHorizontalHeaderLabels(["conn", "status", "tokens", "tick mila", "LIVE", "STALE",
                                                 "NEVER", "depth", "tick-msgs", "note"])
        lay.addWidget(self.tbl_conn, 1)
        for t in (self.tbl_kind, self.tbl_conn):
            t.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
            t.verticalHeader().setVisible(False)
            t.setAlternatingRowColors(True)
            t.horizontalHeader().setSectionResizeMode(QtWidgets.QHeaderView.Stretch)
        return w

    def _build_diag_tab(self):
        w = QtWidgets.QWidget()
        lay = QtWidgets.QVBoxLayout(w)
        top = QtWidgets.QHBoxLayout()
        self.btn_diag = QtWidgets.QPushButton("Run diagnosis")
        self.btn_diag.clicked.connect(self.run_diagnosis)
        self.cb_verify = QtWidgets.QCheckBox("NO_DATA ko REST se verify karo")
        self.cb_verify.setChecked(False)
        self.cmb_reason = QtWidgets.QComboBox()
        self.cmb_reason.addItem("All reasons")
        self.cmb_reason.currentTextChanged.connect(self.apply_diag_filter)
        self.btn_export = QtWidgets.QPushButton("Export CSV")
        self.btn_export.clicked.connect(self.export_diag)
        self.lbl_diag = QtWidgets.QLabel("Feed chalane (ya Stop karne) ke baad 'Run diagnosis' dabao.")
        top.addWidget(self.btn_diag)
        top.addWidget(self.cb_verify)
        top.addWidget(self.cmb_reason)
        top.addWidget(self.btn_export)
        top.addWidget(self.lbl_diag, 1)
        lay.addLayout(top)

        def color(c, v):
            if c == 0:
                return QtGui.QBrush(RED if v in ("EXPIRED", "INVALID_TOKEN", "CONN_FAILED", "TOKEN_MISMATCH")
                                    else AMBER)
            return None

        self.dmodel = ListTableModel(
            ["Reason", "Symbol", "Token", "Type", "Conn", "Expiry", "Strike", "Ticks", "LTP", "Volume",
             "Last tick age (s)", "Detail"],
            numeric={2, 6, 7, 8, 9, 10}, color_fn=color)
        self.dproxy = QtCore.QSortFilterProxyModel()
        self.dproxy.setSourceModel(self.dmodel)
        self.dproxy.setSortRole(Qt.UserRole)
        self.dproxy.setFilterKeyColumn(0)
        self.dview = QtWidgets.QTableView()
        self.dview.setModel(self.dproxy)
        self.dview.setSortingEnabled(True)
        self.dview.setAlternatingRowColors(True)
        self.dview.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.dview.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.dview.verticalHeader().setVisible(False)
        self.dview.verticalHeader().setDefaultSectionSize(22)
        self.dview.horizontalHeader().setStretchLastSection(True)
        for i, wd in enumerate([120, 190, 80, 55, 65, 85, 70, 55, 80, 80, 110]):
            self.dview.setColumnWidth(i, wd)
        self.dview.doubleClicked.connect(self.on_diag_double_clicked)
        lay.addWidget(self.dview, 1)
        self.lbl_reason_help = QtWidgets.QLabel("Row par double-click = Market Watch me wahi symbol khul jayega.")
        self.lbl_reason_help.setStyleSheet("color:#8a93a3;")
        lay.addWidget(self.lbl_reason_help)
        return w

    def _build_log_tab(self):
        self.log_view = QtWidgets.QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumBlockCount(5000)
        self.log_view.setFont(QtGui.QFontDatabase.systemFont(QtGui.QFontDatabase.FixedFont))
        return self.log_view

    # ------------------------------------------------------------ settings
    def _load_settings(self):
        s = self.settings
        for key, w in (("connections", self.sp_conn), ("per_conn", self.sp_per),
                       ("stagger", self.sp_stagger), ("live", self.sp_live), ("low", self.sp_low)):
            v = s.value(key)
            if v is not None:
                try:
                    w.setValue(type(w.value())(v))
                except (ValueError, TypeError):
                    pass
        if s.value("csv") and self.ed_csv.text() == "instruments.csv":
            self.ed_csv.setText(str(s.value("csv")))

    def _save_settings(self):
        s = self.settings
        s.setValue("connections", self.sp_conn.value())
        s.setValue("per_conn", self.sp_per.value())
        s.setValue("stagger", self.sp_stagger.value())
        s.setValue("live", self.sp_live.value())
        s.setValue("low", self.sp_low.value())
        s.setValue("csv", self.ed_csv.text())     # enctoken jaan-bujh ke save nahi hota

    def cfg(self):
        return SimpleNamespace(per_conn=self.sp_per.value(), connections=self.sp_conn.value(),
                               stagger=self.sp_stagger.value(), connect_timeout=10.0,
                               live_window=float(self.sp_live.value()), ok_ratio=0.90,
                               low_ticks=self.sp_low.value(), verify_max=5000)

    # ------------------------------------------------------------ actions
    def append_log(self, msg):
        self.log_view.appendPlainText(f"[{datetime.now():%H:%M:%S}] {msg}")

    def on_error(self, msg):
        self.append_log("ERROR: " + msg)
        QtWidgets.QMessageBox.warning(self, "Error", msg)

    def browse_csv(self):
        path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "instruments CSV", "", "CSV (*.csv);;All (*)")
        if path:
            self.ed_csv.setText(path)
            self.try_load_instruments()

    def try_load_instruments(self):
        """Start se pehle hi list dikhane ke liye (CSV ya demo)."""
        if self.engine.running:
            return
        try:
            if self.engine.demo:
                tokens, kinds, meta, src = load_tokens(None, FakeKite())
            else:
                tokens, kinds, meta, src = load_tokens(self.ed_csv.text().strip(), None)
        except FileNotFoundError:
            self.append_log("instruments CSV nahi mili -- Start dabane par Kite se MCX list aayegi.")
            return
        except Exception as e:
            self.append_log(f"CSV padhne me error: {e!r}")
            return
        self.engine.set_instruments(tokens, kinds, meta, src)
        self.imodel.set_instruments(tokens, kinds, meta)
        self.update_count()
        self.append_log(f"{len(tokens)} MCX instruments load hue ({src}).")

    def on_start(self):
        token = clean_enctoken(self.ed_token.text() or os.environ.get("KITE_ENCTOKEN", ""))
        if not token and not self.engine.demo:
            QtWidgets.QMessageBox.information(self, "enctoken chahiye",
                                              "enctoken paste karo (ya --enctoken / env KITE_ENCTOKEN).")
            return
        self._save_settings()
        if not self.engine.demo:
            kite_trade.print = lambda *a, **k: None    # 36 connections ka shor kam
        self.engine.start(self.cfg(), token, self.ed_csv.text().strip())

    def on_running_changed(self, running):
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)
        for w in (self.sp_conn, self.sp_per, self.sp_stagger):
            w.setEnabled(not running)
        self.sb_state.setText("RUNNING" if running else "Stopped / Ready")

    def on_instruments_ready(self):
        e = self.engine
        self.imodel.set_instruments(e.tokens, e.kinds, e.meta)
        self.update_count()

    def apply_filters(self):
        self.iproxy.set_filters(self.ed_search.text(), self.cmb_kind.currentText(), self.cmb_status.currentText())
        self.update_count()

    def update_count(self):
        self.lbl_count.setText(f"{self.iproxy.rowCount():,} / {self.imodel.rowCount():,}")

    def token_from_proxy_index(self, idx):
        if not idx.isValid():
            return None
        return self.imodel.token_at(self.iproxy.mapToSource(idx).row())

    def on_row_selected(self, cur, prev):
        tok = self.token_from_proxy_index(cur)
        if tok is not None:
            self.depth.set_token(tok)

    def on_row_double_clicked(self, idx):
        tok = self.token_from_proxy_index(idx)
        if tok is None:
            return
        win = DepthWindow(self.engine, tok, self)
        self.depth_windows.append(win)
        win.destroyed.connect(lambda *_a, w=win: self.depth_windows.remove(w) if w in self.depth_windows else None)
        win.show()

    def select_in_watch(self, token):
        row = self.imodel.row_of.get(token)
        if row is None:
            return
        self.cmb_kind.setCurrentIndex(0)
        self.cmb_status.setCurrentIndex(0)
        self.ed_search.clear()
        pidx = self.iproxy.mapFromSource(self.imodel.index(row, 0))
        if pidx.isValid():
            self.tabs.setCurrentIndex(0)
            self.view.selectRow(pidx.row())
            self.view.scrollTo(pidx, QtWidgets.QAbstractItemView.PositionAtCenter)
            self.depth.set_token(token)

    # ------------------------------------------------------------ diagnosis
    def run_diagnosis(self):
        e = self.engine
        if not e.conns:
            QtWidgets.QMessageBox.information(self, "Diagnosis", "Pehle feed Start karo (kuch data aane do).")
            return
        self.btn_diag.setEnabled(False)
        self.lbl_diag.setText("Diagnosis chal raha hai ...")
        cfg, verify, now = self.cfg(), self.cb_verify.isChecked(), e.now_ref()
        conns = list(e.conns)

        def work():
            entries = build_diagnosis(conns, e.kinds, e.meta, cfg, now, date.today())
            if entries and verify and e.kite is not None:
                verify_no_data(e.kite, entries, cfg, e.log.emit)
            QtCore.QMetaObject.invokeMethod(self, "_diag_done", Qt.QueuedConnection,
                                            QtCore.Q_ARG(object, entries))

        threading.Thread(target=work, daemon=True).start()

    @QtCore.pyqtSlot(object)
    def _diag_done(self, entries):
        self.diag_entries = entries
        order = {r: i for i, r in enumerate(REASON_ORDER)}
        entries.sort(key=lambda e: (order.get(e["reason"], 99), e["symbol"]))
        rows = [(e["reason"], e["symbol"], e["token"], e["kind"], e["conn"], e["expiry"],
                 e["strike"] or None, e["ticks"], e["ltp"], e["volume"], e["last_tick_age_s"], e["detail"])
                for e in entries]
        self.dmodel.set_rows(rows)
        cnt = Counter(e["reason"] for e in entries)
        self.cmb_reason.blockSignals(True)
        self.cmb_reason.clear()
        self.cmb_reason.addItem("All reasons")
        for r in REASON_ORDER:
            if cnt.get(r):
                self.cmb_reason.addItem(f"{r} ({cnt[r]})", r)
                self.cmb_reason.setItemData(self.cmb_reason.count() - 1, REASON_TEXT[r], Qt.ToolTipRole)
        self.cmb_reason.blockSignals(False)
        self.apply_diag_filter()
        self.btn_diag.setEnabled(True)
        if entries:
            self.lbl_diag.setText(f"Problem wale instruments: {len(entries):,}   (reason dropdown me hover = matlab)")
        else:
            self.lbl_diag.setText("Sab instruments theek chal rahe hai.")
        self.append_log(f"Diagnosis done: {len(entries)} problem instruments {dict(cnt)}")

    def apply_diag_filter(self, *_):
        reason = self.cmb_reason.currentData()
        self.dproxy.setFilterRegExp(f"^{reason}$" if reason else "")

    def on_diag_double_clicked(self, idx):
        src = self.dproxy.mapToSource(idx)
        tok = self.dmodel.rows[src.row()][2]
        self.select_in_watch(tok)

    def export_diag(self):
        if not self.diag_entries:
            QtWidgets.QMessageBox.information(self, "Export", "Pehle diagnosis run karo.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Diagnosis CSV", "mcx_instrument_diagnosis.csv",
                                                        "CSV (*.csv)")
        if path:
            try:
                write_diag_csv(self.diag_entries, path)
                self.append_log(f"Diagnosis CSV saved: {path}")
            except Exception as e:
                QtWidgets.QMessageBox.warning(self, "Export", f"Save nahi hua: {e!r}")

    # ------------------------------------------------------------ periodic refresh
    def refresh_depths(self):
        if self.tabs.currentIndex() == 0:
            self.depth.refresh()
        for w in list(self.depth_windows):
            w.panel.refresh()

    def refresh_all(self):
        e = self.engine
        conns = list(e.conns)
        now = e.now_ref()
        cfg = self.cfg()
        tab = self.tabs.currentIndex()

        ticks = sum(c.tick_msgs for c in conns)
        t = time.time()
        rate = (ticks - self._last_ticks) / max(t - self._last_t, 1e-6) if e.running else 0.0
        self._last_ticks, self._last_t = ticks, t
        up = sum(1 for c in conns if c.state == "CONNECTED")
        self.sb_conn.setText(f"connections up {up}/{len(conns)}   ")
        self.sb_rate.setText(f"{rate:,.0f} tick-msgs/sec   ")
        if e.running and conns:
            self.sb_state.setText(f"RUNNING  •  user {e.user_id}  •  {e.src}")

        if not conns:
            return

        if tab == 0:
            self.imodel.set_dyn(build_dyn(conns, cfg.live_window, now))
            if self.iproxy.status != "All":
                self.iproxy.invalidateFilter()
                self.update_count()
        elif tab == 1:
            self.refresh_dashboard(conns, cfg, now, ticks, rate)

    def refresh_dashboard(self, conns, cfg, now, ticks, rate):
        rows, by_kind = snapshot(conns, self.engine.kinds, cfg, now)
        cnt = Counter(r["verdict"] for r in rows)
        total = sum(r["tokens"] for r in rows)
        got = sum(r["got"] for r in rows)
        live = sum(r["live"] for r in rows)
        stale = sum(r["stale"] for r in rows)
        never = sum(r["never"] for r in rows)
        depth = sum(r["depth"] for r in rows)

        def pct(a, b):
            return f"{100.0 * a / b:.1f}%" if b else "n/a"

        def setc(name, val, sub=""):
            v, s = self.cards[name]
            v.setText(val)
            s.setText(sub)

        setc("Connections", str(len(rows)), "total khuli")
        setc("HEALTHY", str(cnt["HEALTHY"]), f">= {cfg.ok_ratio * 100:.0f}% tokens ko tick")
        setc("PARTIAL / DEAD", f"{cnt['PARTIAL']} / {cnt['DEAD']}", "kam tick / 0 tick")
        setc("DROPPED / FAILED", f"{cnt['DROPPED']} / {cnt['FAILED']}", "beech me tuta / connect nahi hua")
        setc("Total instruments", f"{total:,}", "subscribe list")
        setc("Tick aa raha hai", f"{got:,}", pct(got, total))
        setc("LIVE", f"{live:,}", f"{pct(live, total)}  (<= {int(cfg.live_window)}s)")
        setc("STALE", f"{stale:,}", pct(stale, total))
        setc("NEVER (tick nahi)", f"{never:,}", pct(never, total))
        setc("5-level depth mila", f"{depth:,}", pct(depth, total))
        setc("Tick msgs/sec", f"{rate:,.0f}")
        setc("Total tick msgs", f"{ticks:,}")

        colors = {"HEALTHY": GREEN, "PARTIAL": AMBER, "DEAD": RED, "DROPPED": RED, "FAILED": RED}

        def fill(tbl, data, status_col=None):
            tbl.setRowCount(len(data))
            for r, rowv in enumerate(data):
                for c, v in enumerate(rowv):
                    it = tbl.item(r, c)
                    if it is None:
                        it = QtWidgets.QTableWidgetItem()
                        tbl.setItem(r, c, it)
                    it.setText(str(v))
                    if status_col is not None and c == status_col:
                        it.setForeground(QtGui.QBrush(colors.get(v, QtGui.QColor("#e6e9ef"))))

        kd = []
        for k in ("FUT", "INDEX", "OPT", "OTHER"):
            if k in by_kind:
                d = by_kind[k]
                kd.append((k, f"{d['total']:,}", f"{d['got']:,}", pct(d["got"], d["total"]),
                           f"{d['live']:,}", pct(d["live"], d["total"])))
        fill(self.tbl_kind, kd)

        cd = []
        for r in rows:
            note = ""
            if r["verdict"] in ("FAILED", "DROPPED", "DEAD") and (r["error"] or r["close"]):
                note = (r["error"] or str(r["close"]))[:60]
            cd.append((r["label"], r["verdict"], r["tokens"], r["got"], r["live"], r["stale"], r["never"],
                       r["depth"], f"{r['ticks']:,}", note))
        fill(self.tbl_conn, cd, status_col=1)

    def closeEvent(self, ev):
        self._save_settings()
        try:
            self.engine.stop()
        except Exception:
            pass
        super().closeEvent(ev)


# =====================================================================================
def main():
    ap = argparse.ArgumentParser(description="MCX live feed + market depth GUI")
    ap.add_argument("--enctoken", default=None, help="Kite enctoken (ya GUI me paste karo / env KITE_ENCTOKEN)")
    ap.add_argument("--csv", default="instruments.csv", help="instruments CSV ka path")
    ap.add_argument("--demo", action="store_true", help="nakli ticks se GUI chalao (login nahi chahiye)")
    args = ap.parse_args()

    global KiteTicker20, KiteApp
    if args.demo:
        KiteTicker20, KiteApp = FakeTicker, FakeKite

    QtWidgets.QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    app = QtWidgets.QApplication(sys.argv)
    apply_dark(app)
    token = clean_enctoken(args.enctoken or os.environ.get("KITE_ENCTOKEN", ""))
    win = MainWindow(Engine(demo=args.demo), enctoken=token, csv_path=args.csv)
    win.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
