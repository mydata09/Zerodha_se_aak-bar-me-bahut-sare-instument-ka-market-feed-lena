"""
PRODUCTION multi-connection ticker streamer.

Test se pata chala tha:
  - Ek KiteTicker20 (enctoken/web-session) connection max ~500 instruments
    allow karta hai.
  - Same enctoken se kam se kam 5 connections bina kisi issue ke simultaneously
    ban jaate hai (upar ki limit test nahi hui).

Ye script CSV ke saare instrument_token ko chunks (default 450/chunk, 500 ki
limit se thoda buffer) me baant kar, har chunk ke liye ek alag WebSocket
connection banata hai, aur sabko parallel chalata hai:
  - Har connection apna kaam khud sambhalta hai: connect -> subscribe ->
    agar drop ho jaye to exponential-backoff ke saath auto-reconnect.
  - Saare connections ke ticks ek shared, thread-safe TickStore me jama
    hote hai (latest tick per instrument_token).
  - Har STATUS_INTERVAL sec me overall health print hoti hai (kitne
    connections up hai, kitne unique instruments me tick aaya, etc).
  - Har SNAPSHOT_INTERVAL sec me latest_ticks_snapshot.json me saare
    latest ticks dump ho jaate hai (downstream use ke liye).
  - Ctrl+C dabane par gracefully saare connections band ho jaate hai.

+++ ZAROORI WARNING +++
Itne saare (100+) simultaneous WebSocket connections ek hi enctoken/IP se
kholna Zerodha ki reasonable-use policy todh sakta hai -- isse account
temporarily flag/block ho sakta hai. Pehle MAX_CONNECTIONS ko chhota
(jaise 5-10) rakh ke test karo. Agar sach me bade scale (50k-100k+
instruments) par production streaming chahiye, to enctoken ki jagah
official Kite Connect API (paid subscription, proper access_token) use
karna zyada safe/legit hai -- wahan 3000/connection aur 9000/api_key
milta hai, aur ye Zerodha ki officially-supported use-case hai.
"""

import csv
import json
import signal
import sys
import threading
import time

from kite_trade import KiteApp, KiteTicker20

# ---------------- CONFIG (apni zaroorat ke hisaab se badlo) ----------------

ENCTOKEN = "KnEXeW8bCXY+oth9dUiSK364jwWJoYzkDiybjW88rFUuFRTk1GiPvw8ltb0zBUE50IAzfeK6QL1w1HIZgVP/x1otsBpgFC9knv/qSE9pvoKZFHHe9ySIZA=="

CSV_PATH = "instruments.csv"
LOG_PATH = "streamer_log.txt"

TOKENS_PER_CONNECTION = 450       # 500 ki confirmed limit se buffer rakha hai
MAX_CONNECTIONS = 5               # PEHLE CHHOTA RAKHO AUR TEST KARO! None = saare chunks (bahut zyada connections)
CONNECT_STAGGER_DELAY = 3         # naya connection kholne se pehle kitna rukna (sec) -- burst se bachne ke liye
RECONNECT_DELAY = 5               # drop hone par retry se pehle kitna rukna (sec)
MAX_RECONNECT_DELAY = 60          # exponential backoff ka upper cap (sec)

TICK_MODE = "full"                # depth chahiye to "full" ya "full_cas" (USE_DEPTH20=True ke saath) use karo
USE_DEPTH20 = False                # True karoge to 20-level depth milega (492-byte packet), lekin bandwidth/CPU
                                    # bahut zyada lagega -- bade scale (50+ connections) par savdhaani se use karo

STATUS_INTERVAL = 15               # (sec) overall health print interval
SNAPSHOT_PATH = "latest_ticks_snapshot.json"
SNAPSHOT_INTERVAL = 30              # (sec) latest ticks disk par dump karne ka interval

# ---------------------------------------------------------------------------


def load_tokens(csv_path):
    tokens = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                tokens.append(int(row["instrument_token"]))
            except (KeyError, ValueError, TypeError):
                continue
    return tokens


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


class TickStore:
    """Thread-safe shared store -- saare connections apne latest ticks yaha dalte hai."""

    def __init__(self):
        self._lock = threading.Lock()
        self._latest = {}
        self.total_tick_messages = 0

    def update(self, ticks):
        with self._lock:
            for t in ticks:
                token = t.get("instrument_token")
                if token is not None:
                    self._latest[token] = t
            self.total_tick_messages += len(ticks)

    def snapshot(self):
        with self._lock:
            return dict(self._latest)

    def unique_count(self):
        with self._lock:
            return len(self._latest)


class ConnectionWorker:
    """Ek chunk ke tokens ke liye ek KiteTicker20 connection manage karta hai --
    connect, subscribe, aur drop hone par auto-reconnect (exponential backoff)."""

    def __init__(self, label, user_id, tokens, tick_store, log_fn):
        self.label = label
        self.user_id = user_id
        self.tokens = tokens
        self.tick_store = tick_store
        self.log = log_fn

        self.kws = None
        self.connected = False
        self.stopped = False
        self.tick_count = 0
        self.last_error = None
        self.reconnect_attempts = 0

        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self.stopped = True
        if self.kws:
            try:
                self.kws.close()
            except Exception:
                pass

    def _run(self):
        while not self.stopped:
            ok = self._connect_and_subscribe()
            if not ok:
                delay = min(RECONNECT_DELAY * (2 ** self.reconnect_attempts), MAX_RECONNECT_DELAY)
                self.reconnect_attempts += 1
                self.log(f"[{self.label}] Connect fail. {delay}s baad retry (attempt {self.reconnect_attempts}).")
                time.sleep(delay)
                continue

            self.reconnect_attempts = 0
            while not self.stopped and self.connected:
                time.sleep(1)

            if not self.stopped:
                self.log(f"[{self.label}] Connection drop hui, reconnect karenge...")

    def _connect_and_subscribe(self):
        kws = KiteTicker20(enctoken=ENCTOKEN, user_id=self.user_id, depth20=USE_DEPTH20)
        self.kws = kws
        mode_map = {"ltp": kws.MODE_LTP, "quote": kws.MODE_QUOTE, "full": kws.full_mode}
        mode = mode_map[TICK_MODE]

        def on_connect(ws):
            self.connected = True
            kws.subscribe(self.tokens)
            kws.set_mode(mode, self.tokens)
            self.log(f"[{self.label}] Connected & subscribed {len(self.tokens)} tokens (mode={TICK_MODE}).")

        def on_close(ws, code, msg):
            self.connected = False
            self.log(f"[{self.label}] Closed. code={code} msg={msg}")

        def on_error(ws, error):
            self.last_error = error
            self.log(f"[{self.label}] Error: {error!r}")

        def on_ticks(ws, ticks):
            self.tick_count += len(ticks)
            self.tick_store.update(ticks)

        kws.on_connect = on_connect
        kws.on_close = on_close
        kws.on_error = on_error
        kws.on_ticks = on_ticks

        kws.connect(threaded=True)

        for _ in range(10):
            if kws.is_connected():
                return True
            if self.stopped:
                return False
            time.sleep(1)
        return False


def make_logger(log_path):
    lock = threading.Lock()
    f = open(log_path, "a", encoding="utf-8")

    def log(msg):
        line = f"[{time.strftime('%H:%M:%S')}] {msg}"
        with lock:
            print(line)
            f.write(line + "\n")
            f.flush()

    return log, f


def status_reporter(workers, tick_store, stop_event):
    while not stop_event.wait(STATUS_INTERVAL):
        connected = sum(1 for w in workers if w.connected)
        total_ticks = sum(w.tick_count for w in workers)
        print(
            f"\n[STATUS] {connected}/{len(workers)} connections up | "
            f"unique instruments with ticks: {tick_store.unique_count()} | "
            f"total tick messages: {total_ticks}\n"
        )


def snapshot_writer(tick_store, stop_event):
    while not stop_event.wait(SNAPSHOT_INTERVAL):
        snap = tick_store.snapshot()
        try:
            with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
                json.dump(snap, f, default=str)
        except Exception as e:
            print(f"[snapshot] write failed: {e!r}")


def main():
    tokens = load_tokens(CSV_PATH)
    print(f"CSV me total {len(tokens)} instrument tokens mile.")
    if not tokens:
        print("Koi valid instrument_token nahi mila, CSV check karo.")
        sys.exit(1)

    chunks = list(chunk_list(tokens, TOKENS_PER_CONNECTION))
    if MAX_CONNECTIONS is not None:
        chunks = chunks[:MAX_CONNECTIONS]

    print(
        f"{len(chunks)} connections banengi ({TOKENS_PER_CONNECTION} tokens/connection), "
        f"total {sum(len(c) for c in chunks)} tokens cover honge "
        f"(CSV me total {len(tokens)} the)."
    )
    if MAX_CONNECTIONS is not None and len(tokens) > MAX_CONNECTIONS * TOKENS_PER_CONNECTION:
        print(
            "NOTE: MAX_CONNECTIONS ki wajah se saare CSV tokens cover nahi ho rahe. "
            "Zyada cover karne ke liye MAX_CONNECTIONS badhao (dhyan se, upar ki WARNING padho)."
        )

    kite = KiteApp(enctoken=ENCTOKEN)
    user_id = kite.profile()["user_id"]
    print(f"Login OK, user_id={user_id}")

    log, log_file = make_logger(LOG_PATH)
    tick_store = TickStore()

    workers = []
    for i, tok_chunk in enumerate(chunks, start=1):
        label = f"conn-{i}"
        worker = ConnectionWorker(label, user_id, tok_chunk, tick_store, log)
        workers.append(worker)
        worker.start()
        time.sleep(CONNECT_STAGGER_DELAY)

    stop_event = threading.Event()
    threading.Thread(target=status_reporter, args=(workers, tick_store, stop_event), daemon=True).start()
    threading.Thread(target=snapshot_writer, args=(tick_store, stop_event), daemon=True).start()

    def shutdown(signum=None, frame=None):
        print("\nShutdown ho raha hai, saare connections band kar rahe hai...")
        stop_event.set()
        for w in workers:
            w.stop()
        log_file.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    print("\nSaari connections chal rahi hai. Ctrl+C dabao rokne ke liye.\n")
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()