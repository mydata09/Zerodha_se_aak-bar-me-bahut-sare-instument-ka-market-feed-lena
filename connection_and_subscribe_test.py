"""
Ye script do cheezein test karta hai (same enctoken/web-session ke saath):

  PHASE 1: Same enctoken se ek saath (simultaneously) kitne WebSocket
           connections ban sakte hai. Ye bhi check karta hai ki naya
           connection khulne par koi purana connection kick to nahi ho gaya.

  PHASE 2: Har successfully-khule connection par, tokens ka alag-alag slice
           de kar batch-by-batch subscribe karta hai taaki us connection
           ki exact subscribe-limit pata chale.

Result console + connection_and_subscribe_test_log.txt me save hota hai.

Chalane se pehle: apna fresh ENCTOKEN daal do (Kite web login -> DevTools ->
Cookies -> enctoken).
"""

import csv
import json
import sys
import time

from kite_trade import KiteApp, KiteTicker20

# ---------------- CONFIG (apni zaroorat ke hisaab se badlo) ----------------

ENCTOKEN = "KnEXeW8bCXY+oth9dUiSK364jwWJoYzkDiybjW88rFUuFRTk1GiPvw8ltb0zBUE50IAzfeK6QL1w1HIZgVP/x1otsBpgFC9knv/qSE9pvoKZFHHe9ySIZA=="

CSV_PATH = "instruments.csv"
LOG_PATH = "connection_and_subscribe_test_log.txt"

# --- Phase 1: connection count test ---
MAX_CONNECTIONS_TO_TRY = 5        # ek saath max kitne connections try karein
DELAY_BETWEEN_CONNECTIONS = 2     # naya connection kholne se pehle kitna rukna hai (sec)
CONNECT_TIMEOUT = 10              # ek connection ke "connected" hone ka max wait (sec)

# --- Phase 2: per-connection subscribe limit test ---
TOKENS_PER_CONNECTION_TEST = 700  # har connection ko kitne tokens tak test karna hai
                                   # (pichli baar 500 par error mila tha, isliye 700
                                   #  rakha hai taaki exact limit dubara confirm ho)
BATCH_SIZE = 50                   # chhota batch -> zyada precise limit milegi
DELAY_BETWEEN_BATCHES = 1         # sec

USE_DEPTH20 = True                # enctoken (web session) sirf wss://ws.zerodha.com
                                   # (depth20=True) par valid hota hai

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


class TestTicker(KiteTicker20):
    """KiteTicker20 ka subclass -- text/JSON error messages bhi pakadta hai
    (asli class sirf binary tick packets handle karti hai)."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = []
        self.closed = False
        self.close_info = None

    def _on_message(self, ws, message):
        if isinstance(message, (bytes, bytearray)):
            return super()._on_message(ws, message)
        try:
            data = json.loads(message)
        except Exception:
            data = message
        if isinstance(data, dict) and data.get("type") == "error":
            err = data.get("data", data)
            self.errors.append(err)
            print(f"    [SERVER ERROR] {err}")


def log(msg, f):
    print(msg)
    f.write(msg + "\n")
    f.flush()


def open_connection(user_id, label):
    kws = TestTicker(enctoken=ENCTOKEN, user_id=user_id, depth20=USE_DEPTH20)

    def on_connect(ws):
        print(f"  [{label}] Connected")

    def on_close(ws, code, msg):
        kws.closed = True
        kws.close_info = (code, msg)
        print(f"  [{label}] Connection closed. code={code} msg={msg}")

    def on_error(ws, error):
        print(f"  [{label}] on_error: {error!r}")

    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_ticks = lambda ws, ticks: None  # is test me actual ticks se matlab nahi

    kws.connect(threaded=True)

    for _ in range(CONNECT_TIMEOUT):
        if kws.is_connected():
            return kws
        if kws.closed:
            return None
        time.sleep(1)
    return None  # timeout ho gaya, connect nahi hua


def subscribe_test(kws, tokens, label, f):
    """Batch-by-batch subscribe karke pata karta hai ki is connection par
    kitne instruments tak koi error nahi aata. Safe count return karta hai."""
    subscribed_count = 0
    for i in range(0, len(tokens), BATCH_SIZE):
        if kws.closed:
            log(f"[{label}] Connection beech me band ho gaya (subscribed ~{subscribed_count} tak).", f)
            break

        batch = tokens[i : i + BATCH_SIZE]
        try:
            kws.subscribe(batch)
            kws.set_mode(kws.MODE_QUOTE, batch)
        except Exception as e:
            log(f"[{label}] Batch {i}-{i + len(batch)} par exception: {e!r}", f)
            break

        subscribed_count += len(batch)
        log(f"[{label}] Subscribed so far: {subscribed_count}", f)
        time.sleep(DELAY_BETWEEN_BATCHES)

        if kws.errors:
            log(f"[{label}] Server error mila: {kws.errors[-1]}", f)
            log(f"[{label}] Safe limit ~ {subscribed_count - len(batch)} instruments.", f)
            return subscribed_count - len(batch)

    if not kws.closed and not kws.errors:
        log(
            f"[{label}] Saare {subscribed_count} tokens bina error ke subscribe ho gaye "
            f"(test range khatam -- real limit isse zyada bhi ho sakti hai, "
            f"TOKENS_PER_CONNECTION_TEST badhao aur try karo).",
            f,
        )
    return subscribed_count


def main():
    tokens = load_tokens(CSV_PATH)
    print(f"CSV me total {len(tokens)} instrument tokens mile.")
    if not tokens:
        print("Koi valid instrument_token nahi mila, CSV check karo.")
        sys.exit(1)

    kite = KiteApp(enctoken=ENCTOKEN)
    user_id = kite.profile()["user_id"]
    print(f"Login OK, user_id={user_id}")

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        log("========== PHASE 1: Kitne WebSocket connections (same enctoken se) ek saath ban sakte hai ==========", f)

        connections = []  # list of (label, kws)
        for c in range(1, MAX_CONNECTIONS_TO_TRY + 1):
            label = f"conn-{c}"
            log(f"[{label}] Connect karne ki koshish ({len(connections)} pehle se open hai)...", f)
            kws = open_connection(user_id, label)

            if kws is None:
                log(f"[{label}] Connect NAHI hua / timeout ho gaya. Yahi rukte hain.", f)
                break

            log(f"[{label}] Connect ho gaya.", f)
            connections.append((label, kws))

            # check karo ki naya connection khulne se pehle wale connections
            # kick to nahi ho gaye
            for prev_label, prev_kws in connections[:-1]:
                if prev_kws.closed:
                    log(
                        f"  -> NOTE: {prev_label} band ho gaya jab {label} khula "
                        f"(naya connection purane ko kick kar sakta hai).",
                        f,
                    )

            time.sleep(DELAY_BETWEEN_CONNECTIONS)
        else:
            log(
                f"MAX_CONNECTIONS_TO_TRY ({MAX_CONNECTIONS_TO_TRY}) tak koi dikkat nahi aayi -- "
                f"shayad aur bhi ban sakte hai, MAX_CONNECTIONS_TO_TRY badha ke dobara try karo.",
                f,
            )

        still_open = [c for c in connections if not c[1].closed]
        log(
            f"\n>>> RESULT PHASE 1: {len(connections)} connections successfully bane, "
            f"{len(still_open)} abhi bhi open/zinda hai. <<<\n",
            f,
        )

        log("========== PHASE 2: Har open connection par kitne instruments subscribe ho rahe hai ==========", f)

        results = {}
        for idx, (label, kws) in enumerate(still_open):
            start = idx * TOKENS_PER_CONNECTION_TEST
            end = start + TOKENS_PER_CONNECTION_TEST
            token_slice = tokens[start:end]
            if not token_slice:
                log(f"[{label}] CSV me is connection ke liye aur tokens nahi bache, skip.", f)
                continue

            log(f"\n--- {label} par test start ({len(token_slice)} tokens available) ---", f)
            limit_found = subscribe_test(kws, token_slice, label, f)
            results[label] = limit_found

        log("\n========== FINAL SUMMARY ==========", f)
        log(f"Total simultaneous connections bane: {len(connections)} (isi enctoken se)", f)
        for label, limit in results.items():
            log(f"  {label}: ~{limit} instruments tak subscribe hue, uske baad error/limit aayi", f)

        for _, kws in connections:
            try:
                kws.close()
            except Exception:
                pass

    print(f"\nPoora log yaha save hai: {LOG_PATH}")


if __name__ == "__main__":
    main()
