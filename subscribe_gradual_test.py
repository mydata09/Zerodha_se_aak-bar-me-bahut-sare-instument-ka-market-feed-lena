"""
Ye script instruments.csv se saare instrument_token uthata hai aur unhe
dheere-dheere (batch-by-batch) KiteTicker20 par subscribe karta hai, taaki
pata chale ki kitne instruments tak koi error/disconnect nahi aata.

Zerodha ki official limit: ek websocket connection par max ~3000 instruments
(https://kite.trade/forum -> "3,000 instruments per connection, 9,000 per api_key").
Is script me MAX_TOKENS variable se aap ye cap control kar sakte ho.

Kaise chalayein:
    python subscribe_gradual_test.py
"""

import csv
import json
import sys
import time

from kite_trade import KiteApp, KiteTicker20

# ---------------- CONFIG (apni zaroorat ke hisaab se badlo) ----------------

ENCTOKEN = "KnEXeW8bCXY+oth9dUiSK364jwWJoYzkDiybjW88rFUuFRTk1GiPvw8ltb0zBUE50IAzfeK6QL1w1HIZgVP/x1otsBpgFC9knv/qSE9pvoKZFHHe9ySIZA=="

CSV_PATH = "instruments.csv"          # instrument_token wali CSV file
LOG_PATH = "subscribe_test_log.txt"   # progress/error log yaha save hoga

BATCH_SIZE = 200                      # ek baar me kitne naye tokens subscribe karein
DELAY_BETWEEN_BATCHES = 2             # (seconds) har batch ke baad kitna rukein
MAX_TOKENS = 3000                     # Zerodha ki per-connection safe limit
FINAL_OBSERVE_SECONDS = 20            # last batch ke baad kitni der aur dekhein

USE_DEPTH20 = True                    # enctoken (web session) sirf isi endpoint (wss://ws.zerodha.com)
                                       # par valid hota hai -- False karne par "Invalid auth token"
                                       # aayega kyunki wss://ws.kite.trade ko access_token chahiye.


# ---------------------------------------------------------------------------


def load_tokens(csv_path):
    """CSV se instrument_token column nikalta hai (int list return karta hai)."""
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
    """
    KiteTicker20 ka subclass -- asli class sirf BINARY tick packets handle
    karti hai (_on_message me text/JSON messages ignore ho jate hain).
    Zerodha subscription/limit errors TEXT frame (JSON) ke roop me aate hain,
    isliye unhe pakadne ke liye _on_message override kiya gaya hai.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.errors = []

    def _on_message(self, ws, message):
        if isinstance(message, (bytes, bytearray)):
            # normal binary tick -> parent class handle karegi
            return super()._on_message(ws, message)

        # text frame -> server se error/status JSON
        try:
            data = json.loads(message)
        except Exception:
            data = message

        if isinstance(data, dict) and data.get("type") == "error":
            err = data.get("data", data)
            print(f"[SERVER ERROR] {err}")
            self.errors.append(err)
        else:
            print(f"[TEXT MSG] {data}")


def log(msg, f):
    print(msg)
    f.write(msg + "\n")
    f.flush()


def main():
    tokens = load_tokens(CSV_PATH)
    print(f"CSV me total {len(tokens)} instrument tokens mile.")

    if not tokens:
        print("Koi valid instrument_token nahi mila, CSV check karo.")
        sys.exit(1)

    if len(tokens) > MAX_TOKENS:
        print(
            f"NOTE: CSV me {len(tokens)} tokens hai, lekin ek websocket "
            f"connection par safe limit {MAX_TOKENS} maani gayi hai."
        )
        print(f"Sirf pehle {MAX_TOKENS} tokens is run me test honge.")
        tokens = tokens[:MAX_TOKENS]

    kite = KiteApp(enctoken=ENCTOKEN)
    user_id = kite.profile()["user_id"]
    print(f"Login OK, user_id={user_id}")

    kws = TestTicker(enctoken=ENCTOKEN, user_id=user_id, depth20=USE_DEPTH20)

    state = {"closed": False, "close_info": None}

    def on_connect(ws):
        print("WebSocket : Connected")

    def on_close(ws, code, msg):
        state["closed"] = True
        state["close_info"] = (code, msg)

    def on_error(ws, error):
        print(f"[on_error] {error!r}")

    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_ticks = lambda ws, ticks: None  # is test me actual ticks se matlab nahi

    kws.connect(threaded=True)

    for _ in range(10):
        if kws.is_connected():
            break
        time.sleep(1)
    else:
        print("Connect nahi hua - upar wala [KiteTicker20] error/close message dekho.")
        sys.exit(1)

    with open(LOG_PATH, "w", encoding="utf-8") as f:
        log(
            f"=== Subscription test start: {len(tokens)} tokens, "
            f"batch_size={BATCH_SIZE}, delay={DELAY_BETWEEN_BATCHES}s ===",
            f,
        )

        subscribed_count = 0
        stopped_early = False

        for i in range(0, len(tokens), BATCH_SIZE):
            if state["closed"]:
                log(
                    f"Connection beech me band ho gaya (subscribed ~{subscribed_count} "
                    f"tak). close_info={state['close_info']}",
                    f,
                )
                stopped_early = True
                break

            batch = tokens[i : i + BATCH_SIZE]
            try:
                kws.subscribe(batch)
                kws.set_mode(kws.MODE_QUOTE, batch)  # halka mode, testing ke liye
            except Exception as e:
                log(f"Batch {i}-{i + len(batch)} par exception aayi: {e!r}", f)
                stopped_early = True
                break

            subscribed_count += len(batch)
            log(f"Subscribed so far: {subscribed_count} (is batch me: {len(batch)})", f)

            time.sleep(DELAY_BETWEEN_BATCHES)

            if kws.errors:
                log(f"Server se errors mile (total {len(kws.errors)}): {kws.errors[-3:]}", f)
                log(f"Yahi ruk rahe hain. Aakhri safe count ~ {subscribed_count - len(batch)}", f)
                stopped_early = True
                break

        if not stopped_early:
            log(f"Saare {subscribed_count} tokens bina kisi error ke subscribe ho gaye.", f)

        log(
            f"=== Batches khatam, {FINAL_OBSERVE_SECONDS} sec aur observe kar rahe hain "
            f"(delayed error/disconnect check) ===",
            f,
        )
        time.sleep(FINAL_OBSERVE_SECONDS)

        if state["closed"]:
            log(f"Connection final wait ke dauraan band ho gaya: {state['close_info']}", f)
        elif kws.errors:
            log(f"Total {len(kws.errors)} server errors observe hue puri run me.", f)
        else:
            log("Koi error ya disconnect observe nahi hua - subscription stable raha.", f)

        log(f"=== FINAL: {subscribed_count} tokens tak test kiya gaya ===", f)

    kws.close()
    print(f"\nPoora log yaha save hai: {LOG_PATH}")


if __name__ == "__main__":
    main()