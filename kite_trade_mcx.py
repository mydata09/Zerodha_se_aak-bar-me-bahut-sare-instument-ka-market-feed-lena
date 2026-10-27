import os
try:
    import requests
except ImportError:
    os.system('python -m pip install requests')
try:
    import dateutil
except ImportError:
    os.system('python -m pip install python-dateutil')
try:
    import websocket
except ImportError:
    os.system('python -m pip install websocket-client')


import json
import struct
import threading
import time
import urllib.parse
import requests
import dateutil.parser
import websocket


def get_enctoken(userid, password, twofa):
    session = requests.Session()
    response = session.post('https://kite.zerodha.com/api/login', data={
        "user_id": userid,
        "password": password
    })
    response = session.post('https://kite.zerodha.com/api/twofa', data={
        "request_id": response.json()['data']['request_id'],
        "twofa_value": twofa,
        "user_id": response.json()['data']['user_id']
    })
    enctoken = response.cookies.get('enctoken')
    if enctoken:
        return enctoken
    else:
        raise Exception("Enter valid details !!!!")


INSTRUMENTS_URL = "https://api.kite.trade/instruments"


def fetch_instruments(exchange=None, session=None, timeout=60):
    """Kite se live instrument dump download karke list[dict] return karta hai.

    exchange="MCX" dene par sirf wahi exchange ka chhota dump aata hai
    (https://api.kite.trade/instruments/MCX), warna poori list.
    Login/enctoken ki zarurat nahi -- ye public endpoint hai.
    """
    import csv
    import io
    sess = session or requests.session()
    url = f"{INSTRUMENTS_URL}/{exchange}" if exchange else INSTRUMENTS_URL
    resp = sess.get(url, timeout=timeout)
    if resp.status_code != 200 and exchange:      # per-exchange endpoint fail ho to poori list se filter
        resp = sess.get(INSTRUMENTS_URL, timeout=timeout)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.content.decode("utf-8")))
    out = []
    for row in reader:
        if exchange and row.get("exchange") != exchange:
            continue
        try:
            expiry = dateutil.parser.parse(row["expiry"]).date() if row.get("expiry") else None
            out.append({
                "instrument_token": int(row["instrument_token"]),
                "exchange_token": row["exchange_token"],
                "tradingsymbol": row["tradingsymbol"],
                "name": row["name"],
                "last_price": float(row["last_price"] or 0),
                "expiry": expiry,
                "strike": float(row["strike"] or 0),
                "tick_size": float(row["tick_size"] or 0),
                "lot_size": int(float(row["lot_size"] or 0)),
                "instrument_type": row["instrument_type"],
                "segment": row["segment"],
                "exchange": row["exchange"],
            })
        except (KeyError, ValueError, TypeError):
            continue
    return out


class KiteApp:
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"

    # Varities
    VARIETY_REGULAR = "regular"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Validity
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"

    def __init__(self, enctoken):
        self.enctoken = enctoken
        self.headers = {"Authorization": f"enctoken {self.enctoken}"}
        self.session = requests.session()
        self.root_url = "https://kite.zerodha.com/oms"
        self.session.get(self.root_url, headers=self.headers)

    def instruments(self, exchange=None):
        """Seedha Kite se instruments list (koi local CSV nahi). Har call par fresh download."""
        return fetch_instruments(exchange, session=self.session)

    def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
        params = {"from": from_date,
                  "to": to_date,
                  "interval": interval,
                  "continuous": 1 if continuous else 0,
                  "oi": 1 if oi else 0}
        lst = self.session.get(
            f"{self.root_url}/instruments/historical/{instrument_token}/{interval}", params=params,
            headers=self.headers).json()["data"]["candles"]
        records = []
        for i in lst:
            record = {"date": dateutil.parser.parse(i[0]), "open": i[1], "high": i[2], "low": i[3],
                      "close": i[4], "volume": i[5],}
            if len(i) == 7:
                record["oi"] = i[6]
            records.append(record)
        return records

    def margins(self):
        margins = self.session.get(f"{self.root_url}/user/margins", headers=self.headers).json()["data"]
        return margins

    def profile(self):
        profile = self.session.get(f"{self.root_url}/user/profile", headers=self.headers).json()["data"]
        return profile

    def orders(self):
        orders = self.session.get(f"{self.root_url}/orders", headers=self.headers).json()["data"]
        return orders

    def positions(self):
        positions = self.session.get(f"{self.root_url}/portfolio/positions", headers=self.headers).json()["data"]
        return positions

    def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None,
                    validity=None, disclosed_quantity=None, trigger_price=None, squareoff=None, stoploss=None,
                    trailing_stoploss=None, tag=None):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]
        order_id = self.session.post(f"{self.root_url}/orders/{variety}",
                                     data=params, headers=self.headers).json()["data"]["order_id"]
        return order_id

    def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None, order_type=None,
                     trigger_price=None, validity=None, disclosed_quantity=None):
        params = locals()
        del params["self"]
        for k in list(params.keys()):
            if params[k] is None:
                del params[k]

        order_id = self.session.put(f"{self.root_url}/orders/{variety}/{order_id}",
                                    data=params, headers=self.headers).json()["data"][
            "order_id"]
        return order_id

    def cancel_order(self, variety, order_id, parent_order_id=None):
        order_id = self.session.delete(f"{self.root_url}/orders/{variety}/{order_id}",
                                       data={"parent_order_id": parent_order_id} if parent_order_id else {},
                                       headers=self.headers).json()["data"]["order_id"]
        return order_id

class KiteTicker20:
    """
    Standalone websocket ticker (sync, no `kiteconnect` package needed)
    jo 5-level ke saath saath Zerodha ka 20-level market depth packet
    (492 bytes, wss://ws20.zerodha.com par) bhi decode karta hai.

    Usage:
        kws = KiteTicker20(enctoken=enctoken, user_id=user_id, depth20=True)
        kws.on_ticks = on_ticks       # def on_ticks(ws, ticks): ...
        kws.on_connect = on_connect   # def on_connect(ws): ...
        kws.connect(threaded=True)
    """

    # 20-depth ke liye asli host (Kite web isi se 20-depth leta hai,
    # browser devtools recording se confirm hua), normal 5-depth ke liye ye
    ROOT_URI_20_DEPTH = "wss://ws.zerodha.com"
    ROOT_URI_5_DEPTH = "wss://ws.kite.trade"

    # Kite web session (enctoken) ke saath ye hi api_key use hota hai
    WEB_API_KEY = "kitefront"

    MODE_LTP = "ltp"
    MODE_QUOTE = "quote"
    MODE_FULL = "full"
    # Kite web isi special mode se 20-level depth bhejta hai (browser
    # devtools recording se confirm hua) -- normal "full" sirf 5-depth
    # deta hai, "full_cas" wahi packet 20-depth ke saath deta hai.
    MODE_FULL_20_DEPTH = "full_cas"

    # instrument_token ke last byte se exchange segment nikalta hai
    _SEGMENT_DIVISOR = {
        3: 10000000.0,   # CDS
        6: 10000.0,      # BCD
    }
    _DEFAULT_DIVISOR = 100.0
    _INDICES_SEGMENT = 9

    def __init__(self, enctoken, user_id, depth20=True,
                 ping_interval=2.5, ping_timeout=1):
        self.enctoken = enctoken
        self.user_id = user_id
        self.depth20 = depth20
        self.root = self.ROOT_URI_20_DEPTH if depth20 else self.ROOT_URI_5_DEPTH
        # depth20=True hone par "full" ki jagah "full_cas" mode use hoga
        self.full_mode = self.MODE_FULL_20_DEPTH if depth20 else self.MODE_FULL
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout

        self.ws = None
        self.ws_thread = None
        self._connected = threading.Event()
        self._subscribed_tokens = {}  # token -> mode

        # callbacks (user assign kar sakta hai)
        self.on_ticks = None
        self.on_connect = None
        self.on_close = None
        self.on_error = None

    # ---------------- connection handling ----------------

    def _build_url(self):
        # enctoken me '+' '/' '=' jaise characters hote hain (base64-style);
        # URL-encode karna zaroori hai warna server ise corrupt token
        # samajh kar "Invalid auth token" bhej deta hai.
        token = urllib.parse.quote(self.enctoken, safe="")
        # Kite web ke DevTools se copy kiya gaya exact query format:
        # ?api_key=kitefront&user_id=...&enctoken=...&uid=<epoch_ms>
        # &user-agent=kite3-web&version=3.0.0
        uid = str(int(time.time() * 1000))
        return (f"{self.root}/?api_key={self.WEB_API_KEY}"
                f"&user_id={self.user_id}"
                f"&enctoken={token}"
                f"&uid={uid}"
                f"&user-agent=kite3-web"
                f"&version=3.0.0")

    # Kite web in headers ke saath connect karta hai (Cookie header ki
    # zaroorat nahi -- auth poori tarah query string me hi hota hai).
    def _headers(self):
        return [
            "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Origin: https://kite.zerodha.com",
        ]

    def connect(self, threaded=True, debug=False):
        url = self._build_url()
        if debug:
            websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(
            url,
            header=self._headers(),
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        run_kwargs = dict(ping_interval=self.ping_interval,
                          ping_timeout=self.ping_timeout)
        if threaded:
            self.ws_thread = threading.Thread(
                target=self.ws.run_forever, kwargs=run_kwargs, daemon=True)
            self.ws_thread.start()
        else:
            self.ws.run_forever(**run_kwargs)

    def is_connected(self):
        return self._connected.is_set()

    def close(self):
        if self.ws:
            self.ws.close()

    def _on_open(self, ws):
        self._connected.set()
        print(f"[KiteTicker20] Connected to {self.root}")
        # pehle se subscribe kiye gaye tokens ko reconnect ke baad resubscribe karo
        for token, mode in self._subscribed_tokens.items():
            self._send_subscribe(token, mode)
        if self.on_connect:
            self.on_connect(self)

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        print(f"[KiteTicker20] Connection closed. code={code} msg={msg}")
        if self.on_close:
            self.on_close(self, code, msg)

    def _on_error(self, ws, error):
        print(f"[KiteTicker20] Error: {error!r}")
        if self.on_error:
            self.on_error(self, error)

    # ---------------- subscribe / mode ----------------

    def subscribe(self, tokens):
        for t in tokens:
            self._subscribed_tokens.setdefault(t, self.MODE_QUOTE)
        if self.is_connected():
            self.ws.send(json.dumps({"a": "subscribe", "v": tokens}))

    def set_mode(self, mode, tokens):
        for t in tokens:
            self._subscribed_tokens[t] = mode
        if self.is_connected():
            self.ws.send(json.dumps({"a": "mode", "v": [mode, tokens]}))

    def unsubscribe(self, tokens):
        for t in tokens:
            self._subscribed_tokens.pop(t, None)
        if self.is_connected():
            self.ws.send(json.dumps({"a": "unsubscribe", "v": tokens}))

    def _send_subscribe(self, token, mode):
        self.ws.send(json.dumps({"a": "subscribe", "v": [token]}))
        self.ws.send(json.dumps({"a": "mode", "v": [mode, [token]]}))

    # ---------------- binary tick parsing ----------------

    @staticmethod
    def _unpack_int(data, start, end, fmt="I"):
        return struct.unpack(">" + fmt, data[start:end])[0]

    @classmethod
    def _split_packets(cls, data):
        if len(data) < 2:
            return []
        packets = []
        num_packets = cls._unpack_int(data, 0, 2, "H")
        j = 2
        for _ in range(num_packets):
            length = cls._unpack_int(data, j, j + 2, "H")
            packets.append(data[j + 2: j + 2 + length])
            j += 2 + length
        return packets

    @classmethod
    def _divisor_for(cls, instrument_token):
        segment = instrument_token & 0xFF
        return cls._SEGMENT_DIVISOR.get(segment, cls._DEFAULT_DIVISOR), segment

    @classmethod
    def _parse_packet(cls, packet):
        tick = {}
        length = len(packet)
        if length < 4:
            return tick

        instrument_token = cls._unpack_int(packet, 0, 4)
        divisor, segment = cls._divisor_for(instrument_token)
        tick["instrument_token"] = instrument_token
        tick["tradable"] = segment != cls._INDICES_SEGMENT

        if length == 8:
            tick["mode"] = cls.MODE_LTP
            tick["last_price"] = cls._unpack_int(packet, 4, 8) / divisor
            return tick

        if length in (28, 32):
            tick["mode"] = cls.MODE_QUOTE if length == 28 else cls.MODE_FULL
            tick["last_price"] = cls._unpack_int(packet, 4, 8) / divisor
            tick["ohlc"] = {
                "high": cls._unpack_int(packet, 8, 12) / divisor,
                "low": cls._unpack_int(packet, 12, 16) / divisor,
                "open": cls._unpack_int(packet, 16, 20) / divisor,
                "close": cls._unpack_int(packet, 20, 24) / divisor,
            }
            return tick

        if length in (44, 184):
            tick["mode"] = cls.MODE_QUOTE if length == 44 else cls.MODE_FULL
            tick["last_price"] = cls._unpack_int(packet, 4, 8) / divisor
            tick["last_traded_quantity"] = cls._unpack_int(packet, 8, 12)
            tick["average_traded_price"] = cls._unpack_int(packet, 12, 16) / divisor
            tick["volume_traded"] = cls._unpack_int(packet, 16, 20)
            tick["total_buy_quantity"] = cls._unpack_int(packet, 20, 24)
            tick["total_sell_quantity"] = cls._unpack_int(packet, 24, 28)
            tick["ohlc"] = {
                "open": cls._unpack_int(packet, 28, 32) / divisor,
                "high": cls._unpack_int(packet, 32, 36) / divisor,
                "low": cls._unpack_int(packet, 36, 40) / divisor,
                "close": cls._unpack_int(packet, 40, 44) / divisor,
            }
            if length == 184:
                tick["oi"] = cls._unpack_int(packet, 48, 52)
                tick["oi_day_high"] = cls._unpack_int(packet, 52, 56)
                tick["oi_day_low"] = cls._unpack_int(packet, 56, 60)
                # 5-level market depth
                depth = {"buy": [], "sell": []}
                for i, p in enumerate(range(64, length, 12)):
                    depth["sell" if i >= 5 else "buy"].append({
                        "quantity": cls._unpack_int(packet, p, p + 4),
                        "price": cls._unpack_int(packet, p + 4, p + 8) / divisor,
                        "orders": cls._unpack_int(packet, p + 8, p + 10, "H"),
                    })
                tick["depth"] = depth
            return tick

        if length == 492:
            # 20-level market depth packet (sirf ws20.zerodha.com se aata hai)
            tick["mode"] = cls.MODE_FULL
            tick["last_price"] = cls._unpack_int(packet, 4, 8) / divisor
            depth20 = {"buy": [], "sell": []}
            for i, p in enumerate(range(12, length, 12)):
                depth20["sell" if i >= 20 else "buy"].append({
                    "quantity": cls._unpack_int(packet, p, p + 4),
                    "price": cls._unpack_int(packet, p + 4, p + 8) / divisor,
                    "orders": cls._unpack_int(packet, p + 8, p + 12),
                })
            tick["depth20"] = depth20
            return tick

        # Naya/anjaana packet format (jaise "full_cas" mode ka) -- isse
        # samajhne ke liye length aur raw hex diagnostic ke roop me bhej
        # rahe hain taaki hum sahi byte-offsets figure out kar sakein.
        tick["mode"] = "unknown"
        tick["_packet_length"] = length
        if length >= 8:
            try:
                tick["last_price_guess"] = cls._unpack_int(packet, 4, 8) / divisor
            except Exception:
                pass
        tick["_hex"] = packet.hex()
        return tick

    def _on_message(self, ws, message):
        if not isinstance(message, (bytes, bytearray)):
            return  # heartbeat ya text/JSON message, tick nahi
        packets = self._split_packets(message)
        if not packets:
            return
        ticks = [self._parse_packet(p) for p in packets if len(p) > 0]
        if ticks and self.on_ticks:
            self.on_ticks(self, ticks)