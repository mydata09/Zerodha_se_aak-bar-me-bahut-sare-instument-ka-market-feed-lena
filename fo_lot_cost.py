"""
F&O stocks ko unke asli (NSE cash-market) stock price * F&O lot size ke
hisaab se sort karke terminal me print karta hai — sabse kam wala sabse upar.

IMPORTANT: REST "quote/ltp" endpoint Zerodha ke internal API par 400 de raha
tha (kite_trade.py me koi built-in ltp()/quote() method bhi nahi hai). Isliye
live price WebSocket (KiteTicker) se li ja rahi hai — jaisa a.py me proven
tareeka hai — jo reliably kaam karta hai.

USAGE:
    python fo_lot_cost.py
"""

from kite_trade import KiteApp
from datetime import date
import csv
import os
import re

# ---- apna enctoken yahan daalo (a.py wala use kar sakte ho) ----
enctoken = "shXoh7f2jS5LEJv3MoqaLqDSiCng287tBXlvPGlmwRkyLCVE3V2iiQBp0c+oCJrZhFquknpV3UwmTyjcdHoIT+YIQf1dDCMactea2uQxgVPI0XoUPi32Gw=="

kite = KiteApp(enctoken=enctoken)

# Agar tumne instruments CSV already download kar rakhi hai to yahan uska
# naam daal do — isse baar baar poora instrument dump download nahi karna
# padega (wo file ~15-20 MB ki hoti hai).
LOCAL_CSV = "instruments(1).csv"

# Poori F&O stock list (NSE tradingsymbols) — isi list ke hisaab se hi
# stocks process honge, chahe CSV me root-symbol matching kuch bhi de.
# Isse guarantee milta hai ki koi bhi valid F&O stock miss na ho.
FO_STOCK_SYMBOLS = [
    "360ONE", "ABB", "ABCAPITAL", "ADANIENSOL", "ADANIENT", "ADANIGREEN",
    "ADANIPORTS", "ADANIPOWER", "ALKEM", "AMBER", "AMBUJACEM", "ANGELONE",
    "APLAPOLLO", "APOLLOHOSP", "ASHOKLEY", "ASIANPAINT", "ASTRAL", "AUBANK",
    "AUROPHARMA", "AXISBANK", "BAJAJ-AUTO", "BAJAJFINSV", "BAJAJHLDNG",
    "BAJFINANCE", "BANDHANBNK", "BANKBARODA", "BANKINDIA", "BDL", "BEL",
    "BHARATFORG", "BHARTIARTL", "BHEL", "BIOCON", "BLUESTARCO", "BOSCHLTD",
    "BPCL", "BRITANNIA", "BSE", "CAMS", "CANBK", "CDSL", "CGPOWER",
    "CHOLAFIN", "CIPLA", "COALINDIA", "COCHINSHIP", "COFORGE", "COLPAL",
    "CONCOR", "CROMPTON", "CUMMINSIND", "DABUR", "DALBHARAT", "DELHIVERY",
    "DIVISLAB", "DIXON", "DLF", "DMART", "DRREDDY", "EICHERMOT", "ETERNAL",
    "FEDERALBNK", "FORCEMOT", "FORTIS", "GAIL", "GLENMARK", "GMRAIRPORT",
    "GODFRYPHLP", "GODREJCP", "GODREJPROP", "GRASIM", "GVT&D", "HAL",
    "HAVELLS", "HCLTECH", "HDFCAMC", "HDFCBANK", "HDFCLIFE", "HEROMOTOCO",
    "HINDALCO", "HINDPETRO", "HINDUNILVR", "HINDZINC", "HYUNDAI",
    "ICICIBANK", "ICICIGI", "ICICIPRULI", "IDEA", "IDFCFIRSTB", "IEX",
    "INDHOTEL", "INDIANB", "INDIGO", "INDUSINDBK", "INDUSTOWER", "INFY",
    "INOXWIND", "IOC", "IREDA", "IRFC", "ITC", "JINDALSTEL", "JIOFIN",
    "JSWENERGY", "JSWSTEEL", "JUBLFOOD", "KALYANKJIL", "KAYNES", "KEI",
    "KFINTECH", "KOTAKBANK", "KPITTECH", "LAURUSLABS", "LICHSGFIN", "LICI",
    "LODHA", "LT", "LTF", "LTM", "LUPIN", "M&M", "MANAPPURAM", "MANKIND",
    "MARICO", "MARUTI", "MAXHEALTH", "MAZDOCK", "MCX", "MFSL", "MOTHERSON",
    "MOTILALOFS", "MPHASIS", "MUTHOOTFIN", "NAM-INDIA", "NATIONALUM",
    "NAUKRI", "NBCC", "NESTLEIND", "NHPC", "NMDC", "NTPC", "NYKAA",
    "OBEROIRLTY", "OFSS", "OIL", "ONGC", "PAGEIND", "PATANJALI", "PAYTM",
    "PERSISTENT", "PETRONET", "PFC", "PGEL", "PHOENIXLTD", "PIDILITIND",
    "PIIND", "PNB", "PNBHOUSING", "POLICYBZR", "POLYCAB", "POWERGRID",
    "POWERINDIA", "PREMIERENE", "PRESTIGE", "RADICO", "RBLBANK", "RECLTD",
    "RELIANCE", "RVNL", "SAIL", "SBICARD", "SBILIFE", "SBIN", "SHREECEM",
    "SHRIRAMFIN", "SIEMENS", "SOLARINDS", "SONACOMS", "SRF", "SUNPHARMA",
    "SUPREMEIND", "SUZLON", "SWIGGY", "TATACONSUM", "TATAELXSI",
    "TATAPOWER", "TATASTEEL", "TCS", "TECHM", "TIINDIA", "TITAN", "TMPV",
    "TORNTPHARM", "TRENT", "TVSMOTOR", "ULTRACEMCO", "UNIONBANK",
    "UNITDSPR", "UNOMINDA", "UPL", "VBL", "VEDL", "VMM", "VOLTAS",
    "WAAREEENER", "WIPRO", "YESBANK", "ZYDUSLIFE",
]


def load_nfo_futures():
    """Returns list of NFO FUT instrument dicts, from local CSV if present
    else fetches fresh from Zerodha."""
    if os.path.exists(LOCAL_CSV):
        print(f"Using local file: {LOCAL_CSV}")
        rows = []
        with open(LOCAL_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["exchange"] == "NFO" and row["instrument_type"] == "FUT":
                    rows.append({
                        "tradingsymbol": row["tradingsymbol"],
                        "name": row["name"],
                        "lot_size": int(row["lot_size"]),
                        "expiry": date.fromisoformat(row["expiry"]) if row["expiry"] else None,
                        "exchange": "NFO",
                    })
        return rows
    else:
        print("Local CSV not found, fetching fresh instrument dump...")
        instruments = kite.instruments("NFO")
        return [i for i in instruments if i["instrument_type"] == "FUT"]


FUT_SUFFIX_RE = re.compile(r'\d{2}[A-Z]{3}FUT$')


def futures_root_symbol(tradingsymbol):
    """'TATASTEEL26JULFUT' -> 'TATASTEEL'. Ye NSE equity tradingsymbol se
    seedha match karta hai. NOTE: 'name' field se match karna galat nikla —
    NFO me name='TATASTEEL' hota hai but NSE me name='TATA STEEL' (space ke
    saath), isliye wo match nahi hota tha aur 190/213 stocks skip ho rahe
    the. tradingsymbol root hamesha match hota hai."""
    return FUT_SUFFIX_RE.sub('', tradingsymbol)


def load_nse_equity_map():
    """Returns dict: {NSE tradingsymbol -> {"symbol": tradingsymbol, "token": instrument_token}}."""
    symbol_to_info = {}
    if os.path.exists(LOCAL_CSV):
        with open(LOCAL_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["exchange"] == "NSE" and row["instrument_type"] == "EQ":
                    symbol_to_info[row["tradingsymbol"]] = {
                        "symbol": row["tradingsymbol"],
                        "token": int(row["instrument_token"]),
                    }
    else:
        print("Local CSV not found, fetching fresh NSE instrument dump...")
        for i in kite.instruments("NSE"):
            if i["instrument_type"] == "EQ":
                symbol_to_info[i["tradingsymbol"]] = {
                    "symbol": i["tradingsymbol"],
                    "token": i["instrument_token"],
                }
    return symbol_to_info


def nearest_expiry_per_stock(futs):
    today = date.today()
    nearest = {}
    for inst in futs:
        if inst["expiry"] is None or inst["expiry"] < today:
            continue
        name = inst["name"]
        if name not in nearest or inst["expiry"] < nearest[name]["expiry"]:
            nearest[name] = inst
    return list(nearest.values())


def fetch_ltp_via_ticker(tokens, timeout=15):
    """tokens: list of instrument_token (int). Returns dict {token: last_price}.

    REST /quote/ltp endpoint 400 de raha tha (Zerodha internal API quirks).
    Lekin WebSocket (KiteTicker) se ticks reliably mil rahe hain (jaisa a.py
    me dikha), isliye ab wahi use kar rahe hain: sab tokens subscribe karo,
    MODE_LTP me, kuch second wait karo ticks aane ke liye, phir disconnect.
    """
    from kiteconnect import KiteTicker
    import time

    prices = {}
    user_id = kite.profile()["user_id"]
    kws = KiteTicker(api_key="AlgoTrader", access_token=enctoken + "&user_id=" + user_id)

    def on_ticks(ws, ticks):
        for t in ticks:
            prices[t["instrument_token"]] = t["last_price"]

    def on_connect(ws, response):
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_LTP, tokens)

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.connect(threaded=True)

    waited = 0
    while not kws.is_connected() and waited < timeout:
        time.sleep(0.5)
        waited += 0.5

    # Ticks aane tak wait karo (ya timeout tak)
    start = time.time()
    while time.time() - start < timeout and len(prices) < len(tokens):
        time.sleep(0.5)

    kws.close()
    print(f"DEBUG: got prices for {len(prices)}/{len(tokens)} tokens via WebSocket")
    return prices


def get_fo_lot_costs():
    futs = load_nfo_futures()
    print(f"Total NFO futures rows: {len(futs)}")
    nearest = nearest_expiry_per_stock(futs)
    print(f"Unique stocks (nearest expiry each): {len(nearest)}")

    print("Loading NSE equity symbol map...")
    nse_symbol_to_info = load_nse_equity_map()

    # Har futures row ka root symbol nikalo aur sirf FO_STOCK_SYMBOLS list
    # wale hi rakho — isse guarantee milta hai ki hum exactly wahi stocks
    # process karein jo tum chahte ho (index futures apne aap exclude ho
    # jaate hain kyunki wo list me nahi hain).
    fo_symbol_set = set(FO_STOCK_SYMBOLS)
    stocks_with_nse = []
    for inst in nearest:
        root = futures_root_symbol(inst["tradingsymbol"])
        if root not in fo_symbol_set:
            continue
        nse_info = nse_symbol_to_info.get(root)
        if nse_info:
            inst["nse_info"] = nse_info
            stocks_with_nse.append(inst)

    matched_roots = {futures_root_symbol(i["tradingsymbol"]) for i in stocks_with_nse}
    missing = fo_symbol_set - matched_roots
    print(f"Matched {len(stocks_with_nse)}/{len(FO_STOCK_SYMBOLS)} symbols from your list")
    if missing:
        print(f"Not found in NFO futures / NSE equity data: {sorted(missing)}")

    print("Fetching live stock (equity) LTP via WebSocket...")
    tokens = [i["nse_info"]["token"] for i in stocks_with_nse]
    prices_by_token = fetch_ltp_via_ticker(tokens)

    rows = []
    for inst in stocks_with_nse:
        token = inst["nse_info"]["token"]
        price = prices_by_token.get(token)
        lot_size = inst["lot_size"]
        if price and lot_size:
            rows.append({
                "name": inst["name"],
                "tradingsymbol": inst["nse_info"]["symbol"],
                "price": price,
                "lot_size": lot_size,
                "total_cost": price * lot_size,
            })

    rows.sort(key=lambda r: r["total_cost"])
    return rows


def print_table(rows):
    header = f'{"Stock":<15}{"Symbol":<20}{"Price":>12}{"Lot Size":>10}{"Total Cost":>15}'
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f'{r["name"]:<15}{r["tradingsymbol"]:<20}{r["price"]:>12.2f}{r["lot_size"]:>10}{r["total_cost"]:>15.2f}')


if __name__ == "__main__":
    rows = get_fo_lot_costs()
    print_table(rows)