import ccxt
import pandas as pd
import os
import csv
import math
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import requests
from dotenv import load_dotenv

from sfp_signals import compute_signals

# ── Load credentials ──────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
API_KEY            = os.getenv("API_KEY")
API_SECRET         = os.getenv("API_SECRET")
API_PASSWORD       = os.getenv("API_PASSWORD")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, API_KEY, API_SECRET, API_PASSWORD]):
    raise SystemExit("❌ Missing credentials in .env")

# ── Bot Configuration ─────────────────────────────────────────────────────────
SYMBOL          = "BTCUSDT"
TIMEFRAME       = "30m"
LEVERAGE        = 10
CANDLE_LIMIT    = 900       # needs enough history for MA_PERIOD (644) + buffer
POLL_INTERVAL   = 60        # seconds between each loop tick
LOG_FILE        = "trade_log.csv"
APP_LOG         = "sfp_bot.log"
DAILY_HOUR_UTC  = 0         # hour to send daily report (UTC)
DAILY_MIN_UTC   = 5

# ── Exchange ──────────────────────────────────────────────────────────────────
exchange = ccxt.bitget({
    "apiKey":        API_KEY,
    "secret":        API_SECRET,
    "password":      API_PASSWORD,
    "enableRateLimit": True,
    "options":       {"defaultType": "swap"},
})
exchange.load_markets()


# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("sfp_bot")
logger.setLevel(logging.INFO)

fh = RotatingFileHandler(APP_LOG, maxBytes=5_000_000, backupCount=3)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

# ── Trade log CSV ─────────────────────────────────────────────────────────────
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow([
            "timestamp", "symbol", "side", "price", "qty",
            "usdt_value", "account_balance", "pnl_usdt", "reason"
        ])

# ── Helpers ───────────────────────────────────────────────────────────────────
def tg_send(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": msg,
            "parse_mode": "HTML"
        }, timeout=10)
    except Exception:
        logger.exception("Telegram send failed")


def fetch_df() -> pd.DataFrame | None:
    try:
        ohlcv = exchange.fetch_ohlcv(SYMBOL, TIMEFRAME, limit=CANDLE_LIMIT)
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df = df.set_index("time")
        return df
    except Exception:
        logger.exception("fetch_ohlcv failed")
        return None


def get_position() -> dict | None:
    try:
        positions = exchange.fetch_positions()
        for p in positions:
            sym = p.get("symbol") or p.get("info", {}).get("symbol", "")
            size = abs(float(p.get("contracts") or p.get("size") or 0))
            if sym == SYMBOL and size > 0:
                return p
    except Exception:
        logger.exception("fetch_positions failed")
    return None


def get_available_usdt() -> float:
    try:
        b = exchange.fetch_balance({"type": "future"})
        usdt = b.get("USDT", {})
        free = usdt.get("free") if isinstance(usdt, dict) else b.get("free", {}).get("USDT", 0)
        return float(free or 0)
    except Exception:
        logger.exception("fetch_balance failed")
        return 0.0


def get_total_balance() -> float:
    try:
        return float(exchange.fetch_balance()["total"].get("USDT", 0))
    except Exception:
        logger.exception("fetch_balance (total) failed")
        return 0.0


def safe_qty(qty: float) -> float:
    """Floor qty to exchange precision and check minimum size."""
    try:
        m = exchange.markets.get(SYMBOL)
        if not m:
            exchange.load_markets()
            m = exchange.markets.get(SYMBOL)

        # precision -> step size (fallback to a sensible default)
        prec = None
        if isinstance(m, dict):
            prec = (m.get("precision") or {}).get("amount")
        step = 10 ** -prec if (prec is not None and prec > 0) else 0.0001

        # floor quantity to precision step
        qty = math.floor(qty / step) * step if step > 0 else qty

        # determine minimum size from market limits; fallback to step
        min_sz = 0.0
        if isinstance(m, dict):
            limits = m.get("limits") or {}
            amount_limits = limits.get("amount") if isinstance(limits, dict) else None
            if isinstance(amount_limits, dict):
                min_val = amount_limits.get("min")
                if min_val is not None:
                    try:
                        min_sz = float(min_val)
                    except Exception:
                        min_sz = 0.0
        if not min_sz:
            min_sz = step

        return qty if qty >= min_sz else 0.0
    except Exception:
        logger.exception("safe_qty failed")
        return 0.0


def place_order(side: str, qty: float, retries: int = 3):
    """Place a market order using CCXT unified `create_order` with basic params and retries."""
    for attempt in range(retries):
        try:
            params = {
                # use unified margin mode value for CCXT: 'cross' or 'isolated'
                "marginMode": "cross",
            }

            # qty is passed as the amount (contracts/base units) expected by CCXT for this market
            order = exchange.create_order(SYMBOL, 'market', side.lower(), qty, None, params)
            logger.info(f"{side} order placed: {order}")
            return order
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{retries} failed: {e}")
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


print("Fetching available USDT...")
available_usdt = get_available_usdt()
print(f"Available USDT: {available_usdt:.2f}")
print( ' fetch total balance...' )
total_balance = get_total_balance()
print(f"Total Balance: {total_balance:.2f}")

# Place SELL order to open short position
# Convert USDT allocation to base-asset amount using current price and leverage
try:
    ticker = exchange.fetch_ticker(SYMBOL)
    last_price = float(ticker.get('last') or ticker.get('close') or 0)
except Exception:
    logger.exception("fetch_ticker failed, defaulting price to 0")
    last_price = 0.0

usdt_alloc = available_usdt * 0.3
if last_price and usdt_alloc > 0:
    # position notional = usdt_alloc / price
    base_amount = (usdt_alloc ) / last_price
else:
    base_amount = 0.0

qty = safe_qty(base_amount)
if qty <= 0:
    logger.info("calculated qty is too small, skipping order placement")
else:
    try:
        resp = place_order("SELL", qty)
        logger.info("SELL order placed (opening short position): %s", resp)

        # Attempt to fetch executed order details, then fallback to position info
        order_id = None
        try:
            order_id = resp.get('id') or resp.get('info', {}).get('orderId')
        except Exception:
            order_id = None

        executed_amount = 0.0
        executed_price = 0.0

        # Give the exchange a short moment to register fills
        for _ in range(6):
            try:
                if order_id:
                    o = exchange.fetch_order(order_id, SYMBOL)
                    filled = o.get('filled') or o.get('amount') or 0
                    avg = o.get('average') or o.get('price') or 0
                    try:
                        executed_amount = float(filled)
                    except Exception:
                        executed_amount = 0.0
                    try:
                        executed_price = float(avg)
                    except Exception:
                        executed_price = 0.0
                    if executed_amount > 0 and executed_price > 0:
                        break
            except Exception:
                # fetch_order may fail initially; ignore and retry
                pass

            # fallback to checking current position
            try:
                pos = get_position()
                if pos:
                    # position info may include contracts/size and openAvgPrice
                    info = pos.get('info') or {}
                    size = pos.get('contracts') or pos.get('size') or info.get('openTotalPos') or info.get('openTotal')
                    price = info.get('openAvgPrice') or info.get('openAvg') or info.get('open_price')
                    try:
                        executed_amount = float(size or 0)
                    except Exception:
                        executed_amount = 0.0
                    try:
                        executed_price = float(price or executed_price)
                    except Exception:
                        pass
                    if executed_amount > 0:
                        break
            except Exception:
                pass

            time.sleep(1)

        notional = executed_amount * executed_price if executed_amount and executed_price else 0.0
        msg = f"Opened SELL position: amount={executed_amount:.6f} BTC at price={executed_price:.2f} USDT (notional={notional:.2f} USDT)"
        print(msg)
        logger.info(msg)
        try:
            tg_send(msg)
        except Exception:
            logger.exception("tg_send failed")

    except Exception:
        logger.exception("Order placement failed")