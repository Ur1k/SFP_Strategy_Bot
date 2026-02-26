import ccxt
import pandas as pd
import os
import csv
import math
import time
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date
import requests
from dotenv import load_dotenv

from sfp_signals import compute_signals, PIVOT_WINDOW

# ── Base directory (ensure files live next to this script) ────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Credentials ───────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
API_KEY            = os.getenv("API_KEY")
API_SECRET         = os.getenv("API_SECRET")
API_PASSWORD       = os.getenv("API_PASSWORD")

if not all([TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, API_KEY, API_SECRET, API_PASSWORD]):
    raise SystemExit("❌ Missing credentials in .env")

# ── Configuration ─────────────────────────────────────────────────────────────
SYMBOL         = "BTCUSDT"
TIMEFRAME      = "30m"
LEVERAGE       = 10
CANDLE_LIMIT   = 900
POLL_INTERVAL  = 60
APP_LOG        = os.path.join(BASE_DIR, "sfp_bot.log")
DAILY_HOUR_UTC = 0
DAILY_MIN_UTC  = 5

# ── Single persistent file: trade_log.csv ────────────────────────────────────
LOG_FILE = os.path.join(BASE_DIR, "trade_log.csv")
LOG_COLS = [
    "timestamp", "symbol", "side",
    "price", "qty", "usdt_value", "account_balance", "pnl_usdt", "reason",
    "entry_price", "entry_candle_ts", "invalidation", "tp",
    "last_entry_candle_ts", "last_daily_date", "tp_order_id",
]

if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="") as f:
        csv.writer(f).writerow(LOG_COLS)

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger("sfp_bot")
logger.setLevel(logging.INFO)
fh = RotatingFileHandler(APP_LOG, maxBytes=5_000_000, backupCount=3)
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)

# ── Exchange ──────────────────────────────────────────────────────────────────
exchange = ccxt.bitget({
    "apiKey":          API_KEY,
    "secret":          API_SECRET,
    "password":        API_PASSWORD,
    "enableRateLimit": True,
    "options":         {"defaultType": "swap"},
})
exchange.load_markets()

try:
    exchange.set_leverage(LEVERAGE, SYMBOL, params={"marginCoin": "USDT"})
    logger.info("Leverage set to %sx for %s", LEVERAGE, SYMBOL)
except Exception as e:
    logger.warning("Could not set leverage: %s", e)

try:
    exchange.set_margin_mode("cross", SYMBOL, params={"marginCoin": "USDT"})
    logger.info("Margin mode set to cross for %s", SYMBOL)
except Exception as e:
    logger.warning("Could not set margin mode: %s", e)


# ── State ─────────────────────────────────────────────────────────────────────
class State:
    def __init__(self):
        self.entry_price:          float | None = None
        self.invalidation:         float | None = None
        self.tp:                   float | None = None
        self.entry_candle_ts:      int   | None = None
        self.last_entry_candle_ts: int   | None = None
        self.last_daily_date:      str         = ""
        self.tp_order_id:          str   | None = None

    def _row(self, side: str,
             price=None, qty=None, usdt_value=None,
             balance=None, pnl=None, reason=None) -> list:
        return [
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), SYMBOL, side,
            _fmt(price), _fmt(qty), _fmt(usdt_value), _fmt(balance),
            _fmt(pnl), reason or "",
            _fmt(self.entry_price),
            self.entry_candle_ts      if self.entry_candle_ts      is not None else "",
            _fmt(self.invalidation),
            _fmt(self.tp),
            self.last_entry_candle_ts if self.last_entry_candle_ts is not None else "",
            self.last_daily_date,
            self.tp_order_id or "",
        ]

    def save(self):
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow(self._row("BOT_STATE"))
        logger.debug("BOT_STATE written")

    def write_trade(self, side: str, price: float, qty: float,
                    pnl: float, reason: str, balance: float):
        row = self._row(side,
                        price=price, qty=qty, usdt_value=price * qty,
                        balance=balance, pnl=pnl, reason=reason)
        with open(LOG_FILE, "a", newline="") as f:
            csv.writer(f).writerow(row)
        logger.info("Trade: %s @ %.4f qty=%.6f pnl=%.2f [%s]", side, price, qty, pnl, reason)

    def load(self):
        if not os.path.exists(LOG_FILE):
            return
        try:
            df = pd.read_csv(LOG_FILE, dtype=str)
            if df.empty:
                return

            state_rows = df[df["side"] == "BOT_STATE"]
            trade_rows = df[df["side"].isin(["LONG_OPEN", "LONG_CLOSE", "TP_ORDER"])]
            source = (state_rows.iloc[-1] if not state_rows.empty
                      else trade_rows.iloc[-1] if not trade_rows.empty else None)
            if source is not None:
                self.last_entry_candle_ts = _int(source.get("last_entry_candle_ts"))
                self.last_daily_date      = str(source.get("last_daily_date") or "")

            opens  = df[df["side"] == "LONG_OPEN"]
            closes = df[df["side"] == "LONG_CLOSE"]
            if opens.empty:
                return
            last_open_ts  = opens["timestamp"].iloc[-1]
            last_close_ts = closes["timestamp"].iloc[-1] if not closes.empty else ""
            if last_open_ts > last_close_ts:
                last = opens.iloc[-1]
                self.entry_price     = _float(last.get("entry_price"))
                self.invalidation    = _float(last.get("invalidation"))
                self.tp              = _float(last.get("tp"))
                self.entry_candle_ts = _int(last.get("entry_candle_ts"))
                tp_rows = df[(df["side"] == "TP_ORDER") & (df["timestamp"] > last_open_ts)]
                if not tp_rows.empty:
                    self.tp_order_id = str(tp_rows["tp_order_id"].iloc[-1] or "").strip() or None
                logger.info(
                    "State loaded — entry=%.4f stop=%s tp=%s tp_order_id=%s",
                    self.entry_price or 0, self.invalidation, self.tp, self.tp_order_id
                )
        except Exception:
            logger.exception("Failed to load state from CSV — starting fresh")

    def clear_position(self):
        self.entry_price     = None
        self.invalidation    = None
        self.tp              = None
        self.entry_candle_ts = None
        self.tp_order_id     = None
        self.save()


def _fmt(v) -> str:
    if v is None:
        return ""
    try:
        return str(round(float(v), 6))
    except (TypeError, ValueError):
        return str(v)


def _float(v) -> float | None:
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _int(v) -> int | None:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


state = State()
state.load()


# ── Helpers ───────────────────────────────────────────────────────────────────
def tg_send(msg: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"
        }, timeout=10)
    except Exception:
        logger.exception("Telegram send failed")


def fetch_df() -> pd.DataFrame | None:
    try:
        ohlcv = exchange.fetch_ohlcv(SYMBOL, TIMEFRAME, limit=CANDLE_LIMIT)
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df.set_index("time")
    except Exception:
        logger.exception("fetch_ohlcv failed")
        return None


def symbols_match(exchange_sym: str, target: str) -> bool:
    return target.upper() in exchange_sym.upper()


def get_position() -> dict | None:
    try:
        for p in exchange.fetch_positions():
            sym  = p.get("symbol") or p.get("info", {}).get("symbol", "")
            size = (p.get("contracts") or p.get("size") or
                    p.get("info", {}).get("size") or p.get("info", {}).get("total") or 0)
            if symbols_match(sym, SYMBOL) and abs(float(size)) > 0:
                return p
    except Exception:
        logger.exception("fetch_positions failed")
    return None


def extract_entry_price(pos: dict, fallback: float) -> float:
    info = pos.get("info", {})
    for v in [pos.get("entryPrice"), pos.get("markPrice"),
              info.get("entryPrice"), info.get("openPriceAvg"),
              info.get("openAvgPrice"), info.get("avgPrice"),
              info.get("averageOpenPrice")]:
        try:
            if v and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            pass
    return fallback


def extract_fill_price(res: dict, fallback: float) -> float:
    info = res.get("info", {})
    for v in [res.get("average"), res.get("price"),
              info.get("priceAvg"), info.get("fillPrice"),
              info.get("avgPrice"), info.get("entryPrice"),
              info.get("openAvgPrice")]:
        try:
            if v and float(v) > 0:
                return float(v)
        except (TypeError, ValueError):
            pass
    return fallback


def get_available_usdt() -> float:
    try:
        b    = exchange.fetch_balance({"type": "future"})
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
        return 0.0


def safe_qty(usdt_amount: float, price: float) -> float:
    """
    usdt_amount is the notional we want (≈ 99% of free USDT).
    With 10x leverage, margin ≈ 10% of that notional.
    """
    try:
        m             = exchange.markets.get(SYMBOL) or exchange.load_markets()[SYMBOL]
        contract_sz   = float(m.get("contractSize") or 1)
        min_contracts = float(m["limits"]["amount"]["min"] or contract_sz)
        contracts     = math.floor(usdt_amount / (price * contract_sz))
        qty           = contracts * contract_sz
        if qty < min_contracts:
            logger.warning("Qty %.6f below minimum %.6f", qty, min_contracts)
            return 0.0
        return round(qty, 8)
    except Exception:
        logger.exception("safe_qty failed")
        return 0.0


def place_order(side: str, qty: float, retries: int = 3):
    """Market order with NetworkError duplicate-fill guard."""
    params = {"marginMode": "cross", "marginCoin": "USDT"}
    for attempt in range(retries):
        try:
            if side == "BUY":
                res = exchange.create_market_buy_order(SYMBOL, qty, params=params)
            else:
                res = exchange.create_market_sell_order(
                    SYMBOL, qty, params={"reduceOnly": True, **params})
            filled    = float(res.get("filled") or 0)
            remaining = float(res.get("remaining") or 0)
            if remaining > 0:
                logger.warning("Partial fill: filled=%.6f remaining=%.6f", filled, remaining)
                tg_send(f"⚠️ <b>Partial fill {side}</b>\nFilled:{filled} Remaining:{remaining}")
            return res
        except ccxt.NetworkError as e:
            logger.warning("NetworkError attempt %d: %s", attempt + 1, e)
            time.sleep(3)
            pos = get_position()
            if side == "BUY" and pos:
                return {"average": None, "filled": qty, "remaining": 0, "_silent_fill": True}
            if side == "SELL" and not pos:
                return {"average": None, "filled": qty, "remaining": 0, "_silent_fill": True}
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def place_tp_limit_order(qty: float, tp_price: float, retries: int = 3) -> str | None:
    """Place reduce-only TP limit order at tp_price."""
    try:
        m          = exchange.markets.get(SYMBOL) or exchange.load_markets()[SYMBOL]
        price_prec = m.get("precision", {}).get("price")
        if price_prec is not None:
            tick     = 10 ** -price_prec
            tp_price = math.floor(tp_price / tick) * tick
            tp_price = round(tp_price, price_prec)
    except Exception:
        logger.warning("Could not determine price precision — using raw tp_price")

    params = {
        "marginMode": "cross",
        "marginCoin": "USDT",
        "reduceOnly": True,
    }
    for attempt in range(retries):
        try:
            res = exchange.create_limit_sell_order(SYMBOL, qty, tp_price, params=params)
            order_id = str(res.get("id") or res.get("info", {}).get("orderId", ""))
            logger.info("TP limit order placed: id=%s price=%.4f qty=%.6f",
                        order_id, tp_price, qty)
            return order_id
        except Exception as e:
            if attempt == retries - 1:
                logger.exception("TP limit order failed after %d attempts", retries)
                tg_send(f"🚨 <b>TP limit order FAILED</b>\nPrice: ${tp_price:,.2f}\nError: {e}")
                return None
            time.sleep(2 ** attempt)


def cancel_tp_order(order_id: str | None):
    """Cancel TP limit order if it exists."""
    if not order_id:
        return
    try:
        exchange.cancel_order(order_id, SYMBOL)
        logger.info("TP limit order cancelled: id=%s", order_id)
    except ccxt.OrderNotFound:
        logger.info("TP order %s already filled or cancelled", order_id)
    except Exception:
        logger.exception("Failed to cancel TP order %s", order_id)
        tg_send(f"⚠️ Could not cancel TP order {order_id} — check manually")


def tp_order_still_open(order_id: str | None) -> bool:
    """
    Return True if TP order is still open/partial.
    Use CCXT unified statuses; allowlist of open statuses.
    """
    if not order_id:
        return False
    try:
        o = exchange.fetch_order(order_id, SYMBOL)
        status = str(o.get("status", "")).lower()
        open_statuses = {"open"}  # CCXT unified
        if status in open_statuses:
            return True
        # Extra safety: check raw Bitget status if present
        info_status = str(o.get("info", {}).get("status", "")).lower()
        raw_open = {"init", "new", "partially_filled"}
        return info_status in raw_open
    except ccxt.OrderNotFound:
        return False
    except Exception:
        logger.exception("fetch_order failed for %s", order_id)
        return False


def recover_levels_from_entry_candle(df_closed: pd.DataFrame) -> bool:
    if state.entry_candle_ts is None:
        return False
    mask = df_closed["ts"] == state.entry_candle_ts
    if not mask.any():
        logger.warning("Entry candle ts=%s not in df (rolled off)", state.entry_candle_ts)
        return False
    pos_idx            = df_closed.index.get_loc(df_closed.index[mask][0])
    state.invalidation = float(df_closed["low"].iloc[pos_idx])
    df_up              = df_closed.iloc[: pos_idx + 1]
    state.tp           = float(
        df_up["high"].rolling(PIVOT_WINDOW, min_periods=1).max().shift(1).iloc[-1]
    )
    logger.info("Levels from entry candle — stop=%.4f  tp=%.4f", state.invalidation, state.tp)
    return True


def send_daily_report(price: float):
    today_str = date.today().strftime("%Y-%m-%d")
    if state.last_daily_date == today_str:
        return
    pos   = get_position()
    lines = [f"📊 <b>Daily Report</b> — {today_str}",
             f"Symbol: {SYMBOL}", f"Price:  ${price:,.2f}"]
    if pos:
        size = abs(float(pos.get("contracts") or pos.get("size") or
                         pos.get("info", {}).get("size") or
                         pos.get("info", {}).get("total") or 0))
        ent  = extract_entry_price(pos, state.entry_price or price)
        pnl  = (price - ent) * size
        lines += [
            "📌 <b>Open Position</b>",
            f"Entry: ${ent:,.2f}", f"Size:  {size} contracts", f"PnL:   ${pnl:,.2f}",
            f"Stop:  ${state.invalidation:,.2f}" if state.invalidation else "Stop:  ⚠️ not set",
            f"TP:    ${state.tp:,.2f}"           if state.tp           else "TP:    ⚠️ not set",
            f"TP order: {state.tp_order_id}"     if state.tp_order_id  else "TP order: ⚠️ not placed",
        ]
    else:
        lines.append("📭 No open position")
    tg_send("\n".join(lines))
    state.last_daily_date = today_str
    state.save()


# ── Startup validation ────────────────────────────────────────────────────────
if state.entry_price is not None:
    pos_check = get_position()
    if pos_check is None:
        logger.warning("CSV has open position but exchange shows none — clearing state")
        cancel_tp_order(state.tp_order_id)
        tg_send("⚠️ <b>Stale state cleared</b>\nCSV had open position but exchange shows none.")
        state.clear_position()
    else:
        if state.tp_order_id and not tp_order_still_open(state.tp_order_id):
            logger.warning("TP order %s is no longer open — checking if position closed",
                           state.tp_order_id)
            size = abs(float(
                pos_check.get("contracts") or pos_check.get("size") or
                pos_check.get("info", {}).get("size") or
                pos_check.get("info", {}).get("total") or 0
            ))
            if size > 0 and state.tp:
                new_id = place_tp_limit_order(size, state.tp)
                state.tp_order_id = new_id
                state.save()
                tg_send(
                    f"♻️ <b>TP order replaced on restart</b>\n"
                    f"New order: {new_id}\nTP price: ${state.tp:,.2f}"
                )
        tg_send(
            f"♻️ <b>Bot restarted — resuming position</b>\n"
            f"Entry:    ${state.entry_price:,.2f}\n"
            f"Stop:     ${state.invalidation:,.2f}\n"
            f"TP:       ${state.tp:,.2f}\n"
            f"TP order: {state.tp_order_id or '⚠️ not set'}\n"
            f"<i>Exact levels from trade_log.csv</i>"
        )

tg_send(
    f"🚀 <b>SFP Bot Started</b>\n"
    f"Symbol: {SYMBOL}  |  TF: {TIMEFRAME}  |  {LEVERAGE}x Cross"
)
logger.info("Bot started — %s %s %sx cross", SYMBOL, TIMEFRAME, LEVERAGE)

# ── Main loop ─────────────────────────────────────────────────────────────────
while True:
    try:
        df = fetch_df()
        if df is None or len(df) < 100:
            time.sleep(POLL_INTERVAL)
            continue

        current_candle_ts = int(df["ts"].iloc[-1])
        df_closed         = df.iloc[:-1]
        price             = float(df_closed["close"].iloc[-1])
        sig               = compute_signals(df_closed)
        pos               = get_position()

        # ── Manual close detection: position gone but state still set ─────────
        if pos is None and state.entry_price is not None:
            # User closed position manually (or via TP/SL outside bot)
            exit_price = price
            size_guess = 0.0
            pnl        = 0.0
            try:
                # We don't know exact size; we log pnl as approximate using last known risk
                size_guess = 0.0
            except Exception:
                pass
            state.write_trade("LONG_CLOSE", exit_price, size_guess, pnl,
                              "MANUAL_CLOSE", get_total_balance())
            tg_send(
                f"ℹ️ <b>Position closed manually or externally</b>\n"
                f"Bot state cleared at price: ${exit_price:,.2f}"
            )
            state.last_entry_candle_ts = current_candle_ts
            state.clear_position()
            time.sleep(POLL_INTERVAL)
            continue

        # ── Recovery: position exists but state is empty ──────────────────────
        if pos and state.entry_price is None:
            candle_ok = (state.entry_candle_ts is not None and
                         recover_levels_from_entry_candle(df_closed))
            if not candle_ok:
                state.entry_price  = extract_entry_price(pos, price)
                state.invalidation = sig["invalidation"]
                state.tp           = sig["tp"]
                tg_send(
                    f"⚠️ <b>Position found — levels approximated</b>\n"
                    f"Entry: ${state.entry_price:,.2f}\n"
                    f"Stop:  ${state.invalidation:,.2f}\n"
                    f"TP:    ${state.tp:,.2f}\n"
                    f"<b>Verify manually!</b>"
                )
            else:
                state.entry_price = extract_entry_price(pos, price)
                tg_send(
                    f"♻️ <b>Levels recovered from entry candle</b>\n"
                    f"Entry: ${state.entry_price:,.2f}\n"
                    f"Stop:  ${state.invalidation:,.2f}\n"
                    f"TP:    ${state.tp:,.2f}"
                )

            if state.tp and not tp_order_still_open(state.tp_order_id):
                size = abs(float(
                    pos.get("contracts") or pos.get("size") or
                    pos.get("info", {}).get("size") or pos.get("info", {}).get("total") or 0
                ))
                new_id = place_tp_limit_order(size, state.tp)
                state.tp_order_id = new_id
                tg_send(f"📋 TP limit order placed: {new_id} @ ${state.tp:,.2f}")

            state.save()

        # ── Manage open position — stop only (TP via limit order) ─────────────
        if pos and state.entry_price:
            size = abs(float(
                pos.get("contracts") or pos.get("size") or
                pos.get("info", {}).get("size") or pos.get("info", {}).get("total") or 0
            ))

            # Stop: last closed candle close below invalidation
            if state.invalidation and price <= state.invalidation:
                pnl = (price - state.entry_price) * size
                try:
                    cancel_tp_order(state.tp_order_id)
                    place_order("SELL", size)
                    state.write_trade("LONG_CLOSE", price, size, pnl,
                                      "STOP_INVALIDATION", get_total_balance())
                    tg_send(
                        f"⛔ <b>STOP HIT</b> — {SYMBOL}\n"
                        f"Exit: ${price:,.2f}\nPnL: ${pnl:,.2f}"
                    )
                    logger.info("Stop hit: exit=%.4f pnl=%.2f", price, pnl)
                except Exception as e:
                    logger.exception("STOP order failed")
                    tg_send(f"🚨 <b>STOP FAILED</b> — close manually!\n{e}")
                finally:
                    state.last_entry_candle_ts = current_candle_ts
                    state.clear_position()
                time.sleep(POLL_INTERVAL)
                continue

            # TP filled check: TP order gone and position closed
            if state.tp_order_id and not tp_order_still_open(state.tp_order_id):
                pos_recheck = get_position()
                if pos_recheck is None:
                    pnl = (state.tp - state.entry_price) * size
                    state.write_trade("LONG_CLOSE", state.tp, size, pnl,
                                      "TP_LIMIT_FILLED", get_total_balance())
                    tg_send(
                        f"✅ <b>TAKE PROFIT FILLED</b> — {SYMBOL}\n"
                        f"TP limit order executed\n"
                        f"Exit: ${state.tp:,.2f}\nPnL: ${pnl:,.2f}"
                    )
                    logger.info("TP limit filled: exit=%.4f pnl=%.2f", state.tp, pnl)
                    state.last_entry_candle_ts = current_candle_ts
                    state.clear_position()
                    time.sleep(POLL_INTERVAL)
                    continue
                else:
                    logger.warning("TP order gone but position still open — replacing TP order")
                    new_id = place_tp_limit_order(size, state.tp)
                    state.tp_order_id = new_id
                    state.save()
                    tg_send(
                        f"⚠️ <b>TP order was cancelled externally — replaced</b>\n"
                        f"New TP order: {new_id} @ ${state.tp:,.2f}"
                    )

        # ── Entry ─────────────────────────────────────────────────────────────
        if pos is None and sig["entry"] and state.last_entry_candle_ts != current_candle_ts:
            avail = get_available_usdt()
            # Use 99% of free USDT as notional; with 10x leverage, margin ≈ 9.9% of free
            qty   = safe_qty(avail * 0.99, price)

            if qty > 0:
                try:
                    res = place_order("BUY", qty)
                    state.entry_price          = extract_fill_price(res, price)
                    state.invalidation         = sig["invalidation"]
                    state.tp                   = sig["tp"]
                    state.entry_candle_ts      = int(df_closed["ts"].iloc[-1])
                    state.last_entry_candle_ts = current_candle_ts

                    tp_id = place_tp_limit_order(qty, state.tp)
                    state.tp_order_id = tp_id

                    state.write_trade("LONG_OPEN", state.entry_price, qty, 0,
                                      "SFP_ENTRY", get_total_balance())
                    state.write_trade("TP_ORDER", state.tp, qty, 0,
                                      f"TP_LIMIT id={tp_id}", get_total_balance())
                    state.save()

                    tg_send(
                        f"🟢 <b>LONG OPENED</b> — {SYMBOL}\n"
                        f"Entry:           ${state.entry_price:,.2f}\n"
                        f"Qty:             {qty} contracts\n"
                        f"Stop (inv low):  ${state.invalidation:,.2f}\n"
                        f"TP (pivot high): ${state.tp:,.2f}\n"
                        f"TP order ID:     {tp_id or '⚠️ failed'}\n"
                        f"Pivot Low ref:   ${sig['pivot_low']:,.2f}\n"
                        f"Risk/contract:   ${state.entry_price - state.invalidation:,.2f}\n"
                        f"Reward/contract: ${state.tp - state.entry_price:,.2f}"
                    )
                    logger.info(
                        "Long opened: entry=%.4f stop=%.4f tp=%.4f tp_order=%s",
                        state.entry_price, state.invalidation, state.tp, tp_id
                    )
                except Exception as e:
                    logger.exception("Entry failed")
                    tg_send(f"⚠️ Entry failed: {e}")
            else:
                tg_send(
                    f"⚠️ SFP signal — order skipped (low funds / min size)\n"
                    f"Stop: ${sig['invalidation']:,.2f}  TP: ${sig['tp']:,.2f}"
                )

        # ── Daily report ──────────────────────────────────────────────────────
        now = datetime.utcnow()
        if now.hour == DAILY_HOUR_UTC and now.minute >= DAILY_MIN_UTC:
            send_daily_report(price)

        time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        tg_send("🛑 <b>SFP Bot stopped</b>")
        break
    except Exception:
        logger.exception("Unhandled loop error")
        time.sleep(POLL_INTERVAL)