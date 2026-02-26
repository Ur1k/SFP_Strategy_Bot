import pandas as pd
import numpy as np

# ── Strategy Parameters ───────────────────────────────────────────────────────
SWING_N         = 6      # bars each side to confirm a swing low
PIVOT_WINDOW    = 273    # lookback for the rolling pivot low level
MA_PERIOD       = 644    # trend MA period
MIN_DISTANCE    = 4      # min bars since last confirmed swing low
VOLUME_LOOKBACK = 12     # bars for average volume baseline
ATR_PERIOD      = 21     # ATR period
ATR_MULTIPLIER  = 2.2    # max candle range = ATR * this multiplier


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    """Simple ATR without external dependencies."""
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=1).mean()


def compute_signals(df: pd.DataFrame) -> dict:
    """
    Compute bullish SFP signals on a DataFrame of OHLCV data.

    Parameters
    ----------
    df : pd.DataFrame
        Must have columns: open, high, low, close, volume
        Index should be a DatetimeIndex (UTC).
        Needs at least PIVOT_WINDOW + MA_PERIOD bars (~900 bars minimum).

    Returns
    -------
    dict with keys:
        entry        (bool)  — True if the LAST closed candle triggers a long entry
        invalidation (float) — stop level: close below this → exit
        tp           (float) — take-profit level: high touches this → exit
        pivot_low    (float) — current rolling pivot low value (for logging/display)
    """
    if df is None or len(df) < MA_PERIOD + SWING_N + 10:
        return {"entry": False, "invalidation": None, "tp": None, "pivot_low": None}

    open_  = df["open"]
    high   = df["high"]
    low    = df["low"]
    close  = df["close"]
    volume = df["volume"]

    # ── Swing lows (centered rolling min, then shift to avoid lookahead) ──────
    swing_low_mask = low == low.rolling(window=2 * SWING_N + 1, center=True).min()
    confirmed_swing_lows = low.where(swing_low_mask).shift(SWING_N)

    # ── Rolling pivot low (the key SFP reference level) ──────────────────────
    pivot_low = confirmed_swing_lows.rolling(window=PIVOT_WINDOW, min_periods=1).min().shift(1)

    # ── Distance from last confirmed swing low ────────────────────────────────
    idx = np.arange(len(low))
    pivot_pos      = np.where(~confirmed_swing_lows.isna(), idx, -1)
    last_pivot_pos = np.maximum.accumulate(pivot_pos)
    distance_from_low = pd.Series(
        np.where(last_pivot_pos >= 0, idx - last_pivot_pos, np.nan),
        index=low.index
    )

    # ── Indicators ────────────────────────────────────────────────────────────
    atr          = _atr(high, low, close, ATR_PERIOD)
    candle_range = high - low
    ma           = close.rolling(MA_PERIOD, min_periods=1).mean()
    vol_avg      = volume.rolling(VOLUME_LOOKBACK).mean().shift(1)

    # ── Raw SFP condition: wick below pivot low, close back above, bullish body
    sfp_raw = (
        (low  < pivot_low) &
        (close > pivot_low) &
        (close > open_)
    )

    # ── Full entry filter ─────────────────────────────────────────────────────
    entry_series = (
        sfp_raw &
        (ma > ma.shift(1)).fillna(False) &                      # MA rising
        (distance_from_low >= MIN_DISTANCE).fillna(False) &     # not at fresh pivot
        (volume > vol_avg.fillna(np.inf)) &                     # above-avg volume
        (candle_range < ATR_MULTIPLIER * atr)                   # not a blow-off spike
    )

    entry = bool(entry_series.iloc[-1])

    # ── Levels for the current bar ────────────────────────────────────────────
    invalidation = float(low.iloc[-1])                                          # entry candle low
    tp           = float(high.rolling(PIVOT_WINDOW).max().shift(1).iloc[-1])   # rolling pivot high
    pivot_low_val = float(pivot_low.iloc[-1]) if not pd.isna(pivot_low.iloc[-1]) else None

    return {
        "entry":        entry,
        "invalidation": invalidation,
        "tp":           tp,
        "pivot_low":    pivot_low_val,
    }