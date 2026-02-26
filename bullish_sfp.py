import vectorbt as vbt
import pandas as pd
import numpy as np

data = pd.read_csv('BTC_30m_binance.csv', index_col='timestamp', parse_dates=True)
data = data.sort_index()
open_ = data['Open']
high = data['High']
low = data['Low']
close = data['Close']
volume = data['Volume']

SWING_N = 6
PIVOT_WINDOW = 273
MA_PERIOD = 644
MIN_DISTANCE = 4
VOLUME_LOOKBACK = 12
ATR_PERIOD = 21
ATR_MULTIPLIER = 2.2

data['swing_low'] = low == low.rolling(window=2 * SWING_N + 1, center=True).min()
confirmed_swing_lows = low.where(data['swing_low']).shift(SWING_N)
pivot_low_val = confirmed_swing_lows.rolling(window=PIVOT_WINDOW, min_periods=1).min().shift(1)

idx = np.arange(len(low))
pivot_pos = np.where(~confirmed_swing_lows.isna(), idx, -1)
last_pivot_pos = np.maximum.accumulate(pivot_pos)
distance_from_last_pivot = np.where(last_pivot_pos >= 0, idx - last_pivot_pos, np.nan)
distance_from_low = pd.Series(distance_from_last_pivot, index=low.index)

atr = vbt.ATR.run(high, low, close, window=ATR_PERIOD).atr
candle_range = high - low
ma = vbt.MA.run(close, MA_PERIOD).ma
ma_increasing = ma > ma.shift(1)
vol_avg = volume.rolling(window=VOLUME_LOOKBACK).mean().shift(1)

bullish_sfp_raw = (low < pivot_low_val) & (close > pivot_low_val) & (close > open_)

entries = (
    bullish_sfp_raw & 
    ma_increasing.fillna(False) & 
    (distance_from_low >= MIN_DISTANCE).fillna(False) & 
    (volume > vol_avg.fillna(np.inf)) & 
    (candle_range < (ATR_MULTIPLIER * atr))
)

invalidation_level = low.where(entries).ffill()
tp_level = high.rolling(window=PIVOT_WINDOW).max().shift(1).where(entries).ffill()

exits = (close < invalidation_level) | (high >= tp_level)

portfolio = vbt.Portfolio.from_signals(
    close=close.astype(np.float64),
    entries=entries,
    exits=exits,
    freq='30min',
    fees=0.0005,
    upon_opposite_entry='close'
)

print(portfolio.stats())