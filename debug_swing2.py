import vectorbt as vbt
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Load Data ─────────────────────────────────────────────────────────────────
data = pd.read_csv('BTC_30m_binance.csv', index_col='timestamp', parse_dates=True)
data = data.sort_index()
data = data[~data.index.duplicated(keep='first')]
data = data.dropna(subset=['Open', 'High', 'Low', 'Close'])

open_ = data['Open']
high  = data['High']
low   = data['Low']
close = data['Close']
volume = data['Volume']

# ── Parameters ────────────────────────────────────────────────────────────────
SWING_N         = 6
PIVOT_WINDOW    = 273
MA_PERIOD       = 644
MIN_DISTANCE    = 4
VOLUME_LOOKBACK = 12
ATR_PERIOD      = 21
ATR_MULTIPLIER  = 2.2

# ── Signal Computation ────────────────────────────────────────────────────────
data['swing_low'] = low == low.rolling(window=2 * SWING_N + 1, center=True).min()
confirmed_swing_lows = low.where(data['swing_low']).shift(SWING_N)
pivot_low_val = confirmed_swing_lows.rolling(window=PIVOT_WINDOW, min_periods=1).min().shift(1)

idx = np.arange(len(low))
pivot_pos = np.where(~confirmed_swing_lows.isna(), idx, -1)
last_pivot_pos = np.maximum.accumulate(pivot_pos)
distance_from_low = pd.Series(
    np.where(last_pivot_pos >= 0, idx - last_pivot_pos, np.nan), index=low.index
)

atr           = vbt.ATR.run(high, low, close, window=ATR_PERIOD).atr
candle_range  = high - low
ma            = vbt.MA.run(close, MA_PERIOD).ma
vol_avg       = volume.rolling(window=VOLUME_LOOKBACK).mean().shift(1)

bullish_sfp_raw = (low < pivot_low_val) & (close > pivot_low_val) & (close > open_) 

entries = (
    bullish_sfp_raw &
    (ma > ma.shift(1)).fillna(False) &
    (distance_from_low >= MIN_DISTANCE).fillna(False) &
    (volume > vol_avg.fillna(np.inf)) &
    (candle_range < ATR_MULTIPLIER * atr)
)

invalidation_level = low.where(entries).ffill()
tp_level = high.rolling(window=PIVOT_WINDOW).max().shift(1).where(entries).ffill()
exits = (close < invalidation_level) | (high >= tp_level)

# ── Backtest ──────────────────────────────────────────────────────────────────
portfolio = vbt.Portfolio.from_signals(
    close=close.astype(np.float64),
    entries=entries,
    exits=exits,
    freq='30min',
    fees=0.0005,
    upon_opposite_entry='close'
)

print(portfolio.stats())

# ── Extract Trades ────────────────────────────────────────────────────────────
trades = portfolio.trades.records_readable
entry_times  = pd.to_datetime(trades['Entry Timestamp']).tolist()
exit_times   = pd.to_datetime(trades['Exit Timestamp']).tolist()
entry_prices = trades['Avg Entry Price'].values
exit_prices  = trades['Avg Exit Price'].values
winning_mask = trades['PnL'].values >= 0

# ── Plot ──────────────────────────────────────────────────────────────────────
fig = make_subplots(specs=[[{"secondary_y": True}]])

fig.add_trace(go.Candlestick(
    x=data.index, open=open_, high=high, low=low, close=close,
    name='Price',
    increasing_line_color='#26a69a', decreasing_line_color='#ef5350',
    increasing_fillcolor='#26a69a', decreasing_fillcolor='#ef5350',
), secondary_y=False)

fig.add_trace(go.Scatter(
    x=data.index, y=pivot_low_val,
    name='Pivot Low', mode='lines',
    line=dict(color='orange', width=1, dash='dash')
), secondary_y=False)

fig.add_trace(go.Scatter(
    x=entry_times, y=entry_prices, mode='markers', name='Entry',
    marker=dict(symbol='triangle-up', size=13,
                color=['lime' if w else 'salmon' for w in winning_mask])
), secondary_y=False)

fig.add_trace(go.Scatter(
    x=exit_times, y=exit_prices, mode='markers', name='Exit',
    marker=dict(symbol='triangle-down', size=13,
                color=['lime' if w else 'salmon' for w in winning_mask])
), secondary_y=False)

equity = portfolio.value()
fig.add_trace(go.Scatter(
    x=equity.index, y=equity.values,
    name='Equity', line=dict(color='gold', width=2),
), secondary_y=True)

fig.update_layout(
    template='plotly_dark', height=700,
    title='Bullish SFP — BTC 30m',
    xaxis_rangeslider_visible=False,
    hovermode='x unified'
)
fig.update_yaxes(title_text='Price (USDT)', secondary_y=False)
fig.update_yaxes(title_text='Equity ($)',   secondary_y=True)

fig.show()