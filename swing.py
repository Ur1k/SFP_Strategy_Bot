import vectorbt as vbt
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import ta

data = pd.read_csv('BTC_30m_binance.csv', index_col='timestamp', parse_dates=True)
data.index = data.index.tz_localize(None)  # Remove timezone info
data = data.sort_index()
data = data[~data.index.duplicated(keep='first')]
data = data.dropna(subset=['Open', 'High', 'Low', 'Close'])

high = data['High']
low = data['Low']
close = data['Close']
open = data['Open']
volume = data['Volume']

# Swing highs / lows
prev_high = high.shift(1)
next_high = high.shift(-1)
prev_low = low.shift(1)
next_low = low.shift(-1)
high_vals = high.values
low_vals = low.values

data['swing_high'] = (high > prev_high) & (high > next_high)
data['swing_low'] = (low < prev_low) & (low < next_low)

# VECTORIZED FVG DETECTION 
# Bullish FVG: low of candle 1 < high of candle 3
bullish_fvg = low.shift(2) < high
data['bullish_fvg'] = bullish_fvg

# Mark candle before bullish_fvg the middle candle of the gap formation
bullish_mid_fvg = data['bullish_fvg'].shift(1)
data['bullish_mid_fvg'] = bullish_mid_fvg

#
swing_fvg_low = data['bullish_fvg'].shift(1) & (open > low.shift(1)) & (close > low.shift(1) & (low < low.shift(1)))
data['swing_fvg_low'] = swing_fvg_low

# Find impulsive_bullish_fvg candles: bullish_fvg candles that have  a swing_low candle before them
impulsive_bullish_fvg = data['bullish_fvg'] & data['swing_low'].shift(1)
data['impulsive_bullish_fvg'] = impulsive_bullish_fvg

print(data.head(20))
print(f"\nTotal swing_fvg_low: {data['swing_fvg_low'].sum()}")
print(f"Total swing_low: {data['swing_low'].sum()}")
print(f"Total bullish_mid_fvg: {data['bullish_mid_fvg'].sum()}")
# --- Plotly visualization ---

# Base candlesticks (all candles)
fig = go.Figure(
    data=[
        go.Candlestick(
            x=data.index,
            open=open,
            high=high,
            low=low,
            close=close,
            name="Price",
            increasing_line_color='green',
            decreasing_line_color='red',
            increasing_fillcolor='green',
            decreasing_fillcolor='red',
            opacity=0.5  # a bit transparent so highlights stand out
        )
    ]
)

# Swing FVG Low candles (overlay with different color)
swing_fvg_low_df = data[data['swing_fvg_low']]

fig.add_trace(
    go.Candlestick(
        x=swing_fvg_low_df.index,
        open=swing_fvg_low_df['Open'],
        high=swing_fvg_low_df['High'],
        low=swing_fvg_low_df['Low'],
        close=swing_fvg_low_df['Close'],
        name="Swing FVG Low",
        increasing_line_color='darkblue',
        decreasing_line_color='darkblue',
        increasing_fillcolor='darkblue',
        decreasing_fillcolor='darkblue',
        opacity=1.0
    )
)

# FVG candles (overlay with another color, e.g., orange)
fvg_df = data[data['bullish_fvg']]

fig.add_trace(
    go.Candlestick(
        x=fvg_df.index,
        open=fvg_df['Open'],
        high=fvg_df['High'],
        low=fvg_df['Low'],
        close=fvg_df['Close'],
        name="Bullish FVG",
        increasing_line_color='orange',
        decreasing_line_color='orange',
        increasing_fillcolor='orange',
        decreasing_fillcolor='orange',
        opacity=1.0
    )
)

fig.update_layout(
    title="BTC 30m with Swing Lows and Bullish FVG",
    xaxis_title="Time",
    yaxis_title="Price",
    xaxis_rangeslider_visible=False
)

fig.show()
