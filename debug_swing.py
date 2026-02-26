import pandas as pd
import numpy as np

data = pd.read_csv('BTC_30m_binance.csv', index_col='timestamp', parse_dates=True)
data.index = data.index.tz_localize(None)
data = data.sort_index()
data = data[~data.index.duplicated(keep='first')]
data = data.dropna(subset=['Open', 'High', 'Low', 'Close'])

high = data['High']
low = data['Low']
close = data['Close']
open_ = data['Open']
volume = data['Volume']

# Swing highs / lows
prev_high = high.shift(1)
next_high = high.shift(-1)
prev_low = low.shift(1)
next_low = low.shift(-1)

data['swing_high'] = (high > prev_high) & (high > next_high)
data['swing_low'] = (low < prev_low) & (low < next_low)

# For each swing_low, count candles to first candle with open < swing_low's low
candles_to_open_below = np.full(len(data), np.nan)
open_vals = open_.values
low_vals = low.values

for i in range(len(data)):
    if data['swing_low'].iloc[i]:
        swing_low_value = low_vals[i]
        for j in range(i + 1, len(data)):
            if open_vals[j] < swing_low_value:
                candles_to_open_below[i] = j - i
                break

data['candles_to_open_below'] = candles_to_open_below

# Detect bullish fair value gaps (FVG): high of 1st candle < low of 3rd candle
bullish_fvg = np.zeros(len(data), dtype=bool)
high_vals = high.values
low_vals = low.values

for i in range(len(data) - 2):
    if high_vals[i] < low_vals[i + 2]:
        bullish_fvg[i + 2] = True

data['bullish_fvg'] = bullish_fvg

# Mark candle before bullish_fvg the middle candle of the gap formation
bullish_mid_fvg = data['bullish_fvg'].shift(1)
data['bullish_mid_fvg'] = bullish_mid_fvg

# Swing low that has a bullish_mid_fvg as the closest candle opening below it
swing_fvg_low = np.zeros(len(data), dtype=bool)
for i in range(len(data)):
    if data['swing_low'].iloc[i]:
        candles_to_open = data['candles_to_open_below'].iloc[i]
        if not np.isnan(candles_to_open):
            j = i + int(candles_to_open)
            if j < len(data) and data['bullish_mid_fvg'].iloc[j]:
                swing_fvg_low[i] = True
data['swing_fvg_low'] = swing_fvg_low

print(f"Total swing_fvg_low: {data['swing_fvg_low'].sum()}")
print(f"Total swing_low: {data['swing_low'].sum()}")
print(f"Total bullish_mid_fvg: {data['bullish_mid_fvg'].sum()}")
print(f"Total bullish_fvg: {data['bullish_fvg'].sum()}")

# Show some debug info
swing_lows = data[data['swing_low']]
print(f"\nFirst 10 swing lows with candles_to_open_below:")
print(swing_lows[['swing_low', 'candles_to_open_below']].head(10))

# Check what the next value at index j is for first swing low
if len(swing_lows) > 0:
    first_swing_low_idx = swing_lows.index[0]
    first_swing_low_pos = data.index.get_loc(first_swing_low_idx)
    candles_to_open = swing_lows['candles_to_open_below'].iloc[0]
    if not np.isnan(candles_to_open):
        j = first_swing_low_pos + int(candles_to_open)
        if j < len(data):
            print(f"\nFirst swing low at position {first_swing_low_pos}")
            print(f"Candles to open below: {candles_to_open}")
            print(f"Target position j: {j}")
            print(f"bullish_mid_fvg at position {j}: {data['bullish_mid_fvg'].iloc[j]}")
            print(f"Candle at j: {data.iloc[j]}")

# Check if there are any swing_fvg_low
if data['swing_fvg_low'].sum() > 0:
    print(f"\nFound swing_fvg_low:")
    print(data[data['swing_fvg_low']][['swing_low', 'candles_to_open_below', 'bullish_mid_fvg']].head(10))
else:
    print("\nNo swing_fvg_low found. Analyzing why...")
    # Check swing lows that have candles_to_open_below
    swing_lows_with_open = data[(data['swing_low']) & (~data['candles_to_open_below'].isna())]
    print(f"\nSwing lows with candles_to_open_below: {len(swing_lows_with_open)}")
    if len(swing_lows_with_open) > 0:
        for idx, row in swing_lows_with_open.head(5).iterrows():
            pos = data.index.get_loc(idx)
            candles_to_open = int(row['candles_to_open_below'])
            j = pos + candles_to_open
            if j < len(data):
                target_bullish_mid = data['bullish_mid_fvg'].iloc[j]
                print(f"Position {pos}: candles_to_open={candles_to_open}, target j={j}, bullish_mid_fvg={target_bullish_mid}")
