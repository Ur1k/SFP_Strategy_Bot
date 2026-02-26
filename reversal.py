import pandas as pd

daily = pd.read_csv('btc_v_d.csv', index_col='Date', parse_dates=True)
daily = pd.DataFrame(daily).drop(columns=['Volume'])
btc = daily[daily.index >= '2021-01-01']

df = btc.copy()

# Calculate candle components
df['upper_wick'] = df['High'] - df[['Open', 'Close']].max(axis=1)
df['lower_wick'] = df[['Open', 'Close']].min(axis=1) - df['Low']
df['body'] = abs(df['Close'] - df['Open'])
df['height'] = df['High'] - df['Low']

# Define reversal signals
df['bullish_wick'] = (2 * df['lower_wick'] > df['height']) & (df['Close'] > df['Open'])
df['bearish_wick'] = (2 * df['upper_wick'] > df['height']) & (df['Close'] < df['Open'])

# Prepare shifted data for next-day analysis
df['next_open'] = df['Open'].shift(-1)
df['next_close'] = df['Close'].shift(-1)
df['next_return'] = df['next_close'] - df['next_open']

# Count bullish and bearish next candles after reversals
bullish_setups = df[df['bullish_wick']]
bearish_setups = df[df['bearish_wick']]

bullish_next_bullish = (bullish_setups['next_close'] > bullish_setups['next_open']).sum()
bullish_next_bearish = (bullish_setups['next_close'] < bullish_setups['next_open']).sum()

bearish_next_bullish = (bearish_setups['next_close'] > bearish_setups['next_open']).sum()
bearish_next_bearish = (bearish_setups['next_close'] < bearish_setups['next_open']).sum()

# Profit and loss after reversals
bullish_pnl = bullish_setups['next_return']
bearish_pnl = -(bearish_setups['next_return'])  # simulate short: open - close

bullish_total_profit = bullish_pnl[bullish_pnl > 0].sum()
bullish_total_loss = bullish_pnl[bullish_pnl < 0].sum()

bearish_total_profit = bearish_pnl[bearish_pnl > 0].sum()
bearish_total_loss = bearish_pnl[bearish_pnl < 0].sum()

# Final Output
print(f"📊 After bullish_reversal:")
print(f"   → Next day bullish candles: {bullish_next_bullish}")
print(f"   → Next day bearish candles: {bullish_next_bearish}")
print(f"   → Total profit on next candles: {bullish_total_profit:.2f}")
print(f"   → Total loss on next candles: {bullish_total_loss:.2f}")

print(f"\n📊 After bearish_reversal:")
print(f"   → Next day bullish candles: {bearish_next_bullish}")
print(f"   → Next day bearish candles: {bearish_next_bearish}")
print(f"   → Total profit on next candles (short): {bearish_total_profit:.2f}")
print(f"   → Total loss on next candles (short): {bearish_total_loss:.2f}")