import pandas as pd
import numpy as np

# 1. Load your 30 m OHLC data, parsing the first column as a datetime index
df = pd.read_csv(
    'btc_30m_binance2020.csv',
    index_col=0,
    parse_dates=True
)
df.sort_index(inplace=True)

# 2. Helper to get ±1 direction as a Series
def sign_series(x: pd.Series) -> pd.Series:
    return pd.Series(np.where(x > 0, 1, -1), index=x.index)

# 3. Compute previous-day → today 00:00 direction
daily_open    = df['Open'].resample('D').first()
prev_day_open = daily_open.shift(1)
prev_day_dir  = sign_series(daily_open - prev_day_open)

# 4. Broadcast onto each 30 m bar
df['prev_day_dir'] = prev_day_dir.reindex(df.index, method='ffill')

# 5. Define only the 12–15 h interval grid
base_intervals = [12, 13, 14, 15]
results = []

for b1 in base_intervals:
    for b2 in base_intervals:
        if b1 >= b2:
            continue       # ensure b1 < b2

        dfb = df.copy()

        # compute interval directions
        dfb['dir1'] = np.where(dfb['Close'].shift(-b1) - dfb['Open'] > 0,  1, -1)
        dfb['dir2'] = np.where(dfb['Close'].shift(-b2) - dfb['Open'] > 0,  1, -1)

        # filter to where both base intervals agree & match prior-day trend
        mask = (dfb['dir1'] == dfb['dir2']) & (dfb['dir1'] == dfb['prev_day_dir'])
        dfb = dfb.loc[mask]

        # compute same-direction % for horizons 1–8 hours
        for h in range(1, 9):
            dfb['h_dir'] = np.where(dfb['Close'].shift(-h) - dfb['Open'] > 0, 1, -1)
            pct = (dfb['h_dir'] == dfb['dir1']).mean() * 100

            results.append({
                'base1':        b1,
                'base2':        b2,
                'horizon_h':    h,
                'same_dir_pct': pct
            })

# 6. Aggregate, then rank & display top/bottom 10
res_df = pd.DataFrame(results)
top10  = res_df.nlargest(10, 'same_dir_pct')
bot10  = res_df.nsmallest(10, 'same_dir_pct')

print("\nTop 10 (base1, base2, horizon_h) by same-direction %:\n")
print(top10.to_string(index=False))

print("\nBottom 10 (base1, base2, horizon_h) by same-direction %:\n")
print(bot10.to_string(index=False))