from datetime import datetime

import pandas as pd
import yfinance as yf

# tickers = ["AMD", "BA", "BABA", "GOOG", "META", "PYPL", "SHOP", "SPY"]
tickers = ["AMD"]

start="2024-01-01",
_today = datetime.today().strftime("%Y-%m-%d")

ticks = yf.download(tickers, period="1d")
hist = yf.download(tickers, start, _today, interval="1d")
hist=ticks.history()
print(hist)

# Sample DataFrame with multi-level index
data = {
  ('Adj Close', 'AMD'): [100, 102, 105, 101, 103, 106],  # 6 data points
  ('Close', 'AMD'): [101, 103, 106, 102, 104, 107],  # 6 data points
  ('Adj Close', 'GOOG'): [2500, 2550, 2600, 2510, 2560, 2610],  # 6 data points
  ('Close', 'GOOG'): [2510, 2560, 2610, 2520, 2570, 2620],  # 6 data points
}
index = pd.MultiIndex.from_product([pd.to_datetime(['2024-11-22', '2024-11-23', '2024-11-24']), ['AMD', 'GOOG']], names=['Date', 'Ticker'])
df = pd.DataFrame(data, index=index)

print(df.info())
print(df)

"""
# Extract data for AMD
amd_df = df.xs(key='AMD', level=1, axis=1)
print(amd_df.info())
print(amd_df)

# Reset the index of the extracted DataFrame to bring 'Date' back as a column
amd_df = amd_df.reset_index()

print(amd_df.info())
print(amd_df)
"""
