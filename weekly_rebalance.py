import numpy as np
import pandas as pd
import yfinance as yf
import datetime as dt
import copy
import matplotlib
import matplotlib.pyplot as plt

# Adjust matplotlib backend if necessary
matplotlib.use("QtAgg")


def CAGR(DF):
    df = DF.copy()
    df["cum_return"] = (1 + df["mon_ret"]).cumprod()
    n = len(df) / 52  # Adjusted for weekly returns
    CAGR = (df["cum_return"].tolist()[-1]) ** (1 / n) - 1
    return CAGR

def volatility(DF):
    df = DF.copy()
    vol = df["mon_ret"].std() * np.sqrt(52)  # Adjusted for weekly returns
    return vol

def sharpe(DF, rf):
    df = DF.copy()
    sr = (CAGR(df) - rf) / volatility(df)
    return sr

def max_dd(DF):
    df = DF.copy()
    df["cum_return"] = (1 + df["mon_ret"]).cumprod()
    df["cum_roll_max"] = df["cum_return"].cummax()
    df["drawdown"] = df["cum_roll_max"] - df["cum_return"]
    df["drawdown_pct"] = df["drawdown"] / df["cum_roll_max"]
    max_dd = df["drawdown_pct"].max()
    return max_dd

# Download historical data (weekly) for NIFTY 50 stocks

tickers = ["ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BPCL","BHARTIARTL","BRITANNIA","CIPLA","COALINDIA","DIVISLAB","DRREDDY","EICHERMOT","GRASIM","HCLTECH","HDFCBANK","HEROMOTOCO","HINDALCO","HINDUNILVR","ICICIBANK","ITC","INDUSINDBK","INFY","JSWSTEEL","KOTAKBANK","LT","M&M","MARUTI","NTPC","NESTLEIND","ONGC","POWERGRID","RELIANCE","SBIN","SUNPHARMA","TCS","TATACONSUM","TATAMOTORS","TATASTEEL","TECHM","TITAN","UPL","ULTRACEMCO","WIPRO"]

ohlc_week = {}  # directory with ohlc value for each stock            
start = dt.datetime.today() - dt.timedelta(3650)  # 10 years of data
end = dt.datetime.today()

for ticker in tickers:
    ohlc_week[ticker] = yf.download(f"{ticker}.NS", start, end, interval='1wk')
    ohlc_week[ticker].dropna(inplace=True, how="all")

tickers = ohlc_week.keys()  # redefine tickers variable after removing any tickers with corrupted data

# converting monthly return calculation to weekly
ohlc_dict = copy.deepcopy(ohlc_week)
return_df = pd.DataFrame()
for ticker in tickers:
    print("calculating weekly return for ", ticker)
    ohlc_dict[ticker]["mon_ret"] = ohlc_dict[ticker]["Adj Close"].pct_change()  # "mon_ret" now holds weekly returns
    return_df[ticker] = ohlc_dict[ticker]["mon_ret"]
return_df.dropna(inplace=True)

# function to calculate portfolio return iteratively with weekly rebalance
def pflio(DF, m, x):
    df = DF.copy()
    portfolio = []
    weekly_ret = [0]
    for i in range(len(df)):
        if len(portfolio) > 0:
            weekly_ret.append(df[portfolio].iloc[i, :].mean())
            bad_stocks = df[portfolio].iloc[i, :].sort_values(ascending=True)[:x].index.values.tolist()
            portfolio = [t for t in portfolio if t not in bad_stocks]
        fill = m - len(portfolio)
        new_picks = df.iloc[i, :].sort_values(ascending=False)[:fill].index.values.tolist()
        portfolio = portfolio + new_picks
    weekly_ret_df = pd.DataFrame(np.array(weekly_ret), columns=["mon_ret"])  # Consider renaming "mon_ret" to "weekly_ret" for clarity
    return weekly_ret_df


#calculating overall strategy's KPIs
strategy_cagr = CAGR(pflio(return_df,15,3))
strategy_sharpe = sharpe(pflio(return_df,15,3),0.07)
strategy_max_dd = max_dd(pflio(return_df,15,3)) 

print(f"CAGR: {round(strategy_cagr * 100, 1)}% Sharpe: {round(strategy_sharpe,1)} Max Drawdown: {round(strategy_max_dd * 100,1)}%")

#calculating KPIs for Index buy and hold strategy over the same period
Nifty = yf.download("^NSEI",dt.date.today()-dt.timedelta(3650),dt.date.today(),interval='1wk')
Nifty["mon_ret"] = Nifty["Adj Close"].pct_change().fillna(0)
nifty_cagr = CAGR(Nifty)
nifty_sharpe = sharpe(Nifty,0.07)
nifty_max_dd = max_dd(Nifty)
print(f"NIFTY CAGR: {round(nifty_cagr * 100, 1)}% Sharpe: {round(nifty_sharpe,1)} Max Drawdown: {round(nifty_max_dd * 100,1)}%")

#visualization
fig, ax = plt.subplots()
plt.plot((1+pflio(return_df,6,4)).cumprod())
plt.plot((1+Nifty["mon_ret"].reset_index(drop=True)).cumprod())
plt.title("Index Return vs Strategy Return")
plt.ylabel("cumulative return")
plt.xlabel("months")
ax.legend(["Strategy Return","Index Return"])
plt.show()
