import os
import datetime as dt
import pandas as pd
import numpy as np
import time
import logging
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s %(levelname)s :: %(message)s")

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler("three_sup_trend.log")
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

api_key = os.getenv('KITETRADE_API_KEY')
api_secret = os.getenv('KITETRADE_API_SECRET')
access_token = os.getenv('KITETRADE_ACCESS_TOKEN')
logger.info(f"API KEY: {api_key}")
kite = KiteConnect(api_key=api_key)
session = None

if not access_token:
    print(kite.login_url())
    request_token = input("Enter request token: ")
    session = kite.generate_session(request_token, api_secret)
    access_token = session["access_token"]
    
kite.set_access_token(access_token)
logger.info("Authentication complete!")

#get dump of all NSE instruments
logger.info("Getting instruments dump")
instrument_dump = kite.instruments("NSE")
instrument_df = pd.DataFrame(instrument_dump)


def instrumentLookup(instrument_df,symbol):
    """Looks up instrument token for a given script from instrument dump"""
    try:
        return instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
    except:
        return -1


def fetchOHLC(ticker,interval,duration):
    """extracts historical data and outputs in the form of dataframe"""
    # logger.info(f"fetch OHLC data for: {ticker}")
    instrument = instrumentLookup(instrument_df,ticker)
    data = pd.DataFrame(kite.historical_data(instrument,dt.date.today()-dt.timedelta(duration), dt.date.today(),interval))
    data.set_index("date",inplace=True)
    return data

def atr(DF,n):
    "function to calculate True Range and Average True Range"
    df = DF.copy()
    df['H-L']=abs(df['high']-df['low'])
    df['H-PC']=abs(df['high']-df['close'].shift(1))
    df['L-PC']=abs(df['low']-df['close'].shift(1))
    df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1,skipna=False)
    df['ATR'] = df['TR'].ewm(com=n,min_periods=n).mean()
    return df['ATR']

def supertrend(DF,n,m):
    """function to calculate Supertrend given historical candle data
        n = n day ATR - usually 7 day ATR is used
        m = multiplier - usually 2 or 3 is used"""
    df = DF.copy()
    df['ATR'] = atr(df,n)
    df["B-U"]=((df['high']+df['low'])/2) + m*df['ATR'] 
    df["B-L"]=((df['high']+df['low'])/2) - m*df['ATR']
    df["U-B"]=df["B-U"]
    df["L-B"]=df["B-L"]
    ind = df.index
    for i in range(n,len(df)):
        if df['close'][i-1]<=df['U-B'][i-1]:
            df.loc[ind[i],'U-B']=min(df['B-U'][i],df['U-B'][i-1])
        else:
            df.loc[ind[i],'U-B']=df['B-U'][i]    
    for i in range(n,len(df)):
        if df['close'][i-1]>=df['L-B'][i-1]:
            df.loc[ind[i],'L-B']=max(df['B-L'][i],df['L-B'][i-1])
        else:
            df.loc[ind[i],'L-B']=df['B-L'][i]  
    df['Strend']=np.nan
    for test in range(n,len(df)):
        if df['close'][test-1]<=df['U-B'][test-1] and df['close'][test]>df['U-B'][test]:
            df.loc[ind[test],'Strend']=df['L-B'][test]
            break
        if df['close'][test-1]>=df['L-B'][test-1] and df['close'][test]<df['L-B'][test]:
            df.loc[ind[test],'Strend']=df['U-B'][test]
            break
    for i in range(test+1,len(df)):
        if df['Strend'][i-1]==df['U-B'][i-1] and df['close'][i]<=df['U-B'][i]:
            df.loc[ind[i],'Strend']=df['U-B'][i]
        elif  df['Strend'][i-1]==df['U-B'][i-1] and df['close'][i]>=df['U-B'][i]:
            df.loc[ind[i],'Strend']=df['L-B'][i]
        elif df['Strend'][i-1]==df['L-B'][i-1] and df['close'][i]>=df['L-B'][i]:
            df.loc[ind[i],'Strend']=df['L-B'][i]
        elif df['Strend'][i-1]==df['L-B'][i-1] and df['close'][i]<=df['L-B'][i]:
            df.loc[ind[i],'Strend']=df['U-B'][i]
    return df['Strend']


def st_dir_refresh(ohlc,ticker):
    """function to check for supertrend reversal"""
    global st_dir
    if ohlc["st1"][-1] > ohlc["close"][-1] and ohlc["st1"][-2] < ohlc["close"][-2]:
        st_dir[ticker][0] = "red"
    if ohlc["st2"][-1] > ohlc["close"][-1] and ohlc["st2"][-2] < ohlc["close"][-2]:
        st_dir[ticker][1] = "red"
    if ohlc["st3"][-1] > ohlc["close"][-1] and ohlc["st3"][-2] < ohlc["close"][-2]:
        st_dir[ticker][2] = "red"
    if ohlc["st1"][-1] < ohlc["close"][-1] and ohlc["st1"][-2] > ohlc["close"][-2]:
        st_dir[ticker][0] = "green"
    if ohlc["st2"][-1] < ohlc["close"][-1] and ohlc["st2"][-2] > ohlc["close"][-2]:
        st_dir[ticker][1] = "green"
    if ohlc["st3"][-1] < ohlc["close"][-1] and ohlc["st3"][-2] > ohlc["close"][-2]:
        st_dir[ticker][2] = "green"

def sl_price(ohlc):
    """function to calculate stop loss based on supertrends"""
    st = ohlc.iloc[-1,[-3,-2,-1]]
    if st.min() > ohlc["close"][-1]:
        sl = (0.6*st.sort_values(ascending = True)[0]) + (0.4*st.sort_values(ascending = True)[1])
    elif st.max() < ohlc["close"][-1]:
        sl = (0.6*st.sort_values(ascending = False)[0]) + (0.4*st.sort_values(ascending = False)[1])
    else:
        sl = st.mean()
    return round(sl,1)

def placeSLOrder(symbol,buy_sell,quantity,sl_price):    
    logger.debug(f"[SL ORDER] {symbol}, {buy_sell}, {quantity}, {sl_price}")
    # Place an intraday stop loss order on NSE - handles market orders converted to limit orders
    if buy_sell == "buy":
        t_type=kite.TRANSACTION_TYPE_BUY
        t_type_sl=kite.TRANSACTION_TYPE_SELL
    elif buy_sell == "sell":
        t_type=kite.TRANSACTION_TYPE_SELL
        t_type_sl=kite.TRANSACTION_TYPE_BUY
    market_order = kite.place_order(tradingsymbol=symbol,
                    exchange=kite.EXCHANGE_NSE,
                    transaction_type=t_type,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_MIS,
                    variety=kite.VARIETY_REGULAR)
    a = 0
    while a < 10:
        try:
            order_list = kite.orders()
            break
        except:
            print("can't get orders..retrying")
            a+=1
    for order in order_list:
        if order["order_id"]==market_order:
            if order["status"]=="COMPLETE":
                kite.place_order(tradingsymbol=symbol,
                                exchange=kite.EXCHANGE_NSE,
                                transaction_type=t_type_sl,
                                quantity=quantity,
                                order_type=kite.ORDER_TYPE_SL,
                                price=sl_price,
                                trigger_price = sl_price,
                                product=kite.PRODUCT_MIS,
                                variety=kite.VARIETY_REGULAR)
            else:
                kite.cancel_order(order_id=market_order,variety=kite.VARIETY_REGULAR)


def ModifyOrder(order_id,price):    
    # Modify order given order id
    logger.debug(f"[MODIFY ORDER] {order_id}, {price}")
    kite.modify_order(order_id=order_id,
                    price=price,
                    trigger_price=price,
                    order_type=kite.ORDER_TYPE_SL,
                    variety=kite.VARIETY_REGULAR)      


def main(capital):
    a,b = 0,0
    while a < 10:
        try:
            pos_df = pd.DataFrame(kite.positions()["day"])
            break
        except:
            logger.error("can't extract position data..retrying")
            a+=1
    while b < 10:
        try:
            ord_df = pd.DataFrame(kite.orders())
            break
        except:
            logger.error("can't extract order data..retrying")
            b+=1
    
    for ticker in tickers:
        print("starting passthrough for.....",ticker)
        try:
            ohlc = fetchOHLC(ticker,"5minute",4)
            print(ohlc)
            ohlc["st1"] = supertrend(ohlc,7,3)
            ohlc["st2"] = supertrend(ohlc,10,3)
            ohlc["st3"] = supertrend(ohlc,11,2)
            
            st_dir_refresh(ohlc,ticker)
            quantity = int(capital/ohlc["close"][-1])
            if len(pos_df.columns)==0:
                if st_dir[ticker] == ["green","green","green"]:
                    placeSLOrder(ticker,"buy",quantity,sl_price(ohlc))
                if st_dir[ticker] == ["red","red","red"]:
                    placeSLOrder(ticker,"sell",quantity,sl_price(ohlc))
            if len(pos_df.columns)!=0 and ticker not in pos_df["tradingsymbol"].tolist():
                if st_dir[ticker] == ["green","green","green"]:
                    placeSLOrder(ticker,"buy",quantity,sl_price(ohlc))
                if st_dir[ticker] == ["red","red","red"]:
                    placeSLOrder(ticker,"sell",quantity,sl_price(ohlc))
            if len(pos_df.columns)!=0 and ticker in pos_df["tradingsymbol"].tolist():
                if pos_df[pos_df["tradingsymbol"]==ticker]["quantity"].values[0] == 0:
                    if st_dir[ticker] == ["green","green","green"]:
                        placeSLOrder(ticker,"buy",quantity,sl_price(ohlc))
                    if st_dir[ticker] == ["red","red","red"]:
                        placeSLOrder(ticker,"sell",quantity,sl_price(ohlc))
                if pos_df[pos_df["tradingsymbol"]==ticker]["quantity"].values[0] != 0:
                    order_id = ord_df.loc[(ord_df['tradingsymbol'] == ticker) & (ord_df['status'].isin(["TRIGGER PENDING","OPEN"]))]["order_id"].values[0]
                    ModifyOrder(order_id,sl_price(ohlc))
        except:
            logger.error("API error for ticker :",ticker)


#############################################################################################################

tickers = ["IRFC","RVNL","HUDCO","SUZLON","IREDA",
           "NBCC","IRCON","PNB","ZOMATO","BHEL","HDFCBANK"] 

#tickers to track - recommended to use max movers from previous day
capital = 5000 #position size
st_dir = {} #directory to store super trend status for each ticker
for ticker in tickers:
    st_dir[ticker] = ["None","None","None"]    
    
starttime=time.time()
timeout = time.time() + 60*60*1  # 60 seconds times 360 meaning 6 hrs
while time.time() <= timeout:
    try:
        main(capital)
        time.sleep(300 - ((time.time() - starttime) % 300.0))
    except KeyboardInterrupt:
        logger.error('\n\nKeyboard exception received. Exiting.')
        exit()        

