import sys
import os
import datetime as dt
import pandas as pd
import numpy as np
import time
import logging
from kiteconnect import KiteConnect, KiteTicker
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
logger.info(f"Authentication complete! {access_token}")

#get dump of all NSE instruments
logger.info("Getting instruments dump")
instrument_dump = kite.instruments("NSE")
instrument_df = pd.DataFrame(instrument_dump)

def tokenLookup(instrument_df,symbol_list):
    """Looks up instrument token for a given script from instrument dump"""
    token_list = []
    for symbol in symbol_list:
        token_list.append(int(instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]))
    return token_list

def tickerLookup(token):
    global instrument_df
    return instrument_df[instrument_df.instrument_token==token].tradingsymbol.values[0] 

def instrumentLookup(instrument_df,symbol):
    """Looks up instrument token for a given script from instrument dump"""
    try:
        return instrument_df[instrument_df.tradingsymbol==symbol].instrument_token.values[0]
    except:
        return -1
        
def fetchOHLC(ticker,interval,duration):
    """extracts historical data and outputs in the form of dataframe"""
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
    return df['ATR'][-1]

def MACD(DF,a,b,c):
    """function to calculate MACD
       typical values a(fast moving average) = 12; 
                      b(slow moving average) =26; 
                      c(signal line ma window) =9"""
    df = DF.copy()
    df["MA_Fast"]=df["close"].ewm(span=a,min_periods=a).mean()
    df["MA_Slow"]=df["close"].ewm(span=b,min_periods=b).mean()
    df["MACD"]=df["MA_Fast"]-df["MA_Slow"]
    df["Signal"]=df["MACD"].ewm(span=c,min_periods=c).mean()
    df.dropna(inplace=True)
    return df

def macd_xover_refresh(macd,ticker):
    global macd_xover
    if macd["MACD"][-1]>macd["Signal"][-1]:
        macd_xover[ticker]="bullish"
    elif macd["MACD"][-1]<macd["Signal"][-1]:
        macd_xover[ticker]="bearish"
        
def renkoBrickSize(ticker):
    return min(10,max(1,round(1.5*atr(fetchOHLC(ticker,"60minute",60),200),0)))
        
def renkoOperation(ticks):
    for tick in ticks:
        try:
            ticker = tickerLookup(int(tick['instrument_token']))
            if renko_param[ticker]["upper_limit"] == None:
                renko_param[ticker]["upper_limit"] = float(tick['last_price']) + renko_param[ticker]["brick_size"]
                renko_param[ticker]["lower_limit"] = float(tick['last_price']) - renko_param[ticker]["brick_size"]
            if float(tick['last_price']) > renko_param[ticker]["upper_limit"]:
                gap = (float(tick['last_price'] - renko_param[ticker]["upper_limit"]))//renko_param[ticker]["brick_size"]
                renko_param[ticker]["lower_limit"] = renko_param[ticker]["upper_limit"] + (gap*renko_param[ticker]["brick_size"]) - renko_param[ticker]["brick_size"]
                renko_param[ticker]["upper_limit"] = renko_param[ticker]["upper_limit"] + ((1+gap)*renko_param[ticker]["brick_size"])
                renko_param[ticker]["brick"] = max(1,renko_param[ticker]["brick"]+(1+gap))
            if float(tick['last_price']) < renko_param[ticker]["lower_limit"]:
                gap = (renko_param[ticker]["lower_limit"] - float(tick['last_price']))//renko_param[ticker]["brick_size"]
                renko_param[ticker]["upper_limit"] = renko_param[ticker]["lower_limit"] - (gap*renko_param[ticker]["brick_size"]) + renko_param[ticker]["brick_size"]
                renko_param[ticker]["lower_limit"] = renko_param[ticker]["lower_limit"] - ((1+gap)*renko_param[ticker]["brick_size"])
                renko_param[ticker]["brick"] = min(-1,renko_param[ticker]["brick"]-(1+gap))
            print("{}: size = {} brick number = {},last price ={}, upper bound ={}, lower bound ={}"\
                  .format(ticker,renko_param[ticker]["brick_size"], renko_param[ticker]["brick"],tick['last_price'],renko_param[ticker]["upper_limit"],renko_param[ticker]["lower_limit"]))
        except Exception as e:
            print(e)
            pass   

def placeSLOrder(symbol,buy_sell,quantity,sl_price):    
    # Place an intraday stop loss order on NSE
    if buy_sell == "buy":
        t_type=kite.TRANSACTION_TYPE_BUY
        t_type_sl=kite.TRANSACTION_TYPE_SELL
    elif buy_sell == "sell":
        t_type=kite.TRANSACTION_TYPE_SELL
        t_type_sl=kite.TRANSACTION_TYPE_BUY
    kite.place_order(tradingsymbol=symbol,
                    exchange=kite.EXCHANGE_NSE,
                    transaction_type=t_type,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_MIS,
                    variety=kite.VARIETY_REGULAR)
    kite.place_order(tradingsymbol=symbol,
                    exchange=kite.EXCHANGE_NSE,
                    transaction_type=t_type_sl,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_SL,
                    price=round(sl_price,1),
                    trigger_price = round(sl_price,1),
                    product=kite.PRODUCT_MIS,
                    variety=kite.VARIETY_REGULAR)
    trade_count = trade_count + 1

def ModifyOrder(order_id,price):    
    # Modify order given order id
    kite.modify_order(order_id=order_id,
                    price=round(price,1),
                    trigger_price=price,
                    order_type=kite.ORDER_TYPE_SL,
                    variety=kite.VARIETY_REGULAR) 
    
def main(capital):
    global renko_param
    a,b = 0,0
    while a < 10:
        try:
            pos_df = pd.DataFrame(kite.positions()["day"])
            break
        except:
            print("can't extract position data..retrying")
            a+=1
    while b < 10:
        try:
            ord_df = pd.DataFrame(kite.orders())
            break
        except:
            print("can't extract order data..retrying")
            b+=1
    
    for ticker in tickers:
        print(f"starting passthrough for {ticker} {macd_xover[ticker]}")
        try:
            ohlc = fetchOHLC(ticker,"5minute",4)
            macd = MACD(ohlc,12,26,9)
            macd_xover_refresh(macd,ticker)
            quantity = int(capital/ohlc["close"][-1])
            if len(pos_df.columns)==0:
                if macd_xover[ticker] == "bullish" and renko_param[ticker]["brick"] >=2:
                    placeSLOrder(ticker,"buy",quantity,renko_param[ticker]["lower_limit"])
                if macd_xover[ticker] == "bearish" and renko_param[ticker]["brick"] <=-2:
                    placeSLOrder(ticker,"sell",quantity,renko_param[ticker]["upper_limit"])
            if len(pos_df.columns)!=0 and ticker not in pos_df["tradingsymbol"].tolist():
                if macd_xover[ticker] == "bullish" and renko_param[ticker]["brick"] >=2:
                    placeSLOrder(ticker,"buy",quantity,renko_param[ticker]["lower_limit"])
                if macd_xover[ticker] == "bearish" and renko_param[ticker]["brick"] <=-2:
                    placeSLOrder(ticker,"sell",quantity,renko_param[ticker]["upper_limit"])
            if len(pos_df.columns)!=0 and ticker in pos_df["tradingsymbol"].tolist():
                if pos_df[pos_df["tradingsymbol"]==ticker]["quantity"].values[0] == 0:
                    if macd_xover[ticker] == "bullish" and renko_param[ticker]["brick"] >=2:
                        placeSLOrder(ticker,"buy",quantity,renko_param[ticker]["lower_limit"])
                    if macd_xover[ticker] == "bearish" and renko_param[ticker]["brick"] <=-2:
                        placeSLOrder(ticker,"sell",quantity,renko_param[ticker]["upper_limit"])
                if pos_df[pos_df["tradingsymbol"]==ticker]["quantity"].values[0] > 0:
                    order_id = ord_df.loc[(ord_df['tradingsymbol'] == ticker) & (ord_df['status'].isin(["TRIGGER PENDING","OPEN"]))]["order_id"].values[0]
                    ModifyOrder(order_id,renko_param[ticker]["lower_limit"])
                if pos_df[pos_df["tradingsymbol"]==ticker]["quantity"].values[0] < 0:
                    order_id = ord_df.loc[(ord_df['tradingsymbol'] == ticker) & (ord_df['status'].isin(["TRIGGER PENDING","OPEN"]))]["order_id"].values[0]
                    ModifyOrder(order_id,renko_param[ticker]["upper_limit"])
        except Exception as e:
            print("API error for ticker :",ticker)
            print(e)
    

#####################update ticker list######################################
tickers = ["ABB","ADANIENSOL","ADANIENT","ADANIGREEN","ADANIPORTS","ATGL","AWL","AMBUJACEM","APOLLOHOSP","ASIANPAINT","DMART","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BAJAJHLDNG","BANKBARODA","BERGEPAINT","BEL","BPCL","BHARTIARTL","BOSCHLTD","BRITANNIA","CANBK","CHOLAFIN","CIPLA","COALINDIA","COLPAL","DLF","DABUR","DIVISLAB","DRREDDY","EICHERMOT","GAIL","GODREJCP","GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HAVELLS","HEROMOTOCO","HINDALCO","HAL","HINDUNILVR","ICICIBANK","ICICIGI","ICICIPRULI","ITC","IOC","IRCTC","INDUSINDBK","NAUKRI","INFY","INDIGO","JSWSTEEL","JINDALSTEL","KOTAKBANK","LTIM","LT","LICI","M&M","MARICO","MARUTI","MUTHOOTFIN","NTPC","NESTLEIND","ONGC","PIIND","PIDILITIND","POWERGRID","PGHH","PNB","RELIANCE","SBICARD","SBILIFE","SRF","MOTHERSON","SHREECEM","SHRIRAMFIN","SIEMENS","SBIN","SUNPHARMA","TVSMOTOR","TCS","TATACONSUM","TATAMTRDVR","TATAMOTORS","TATAPOWER","TATASTEEL","TECHM","TITAN","TORNTPHARM","TRENT","UPL","ULTRACEMCO","MCDOWELL-N","VBL","VEDL","WIPRO","ZOMATO","ZYDUSLIFE"] 

#############################################################################
capital = 6000 #position size
trade_count = 0
macd_xover = {}
renko_param = {}
for ticker in tickers:
    renko_param[ticker] = {"brick_size":renkoBrickSize(ticker),"upper_limit":None, "lower_limit":None,"brick":0}
    macd_xover[ticker] = None
    
#create KiteTicker object
kws = KiteTicker(api_key,kite.access_token)
tokens = tokenLookup(instrument_df,tickers)

start_minute = dt.datetime.now().minute
def on_ticks(ws,ticks):
    global start_minute
    renkoOperation(ticks)
    now_minute = dt.datetime.now().minute
    if abs(now_minute - start_minute) >= 5:
        start_minute = now_minute
        main(capital)

def on_connect(ws,response):
    ws.subscribe(tokens)
    ws.set_mode(ws.MODE_LTP,tokens)

while True:
    now = dt.datetime.now()
    if (now.hour >= 9):
        kws.on_ticks=on_ticks
        kws.on_connect=on_connect
        kws.connect()
    if (now.hour >= 14 and now.minute >= 30):
        sys.exit()