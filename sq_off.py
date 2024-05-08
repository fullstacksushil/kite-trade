import os
import pandas as pd
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

file_handler = logging.FileHandler("square_off.log")
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

def placeMarketOrder(symbol,buy_sell,quantity):    
    logger.debug(f"[MARKET ORDER] {symbol}, {buy_sell}, {quantity}")
    # Place an intraday market order on NSE
    if buy_sell == "buy":
        t_type=kite.TRANSACTION_TYPE_BUY
    elif buy_sell == "sell":
        t_type=kite.TRANSACTION_TYPE_SELL
    kite.place_order(tradingsymbol=symbol,
                    exchange=kite.EXCHANGE_NSE,
                    transaction_type=t_type,
                    quantity=quantity,
                    order_type=kite.ORDER_TYPE_MARKET,
                    product=kite.PRODUCT_MIS,
                    variety=kite.VARIETY_REGULAR)
    
def CancelOrder(order_id):   
    logger.debug(f"[CANCEL ORDER] {order_id}")
    # Modify order given order id
    kite.cancel_order(order_id=order_id,
                    variety=kite.VARIETY_REGULAR)  

#fetching orders and position information   
a,b = 0,0
while a < 10:
    try:
        pos_df = pd.DataFrame(kite.positions()["day"])
        break
    except:
        logger.info("can't extract position data..retrying")
        a+=1
while b < 10:
    try:
        ord_df = pd.DataFrame(kite.orders())
        break
    except:
        logger.info("can't extract order data..retrying")
        b+=1

#closing all open position      
for i in range(len(pos_df)):
    ticker = pos_df["tradingsymbol"].values[i]
    if pos_df["quantity"].values[i] >0:
        quantity = pos_df["quantity"].values[i]
        placeMarketOrder(ticker,"sell", quantity)
    if pos_df["quantity"].values[i] <0:
        quantity = abs(pos_df["quantity"].values[i])
        placeMarketOrder(ticker,"buy", quantity)

#closing all pending orders
pending = ord_df[ord_df['status'].isin(["TRIGGER PENDING","OPEN"])]["order_id"].tolist()
drop = []
attempt = 0
while len(pending)>0 and attempt<5:
    pending = [j for j in pending if j not in drop]
    for order in pending:
        try:
            CancelOrder(order)
            drop.append(order)
        except:
            logger.info("unable to delete order id : ",order)
            attempt+=1
            