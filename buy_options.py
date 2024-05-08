import os
import argparse
import requests, json, pyotp
from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
from urllib.parse import urlparse
from urllib.parse import parse_qs
import pandas as pd
import numpy as np
import datetime as dt
from dotenv import load_dotenv
import time
import yfinance as yf
import sys
import random
from asynctools import multitasking, RecurringTask
import tools
import logging

load_dotenv()
# =============================================
# check min, python version
if sys.version_info < (3, 4):
    raise SystemError("Python version >= 3.4")

# =============================================
# Configure logging
tools.createLogger(__name__)

# =============================================
# set up threading pool
__threads__ = 4 #tools.read_single_argv("--threads")
__threads__ = int(__threads__) if tools.is_number(__threads__) else None
multitasking.createPool(__name__, __threads__)

# =============================================

class ZerodhaOptionBuyer:
    def __init__(self):
        # detect algo name
        self.name = str(self.__class__).split('.')[-1].split("'")[0]
        # initilize logger
        self.log = logging.getLogger(self.name)
        
        self.api_key = os.getenv('KITE_API_KEY')
        self.api_secret = os.getenv('KITE_API_SECRET')
        self.access_token = os.getenv('KITE_ACCESS_TOKEN')
        self.user_id = os.getenv('KITE_USER_ID')
        self.user_password = os.getenv('KITE_USER_PASSWORD')
        self.totp_token = os.getenv('KITE_TOPT_TOKEN')
        
        self.kite = KiteConnect(api_key=self.api_key)
        
        # if access token is not present
        if not self.access_token:
            _auto_login = self.auto_login()
            if not _auto_login:
                print('Authentication Failed!')
                exit()
        self.kite.set_access_token(self.access_token)        
        print(f"Authentication complete! {self.access_token}")
        
        #get dump of all NSE instruments
        self.instrument_list = self.kite.instruments("NFO")
        self.instrument_df = pd.DataFrame(self.instrument_list)
        
        # Arguments
        parser = argparse.ArgumentParser(description='Process options.')
        parser.add_argument('--underlying', choices=['NIFTY', 'BANKNIFTY'], default='NIFTY', help='NIFTY, BANKNIFTY')
        parser.add_argument('--option_type', choices=['CE', 'PE'], default='CE', help='The option type (CE for Call European, PE for Put European)')
        parser.add_argument('--stoploss', type=int, default=5, help='Stoploss value in points. Default is 5.')
        parser.add_argument('--takeprofit', type=int, default=5, help='Take profit value in points. Default is 5.')
        parser.add_argument('--lots', type=int, default=2, help='No. of lots. Default is 2.')
        parser.add_argument('--exp_offset', type=int, default=0, help='Expiry offset Default is 0.')
        parser.add_argument('--atm_offset', type=int, default=3, help='OTM/ATM/ITM Default is 3.')


        self.args = parser.parse_args()

        # Access the arguments
        self.underlying = self.args.underlying.upper()
        self.option_type = self.args.option_type.upper()
        self.stoploss = self.args.stoploss # points
        self.takeprofit = self.args.takeprofit # points 
        self.lots = self.args.lots # Dont Change this!
        
        # Schedule Strategy Callback
        self.interval = 2 # run every 2 secs
        self.duration = 60 * 60 * 5 # run for 5 hours
        self.starttime = time.time()
        self.timeout = self.starttime + self.duration
        
        # Set this to true when order is already placed
        # TODO: GET Buy ID if order is already placed
        self.order_placed = False
        sym_map = {
            "NIFTY": "NSE:NIFTY 50",
            "BANKNIFTY": "NSE:NIFTY BANK",
        }

        self.underlying_price = self.kite.ltp(sym_map[self.underlying])[sym_map[self.underlying]]["last_price"]
        print(f"{self.underlying} Last Traded Price: {self.underlying_price}")
        
        self.expiry_idx = self.args.exp_offset
        self.atm_offset = self.args.atm_offset
        self.opt_chain = self.get_atm_contract(duration=self.expiry_idx, offset=self.atm_offset) 
        symbol = self.opt_chain.tradingsymbol.to_list()[0]
        contract_price = self.kite.ltp(f"NFO:{symbol}")[f"NFO:{symbol}"]["last_price"]
        
        print(f"{'ATM' if self.atm_offset == 0 else f'OTM {self.atm_offset}'} - {symbol} - {contract_price}")
        
        self.tokens = self.opt_chain["instrument_token"].to_list()
        self.symbol_dict = dict(zip(self.opt_chain.instrument_token, self.opt_chain.tradingsymbol))
        self.option_data = {self.symbol_dict[i]:{} for i in self.tokens}
        
        for symbol in self.opt_chain.tradingsymbol:
            self.option_data[symbol]["strike"] = self.opt_chain.loc[self.opt_chain.tradingsymbol == symbol, "strike"].to_list()[0]
            self.option_data[symbol]["type"] = self.opt_chain.loc[self.opt_chain.tradingsymbol == symbol, "instrument_type"].to_list()[0]
            self.option_data[symbol]["time_to_expiry"] = self.opt_chain.loc[self.opt_chain.tradingsymbol == symbol, "time_to_expiry"].to_list()[0]
            self.option_data[symbol]["lot_size"] = self.opt_chain.loc[self.opt_chain.tradingsymbol == symbol, "lot_size"].to_list()[0]
        
    def auto_login(self):
        
        try:
            http_session = requests.Session()
            url = http_session.get(url='https://kite.trade/connect/login?v=3&api_key='+self.api_key).url
            response = http_session.post(url='https://kite.zerodha.com/api/login', data={'user_id':self.user_id, 'password':self.user_password})
            resp_dict = json.loads(response.content)
            http_session.post(url='https://kite.zerodha.com/api/twofa', data={'user_id':self.user_id, 'request_id':resp_dict["data"]["request_id"], 'twofa_value':pyotp.TOTP(self.totp_token).now()})
            url = url + "&skip_session=true"
            response = http_session.get(url=url, allow_redirects=True).url
            request_token = parse_qs(urlparse(response).query)['request_token'][0]

            self.kite_session = self.kite.generate_session(request_token, api_secret=self.api_secret)
            self.access_token = self.kite_session["access_token"]
            
            return True
        except Exception as e:
            print(f"Auto Login Failed {e}")
            try:
                print(self.kite.login_url())
                request_token = input("Enter request token: ")
                self.kite_session = self.kite.generate_session(request_token, self.api_secret)
                self.access_token = self.kite_session["access_token"]
                self.kite.set_access_token(self.access_token)
                return True    
            except Exception as e:
                print(f"Manual Login Failed {e}")
                return False 
            
    
    @multitasking.task
    def start_streaming(self):
        kws = KiteTicker(self.api_key, self.kite.access_token) 
        kws.on_ticks = self.on_ticks
        kws.on_connect = self.on_connect
        kws.on_close = self.on_close
        
        # Connect Web Socket
        kws.connect(threaded=True)
    
    def run(self):
        while time.time() <= self.timeout:
            try:
                self.strategy()
                time_left = self.interval - ((time.time() - self.starttime)%self.interval)
                time.sleep(time_left)
            except KeyboardInterrupt:
                self.at_exit()
        self.at_exit()
        
    def at_exit(self):
        print('\n\nKeyboard exception received. Do you want to square off? [Y/N]: ', end='')
        response = input().strip().lower()
        if response == 'y':
            print('Squaring off... Exiting')
            self.squareOff()
            time.sleep(1)
            exit()
        else:
            print('Exiting.')
            exit()
    
    @multitasking.task
    def on_ticks(self, ws, ticks):
        # print('tick recieved')
        self.processTick(ticks)
        
    @multitasking.task
    def on_connect(self, ws, response):
        # Callback on successful connect.
        ws.subscribe(self.tokens)
        ws.set_mode(ws.MODE_FULL, self.tokens)
    @multitasking.task
    def on_close(self, ws, code, reason):
        # On connection close stop the main loop
        # Reconnection will not happen after executing `ws.stop()`
        ws.stop()
    
    def processTick(self, ticks):
        for tick in ticks:
            self.option_data[self.symbol_dict[tick['instrument_token']]]["price"] = float(tick["last_price"])
            self.option_data[self.symbol_dict[tick['instrument_token']]]["oi"] = int(tick["oi"])
            self.option_data[self.symbol_dict[tick['instrument_token']]]["volume"] = int(tick["volume_traded"])
            self.option_data[self.symbol_dict[tick['instrument_token']]]["bid"] = float(tick["depth"]["buy"][0]["price"])
            self.option_data[self.symbol_dict[tick['instrument_token']]]["ask"] = float(tick["depth"]["sell"][0]["price"])
            self.option_data[self.symbol_dict[tick['instrument_token']]]["mid_price"] = (float(tick["depth"]["buy"][0]["price"]) + float(tick["depth"]["sell"][0]["price"]))/2
            
    def option_contracts(self):
        option_contracts = []
        instrument_list = self.kite.instruments("NFO")
        for instrument in instrument_list:
            if instrument["name"] == self.underlying and instrument["instrument_type"]==self.option_type:
                option_contracts.append(instrument)
        return pd.DataFrame(option_contracts)
    
    def get_atm_contract(self, duration = 0, offset = 0):
        self.df_opt_contracts = self.option_contracts()
        
        self.df_opt_contracts["time_to_expiry"] = (pd.to_datetime(self.df_opt_contracts["expiry"]) + dt.timedelta(0,16*3600) - dt.datetime.now()).dt.total_seconds() / dt.timedelta(days=1).total_seconds() + 1 # add 1 to get around the issue of time to expiry becoming 0 for options maturing on trading day   
        min_day_count = np.sort(self.df_opt_contracts["time_to_expiry"].unique())[duration]
        
        temp = (self.df_opt_contracts[self.df_opt_contracts["time_to_expiry"] == min_day_count]).reset_index(drop=True)
        temp.sort_values(by=["strike"],inplace=True, ignore_index=True)
        atm_idx = abs(temp["strike"] - self.underlying_price).argmin()
        
        loc = atm_idx + offset if self.option_type == "CE" else atm_idx - offset
        return temp.iloc[[loc]]

    def is_contract_present(self,df):
        if len(df)>0:
            if len([i for i in df.tradingsymbol if i in self.option_data_df.index]) > 0:
                return True
    
        return False
    
    def instrumentLookup(self, symbol):
        """Looks up instrument token for a given script from instrument dump"""
        try:
            return self.instrument_df[self.instrument_df.tradingsymbol==symbol].instrument_token.values[0]
        except:
            return -1
    
    def fetchOHLC(self, ticker, interval, duration):
        """extracts historical data and outputs in the form of dataframe"""
        instrument = self.instrumentLookup(ticker)
        data = pd.DataFrame(self.kite.historical_data(instrument,dt.date.today()-dt.timedelta(duration), dt.date.today(),interval))
        data.set_index("date",inplace=True)
        return data
    
    def create_order_params(self):
        order_params = []
        # Check if option_data_df is not empty and has 'price' column
        if not self.option_data_df.empty and 'price' in self.option_data_df.columns:
            tradingsymbol = self.option_data_df.index.to_list()[0]
            price = self.option_data_df['price'].to_list()[0]  # Access price using column name

            # Check if opt_chain has 'lot_size' attribute
            if hasattr(self.opt_chain, 'lot_size') and not self.opt_chain.lot_size.empty:
                quantity = self.opt_chain.lot_size.to_list()[0]
                
                order_params = [{
                                "exchange": "NFO",
                                "tradingsymbol": tradingsymbol,
                                "transaction_type": "BUY",
                                "variety": "regular",
                                "product": "NRML",
                                "order_type": "LIMIT",
                                "quantity": quantity,
                                "price": price
                                }]
        return order_params

    def risk_reward(self):
        strikes = self.option_data.strike.to_list()
        price = self.option_data.price.to_list()
        risk_to_reward = (float(price[0]) - float(price[1]))/(int(strikes[1]) - int(strikes[0]))
        print(f"Risk/Reward: {risk_to_reward}")
        return round(risk_to_reward,2)
    
    @multitasking.task
    def check_margin(self, order_param, threshold=0.5):
        margin = self.kite.margins()   
        cash_avl = margin["equity"]["net"]
        bskt_order_margin = self.kite.basket_order_margins(order_param)
        req_margin = bskt_order_margin["final"]["total"]
        print(f"Required Margin: {req_margin} Available Cash: {cash_avl} Cash Allocated: {threshold * cash_avl}")
        if float(req_margin) < threshold * cash_avl:
            return True
        else:
            return
    
    @multitasking.task
    def placeSLOrder(self, order_params):
        if len(order_params) > 0:
            order = order_params[0]  
            # Place an intraday stop loss order on NSE
            buy_order = {
                "tradingsymbol":order["tradingsymbol"],
                "exchange":self.kite.EXCHANGE_NFO,
                "transaction_type":self.kite.TRANSACTION_TYPE_BUY,
                "quantity":order['quantity'] * self.lots,
                "order_type":self.kite.ORDER_TYPE_MARKET,
                "product":self.kite.PRODUCT_MIS,
                "variety":self.kite.VARIETY_REGULAR
            }
            sl_sell_order = {
                "tradingsymbol":order["tradingsymbol"],
                "exchange":self.kite.EXCHANGE_NFO,
                "transaction_type":self.kite.TRANSACTION_TYPE_SELL,
                "quantity":order['quantity'] * self.lots,
                "order_type":self.kite.ORDER_TYPE_SL,
                "price":round(order["price"] - self.stoploss,1),
                "trigger_price ": round(order["price"] - self.stoploss,1),
                "product":self.kite.PRODUCT_MIS,
                "variety":self.kite.VARIETY_REGULAR
            }
            # try:
            print(f"[MARKET] Buy Order {buy_order}")
            self.buy_order_id = self.kite.place_order(tradingsymbol=buy_order["tradingsymbol"],
                    exchange=buy_order["exchange"],
                    transaction_type=buy_order["transaction_type"],
                    quantity=buy_order["quantity"],
                    order_type=buy_order["order_type"],
                    product=buy_order["product"],
                    variety=buy_order["variety"])
            
            self.order_status_check(self.buy_order_id)
            
            print(f"[SL ORDER] Sell Order {sl_sell_order}")
            self.sell_order_id = self.kite.place_order(tradingsymbol=sl_sell_order["tradingsymbol"],
                    exchange=sl_sell_order["exchange"],
                    transaction_type=sl_sell_order["transaction_type"],
                    quantity=sl_sell_order["quantity"],
                    order_type=sl_sell_order["order_type"],
                    price=round(order["price"] - self.stoploss,1),
                    trigger_price=round(order["price"] - self.stoploss,1),
                    product=sl_sell_order["product"],
                    variety=sl_sell_order["variety"])
            
            print(f"Order Executed - Buy Order: {self.buy_order_id}, Sell Order: {self.sell_order_id}")
            self.order_placed = True
            # except Exception as e:
                # print(e)
                
    def placeBasketOrder(self, order_param_list, assure_execution=[0]):
        #the first order param in the list should be the buy/hedge order
        for count, order in enumerate(order_param_list):
            order_id = self.placeLimitOrder(order)
            if count in assure_execution:
                self.order_status_check(order_id)
    
    def placeLimitOrder(self, order_params):    
        # Place an intraday limit order on NFO
        order_id = self.kite.place_order(tradingsymbol=order_params['tradingsymbol'],
                                    exchange=order_params['exchange'],
                                    transaction_type=order_params['transaction_type'],
                                    quantity=order_params['quantity'],
                                    price=order_params['price'],
                                    order_type=order_params['order_type'],
                                    product=order_params['product'],
                                    variety=order_params['variety'])
        return order_id
    
    def modifyOrder(self,order_id,price):    
        # Modify order given order id
        order_params = {
        "order_id":order_id,
        "price":round(price,1) - 0.5,
        "trigger_price":price,
        "order_type":self.kite.ORDER_TYPE_SL,
        "variety":self.kite.VARIETY_REGULAR
        }
        print(f"Modifiying order {order_params}")
        self.kite.modify_order(order_id=order_id,
                    price=round(price,1),
                    trigger_price=price,
                    order_type=self.kite.ORDER_TYPE_SL,
                    variety=self.kite.VARIETY_REGULAR) 
    
    def placeMarketOrder(self, symbol,buy_sell,quantity):    
        print(f"[MARKET ORDER] {symbol}, {buy_sell}, {quantity}")
        # Place an intraday market order on NSE
        if buy_sell == "buy":
            t_type=self.kite.TRANSACTION_TYPE_BUY
        elif buy_sell == "sell":
            t_type=self.kite.TRANSACTION_TYPE_SELL
        self.kite.place_order(tradingsymbol=symbol,
                        exchange=self.kite.EXCHANGE_NFO,
                        transaction_type=t_type,
                        quantity=quantity,
                        order_type=self.kite.ORDER_TYPE_MARKET,
                        product=self.kite.PRODUCT_MIS,
                        variety=self.kite.VARIETY_REGULAR)
        
    def cancelOrder(self, order_id):   
        print(f"[CANCEL ORDER] {order_id}")
        # Modify order given order id
        self.kite.cancel_order(order_id=order_id,
                        variety=self.kite.VARIETY_REGULAR)  
    
    def squareOff(self):
        #fetching orders and position information   
        a,b = 0,0
        while a < 10:
            try:
                pos_df = pd.DataFrame(self.kite.positions()["day"])
                break
            except:
                print("can't extract position data..retrying")
                a+=1
        while b < 10:
            try:
                ord_df = pd.DataFrame(self.kite.orders())
                break
            except:
                print("can't extract order data..retrying")
                b+=1


        #closing all pending orders
        if not ord_df.empty and 'status' in ord_df.columns:
            pending = ord_df[ord_df['status'].isin(["TRIGGER PENDING","OPEN"])]["order_id"].tolist()
            drop = []
            attempt = 0
            while len(pending)>0 and attempt<5:
                pending = [j for j in pending if j not in drop]
                for order in pending:
                    try:
                        self.cancelOrder(order)
                        drop.append(order)
                    except:
                        print("unable to delete order id : ",order)
                        attempt+=1   
                        

        #closing all open position      
        for i in range(len(pos_df)):
            ticker = pos_df["tradingsymbol"].values[i]
            if pos_df["quantity"].values[i] > 0:
                quantity = pos_df["quantity"].values[i]
                self.placeMarketOrder(ticker,"sell", quantity)
            if pos_df["quantity"].values[i] < 0:
                quantity = abs(pos_df["quantity"].values[i])
                self.placeMarketOrder(ticker,"buy", quantity)
    
    def order_status_check(self, ord_id):
        pending_complete = True
        while pending_complete:
            orders = self.kite.orders()
            orders_df = pd.DataFrame(orders)
            status = orders_df.loc[orders_df.order_id == ord_id, "status"].to_list()[0]
            if status == "COMPLETE":
                print(f"Order Executed: {ord_id}")
                # print(f"Order Details: {orders_df.loc[orders_df.order_id == ord_id]}")
                break
            time.sleep(0.5)
           
    def is_present(self, df):
        if len(df)>0:
            if len([i for i in df.tradingsymbol if i in self.option_data_df.index]) > 0:
                return True
    
    @multitasking.task
    def strategy(self):
        a,b,c = 0,0,0
        while a < 10:
            try:
                pos_df = pd.DataFrame(self.kite.positions()["day"])
                break
            except:
                print("can't extract position data..retrying")
                a+=1
        while b < 10:
            try:
                ord_df = pd.DataFrame(self.kite.orders())
                break
            except:
                print("can't extract order data..retrying")
                b+=1
        # while c < 10:
        #     try:
        #         holding_df = pd.DataFrame(self.kite.holdings())
        #         break
        #     except:
        #         print("can't extract order data..retrying")
        #         c+=1

        self.option_data_df = pd.DataFrame(self.option_data).T
        
        # check if already in trade
        if self.order_placed is True:
            # print('Already in trade. Checking for SL and Take Profit')
            # for symbol in self.opt_chain.tradingsymbol:
            symbol = self.opt_chain.tradingsymbol.to_list()[0]
            if not pos_df.empty:
                if symbol in pos_df["tradingsymbol"].to_list():
                    # print(ord_df.head())
                    filtered_orders = ord_df.loc[(ord_df['tradingsymbol'] == symbol) & (ord_df['status'].isin(["TRIGGER PENDING", "OPEN"]))]
                    if not filtered_orders.empty:
                        pending_order_id = filtered_orders["order_id"].values[0]
                        buy_price = ord_df.loc[ord_df.order_id == self.buy_order_id]["average_price"].values[0]
                        ltp = self.option_data_df['price'].to_list()[0] 
                        # pnl = self.option_data_df['pnl'].to_list()[0] 
                        ask_price = self.option_data_df['ask'].to_list()[0] 
                        # mid_price = self.option_data_df['mid_price'].to_list()[0] 
                        # sell_order = ord_df.loc[ord_df.order_id == self.sell_order_id]
                        
                        stop_loss_price = round(buy_price - self.stoploss,1)
                        take_profit_price = round(buy_price  + self.takeprofit,1)
                        
                        # print(f"{buy_price}/{ltp}/{mid_price}")
                        # print(f"\r{buy_price} | {take_profit_price} | {stop_loss_price} | {ltp}")
                        print(f"\r{buy_price} | {take_profit_price} | {stop_loss_price} | {ltp}", end='', flush=True)

                        # Determine the new price based on LTP
                        try:
                            if ltp <= stop_loss_price:
                                self.modifyOrder(pending_order_id, ask_price)
                                print("Stop loss condition met... Exiting")
                                time.sleep(2)
                            elif ltp >= take_profit_price:
                                self.modifyOrder(pending_order_id, ask_price)
                                print("Take profit condition met... Exiting")
                                time.sleep(2)
                        except Exception as e:
                            pass
                    else:
                        # Handle the case when no matching orders are found
                        pending_order_id = None
                        print('No SL Order found')
                        exit()
                        
            else:
                print('EMPTY POSITIONS')
                    
        else:
            # print(f"Creating ORDER")
            order_params = self.create_order_params()
            
            if self.check_margin(order_params):
                self.order_placed = True
                self.placeSLOrder(order_params)
            else:
                print(f"insufficient margin to place order {order_params}")
                
if __name__ == "__main__":
    kt = ZerodhaOptionBuyer()
    
    try:
        kt.start_streaming()
        time.sleep(3)
        kt.run()
    
    except Exception as e:
        print(e)
    