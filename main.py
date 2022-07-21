import os
import time
import copy
import json
import builtins
import math
import random

from binance.spot import Spot
from binance.websocket.spot.websocket_client import SpotWebsocketClient
from binance.error import ClientError
from tradingview_ta import get_multiple_analysis, Interval

####################
### CONFIG BEGIN ###
####################

# User APIs
API = {
    'Tulu': {
        'key': 'xd4e3uY3A2ac9hj1nI7adWRiI9tRevGoeM35QhQD5pCO680CnboWE5vJ988DL95L',
        'secret': '3uBjBgGyEaqyDOmVjLKRGx2JEuRJ3t2SEosbF925hCv9CCunjtj8KIHyR1nss1q3',
    },
    'Ozhan': {
        'key': 's7AVdedXVVhGYOuswx6pOHy6OyPPKDimBLfhKR4JMIcWK9feH5PN7zrItpkVXIVL',
        'secret': '34Yq97P81r0GUCQudQHMFfAvb7orlmtpCiNBKNvVzgcwEeUzCRmq00lRWPalIB9I',
    },
}

# Trade using these coins
PROGRAM_QUOTE_ASSETS = ['BUSD', 'BNB', 'USDT', 'BTC', 'ETH']
# The amount of time between loops
LOOP_DELAY_SECONDS = 1
# Proportion of bot's min notional; if the balance is higher than this then the bot will not buy in an asset pair
NOTIONAL_HOLD_THRESHOLD_FAC = 0.95
# Proportion of Binance's min notional to use for calculating floored buy quantity
MIN_NOTIONAL_BUY_FAC = 1.03
# Proportion of available quote asset balance to use on each BUY order
QUOTE_PER_TRANSACTION_FAC = 1/1
# Sell coins when the price increases by this percent
PRICE_INCREASE_SELL_THRESHOLD_PERCENT = 0.2
# As above, but for the short-term strategy
SHORT_TERM_PRICE_INCREASE_SELL_THRESHOLD_PERCENT = 0.01
# Ignore coins below this volume
QUOTE_VOLUME_MIN = 0.0
# Do not buy or sell these coins
EXCLUDE_BASE_ASSETS = {'TUSD', 'USDC', 'EUR', 'BTC', 'BUSD', 'BNB', 'USDP', 'AUD', 'UST', 'LUNA'}


def run_trade_strategy(rec, last_rec, _did_hit_strong_sell):
    ####### TRADING STRATEGY #######
    
    if (
    ### SELL STRATEGY
        
        # rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'BUY'} and
        # rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_1_HOUR]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_HOUR]['osc'] in {'BUY'} and
        # rec[Interval.INTERVAL_1_HOUR]['mav'] in {'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_2_HOURS]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_2_HOURS]['osc'] in {'BUY'} and
        # rec[Interval.INTERVAL_2_HOURS]['mav'] in {'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_4_HOURS]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_4_HOURS]['osc'] in {'BUY'} and
        # rec[Interval.INTERVAL_4_HOURS]['mav'] in {'STRONG_BUY'} and
        
        
        #rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_DAY]['osc'] in {'BUY'} and
        #rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_WEEK]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MONTH]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_BUY'} and
    ###
    True):
        return -1
    elif (
    ### BUY STRATEGY
    
        # rec[Interval.INTERVAL_1_MINUTE]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MINUTE]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MINUTE]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_5_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_5_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_5_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'SELL', 'STRONG_SELL'} and
        
        # rec[Interval.INTERVAL_1_HOUR]['sum'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_1_HOUR]['osc'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_1_HOUR]['mav'] in {'SELL', 'STRONG_SELL'} and
        
        # rec[Interval.INTERVAL_2_HOURS]['sum'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_2_HOURS]['osc'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_2_HOURS]['mav'] in {'SELL', 'STRONG_SELL'} and
        
        # rec[Interval.INTERVAL_4_HOURS]['sum'] in {'SELL', 'STRONG_SELL', 'NEUTRAL'} and
        # rec[Interval.INTERVAL_4_HOURS]['osc'] in {'SELL', 'STRONG_SELL', 'NEUTRAL'} and
        # rec[Interval.INTERVAL_4_HOURS]['mav'] in {'SELL', 'STRONG_SELL', 'NEUTRAL'} and
        
        # _did_hit_strong_sell.get(Interval.INTERVAL_15_MINUTES, {}).get('sum', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_15_MINUTES, {}).get('osc', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_15_MINUTES, {}).get('mav', False) and
        
        # _did_hit_strong_sell.get(Interval.INTERVAL_30_MINUTES, {}).get('sum', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_30_MINUTES, {}).get('osc', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_30_MINUTES, {}).get('mav', False) and
        
        # _did_hit_strong_sell.get(Interval.INTERVAL_1_HOUR, {}).get('sum', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_1_HOUR, {}).get('osc', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_1_HOUR, {}).get('mav', False) and
        
        # _did_hit_strong_sell.get(Interval.INTERVAL_2_HOURS, {}).get('sum', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_2_HOURS, {}).get('osc', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_2_HOURS, {}).get('mav', False) and
        
        # _did_hit_strong_sell.get(Interval.INTERVAL_4_HOURS, {}).get('sum', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_4_HOURS, {}).get('osc', False) and
        # _did_hit_strong_sell.get(Interval.INTERVAL_4_HOURS, {}).get('mav', False) and
        
        #rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_DAY]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_WEEK]['osc'] in {'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_SELL'} and
        
        # rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_SELL'} and
        # rec[Interval.INTERVAL_1_MONTH]['osc'] in {'SELL', 'STRONG_SELL'} and
        # rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_SELL'} and
    ###
    True):
        return 1
    else:
        return 0
        

def run_short_term_trade_strategy(rec):
    if (
        # short-term BUY strategy
    
        # rec[Interval.INTERVAL_1_MINUTE]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MINUTE]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_MINUTE]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_5_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_5_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_5_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_1_HOUR]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_HOUR]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_1_HOUR]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_2_HOURS]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_2_HOURS]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_2_HOURS]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        # rec[Interval.INTERVAL_4_HOURS]['sum'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_4_HOURS]['osc'] in {'BUY', 'STRONG_BUY'} and
        # rec[Interval.INTERVAL_4_HOURS]['mav'] in {'BUY', 'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_MONTH]['osc'] in {'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_SELL'} and
        
    True):
        return 1
    else:
        return 0


def run_index_strategy(rec, last_rec):
    ####### INDEX STRATEGY #######
    
    if (
    ### SELL STRATEGY
        
        rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_HOUR]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_HOUR]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_1_HOUR]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_2_HOURS]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_2_HOURS]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_2_HOURS]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_4_HOURS]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_4_HOURS]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_4_HOURS]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_DAY]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_WEEK]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_BUY'} and
        
        rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_MONTH]['osc'] in {'BUY'} and
        rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_BUY'} and
    ###
    True):
        return -1
    elif (
    ### BUY STRATEGY
        
        rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_1_HOUR]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_HOUR]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_HOUR]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_2_HOURS]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_2_HOURS]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_2_HOURS]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_4_HOURS]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_4_HOURS]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_4_HOURS]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_DAY]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_WEEK]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_SELL'} and
        
        rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_MONTH]['osc'] in {'NEUTRAL', 'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_SELL'} and
    ###
    True):
        return 1
    else:
        return 0


##################
### CONFIG END ###
##################

# Override print to include timestamps
def print(s):
    builtins.print('[{}] {}'.format(time.strftime("%Hh %Mm %Ss", time.localtime()), s))


print('Starting up...')

# Calculate factor from percent increase config
PRICE_INCREASE_SELL_THRESHOLD_FACTOR = (100 + PRICE_INCREASE_SELL_THRESHOLD_PERCENT) / 100
# As above, but for the short-term strategy
SHORT_TERM_PRICE_INCREASE_SELL_THRESHOLD_FACTOR = (100 + SHORT_TERM_PRICE_INCREASE_SELL_THRESHOLD_PERCENT) / 100

# Set up tradingview-ta
ta_intervals = list()
i = Interval()
ta_intervals = [i.__getattribute__(e) for e in dir(i) if (e.startswith('INTERVAL_'))]
del i
print('Intervals: {}'.format(ta_intervals))

    
# Handle Binance API exceptions
def except_api(e):
    print(e)
    # BAD_SYMBOL
    if e.error_code == -1021:
      pass
    # NEW_ORDER_REJECTED
    elif e.error_code == -2010:
        pass
    # CANCEL_REJECTED
    elif e.error_code == -2011:
        pass
    elif e.error_code == -1003:
        raise PermissionError(60)
    else:
        raise Exception


# Basically initialises the bot, then returns a function to run the loop
def create_loop_environment():

    #################
    ### Functions ###
    #################

    # Save previous buy prices etc in a file
    def record_session_data():
        with open('buy_prices.json', 'w') as f:
            f.write(json.dumps(buy_prices))
        
        with open('short_term_buy_prices.json', 'w') as f:
            f.write(json.dumps(short_term_buy_prices))
            
        with open('short_term_balances.json', 'w') as f:
            f.write(json.dumps(short_term_balances))
        
        with open('short_term_pending_orders.json', 'w') as f:
            f.write(json.dumps(short_term_pending_orders))
        
        with open('did_hit_strong_sell.json', 'w') as f:
            f.write(json.dumps(did_hit_strong_sell))


    # Shut down websockets (e.g. before quitting)
    def wsquit():
        print('Closing websockets...')
        # Stop client websockets
        for ws in binance_ws_clients.values():
            try:
                ws.stop()
            except Exception as e:
                print(e)
        # Stop ticker websocket
        try:
            binance_ticker_ws.stop()
        except Exception as e:
            print(e)
   

    # Create a user data websocket message handler
    # See https://stackoverflow.com/a/3431699
    def make_ws_message_handler(user):
        # Construct a message handler
        def on_message(msg):
            if msg.get('e') == 'executionReport':
                if msg.get('x') == 'TRADE':
                    s = msg.get('s')
                    client_order_id = msg.get('c')
                    if msg.get('S') == 'BUY':
                        # For buy orders
                        price = float(msg.get('p'))
                        # Check if this was a short-term order
                        if client_order_id in short_term_pending_orders:
                            # Track balances for short-term orders
                            short_term_buy_prices[user][s] = price
                            short_term_balances[user][s] = short_term_balances[user].get(s, 0) + float(msg.get('q'))
                            # Remove filled orders from pending orders
                            if msg.get('X') == 'FILLED':
                                short_term_pending_orders.pop(client_order_id)
                            print('record short-term price for api#{} {} @ {}'.format(user, s, price))
                        else:
                            buy_prices[user][s] = float(price)
                            print('record price for api#{} {} @ {}'.format(user, s, price))
                    elif msg.get('S') == 'SELL':
                        # Track how much is left after selling
                        if client_order_id in short_term_pending_orders:
                            short_term_balances[user][s] = short_term_balances[user].get(s, 0) - float(msg.get('q'))
                            # Remove filled orders from pending orders
                            if msg.get('X') == 'FILLED':
                                short_term_pending_orders.pop(client_order_id)
                            
                    record_session_data()
            elif msg.get('e') == 'error':
                # This thing had an error, so replace it with a new one
                print('Socket error for api#{}: {}'.format(user, msg.get('m')))
                binance_ws_clients[user] = SpotWebsocketClient()
                binance_ws_clients[user].start()
                listen_key = binance_clients[user].new_listen_key().get('listenKey')
                binance_ws_clients[user].user_data(
                    listen_key=listen_key,
                    id=1,
                    callback=make_ws_message_handler(user)
                )
    
        return on_message


    # Send a limit order
    def send_order(api_index, symbol, side, quantity, price):
        quantity = f'{quantity:.10f}'.rstrip('0').rstrip('.')
        print('api#{} {} {} x {} @ {}'.format(api_index, side, symbol, quantity, price))
        try:
            binance_clients[api_index].new_order(
                symbol=symbol,
                side=side,
                type='LIMIT_MAKER',
                quantity=quantity,
                price=price
            )
        except ClientError as e:
            except_api(e)


    def send_record_short_term_order(api_index, symbol, side, quantity, price):
        quantity = f'{quantity:.10f}'.rstrip('0').rstrip('.')
        print('short term api#{} {} {} x {} @ {}'.format(api_index, side, symbol, quantity, price))
        try:
            order_out = binance_clients[api_index].new_order(
                symbol=symbol,
                side=side,
                type='LIMIT_MAKER',
                quantity=quantity,
                price=price
            )
            short_term_pending_orders[order_out.get('clientOrderId')] = {
                'apiIndex': api_index,
                'side': side
            }
        except ClientError as e:
            except_api(e)


    # Cancel unfilled orders before placing new ones
    def cancel_orders():
        for user, client in binance_clients.items():
            open_orders = client.get_open_orders()
            for o in open_orders:
                # Don't cancel partially-filled orders
                # However, the price may change after an order is partially-filled,
                # meaning that the order may never finish.
                if o.get('status') != 'PARTIALLY_FILLED':
                    client.cancel_order(symbol=o.get('symbol'), orderId=o.get('orderId'))
                    print("Cancel order: api#{} {} {} {} {} {}".format(
                        user,
                        o.get('orderId'),
                        o.get('status'),
                        o.get('side'),
                        o.get('symbol'),
                        o.get('price')
                    ))
                else:
                    print("Do not cancel order: api#{} {} {} {} {} {}".format(
                        user,
                        o.get('orderId'),
                        o.get('status'),
                        o.get('side'),
                        o.get('symbol'),
                        o.get('price')
                    ))

    #############
    ### Setup ###
    #############
    
    # Record most recent buy prices per API per symbol
    # { api: { symbol: price } }
    buy_prices = dict()
    # Try to read previous buy prices from the file
    try:
        f = open('buy_prices.json', 'r')
        res = f.read()
        f.close()
        buy_prices = json.loads(res)
        print('Loaded `buy_prices`')
    except:
        print('Could not load `buy_prices`, starting fresh')
        
    # As above, but for the short-term strategy
    short_term_buy_prices = dict()
    try:
        f = open('short_term_buy_prices.json', 'r')
        res = f.read()
        f.close()
        short_term_buy_prices = json.loads(res)
        print('Loaded `short_term_buy_prices`')
    except:
        print('Could not load `short_term_buy_prices`, starting fresh')
        
    # { api: { symbol: balance } }
    short_term_balances = dict()
    try:
        f = open('short_term_balances.json', 'r')
        res = f.read()
        f.close()
        short_term_balances = json.loads(res)
        print('Loaded `short_term_balances`')
    except:
        print('Could not load `short_term_balances`, starting fresh')
        
    # { clientOrderId: api, side }
    short_term_pending_orders = dict()
    try:
        f = open('short_term_pending_orders.json', 'r')
        res = f.read()
        f.close()
        short_term_pending_orders = json.loads(res)
        print('Loaded `short_term_pending_orders`')
    except:
        print('Could not load `short_term_pending_orders`, starting fresh')
    
    # { symbol: { interval: { type: bool } }
    did_hit_strong_sell = dict()
    try:
        f = open('did_hit_strong_sell.json', 'r')
        res = f.read()
        f.close()
        did_hit_strong_sell = json.loads(res)
        print('Loaded `did_hit_strong_sell`')
    except:
        print('Could not load `did_hit_strong_sell`, starting fresh')

    # Set up Binance clients
    print('Create Binance clients...')
    binance_clients = dict()
    binance_ws_clients = dict()
    binance_default_client = None
    for user, v in API.items():
        # Create a client
        binance_clients[user] = Spot(key=v.get('key'), secret=v.get('secret'))
        # Set a default SPOT client for getting information from Binance
        if not binance_default_client:
            binance_default_client = binance_clients[user]
        # Create a WS client
        binance_ws_clients[user] = SpotWebsocketClient()
        binance_ws_clients[user].start()
        # Set up user data stream
        listen_key = binance_clients[user].new_listen_key().get('listenKey')
        binance_ws_clients[user].user_data(
            listen_key=listen_key,
            id=1,
            callback=make_ws_message_handler(user)
        )
        # Create buy price dictionary for this user
        if not user in buy_prices:
            buy_prices[user] = dict()
        # As above, but for the short-term strategy
        if not user in short_term_buy_prices:
            short_term_buy_prices[user] = dict()
        # As above, but for tracking short-term trading balances
        if not user in short_term_balances:
            short_term_balances[user] = dict()


    # Set up trading pairs
    symbol_infos = dict()
    symbol_list = list()
    # Get symbol info from exchange info endpoint
    print('Get symbols from Binance...')
    exchange_info_symbols = binance_default_client.exchange_info().get('symbols')
    for e in exchange_info_symbols:
        ba = e.get('baseAsset')
        qa = e.get('quoteAsset')
        # Filter out symbols to ignore
        if (
            qa in PROGRAM_QUOTE_ASSETS and
            ba not in EXCLUDE_BASE_ASSETS and
            'BULL' not in ba and
            'BEAR' not in ba
        ):
            # Store the symbols and some useful info
            s = e.get('symbol')
            symbol_list.append(s)
            f = e.get('filters')
            
            min_notional = float(next(
                (e for e in f if e.get('filterType') == 'MIN_NOTIONAL')
            ).get('minNotional'))
            
            symbol_infos[s] = {
                'quoteAsset': qa,
                'baseAsset': ba,
                
                'minNotional': min_notional,
                'minNotionalBuy': min_notional * MIN_NOTIONAL_BUY_FAC,
                'holdThreshold': min_notional * NOTIONAL_HOLD_THRESHOLD_FAC,
                
                'stepSize': float(next(
                    (e for e in f if e.get('filterType') == 'LOT_SIZE')
                ).get('stepSize')),
            }
    del exchange_info_symbols
    # List of trading symbols for use with tradingview-ta
    prefixed_symbol_list = ['BINANCE:{}'.format(s) for s in symbol_list]
    
    # Update prices and volumes using another websocket
    print('Subscribe to Binance tickers...')
    binance_ticker_ws = SpotWebsocketClient()
    binance_ticker_ws.start()

    def book_ticker(message):
        s = message.get('s')
        if s in symbol_infos:
            symbol_infos[s]['bidPrice'] = message.get('b')
            symbol_infos[s]['askPrice'] = message.get('a')

    def mini_ticker(message):
        if isinstance(message, list):
            for t in message:
                s = t.get('s')
                if s in symbol_infos:
                    symbol_infos[s]['quoteVolume'] = t.get('q')
    
    binance_ticker_ws.book_ticker(id=1, callback=book_ticker)
    binance_ticker_ws.mini_ticker(id=2, callback=mini_ticker)
    
    # Record prices and volumes for all relevant symbols
    print('Get tickers from Binance...')
    
    # Add BTCBUSD for use as an index and split up the list into manageable request sizes (thanks Binance)
    long_symbol_list = symbol_list + ['BTCBUSD']
    REQUEST_LIST_SIZE_MAX = 400
    pointer_index = 0
    while pointer_index < len(long_symbol_list):
        end_index = min(pointer_index + REQUEST_LIST_SIZE_MAX, len(long_symbol_list))
        request_symbol_list = long_symbol_list[pointer_index:end_index]
        pointer_index += REQUEST_LIST_SIZE_MAX
        
        ticker_24hr = binance_default_client.ticker_24hr(symbols=request_symbol_list) # get BTCBUSD for use as an index
        for t in ticker_24hr:
            s = t.get('symbol')
            if s in symbol_infos:
                symbol_infos[s]['bidPrice'] = t.get('bidPrice')
                symbol_infos[s]['askPrice'] = t.get('askPrice')
                symbol_infos[s]['quoteVolume'] = t.get('quoteVolume')
    
    del long_symbol_list
    del REQUEST_LIST_SIZE_MAX
    del pointer_index
    del end_index
    del request_symbol_list
    del ticker_24hr
    
    # Make the dictionary for the symbols' recommendations
    # { symbol: { interval: { type: rec } } }
    symbol_recommendations = dict()
    for s in (symbol_list + ['BTCBUSD']):
        symbol_recommendations[s] = dict()
        for i in ta_intervals:
            symbol_recommendations[s][i] = {
                'sum': 'ERROR',
                'osc': 'ERROR',
                'mav': 'ERROR',
                
                'macd': 'ERROR'
            }


    def loop_iter():
        # Get TradingView analyses for all intervals
        interval_analyses = dict()
        try:
            for i in ta_intervals:
                interval_analyses[i] = get_multiple_analysis(
                    screener='crypto',
                    interval=i,
                    symbols=prefixed_symbol_list + ['BINANCE:BTCBUSD'] # get BTCBUSD for use as an index
                )
        except Exception as e:
            # Non-fatal, but skip this iteration and try again next time
            print(e)
            return {'error': -1}

        # Record previous recommendations before writing new ones
        symbol_last_recommendations = copy.deepcopy(symbol_recommendations)
        # Restructure analyses by symbol
        for interval, analyses in interval_analyses.items():
            for key, analysis in analyses.items():
                if analysis:
                    sum_rec = analysis.summary.get('RECOMMENDATION', 'ERROR')
                    osc_rec = analysis.oscillators.get('RECOMMENDATION', 'ERROR')
                    mav_rec = analysis.moving_averages.get('RECOMMENDATION', 'ERROR')
                    # Remember the last significant recommendations for BTCBUSD
                    if analysis.symbol == 'BTCBUSD':
                        if sum_rec in {'SELL', 'NEUTRAL', 'BUY'}:
                            sum_rec = symbol_recommendations[analysis.symbol][interval]['sum']
                        if osc_rec in {}:
                            osc_rec = symbol_recommendations[analysis.symbol][interval]['sum']
                        if mav_rec in {'SELL', 'NEUTRAL', 'BUY'}:
                            mav_rec = symbol_recommendations[analysis.symbol][interval]['sum']
                    
                    symbol_recommendations[analysis.symbol][interval] = {
                        'sum': sum_rec,
                        'osc': osc_rec,
                        'mav': mav_rec,
                        
                        'macd': analysis.oscillators.get('COMPUTE', {}).get('MACD')
                    }
                    
                    if not analysis.symbol in did_hit_strong_sell:
                        did_hit_strong_sell[analysis.symbol] = dict()
                    
                    if not interval in did_hit_strong_sell[analysis.symbol]:
                        did_hit_strong_sell[analysis.symbol][interval] = dict()
                    
                    if sum_rec in {'NEUTRAL', 'BUY', 'STRONG_BUY'}:
                        did_hit_strong_sell[analysis.symbol][interval]['sum'] = False
                    elif sum_rec == 'STRONG_SELL':
                        did_hit_strong_sell[analysis.symbol][interval]['sum'] = True
                    
                    if osc_rec in {'NEUTRAL', 'BUY', 'STRONG_BUY'}:
                        did_hit_strong_sell[analysis.symbol][interval]['osc'] = False
                    elif osc_rec == 'STRONG_SELL':
                        did_hit_strong_sell[analysis.symbol][interval]['osc'] = True
                        
                    if mav_rec in {'NEUTRAL', 'BUY', 'STRONG_BUY'}:
                        did_hit_strong_sell[analysis.symbol][interval]['mav'] = False
                    elif mav_rec == 'STRONG_SELL':
                        did_hit_strong_sell[analysis.symbol][interval]['mav'] = True
                else:
                    # No analysis retrieved, keep empty
                    symbol_recommendations[key[8:]][interval] = {
                        'sum': 'ERROR',
                        'osc': 'ERROR',
                        'mav': 'ERROR',
                        
                        'macd': 'ERROR'
                    }
        
        # Figure out which symbols to buy and which ones to sell
        buy = list()
        sell = list()
        buy_short_term = list()

        for symbol, recommendation in symbol_recommendations.items():
            if symbol == 'BTCBUSD': # skip the index symbol
                continue
            trade = run_trade_strategy(recommendation, symbol_last_recommendations[symbol], did_hit_strong_sell.get(symbol, {}))
            if trade > 0:
                buy.append(symbol)
            elif trade < 0:
                sell.append(symbol)
        
        # for symbol, recommendation in symbol_last_recommendations.items():
            # if symbol == 'BTCBUSD':
                # continue
            # trade = run_short_term_trade_strategy(recommendation)
            # if trade > 0:
                # buy_short_term.append(symbol)
        
        # Check if bitcoin is buy or sell first, and only buy or sell based on that
        # btc_trade = run_index_strategy(symbol_recommendations['BTCBUSD'], symbol_last_recommendations['BTCBUSD'])
        # if btc_trade >= 0:
            # print('BTCBUSD says do not SELL')
            # sell = list()
        # if btc_trade <= 0:
            # print('BTCBUSD says do not BUY')
            # buy = list()
        
        print('BUY: {}'.format(buy))
        print('SELL: {}'.format(sell))

        # Cancel orders
        try:
            cancel_orders()
        except ClientError as e:
            except_api(e)
        except PermissionError as e:
            # Fatal, quit loop and try again
            wsquit()
            return {'error': 2, 'sleep_time': e.args[0]}
        except Exception as e:
            # Fatal, quit loop and try again
            print(e)
            wsquit()
            return {'error': 1}

        # Sell and buy
        for user, client in binance_clients.items():
            # Get account balance
            try:
                balances = client.account().get('balances')
            except Exception as e:
                # Skip this user
                print(e)
                continue
                
            organised_balances = dict()
            for b in balances:
                organised_balances[b.get('asset')] = float(b.get('free', 0))

            ######## SHORT-TERM SELL ########
            # Check if short-term prices have increased above the sell threshold
            # for symbol, price in short_term_buy_prices[user].items():
                # symbol_info = symbol_infos.get(symbol, None)
                # sell_threshold = price * SHORT_TERM_PRICE_INCREASE_SELL_THRESHOLD_FACTOR
                # if symbol_info and float(symbol_info.get('bidPrice')) > sell_threshold:
    
                    # base_asset = symbol_info.get('baseAsset')
                    # # Check how much of this coin the user has
                    # base_asset_balance = organised_balances.get(base_asset, 0)
                    
                    # # Skip excluded coins
                    # if base_asset in EXCLUDE_BASE_ASSETS:
                        # continue

                    # # Get filters
                    # fv_step_size = symbol_info.get('stepSize')
                    # fv_min_notional = symbol_info.get('minNotional')
    
                    # price = symbol_info.get('askPrice')
    
                    # # Sell as much of the base asset as possible
                    # base_asset_order_size = math.floor(base_asset_balance / fv_step_size) * fv_step_size
                    # if base_asset_order_size * float(price) >= fv_min_notional:
                        # send_record_short_term_order(user, symbol, 'SELL', base_asset_order_size, price)

            ######## LONG-TERM SELL ########
            # Check if prices have increased above the sell threshold
            # for symbol, price in buy_prices[user].items():
                # symbol_info = symbol_infos.get(symbol, None)
                # sell_threshold = price * PRICE_INCREASE_SELL_THRESHOLD_FACTOR
                # if symbol_info and float(symbol_info.get('bidPrice')) > sell_threshold:
    
                    # base_asset = symbol_info.get('baseAsset')
                    # # Skip excluded coins
                    # if base_asset in EXCLUDE_BASE_ASSETS:
                        # continue
                        
                    # # Check how much of this coin the user has
                    # base_asset_balance = organised_balances.get(base_asset, 0) - short_term_balances[user].get(symbol, 0)
                    # # Skip coins with no long-term balance
                    # if base_asset_balance <= 0:
                        # continue

                    # # Get filters
                    # fv_step_size = symbol_info.get('stepSize')
                    # fv_min_notional = symbol_info.get('minNotional')
    
                    # price = symbol_info.get('askPrice')
    
                    # # Sell as much of the base asset as possible
                    # base_asset_order_size = math.floor(base_asset_balance / fv_step_size) * fv_step_size
                    # if base_asset_order_size * float(price) >= fv_min_notional:
                        # send_order(user, symbol, 'SELL', base_asset_order_size, price)

            # Sell stuff based on indicators
            for symbol in sell:
                symbol_info = symbol_infos.get(symbol, None)
    
                base_asset = symbol_info.get('baseAsset')
                # Skip excluded coins
                if base_asset in EXCLUDE_BASE_ASSETS:
                    continue
                
                # Check how much of this coin the user has
                base_asset_balance = organised_balances.get(base_asset, 0) - short_term_balances[user].get(symbol, 0)
                if base_asset_balance <= 0:
                    continue

                # Get filters
                fv_step_size = symbol_info.get('stepSize')
                fv_min_notional = symbol_info.get('minNotional')
    
                price = symbol_info.get('askPrice')
    
                # Sell as much of the base asset as possible
                base_asset_order_size = math.floor(base_asset_balance / fv_step_size) * fv_step_size
                if base_asset_order_size * float(price) >= fv_min_notional:
                    send_order(user, symbol, 'SELL', base_asset_order_size, price)

            # Figure out how much to spend on each coin
            # This is mostly just to easily track the balance between each quote asset;
            # Not including the for loop may honestly be faster
            for qa in PROGRAM_QUOTE_ASSETS:
            ########################################################################
                balance_quote = organised_balances.get(qa, 0)
                print('api#{} quote asset {}: {}'.format(user, qa, balance_quote))
                # This is how much the bot wants to spend on each coin
                # Note that if this is below the minNotionalBuy value, the bot will use that value instead
                ideal_quote_per_transaction = balance_quote * QUOTE_PER_TRANSACTION_FAC
                
                ######## LONG-TERM BUY ########
                # Randomise the buy list between users
                random.shuffle(buy)
                # Buy stuff
                for symbol in buy:
                    symbol_info = symbol_infos.get(symbol, None)

                    # Make sure the quote asset is correct!
                    if symbol_info.get('quoteAsset') != qa:
                        continue
                        
                    base_asset = symbol_info.get('baseAsset')
                    # Skip excluded coins
                    if base_asset in EXCLUDE_BASE_ASSETS:
                        continue
                        
                    # Stop looping if the quote asset balance is too low
                    if balance_quote < ideal_quote_per_transaction:
                        #print('WTB {}, cannot because balance {} < ideal {}'.format(symbol, balance_quote, ideal_quote_per_transaction))
                        break
        
                    # Skip if volume is too low
                    if float(symbol_info.get('quoteVolume')) < QUOTE_VOLUME_MIN:
                        #print('WTB {}, cannot because quote volume {} < minimum {}'.format(symbol, symbol_info.get('quoteVolume'), QUOTE_VOLUME_MIN))
                        continue
                    
                    # Check how much of this coin the user has
                    base_asset_balance = organised_balances.get(base_asset, 0) - short_term_balances[user].get(symbol, 0)
        
                    # Get filters
                    fv_step_size = symbol_info.get('stepSize')
                    fv_min_notional = symbol_info.get('minNotional')
                
                    # This is how much the bot will actually try to spend on this coin
                    # In case the ideal is lower than the configured minimum buying size, the latter will be used instead
                    quote_per_transaction = max(symbol_info.get('minNotionalBuy'), ideal_quote_per_transaction)
        
                    price = symbol_info.get('bidPrice')
                    f_price = float(price)
        
                    base_asset_order_size = math.floor(
                        quote_per_transaction / f_price / fv_step_size) * fv_step_size
                    # Check if the user already has enough of this coin
                    quote_order_size = base_asset_order_size * f_price
                    if (
                        balance_quote >= quote_order_size >= fv_min_notional and
                        base_asset_balance * f_price < symbol_info.get('holdThreshold')
                    ):
                        balance_quote -= quote_order_size
                        send_order(user, symbol, 'BUY', base_asset_order_size, price)
                    else:
                        #print('WTB {}, cannot because order size too small or holding base asset instead'.format(symbol))
                        pass
                
                ######## SHORT-TERM BUY ########
                # Randomise the short-term list too
                # random.shuffle(buy_short_term)
                # for symbol in buy_short_term:
                    # # Stop looping if the quote asset balance is too low
                    # if balance_quote < ideal_quote_per_transaction:
                        # break
                    
                    # symbol_info = symbol_infos.get(symbol, None)

                    # # Make sure the quote asset is correct!
                    # if symbol_info.get('quoteAsset') != qa:
                        # continue
        
                    # # Skip if volume is too low
                    # if float(symbol_info.get('quoteVolume')) < QUOTE_VOLUME_MIN:
                        # continue
        
                    # base_asset = symbol_info.get('baseAsset')
                    # # Skip excluded coins
                    # if base_asset in EXCLUDE_BASE_ASSETS:
                        # continue
                    
                    # # Check how much of this coin the user has
                    # base_asset_balance = organised_balances.get(base_asset, 0)
        
                    # # Get filters
                    # fv_step_size = symbol_info.get('stepSize')
                    # fv_min_notional = symbol_info.get('minNotional')
                
                    # # This is how much the bot will actually try to spend on this coin
                    # # In case the ideal is lower than the configured minimum buying size, the latter will be used instead
                    # quote_per_transaction = max(symbol_info.get('minNotionalBuy'), ideal_quote_per_transaction)
        
                    # price = symbol_info.get('bidPrice')
                    # f_price = float(price)
        
                    # base_asset_order_size = math.floor(
                        # quote_per_transaction / f_price / fv_step_size) * fv_step_size
                    # # Check if the user already has enough of this coin
                    # quote_order_size = base_asset_order_size * f_price
                    # if (
                        # balance_quote >= quote_order_size >= fv_min_notional and
                        # base_asset_balance * f_price < symbol_info.get('holdThreshold')
                    # ):
                        # balance_quote -= quote_order_size
                        # send_record_short_term_order(user, symbol, 'BUY', base_asset_order_size, price)

        # Write stuff down
        record_session_data()

        # Iteration finished
        return {'error': 0}

    
    def loop():
        iters = 0
        while True:
            iters += 1
            print('>> loop iter #{:0>8}:'.format(iters))
            # Break (and return) upon fatal exception
            status = loop_iter()
            if status.get('error') > 0:
                time.sleep(status.get('sleep_time', 0))
                break
            print('>> loop iter end')
            time.sleep(LOOP_DELAY_SECONDS)

    
    return loop


while True:
    fn_loop = create_loop_environment()
    fn_loop()