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
        'key': 'fdQv61rrqolmWGc7ordfLi2AZuFlxhjjEtd0odCYscba8BPkznKp38e1wfNueE5R',
        'secret': 'xels1dAlGLD00ZhwgBqA0FmzNMgHHSCSOdOY5rH3AsgTSkyQg9J8WI3MLIrxkBxy',
    },
    'Ozhan': {
        'key': 's7AVdedXVVhGYOuswx6pOHy6OyPPKDimBLfhKR4JMIcWK9feH5PN7zrItpkVXIVL',
        'secret': '34Yq97P81r0GUCQudQHMFfAvb7orlmtpCiNBKNvVzgcwEeUzCRmq00lRWPalIB9I',
    },
    'Reis': {
        'key': 'EHdrguoIKWClqSOT5UqbYIfDpUIvrDGb8CmqreyNK4mBgFnbDoI71gk7frX8q3uL',
        'secret': '3VqVf0mEp8a8SA3my3zOoicfyDITLtAEohJplr80qTci8jDuhxDtLuBpNRqpl1ZJ',
    },
}

# Trade using these coins, but ignore pairs that are below the specified volumes
PROGRAM_QUOTE_ASSETS_AND_MIN_VOLUMES = {
    'BUSD': 0.000000000000000000000000000000000000000000000000001,
    'USDT': 0.0000000000000000000000000000000000000000000000000001,
    'BNB': 0.00000000000000000000000000000000000000000000000000001,
    'BTC': 0.00000000000000000000000000000000000000000000000000001,
    'ETH': 0.00000000000000000000000000000000000000000000000000001,
}
# The amount of time between loops
LOOP_DELAY_SECONDS = 85
# Proportion of bot's min notional; if the balance is higher than this then the bot will not buy in an asset pair
NOTIONAL_HOLD_THRESHOLD_FAC = 0.95
# Proportion of Binance's min notional to use for calculating floored buy quantity
MIN_NOTIONAL_BUY_FAC = 1.2
# Proportion of available quote asset balance to use on each BUY order
QUOTE_PER_TRANSACTION_FAC = 1/1
# Sell coins when the price increases by this percent
PRICE_INCREASE_SELL_THRESHOLD_PERCENT = 10000000000000000000000000000000000000000000000000000000000
# Sell coins when the price decreases by this percent
STOP_LOSS_PRICE_THRESHOLD_PERCENT =  100000000000000000000000000000000000000000000000000000000000000000000000000000000
# Do not buy or sell these coins
EXCLUDE_BASE_ASSETS = {'TUSD', 'USDC', 'USDT', 'BUSD', 'EUR', 'BNB', 'ETH', 'EUR', 'BTC', 'WBTC', 'TRY', 'USDP', 'PAXG', 'FDUSD', 'FDUSD'}


def run_trade_strategy(rec, last_rec):
    ####### TRADING STRATEGY #######

    if (
    ### SELL STRATEGY

        #rec[Interval.INTERVAL_1_MINUTE]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_MINUTE]['osc'] in {'BUY', 'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_MINUTE]['mav'] in {'BUY', 'STRONG_BUY'} and

        #rec[Interval.INTERVAL_5_MINUTES]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_5_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        #rec[Interval.INTERVAL_5_MINUTES]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_1_HOUR]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_HOUR]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_HOUR]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_2_HOURS]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_2_HOURS]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_2_HOURS]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_4_HOURS]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_4_HOURS]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_4_HOURS]['mav'] in {'STRONG_BUY'} and


        #rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_DAY]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_WEEK]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_BUY'} and

        #rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_MONTH]['osc'] in {'BUY', 'STRONG_BUY'} and
        rec[Interval.INTERVAL_1_MONTH]['mav'] in {'STRONG_BUY'} and

    True):
        return -1
    elif (
    ### BUY STRATEGY3

        #rec[Interval.INTERVAL_1_MINUTE]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_MINUTE]['osc'] in {'BUY', 'STRONG_BUY'} and
        #rec[Interval.INTERVAL_1_MINUTE]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_5_MINUTES]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_5_MINUTES]['osc'] in {'BUY', 'STRONG_BUY'} and
        #rec[Interval.INTERVAL_5_MINUTES]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_15_MINUTES]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_15_MINUTES]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_15_MINUTES]['mav'] in {'STRONG SELL'} and

        #rec[Interval.INTERVAL_30_MINUTES]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_30_MINUTES]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_30_MINUTES]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_1_HOUR]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_HOUR]['osc'] in {'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_HOUR]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_2_HOURS]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_2_HOURS]['osc'] in {'SELL', 'STRONG_SELL'} and
        rec[Interval.INTERVAL_2_HOURS]['mav'] in {'STRONG_SELL'} and


        #rec[Interval.INTERVAL_4_HOURS]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_4_HOURS]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_4_HOURS]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_1_DAY]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_DAY]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_DAY]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_1_WEEK]['sum'] in {'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_WEEK]['osc'] in {'SELL', 'STRONG_SELL'} and
        #rec[Interval.INTERVAL_1_WEEK]['mav'] in {'STRONG_SELL'} and

        #rec[Interval.INTERVAL_1_MONTH]['sum'] in {'STRONG_SELL'} and
        rec[Interval.INTERVAL_1_MONTH]['osc'] in {'BUY', 'STRONG_BUY'} and
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
STOP_LOSS_PRICE_THRESHOLD_FACTOR = (100 - STOP_LOSS_PRICE_THRESHOLD_PERCENT) / 100

# Set up tradingview-ta
ta_intervals = list()
i = Interval()
ta_intervals = [i.__getattribute__(e) for e in dir(i) if (e.startswith('INTERVAL_'))]
del i
print('Intervals: {}'.format(ta_intervals))


# Handle Binance API exceptions
def except_api(e):
    print(e)
    # INVALID_TIMESTAMP
    if e.error_code == -1021:
        pass
    # NEW_ORDER_REJECTED
    elif e.error_code == -2010:
        pass
    # CANCEL_REJECTED
    elif e.error_code == -2011:
        pass
    elif e.error_code == -1003:
        retry_after = e.header.get("retry-after", 0)
        raise PermissionError(int(retry_after) * 2 + 60)
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

        with open('pair_balances.json', 'w') as f:
            f.write(json.dumps(pair_balances))


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
                    if msg.get('S') == 'BUY':
                        # For buy orders
                        price = float(msg.get('p'))
                        # Normal (presumably long-term) orders
                        buy_prices[user][s] = float(price)
                        # Track balances per-quote-asset (or rather just using complete symbols
                        pair_balances[user][s] = pair_balances[user].get(s, 0) + float(msg.get('q'))
                        print('record price for api#{} {} @ {}'.format(user, s, price))
                    elif msg.get('S') == 'SELL':
                        # Track how much is left after selling
                        # Track balances per-quote-asset (or rather just using complete symbols), and clamp to 0+
                        pair_balances[user][s] = max(pair_balances[user].get(s, 0) - float(msg.get('q')), 0)

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
            try:
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
        with open('buy_prices.json', 'r') as f:
            res = f.read()
        buy_prices = json.loads(res)
        print('Loaded `buy_prices`')
    except:
        print('Could not load `buy_prices`, starting fresh')

    # { api: { symbol: balance } }
    pair_balances = dict()
    try:
        with open('pair_balances.json', 'r') as f:
            res = f.read()
        pair_balances = json.loads(res)
        print('Loaded `pair_balances`')
    except:
        print('Could not load `pair_balances`, starting fresh')

    # Set up Binance clients
    print('Create Binance clients...')
    binance_clients = dict()
    binance_ws_clients = dict()
    binance_default_client = None
    for user, v in API.items():
        # Create a client
        binance_clients[user] = Spot(api_key=v.get('key'), api_secret=v.get('secret'))
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
        if not user in pair_balances:
            pair_balances[user] = dict()


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
            qa in PROGRAM_QUOTE_ASSETS_AND_MIN_VOLUMES and
            ba not in EXCLUDE_BASE_ASSETS and
            'BULL' not in ba and
            'BEAR' not in ba and
            not ba.endswith("DOWN") and not ba.endswith("UP")
        ):
            # Store the symbols and some useful info
            s = e.get('symbol')
            symbol_list.append(s)
            f = e.get('filters')

            min_notional = float(
                next(
                    (e for e in f if e.get('filterType') in {"NOTIONAL", "MIN_NOTIONAL"})
                ).get("minNotional")
            )

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

    binance_ticker_ws.live_subscribe(stream="bookTicker", id=1, callback=book_ticker)
    binance_ticker_ws.live_subscribe(stream="miniTicker", id=2, callback=mini_ticker)

    # Record prices and volumes for all relevant symbols
    print('Get tickers from Binance...')

    # Split up the list into manageable request sizes (thanks Binance)
    long_symbol_list = symbol_list
    REQUEST_LIST_SIZE_MAX = 400
    pointer_index = 0
    while pointer_index < len(long_symbol_list):
        end_index = min(pointer_index + REQUEST_LIST_SIZE_MAX, len(long_symbol_list))
        request_symbol_list = long_symbol_list[pointer_index:end_index]
        pointer_index += REQUEST_LIST_SIZE_MAX

        ticker_24hr = binance_default_client.ticker_24hr(symbols=request_symbol_list)
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
    for s in (symbol_list):
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
                    symbols=prefixed_symbol_list
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
                    symbol_recommendations[analysis.symbol][interval] = {
                        'sum': analysis.summary.get('RECOMMENDATION', 'ERROR'),
                        'osc': analysis.oscillators.get('RECOMMENDATION', 'ERROR'),
                        'mav': analysis.moving_averages.get('RECOMMENDATION', 'ERROR'),

                        'macd': analysis.oscillators.get('COMPUTE', {}).get('MACD')
                    }
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

        for symbol, recommendation in symbol_recommendations.items():
            trade = run_trade_strategy(recommendation, symbol_last_recommendations[symbol])
            if trade > 0:
                buy.append(symbol)
            elif trade < 0:
                sell.append(symbol)
        # Print out which symbols to buy or sell
        buy_with_prices = {
            symbol: symbol_infos.get(symbol, {}).get("askPrice", "MISSING")
            for symbol in buy
        }
        sell_with_prices = {
            symbol: symbol_infos.get(symbol, {}).get("bidPrice", "MISSING")
            for symbol in sell
        }
        print(f"BUY:\n{buy_with_prices}\nSELL:\n{sell_with_prices}")

        # Cancel orders
        try:
            cancel_orders()
        except ClientError as e:
            try:
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

            # Check if prices have increased above the sell threshold/below the stop loss threshold
            # These are already saved by trading pairs
            for symbol, buy_price in buy_prices[user].items():
                symbol_info = symbol_infos.get(symbol, None)
                if not symbol_info:
                    continue
                profit_threshold = buy_price * PRICE_INCREASE_SELL_THRESHOLD_FACTOR
                sell_profit_critical_price = symbol_info.get('bidPrice')
                sell_profit = float(sell_profit_critical_price) > profit_threshold

                stop_loss_threshold = buy_price * STOP_LOSS_PRICE_THRESHOLD_FACTOR
                sell_stop_loss_critical_price = symbol_info.get('askPrice')
                sell_stop_loss = float(sell_stop_loss_critical_price) < stop_loss_threshold
                if sell_profit or sell_stop_loss:

                    base_asset = symbol_info.get('baseAsset')
                    # Skip excluded coins
                    if base_asset in EXCLUDE_BASE_ASSETS:
                        continue

                    # Check how much of this coin the user has in total in the Binance wallet
                    base_asset_balance = organised_balances.get(base_asset, 0)
                    # Also check the balance for this specific coin pair, considering which quote asset was used to buy this coin
                    pair_balance = pair_balances[user].get(symbol, 0)
                    # If the pair info is invalid then try to make it valid idk
                    if base_asset_balance < pair_balance:
                        pair_balances[user][symbol] = base_asset_balance
                        pair_balance = base_asset_balance
                    else:
                        base_asset_balance = pair_balance
                    # Skip if there is no base asset to sell
                    if base_asset_balance <= 0:
                        continue

                    # Get filters
                    fv_step_size = symbol_info.get('stepSize')
                    fv_min_notional = symbol_info.get('minNotional')
                    # Try to ensure a LIMIT MAKER order
                    price = symbol_info.get('askPrice')

                    # Sell as much of the base asset as possible
                    base_asset_order_size = math.floor(base_asset_balance / fv_step_size) * fv_step_size
                    if base_asset_order_size * float(price) >= fv_min_notional:
                        if sell_profit:
                            print(f"Placing order because of TAKE PROFIT at {profit_threshold:10.10f} (from {buy_price:10.10f})")
                        elif sell_stop_loss:
                            print(f"Placing order because of STOP LOSS at {stop_loss_threshold:10.10f} (from {buy_price:10.10f})")
                        send_order(user, symbol, 'SELL', base_asset_order_size, price)
                    # Issue a warning if the price crossed below the stop loss but the asset could not be sold
                    elif sell_stop_loss and base_asset_order_size > 0:
                        print(f"WARNING: api#{user} could not sell STOP LOSS {base_asset_order_size} x {symbol} @ {price}: order size below min notional {fv_min_notional}")

            # Sell stuff based on indicators
            for symbol in sell:
                symbol_info = symbol_infos.get(symbol, None)

                base_asset = symbol_info.get('baseAsset')
                # Skip excluded coins
                if base_asset in EXCLUDE_BASE_ASSETS:
                    continue

                # Check how much of this coin the user has
                base_asset_balance = organised_balances.get(base_asset, 0)
                # Also check the balance for this specific coin pair, considering which quote asset was used to buy this coin
                pair_balance = pair_balances[user].get(symbol, 0)
                # If the pair info is invalid then try to make it valid idk
                if base_asset_balance < pair_balance:
                    pair_balances[user][symbol] = base_asset_balance
                    pair_balance = base_asset_balance
                else:
                    base_asset_balance = pair_balance

                # Skip if there is no base asset to sell
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
            for qa, volume_min in PROGRAM_QUOTE_ASSETS_AND_MIN_VOLUMES.items():
            ########################################################################
                balance_quote = organised_balances.get(qa, 0)
                #print('api#{} quote asset {}: {}'.format(user, qa, balance_quote))
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
                        break

                    # Skip if volume is too low
                    if float(symbol_info.get('quoteVolume')) < volume_min:
                        continue

                    # Check how much of this coin the user has in this pair
                    pair_balance = pair_balances[user].get(symbol, 0)

                    # Get filters
                    fv_step_size = symbol_info.get('stepSize')
                    fv_min_notional = symbol_info.get('minNotional')

                    # This is how much the bot will actually try to spend on this coin
                    # In case the ideal is lower than the configured minimum buying size, the latter will be used instead
                    quote_per_transaction = max(symbol_info.get('minNotionalBuy'), ideal_quote_per_transaction)

                    price = symbol_info.get('bidPrice')
                    f_price = float(price)

                    if not f_price:
                        print(f"Skipping BUY for {symbol} because price is 0")
                        continue
                    if fv_step_size:
                        base_asset_order_size = math.floor(
                            quote_per_transaction / f_price / fv_step_size) * fv_step_size
                    else:
                        base_asset_order_size = round(quote_per_transaction / f_price, 4)  # Idk what number to put here
                    # Check if the user already has enough of this coin
                    quote_order_size = base_asset_order_size * f_price
                    if (
                        balance_quote >= quote_order_size >= fv_min_notional and
                        pair_balance * f_price < symbol_info.get('holdThreshold')
                    ):
                        balance_quote -= quote_order_size
                        send_order(user, symbol, 'BUY', base_asset_order_size, price)
                    else:
                        #print('WTB {}, cannot because order size too small or holding base asset instead'.format(symbol))
                        pass

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