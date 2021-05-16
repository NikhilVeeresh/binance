# Part 1. Coding
# Write a script (python/similar) to export data from https://www.binance.com/ for the trading pairs
# BTC-USD.
# 1. With 1 minute candles grain and store it into the assumed Data Lake. Refer Binance API
# clients for this task.
# Required columns in the output are:
# ● Trading Pair,Open Price,Close Price,High Price,Low Price,BTC Volume,USD
# Volume,Number of Trades,Candle Open Time,Candle Close Time
# 2. Get Account orders for the same trading pair for a sample size (500 to 1000 limit OR for
# a 15 minute period, as the volume will be high).
# Required columns in the output are:
# ○ Symbol, OrderID, Price, OrigQty, ExecutedQty, status, type, side

import pandas as pd
import time
import dateparser
import pytz
import json

from datetime import datetime
from binance.client import Client


def date_to_milliseconds(date_str):
    """Convert UTC date to milliseconds
    If using offset strings add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    See dateparse docs for formats http://dateparser.readthedocs.io/en/latest/
    :param date_str: date in readable format, i.e. "January 01, 2018", "11 hours ago UTC", "now UTC"
    :type date_str: str
    """
    # get epoch value in UTC
    epoch = datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)
    # parse our date string
    d = dateparser.parse(date_str)
    # if the date is not timezone aware apply UTC timezone
    if d.tzinfo is None or d.tzinfo.utcoffset(d) is None:
        d = d.replace(tzinfo=pytz.utc)

    # return the difference in time
    return int((d - epoch).total_seconds() * 1000.0)


def interval_to_milliseconds(interval):
    """Convert a Binance interval string to milliseconds
    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str
    :return:
         None if unit not one of m, h, d or w
         None if string not in correct format
         int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms = int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms


def get_historical_klines(symbol, interval, start_str, end_str=None):
    """Get Historical Klines from Binance
    See dateparse docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/
    If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"
    :param symbol: Name of symbol pair e.g BNBBTC
    :type symbol: str
    :param interval: Biannce Kline interval
    :type interval: str
    :param start_str: Start date string in UTC format
    :type start_str: str
    :param end_str: optional - end date string in UTC format
    :type end_str: str
    :return: list of OHLCV values
    """
    # create the Binance client, no need for api key
    client = Client("", "")

    # init our list
    output_data = []

    # setup the max limit
    limit = 500

    # convert interval to useful value in seconds
    timeframe = interval_to_milliseconds(interval)

    # convert our date strings to milliseconds
    start_ts = date_to_milliseconds(start_str)

    # if an end time was passed convert it
    end_ts = None
    if end_str:
        end_ts = date_to_milliseconds(end_str)

    idx = 0
    # it can be difficult to know when a symbol was listed on Binance so allow start time to be before list date
    symbol_existed = False
    while True:
        # fetch the klines from start_ts up to max 500 entries or the end_ts if set
        temp_data = client.get_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            startTime=start_ts,
            endTime=end_ts
        )

        # handle the case where our start date is before the symbol pair listed on Binance
        if not symbol_existed and len(temp_data):
            symbol_existed = True

        if symbol_existed:
            # append this loops data to our output data
            output_data += temp_data

            # update our start timestamp using the last value in the array and add the interval timeframe
            start_ts = temp_data[len(temp_data) - 1][0] + timeframe
        else:
            # it wasn't listed yet, increment our start date
            start_ts += timeframe

        idx += 1
        # check if we received less than the required limit and exit the loop
        if len(temp_data) < limit:
            # exit the while loop
            break

        # sleep after every 3rd call to be kind to the API
        if idx % 3 == 0:
            time.sleep(1)

    return output_data


symbol = "BTCSTBUSD"
# BTCSTBTC
# BTCSTBUSD
# BTCSTUSDT
start = "1 May, 2021"
end = "1 Jan, 2018"
interval = Client.KLINE_INTERVAL_1MINUTE

klines = get_historical_klines(symbol, interval, start, end)

tradingpairs_columns = ["Candle Open Time", "Open Price", "High Price", "Low Price", "Close Price", "BTC Volume",
                        "Candle Close Time", "USD Volume", "Number of Trades", "Taker buy base asset volume",
                        "Taker buy quote asset volume", "Ignore"]

# trading_pair
df = pd.DataFrame(klines, columns=tradingpairs_columns)

print(klines)
# open a file with filename including symbol, interval and start and end converted to milliseconds
# with open(
#     "Binance_{}_{}_{}-{}.json".format(
#         symbol,
#         interval,
#         date_to_milliseconds(start),
#         date_to_milliseconds(end)
#     ),
#     'w'  # set file write mode
# ) as f:
#     f.write(json.dumps(klines))