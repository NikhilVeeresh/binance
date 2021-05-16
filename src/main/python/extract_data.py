from datetime import datetime

import dateparser
import pandas as pd
import pytz
from binance import Client

# Part 1. Coding
# Write a script (python/similar) to export data from https://www.binance.com/ for the trading pairs
# BTC-USD.
# 1. With 1 minute candles grain and store it into the assumed Data Lake. Refer Binance API
# clients for this task.
# Required columns in the output are:
# Trading Pair,Open Price,Close Price,High Price,Low Price,BTC Volume,USD Volume,Number of Trades,candle_open_time,
# candle_close_time
# 2. Get Account orders for the same trading pair for a sample size (500 to 1000 limit OR for
# a 15 minute period, as the volume will be high).
# Required columns in the output are:
# ○ Symbol, OrderID, Price, OrigQty, ExecutedQty, status, type, side

api_key = "Pj5Xb4kucjR8SChJWqycU2csR8Hgzt7WOPEKYhCHrMkFcoSIrq3MQzmmN7CrvGcy"
api_secret = "CbVKRyHhd7ATKWQrrxfIozYc9q0dDyXQRfZtyKUNSESRX51dgbzWvffxamZna20"
client = Client(api_key, api_secret, tld='us')
# client = Client("", "")
# client = Client()

symbol = "BTCUSDT"
# BTCSTBTC
# BTCSTBUSD
# BTCSTUSDT
start = "1 May, 2021"
end = "2 May, 2021"
interval = Client.KLINE_INTERVAL_1MINUTE

klines = client.get_historical_klines(symbol, interval, start, end)
tradingpairs_columns = ["candle_open_time", "open_price", "high_price", "low_price", "close_price", "btc_volume",
                        "candle_close_time", "usd_volume", "number_of_trades", "taker_buy_base_asset_volume",
                        "taker_buy_quote_asset_volume", "ignore"]


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


# trading_pair

df = pd.DataFrame(klines, columns=tradingpairs_columns)
print(df['candle_open_time'].unique())
df['candle_open_time'] = df['candle_open_time'].astype(str)
df['candle_close_time'] = df['candle_close_time'].astype(str)

df['candle_open_time'] = pd.to_datetime(df['candle_open_time'], unit='ms')
df['candle_close_time'] = pd.to_datetime(df['candle_close_time'], unit='ms')

print(df['candle_open_time'].min())
print(df['candle_open_time'].max())
datalake_arn = 'arn:aws:s3:::data-lake-us-east-1-650649476196'
df.to_csv("TradePairs_BTCSTBUSD.csv", index=False)

# account_orders = client.get_all_orders(symbol="BTCSTBUSD", limit=10)
# from binance.enums import *
# order = client.create_order(
#     symbol='BNBBTC',
#     side=SIDE_BUY,
#     type=ORDER_TYPE_LIMIT,
#     timeInForce=TIME_IN_FORCE_GTC,
#     quantity=100,
#     price='0.00001')

# RECV_WINDOW = 59999
# orders = client.get_all_orders(symbol=symbol, limit=10)
# account = client.get_account(recvWindow=RECV_WINDOW)
# import boto3
# from io import StringIO
# def _write_dataframe_to_csv_on_s3(dataframe, filename):
#     """ Write a dataframe to a CSV on S3 """
#     print("Writing {} records to {}".format(len(dataframe), filename))
#      # Create buffer
#     csv_buffer = StringIO()
#     # Write dataframe to buffer
#     df.to_csv(csv_buffer, sep="|", index=False)
#     # Create S3 object
#     s3_resource = boto3.resource("s3")
#     # Write buffer to S3 object
#     # s3_resource.Object(DESTINATION, filename).put(Body=csv_buffer.getvalue())
#
# import requests
# import urllib
# import time
# import hmac
# import hashlib
#
# # api_key = ''
# # api_secret = ''
# request_url = 'https://api.binance.com/api/v3/openOrders?'
#
# timestamp = int(time.time() * 1000)
# symbol = 'BNBBTC'
#
# querystring = urllib.parse.urlencode({'symbol': symbol, 'timestamp': timestamp})
#
# signature = hmac.new(api_secret.encode('utf-8'), querystring.encode('utf-8'), hashlib.sha256).hexdigest()
#
# request_url += querystring + '&signature=' + signature
#
# r = requests.get(request_url, headers={"X-MBX-APIKEY": api_key})
# print(r)

import os

import pandas as pd

AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

symbol = "BTCSTBUSD"
# BTCSTBTC
# BTCSTBUSD
# BTCSTUSDT
start = "1 May, 2021"
end = "1 Jan, 2018"
interval = Client.KLINE_INTERVAL_1MINUTE

key = "files/books.csv"
AWS_ACCESS_KEY_ID = 'AKIAZO7N3DRSDMPKCB3Q'
AWS_SECRET_ACCESS_KEY = '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'
AWS_S3_BUCKET = 'data-lake-us-east-1-650649476196'
key = 'tradingpairs/{}{}{}{}{}'.format(symbol,
                                       interval,
                                       date_to_milliseconds(start),
                                       date_to_milliseconds(end),
                                       ".csv")
# candle_open_time
# timestamp, Open_Price
# float, High_Price
# float, Low_Price
# float, Close_Price
# float, BTC_Volume
# float, candle_close_time
# timestamp, USD_Volume
# float, Number_of_Trades
# int, Taker_buy_base_asset_volume
# float, Taker_buy_quote_asset_volume
# float, Ignore
# float
df.to_csv(
    f"s3://{AWS_S3_BUCKET}/{key}",
    index=False,
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
    },
)

# "token": AWS_SESSION_TOKEN,
