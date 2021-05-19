import pandas as pd

from binance import Client
from helper import *
import time

# -----------------------------------------PROBLEM STATEMENT------------------------------------------------------------
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
# generate brand new key
# ----------------------------------------------------------------------------------------------------------------------


api_key = "CzzT0PC7Hv4ZjEVaEbKxsPS6PwI73ucSTQXwl3ygEdSoIr0eDHykzeUM1dd1OyHe"
api_secret = "45HrenPNhvvFr86qhaniG6fL00rtCD4F8mysAZGa35afX8IPDHUoUz4Hov9i6iL0"
client = Client(api_key, api_secret, tld='us')


def get_trading_pairs(start: str = "1 May, 2021", end: str = "16 May, 2021") -> pd.DataFrame:
    symbol = "BTCUSDT"
    # [BTCUSDT, BTCSTBTC, BTCSTBUSD, BTCSTUSDT]
    interval = Client.KLINE_INTERVAL_1MINUTE

    klines = client.get_historical_klines(symbol, interval, start, end)
    tradingpairs_columns = ["candle_open_time", "open_price", "high_price", "low_price", "close_price", "btc_volume",
                            "candle_close_time", "usd_volume", "number_of_trades", "taker_buy_base_asset_volume",
                            "taker_buy_quote_asset_volume", "ignore"]

    df = pd.DataFrame(klines, columns=tradingpairs_columns)
    print(df['candle_open_time'].unique())
    df['candle_open_time'] = df['candle_open_time'].astype(str)
    df['candle_close_time'] = df['candle_close_time'].astype(str)

    df['candle_open_time'] = pd.to_datetime(df['candle_open_time'], unit='ms')
    df['candle_close_time'] = pd.to_datetime(df['candle_close_time'], unit='ms')

    return df


def get_account_orders() -> pd.DataFrame:
    timestamp = int(time.time() * 1000)
    account_orders = client.get_all_orders(symbol="BTCUSDT", limit=10)
    # account_orders_open = client.get_open_orders(symbol="BTCUSDT", recvWindow=5000, timestamp=timestamp)

    return account_orders


def write_to_s3_from_dataframe(df_input, start, end):
    symbol = "BTCSTBUSD"
    # BTCSTBTC
    # BTCSTBUSD
    # BTCSTUSDT

    interval = Client.KLINE_INTERVAL_1MINUTE

    AWS_ACCESS_KEY_ID = 'AKIAZO7N3DRSDMPKCB3Q'
    AWS_SECRET_ACCESS_KEY = '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'
    AWS_S3_BUCKET = 'data-lake-us-east-1-650649476196'
    key = 'tradingpairs/{}{}{}{}{}'.format(symbol,
                                           interval,
                                           date_to_milliseconds(start),
                                           date_to_milliseconds(end),
                                           ".csv")

    df_input.to_csv(
        f"s3://{AWS_S3_BUCKET}/{key}",
        index=False,
        storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY,
        },
    )


def main():
    start_date = "17 May, 2021"
    end_date = "19 May, 2021"
    # Get the trade Pairs for the date range and write to S3 bucket
    df = get_trading_pairs(start_date, end_date)
    write_to_s3_from_dataframe(df, start_date, end_date)


main()
