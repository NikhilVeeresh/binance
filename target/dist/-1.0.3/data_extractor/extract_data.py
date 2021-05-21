import pandas as pd
from binance import Client

from config import Config
from data_type import DataType
from helper import date_to_milliseconds
from trade_symbols import TradeSymbol


class BinanceGemini:

    def __init__(self):
        self._config = Config()
        self._client = Client(self._config.binance_api_key, self._config.binance_api_secret,
                              tld=self._config.binance_tld)
        self._symbol = TradeSymbol.BTCUSDT
        self._interval = Client.KLINE_INTERVAL_1MINUTE

    def prepare_trading_pairs(self, start: str, end: str,
                              interval: str = Client.KLINE_INTERVAL_1MINUTE) -> pd.DataFrame:
        k_lines_extract = self._client.get_historical_klines(self._symbol.value, interval, start,
                                                             end)
        tradingpairs_columns = ["candle_open_time", "open_price", "high_price", "low_price",
                                "close_price",
                                "btc_volume", "candle_close_time", "usd_volume",
                                "number_of_trades",
                                "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume",
                                "ignore"]

        trading_date = pd.DataFrame(k_lines_extract, columns=tradingpairs_columns)
        trading_date['candle_open_time'] = trading_date['candle_open_time'].astype(str)
        trading_date['candle_close_time'] = trading_date['candle_close_time'].astype(str)
        trading_date['candle_open_time'] = pd.to_datetime(trading_date['candle_open_time'],
                                                          unit='ms')
        trading_date['candle_close_time'] = pd.to_datetime(trading_date['candle_close_time'],
                                                           unit='ms')
        trading_date['trading_pair'] = self._symbol.value

        return trading_date

    def get_account_orders(self, limit_orders: int = 500) -> pd.DataFrame:
        orders_limited = self._client.get_all_orders(symbol=self._symbol.value, limit=limit_orders)
        account_orders_columns = ["symbol", "orderId", "orderListId", "clientOrderId", "price",
                                  "origQty",
                                  "executedQty", "cummulativeQuoteQty", "status", "timeInForce",
                                  "type",
                                  "side", "stopPrice", "icebergQty", "time", "updateTime",
                                  "isWorking",
                                  "origQuoteOrderQty"]
        df_orders = pd.DataFrame(orders_limited, columns=account_orders_columns)

        return df_orders

    def write_to_s3_from_dataframe(self, bucket_object, df_input, start, end):
        key = 'tradingpairs/{}{}{}{}{}'.format(self._symbol.value,
                                               self._interval,
                                               date_to_milliseconds(start),
                                               date_to_milliseconds(end),
                                               ".csv")

        df_input.to_csv(
            f"s3://{self._config.s3_bucket}/{bucket_object}/{key}",
            index=False,
            storage_options={
                "key": self._config.s3_access_key_id,
                "secret": self._config.s3_secret_access_key,
            },
        )


def main():
    binance_operator = BinanceGemini()
    start_date = "20 May, 2021"
    end_date = "20 May, 2021"
    # Get the trade Pairs for the date range and write to S3 bucket
    df_trading_pairs = binance_operator.prepare_trading_pairs(start_date, end_date)
    df_account_orders = binance_operator.get_account_orders()
    binance_operator.write_to_s3_from_dataframe(DataType.TRADEPAIRS, df_trading_pairs, start_date,
                                                end_date)
    binance_operator.write_to_s3_from_dataframe(DataType.ACCOUNTORDERS, df_account_orders,
                                                start_date, end_date)


main()
