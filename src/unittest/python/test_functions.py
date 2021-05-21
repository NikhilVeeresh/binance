from unittest import TestCase

from data_extractor.data_type import DataType
from data_extractor.extract_data import BinanceGemini


class UtilsTest(TestCase):

    def test_data_extraction(self):
        binance_operator = BinanceGemini()
        start_date = "20 May, 2021"
        end_date = "20 May, 2021"
        # Get the trade Pairs for the date range and write to S3 bucket
        df_trading_pairs = binance_operator.prepare_trading_pairs(start_date, end_date)
        df_account_orders = binance_operator.get_account_orders()
        binance_operator.write_to_s3_from_dataframe(DataType.TRADEPAIRS,
                                                    df_trading_pairs, start_date, end_date)
        binance_operator.write_to_s3_from_dataframe(DataType.ACCOUNTORDERS,
                                                    df_account_orders, start_date, end_date)

        self.assertEqual(13, len(df_trading_pairs.columns))
        self.assertEqual(18, len(df_account_orders.columns))
