import json
import os

CONFIG_FILENAME = 'datastore_config.json'


class Config:
    def __init__(self, config_path=None):
        if config_path is not None:
            config = config_path
        else:
            config = '../resources/' + CONFIG_FILENAME  # running from scripts directory
            possible_config_paths = [
                '../../../main/resources/',  # unittest execution from pycharm
                '../../main/resources/',  # training data preparer from pycharm
                'src/main/resources/'  # unittest running via pybuilder
            ]

            for path in possible_config_paths:
                config_file = path + CONFIG_FILENAME
                if os.path.isfile(config_file):
                    config = config_file

        with open(config, 'r') as config_fp:
            self._config = json.load(config_fp)

        self._binance_api_key = self._config['binance-api']['api_key']
        self._binance_api_secret = self._config['binance-api']['api_secret']
        self._binance_tld = self._config['binance-api']['api_tld']
        self._s3_access_key_id = self._config['s3-data-lake']['access_key_id']
        self._s3_secret_access_key = self._config['s3-data-lake']['secret_access_key']
        self._s3_bucket = self._config['s3-data-lake']['bucket']
        self._trading_pairs_object = self._config['s3-data-lake']['trading_pairs_object']
        self._account_orders_object = self._config['s3-data-lake']['account_orders_object']

    @property
    def binance_api_key(self):
        return self._binance_api_key

    @property
    def binance_api_secret(self):
        return self._binance_api_secret

    @property
    def binance_tld(self):
        return self._binance_tld

    @property
    def s3_access_key_id(self):
        return self._s3_access_key_id

    @property
    def s3_secret_access_key(self):
        return self._s3_secret_access_key

    @property
    def s3_bucket(self):
        return self._s3_bucket

    @property
    def trading_pairs_object(self):
        return self._trading_pairs_object

    @property
    def account_orders_object(self):
        return self._account_orders_object
