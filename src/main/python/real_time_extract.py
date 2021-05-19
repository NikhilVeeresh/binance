import pandas as pd
import math
import os
import os.path
import time
from binance.client import Client
import datetime as dt
from datetime import timedelta, datetime
from dateutil import parser
import requests
import json
import numpy as np
import pickle
import zlib
import time

# os.chdir("/home/ubuntu/data/")
os.chdir("/workspace/data/")

client = Client()


# given old and new, return the updated records in new, to append to old
# the below code returns combined data
def update_recent_trade_data(old, new):
    if (old.shape[0] > 0) and (new.shape[0] > 0):
        old_max_id = old['id'].max()
        new_max_data = new[new.id >= old_max_id]
        comb_data = pd.concat([new_max_data, old], axis=0)
    elif (old.shape[0] == 0) and (new.shape[0] > 0):
        comb_data = new
    elif (old.shape[0] > 0) and (new.shape[0] == 0):
        comb_data = old
    else:
        comb_data = pd.DataFrame(columns=['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])

    return comb_data


# this is used to update two datasets on index column
def updated_trade_data_based_on_index(old, new):
    old_max_index = old.index.max()
    d = new[new.index > old_max_index]
    old = pd.concat([old, d], axis=0)
    return old


# bitcoin trading data is of high frequency, it is good idea to resample at higher oder interval.
def get_resampled_data(pair, interval=None, sleep_time=None):
    if pair is None:
        pair = 'BTCUSDT'
    if interval is None:
        interval = '1S'
    if sleep_time is None:
        sleep_time = 600
    starttime = time.time()

    try:
        old = client.get_recent_trades(symbol=pair)
        old = pd.DataFrame(old)
        old.drop(['isBestMatch'], axis=1, inplace=True)
        old.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in old.time]
        old.sort_index(inplace=True)
    except Exception as e:
        old = pd.DataFrame(columns=['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])

    try:
        time_diff = 1 + (old.index.max() - old.index.min()).seconds
    except Exception as e:
        time_diff = 1

    # return time_diff
    if time_diff < sleep_time:
        time_span = int(sleep_time / time_diff) + 1
    while time_diff < sleep_time:
        try:
            order_dt = client.get_recent_trades(symbol=pair)
            order_df = pd.DataFrame(order_dt)
            order_df.drop(['isBestMatch'], axis=1, inplace=True)
            order_df.index = [dt.datetime.fromtimestamp(x / 1000.0) for x in order_df.time]
            updated_df = update_recent_trade_data(old, order_df)
        except Exception as e:
            updated_df = old;
        old = updated_df.copy()
        time.sleep(time_span - ((time.time() - starttime) % time_span))
        if old.shape[0] > 0:
            try:
                time_diff = 1 + (old.index.max() - old.index.min()).seconds
            except Exception as e:
                time_diff = 1
                pass
        else:
            time_diff = 1

    if old.shape[0] > 0:
        try:
            ### create the dataframe by sampling at '5S' interval
            old.sort_index(inplace=True)
            ### create the dataframe by sampling at gievn interval
            old.id = old.id.astype(int)
            old.price = old.price.astype(float)
            old.qty = old.qty.astype(float)
            old.quoteQty = old.quoteQty.astype(float)
            old.time = old.time.astype(int)
            old.isBuyerMaker = old.isBuyerMaker.astype(int)
            grp = old.groupby(['isBuyerMaker'])
            price = grp[['price']].resample(interval).mean().reset_index().set_index('level_1').fillna(
                method='bfill').sort_index().reset_index()
            price.isBuyerMaker = price.isBuyerMaker.astype(str)
            price = price.pivot(index='level_1', columns='isBuyerMaker', values='price')
            price.columns = ['taker_price', 'maker_price']
            price.index.name = None
            price.fillna(method='bfill', inplace=True)
            qty = grp[['qty']].resample(interval).sum().reset_index().set_index('level_1').fillna(
                method='bfill').sort_index().reset_index()
            qty.isBuyerMaker = qty.isBuyerMaker.astype(str)
            qty = qty.pivot(index='level_1', columns='isBuyerMaker', values='qty')
            qty.columns = ['taker_qty', 'maker_qty']
            qty.index.name = None
            qty.fillna(method='bfill', inplace=True)
            quoteQty = grp[['quoteQty']].resample(interval).sum().reset_index().set_index('level_1').fillna(
                method='bfill').sort_index().reset_index()
            quoteQty.isBuyerMaker = quoteQty.isBuyerMaker.astype(str)
            quoteQty = quoteQty.pivot(index='level_1', columns='isBuyerMaker', values='quoteQty')
            quoteQty.columns = ['taker_quoteQty', 'maker_quoteQty']
            quoteQty.index.name = None
            quoteQty.fillna(method='bfill', inplace=True)
            isBuy = grp[['isBuyerMaker']].resample(interval).count().rename(
                columns={'isBuyerMaker': 'isbuymaker'}).reset_index()
            isBuy.isBuyerMaker = isBuy.isBuyerMaker.astype(str)
            isBuy = isBuy.pivot(index='level_1', columns='isBuyerMaker', values='isbuymaker')
            isBuy.columns = ['taker_count', 'maker_count']
            isBuy.index.name = None
            isBuy.fillna(method='bfill', inplace=True)
            idd = grp[['id']].resample(interval).max().reset_index().set_index('level_1').fillna(
                method='bfill').sort_index().reset_index()
            idd.isBuyerMaker = idd.isBuyerMaker.astype(str)
            idd = idd.pivot(index='level_1', columns='isBuyerMaker', values='id')
            idd.columns = ['taker_max_id', 'maker_max_id']
            idd.index.name = None
            idd.fillna(method='bfill', inplace=True)
            tim = grp[['time']].resample(interval).max().reset_index().set_index('level_1').fillna(
                method='bfill').sort_index().reset_index()
            tim.isBuyerMaker = tim.isBuyerMaker.astype(str)
            tim = tim.pivot(index='level_1', columns='isBuyerMaker', values='time')
            tim.columns = ['taker_time', 'maker_time']
            tim.index.name = None
            tim.fillna(method='bfill', inplace=True)
            old2 = pd.DataFrame(idd)
            old2 = pd.concat([old2, price], axis=1)
            old2 = pd.concat([old2, qty], axis=1)
            old2 = pd.concat([old2, quoteQty], axis=1)
            old2 = pd.concat([old2, tim], axis=1)
            old2 = pd.concat([old2, isBuy], axis=1)
            old2 = old2.assign(pair=pair)
        except Exception as e:
            old2 = pd.DataFrame(columns=['taker_max_id', 'maker_max_id', 'taker_price', 'maker_price',
                                         'taker_qty', 'maker_qty', 'taker_quoteQty', 'maker_quoteQty',
                                         'taker_time', 'maker_time', 'taker_count', 'maker_count', 'pair'])
    else:
        old2 = pd.DataFrame(columns=['taker_max_id', 'maker_max_id', 'taker_price', 'maker_price',
                                     'taker_qty', 'maker_qty', 'taker_quoteQty', 'maker_quoteQty',
                                     'taker_time', 'maker_time', 'taker_count', 'maker_count', 'pair'])
    return old2


# download first time
old = get_resampled_data(pair="BTCUSDT", interval='1S', sleep_time=20)
# write to disk
dbfile = open('btc_data', 'wb')
compressed = zlib.compress(pickle.dumps(old))
pickle.dump(compressed, dbfile)
dbfile.close()

# While loop with suitable break condition. In this c
while True:
    starttime = time.time()
    new = get_resampled_data(pair="BTCUSDT", interval='1S', sleep_time=120)
    ## read from disk
    dbfile = open('btc_data', 'rb')
    db = pickle.load(dbfile)
    dbfile.close()
    old = pickle.loads(zlib.decompress(db))
    # update the records
    try:
        if new.shape[0] > 0:
            # save to disk
            old = updated_trade_data_based_on_index(old, new)
            dbfile = open('btc_data', 'wb')
            compressed = zlib.compress(pickle.dumps(old))
            pickle.dump(compressed, dbfile)
            dbfile.close()
        else:
            pass
    except Exception as e:
        pass

    # if more than 28 days, stop the program.
    if (old.index.max() - old.index.min()).seconds / 86400 / 28 > 1:  # if more than 4 weeks
        break