
import datetime
import time

import pandas as pd
import pymongo
import QUANTAXIS as QA
from qaenv import mongo_ip
from dask.distributed import Client
from dask.bag import *
import dask.array as da
import dask.dataframe as dd


def job():
    t = datetime.datetime.now()
    coll = pymongo.MongoClient(mongo_ip).qa.ASKBIDCAPPED
    data = from_sequence([item for item in coll.find(
        # {'code':'000001'},
        {},
        {'_id': 0, 'close': 1, 'amount': 1, 'vol':1,'code': 1, 'market': 1, 'datetime': 1},
        batch_size=10000000)]).to_dataframe(meta ={'close':'f64', 'amount': 'f64', 'vol': 'f64', 'code': 'str', 'market': 'str', 'datetime': 'str'})
    res = data.assign(datetime=dd.to_datetime(data.datetime)).set_index('datetime').groupby(
        'code').apply(lambda x: x.resample('1min').apply({
            'close': 'ohlc',
            'vol':'last',
            # 'code': 'last',
            'amount': 'last'
        }, meta ={'open':'f64', 'high': 'f64', 'low': 'f64', 'close': 'f64', 'vol': 'f64', 'amount': 'f64'}))
    res.columns = res.columns.droplevel(0)


    last_minute = '{} 15:00:00'.format(datetime.date.today())
    #data_min =res.loc[(slice(None), last_minute), :]
    re = compute(res.reindex(1))
    #re = (data_min)
    print(re)

    pymongo.MongoClient(mongo_ip).qa.stock_1min.insert_many(
        QA.QA_util_to_json_from_pandas(re.reset_index())
    )


    print(datetime.datetime.now())
if __name__ == '__main__':
    #freeze_support()
    c = Client()
    job()