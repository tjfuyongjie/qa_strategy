import json
import click
import pymongo
from QAPUBSUB.consumer import subscriber_topic
from QUANTAXIS import QA_util_date_stamp, QA
from QUANTAXIS.QAEngine import QA_Thread
from qaenv import mongo_ip,eventmq_ip
from QUANTAXIS.QAUtil import (
    QA_util_date_stamp,
    QA_util_time_stamp
)
class sub_realtime(QA_Thread):
    def __init__(self, host, port):
        super().__init__()
        self.subscriber = subscriber_topic(
            host=host,
            port=port,
            exchange='QARealtimePro_FIX',
            durable=False,
            vhost='/',
            routing_key='*')
        self.initdb()

    def test(self):
        self.initdb()
        data = """[{'datetime': '2021-01-15 10:10:09', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ002790', 'open': 5.24,
          'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '1min', 'pctchange': 0.0}]
        [{'datetime': '2021-01-15 10:10:00', 'updatetime': '2021-01-15 10:10:57', 'code': 'SZ002865', 'open': 18.15,
          'high': 18.15, 'low': 18.13, 'close': 18.15, 'volume': 32362.0, 'frequence': '1min', 'pctchange': 0.0}]
        '
        """
        data = json.dumps(data, indent=2)
        self.callback(1,2,3,data)
    def initdb(self):
        # pymongo.MongoClient(mongo_ip).qa.drop_collection('REALTIMEPRO_FIX')
        # self.db = pymongo.MongoClient(mongo_ip).qa.REALTIMEPRO_FIX
        pymongo.MongoClient(mongo_ip).quantaxis.drop_collection('stock_min')
        self.db = pymongo.MongoClient(mongo_ip).quantaxis.stock_min

        self.db.create_index(
            [
                ('code',
                 pymongo.ASCENDING),
                ('time_stamp',
                 pymongo.ASCENDING),
                ('type',
                 pymongo.ASCENDING),
            ]
        )
    def callback(self, a, b, c, data):
        """这里是订阅处理逻辑

        Arguments:
            a {[type]} -- [description]
            b {[type]} -- [description]
            c {[type]} -- [description]
            data {[type]} -- [description]
        """
        res = json.loads(data, encoding='utf-8')

        '''
        print(res)
        [{'datetime': '2021-01-15 10:10:09', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ002790', 'open': 5.24,
          'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '1min', 'pctchange': 0.0}]
        [{'datetime': '2021-01-15 10:10:00', 'updatetime': '2021-01-15 10:10:57', 'code': 'SZ002865', 'open': 18.15,
          'high': 18.15, 'low': 18.13, 'close': 18.15, 'volume': 32362.0, 'frequence': '1min', 'pctchange': 0.0}]
        '''
        res = [{'datetime': '2021-01-15 10:10:09', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ002790', 'open': 5.24,
          'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min', 'pctchange': 0.0}]

        new_res = []
        for item in res:
            print(item)
            new_dict = self.format_stock_data(item)
            new_res.append(new_dict)
        if len(new_res) != 0:
            print(new_res)
            self.on_fixdata(new_res)

    def on_fixdata(self, data):
        self.db.insert_many(data)

    def format_stock_data(self,item):
        code = item.get('code')
        new_code = code[-6:]
        d = {}
        print(new_code)
        print(item)
        if new_code == '002790' and item.get('frequence') == '5min':
            d['code'] = new_code
            d['open'] = item.get('open')
            d['high'] = item.get('high')
            d['close'] = item.get('close')
            d['low'] = item.get('low')
            d['vol'] = item.get('volume')
            #d['amount'] = item.get('volume')
            d['date_stamp'] = QA_util_date_stamp(item.get('datetime'))
            d['time_stamp'] = QA_util_time_stamp(item.get('datetime'))
            d['date'] = item.get('datetime')[0:10] #2020-10-12
            d['datetime'] = item.get('datetime') #2020-10-12 10:02:00
            d['tradetime'] = item.get('datetime')[0:16]  #2020-10-12 10:02
            d['type'] = item.get('frequence')
        return d

    def run(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


@click.command()
@click.option('--host', default='www.yutiansut.com')
@click.option('--port', default=5678)
def qarealtime_fix(host, port):
    #sub_realtime(host, port).start()
    sub_realtime(host, port).test()

if __name__ == '__main__':

    qarealtime_fix()
