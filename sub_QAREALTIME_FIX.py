import json
import click
import pymongo
from QAPUBSUB.consumer import subscriber_topic
from QUANTAXIS.QAEngine import QA_Thread
from qaenv import mongo_ip,eventmq_ip

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
        #pymongo.MongoClient(mongo_ip).qa.drop_collection('REALTIMEPRO_FIX')
        self.db = pymongo.MongoClient(mongo_ip).qa.REALTIMEPRO_FIX
        '''
        import pandas as pd
        stock_pool_pd = pd.read_csv("/root/sim/stock_strategy/stock_pool.csv", encoding='utf-8',
                                    converters={'code': str});
        self.stock_pool_list = stock_pool_pd['code'].tolist()
        '''


        self.db.create_index(
            [
                ('code',
                 pymongo.ASCENDING),
                ('datetime',
                 pymongo.ASCENDING),
                ('frequence',
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

        new_res = []
        for item in res:
            if (item.get('frequence') != '1min'):
                new_res.append(item)
        if (new_res) :
            print(res)
            self.on_fixdata(new_res)
        return

    def on_fixdata(self, data):
        self.db.insert_many(data)

    def run(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


@click.command()
@click.option('--host', default='www.yutiansut.com')
@click.option('--port', default=5678)
def qarealtime_fix(host, port):
    sub_realtime(host, port).start()


if __name__ == '__main__':
    qarealtime_fix()
