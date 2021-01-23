import json

import QUANTAXIS as QA
import pandas as pd
import pymongo
from QAPUBSUB.consumer import subscriber_routing
from QUANTAXIS.QAEngine import QA_Thread
from pymongo import IndexModel, ASCENDING, DESCENDING
from pymongo.errors import CollectionInvalid


class sub_ab(QA_Thread):
    def __init__(self):
        super().__init__()
        self.subscriber = subscriber_routing(
            host='192.168.2.116',
            exchange='AcMD.FullData',
            durable=True,
            vhost='/',
            routing_key='All')

        # index1 = IndexModel([("code", DESCENDING),
        #                      ("market", ASCENDING)], name="code")
        # index2 = IndexModel([("datetime", DESCENDING)])
        # try:
        #     self.mgdbcapped = pymongo.MongoClient().qa.create_collection('ASKBIDCAPPED', capped=True,
        #                                                                  size=1024 * 1024 * 100, max=120000)
        # except CollectionInvalid:
        #     self.mgdbcapped = pymongo.MongoClient().qa.ASKBIDCAPPED

        # self.mgdbcapped.create_indexes([index1, index2])
        # self.market = {}

    def callback(self, a, b, c, data):
        """这里是订阅处理逻辑

        Arguments:
            a {[type]} -- [description]
            b {[type]} -- [description]
            c {[type]} -- [description]
            data {[type]} -- [description]
        """
        res = json.loads(data, encoding='utf-8')
        self.on_askbid(res)

    def on_askbid(self, data):
        df = pd.DataFrame(data['products'])
        #print(df)
        #market = data['']
        df = df.assign(datetime=pd.to_datetime(df['time']))
        try:
            sh = df.query('market=="SH"')
            sz = df.query('market=="SZ"')

            
            # sh_1 = sh[sh.code.apply(lambda x: x[0] == '6')]
            # sz_1 = sz[sz.code.apply(lambda x: x[0:2] in ['00', '30'])]
            #print(sh.code)
            #print(sh.query("code=='SH000001'"))
            # if len(sh_1) > 0:
            #     self.mgdbcapped.insert_many(
            #         QA.QA_util_to_json_from_pandas(sh_1), ordered=False)
            # if len(sz_1) > 0:
            #     self.mgdbcapped.insert_many(
            #         QA.QA_util_to_json_from_pandas(sz_1), ordered=False)
        except Exception as e:
            print(e)

    def run(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


if __name__ == '__main__':
    sub_ab().start()
