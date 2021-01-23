#
import QUANTAXIS as QA
import json
import pandas as pd
import pymongo
import queue
import time
from QAPUBSUB.consumer import subscriber_routing
from QAWebServer.basehandles import QAWebSocketHandler
from QUANTAXIS.QAEngine import QA_Thread



class sub_l2(QA_Thread):
    def __init__(self, code, host='192.168.2.116', port = 5672):
        super().__init__()

        code = 'SH'+ code if code.startswith('6') else 'Z$'+ code
        self.subscriber = subscriber_routing(
            host=host, port= port, 
            exchange='AcMD.L2',
            durable=True,
            vhost='/',
            routing_key=code)
        self.mgdb = pymongo.MongoClient().qa.LEVEL2

        
    def callback(self, a, b, c, data):
        """这里是订阅处理逻辑

        Arguments:
            a {[type]} -- [description]
            b {[type]} -- [description]
            c {[type]} -- [description]
            data {[type]} -- [description]
        """
        res = json.loads(data, encoding='utf-8')
        df = pd.DataFrame(res)
        df = df.assign(amount=df.Vol * df.Price*100)
        self.on_tick(df)

    def on_tick(self, data):
        res = QA.QA_util_to_json_from_pandas(data)
        print(res)
        self.mgdb.insert_many(res)

    def run_unblocked(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


    def run(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


if __name__ == '__main__':
    sub_l2('000652').start()
