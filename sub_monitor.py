import json

import pymongo
from QAPUBSUB.consumer import subscriber_routing
from QUANTAXIS.QAEngine import QA_Thread


class sub_monitor(QA_Thread):
    def __init__(self):
        super().__init__()
        self.subscriber = subscriber_routing(
            host='192.168.2.116',
            exchange='AcMD.Monitor',
            durable=True,
            vhost='/',
            routing_key='All')
        self.db  = pymongo.MongoClient().qa.monitor


    def callback(self, a, b, c, data):
        res = json.loads(data, encoding='utf-8')
        self.on_askbid(res)

    def on_askbid(self, data):

        print(data['product'])
        ##df = pd.DataFrame(data['product'])
        #self.db.insert_many(data['product'])
        

    def run(self):
        self.subscriber.callback = self.callback
        self.subscriber.start()


if __name__ == '__main__':
    sub_monitor().start()
