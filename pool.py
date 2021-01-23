import QUANTAXIS as QA
import pandas as pd
import qaenv
import pymongo


class QAStockPool():
    def __init__(self):
        self.pool = []
        self.mongo = pymongo.MongoClient(qaenv.mongo_ip)
        self.block = QA.QA_fetch_stock_block_adv()
        self.codelist = QA.QA_fetch_stock_list_adv()

    def add_sub(self, code):
        self.pool.append(code)

    @property
    def current_code(self):
        return list(set(self.pool))

    def qa_fetch_stock_realtime(self, code, frequence='1min'):
        code = 'SH' + code if code.startswith('6') else 'SZ' + code
        data = self.mongo.qa.REALTIMEPRO_FIX.find_one({'code': code, 'frequence': frequence}, {
                                                      '_id': 0}, sort=[('datetime', pymongo.DESCENDING)])
        return data

    @property
    def latest_1min(self):
        return pd.DataFrame([self.qa_fetch_stock_realtime(item, '1min') for item in self.current_code])

    @property
    def latest_5min(self):
        return pd.DataFrame([self.qa_fetch_stock_realtime(item, '5min') for item in self.current_code])

    @property
    def latest_15min(self):
        return pd.DataFrame([self.qa_fetch_stock_realtime(item, '15min') for item in self.current_code])

    @property
    def latest_30min(self):
        return pd.DataFrame([self.qa_fetch_stock_realtime(item, '30min') for item in self.current_code])

    @property
    def latest_60min(self):
        return pd.DataFrame([self.qa_fetch_stock_realtime(item, '60min') for item in self.current_code])

    @property
    def table(self):
        data = self.latest_1min
        return data.assign(name=data.code.apply(lambda x: self.codelist.loc[x[2:]]['name']), zs=data.close/data.open - 1)

    def get_pannel(self):
        pass

    def codename(self):
        pass
