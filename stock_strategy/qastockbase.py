#


"""
stock_base
"""
import uuid
import datetime
import json
import os
import threading
import requests
import pandas as pd
import pymongo
from qaenv import (eventmq_ip, eventmq_password, eventmq_port,
                   eventmq_username, mongo_ip)

import QUANTAXIS as QA
from QUANTAXIS.QAARP import QA_Risk, QA_User
from QUANTAXIS.QAEngine.QAThreadEngine import QA_Thread
from QUANTAXIS.QAUtil.QAParameter import MARKET_TYPE, RUNNING_ENVIRONMENT, ORDER_DIRECTION
from QAPUBSUB.consumer import subscriber_topic, subscriber_routing
from QAPUBSUB.producer import publisher_routing
from QAStrategy.qactabase import QAStrategyCTABase
from QIFIAccount import QIFI_Account
# 自增
from QUANTAXIS.QAUtil import (
    QA_util_date_stamp,
    QA_util_time_stamp
)

from QUANTAXIS.QAData import (QA_DataStruct_Stock_min)

class QAStrategyStockBase(QAStrategyCTABase):

    def __init__(self, code=['000001'], frequence='1min', strategy_id='QA_STRATEGY', risk_check_gap=1,
                 portfolio='default',
                 start='2019-01-01', end='2019-10-21', send_wx=False, market_type='stock_cn',
                 data_host=eventmq_ip, data_port=eventmq_port, data_user=eventmq_username,
                 data_password=eventmq_password,
                 trade_host=eventmq_ip, trade_port=eventmq_port, trade_user=eventmq_username,
                 trade_password=eventmq_password,
                 taskid=None, mongo_ip=mongo_ip):
        super().__init__(code=code, frequence=frequence, strategy_id=strategy_id, risk_check_gap=risk_check_gap,
                         portfolio=portfolio,
                         start=start, end=end, send_wx=send_wx,
                         data_host=eventmq_ip, data_port=eventmq_port, data_user=eventmq_username,
                         data_password=eventmq_password,
                         trade_host=eventmq_ip, trade_port=eventmq_port, trade_user=eventmq_username,
                         trade_password=eventmq_password,
                         taskid=taskid, mongo_ip=mongo_ip)

        self.code = code
        self.send_wx = send_wx
        self.dtcode={}
        import pandas as pd
        stock_pool_pd = pd.read_csv("/root/sim/stock_strategy/stock_pool.csv", encoding='utf-8',
                                    converters={'code': str});
        self.stock_pool_list = stock_pool_pd['code'].tolist()
        '''

        '''
        print('订阅远程行情开始：')
        self.subscriber = subscriber_topic(
            host='www.yutiansut.com',
            port='5678',
            exchange='QARealtimePro_FIX',
            durable=False,
            vhost='/',
            routing_key='*')
        #pymongo.MongoClient(mongo_ip).qa.drop_collection('REALTIMEPRO_FIX')
        self.db = pymongo.MongoClient(mongo_ip).qa.REALTIMEPRO_FIX

    def subscribe_data(self, code, frequence, data_host, data_port, data_user, data_password):
        """[summary]

        Arguments:
            code {[type]} -- [description]
            frequence {[type]} -- [description]
        """

        self.sub = subscriber_topic(exchange='realtime_stock_{}'.format(
            frequence), host=data_host, port=data_port, user=data_user, password=data_password, routing_key='')
        for item in code:
            self.sub.add_sub(exchange='realtime_stock_{}'.format(
                frequence), routing_key=item)
        self.sub.callback = self.callback
        self.subscriber.callback = self.callback

    def getSimStock(self, codes, frequence):
        code_list = []
        for code in codes:
            if (code.startswith('3') or code.startswith('0')):
                code_list.append('SZ'+str(code))
            elif(code.startswith('6')) :
                code_list.append('SH' + str(code))

        cursor = self.db.find(
            {
                'code': {
                    '$in': code_list
                },
                "datetime":
                    {
                        "$gte": str(datetime.date.today()),
                        "$lte": str(datetime.datetime.now())
                    },
                'frequence': frequence
            },
            {"_id": 0},
            batch_size=10000
        )

        new_list = []
        for item in cursor:
            new_list.append(self.format_stock_data(item))

        res = pd.DataFrame([item for item in new_list])
        #res['datetime'] = pd.to_datetime(res['datetime'])
        print(res)
        res = res.query('volume>1').drop_duplicates(['datetime',
                                             'code']).set_index(['datetime', 'code']
                                                      ).loc[:, ['open', 'high', 'low', 'close', 'volume']]

        return res


    def upcoming_data(self, new_bar):
        """upcoming_bar :

        Arguments:
            new_bar {json} -- [description]
        """

        self._market_data = pd.concat([self._old_data.tail(200), new_bar])
        # QA.QA_util_log_info(self._market_data)

        if self.isupdate:
            self.update()
            self.isupdate = False

        self.update_account()
        # self.positions.on_price_change(float(new_bar['close']))

        #print('new_bar...',new_bar)
        self.on_bar(new_bar)

    def ind2str(self, ind, ind_type):
        z = ind.tail(1).reset_index().to_dict(orient='records')[0]
        return json.dumps({'topic': ind_type, 'code': self.code, 'type': self.frequence, 'data': z})

    def callback(self, a, b, c, body):
        """在strategy的callback中,我们需要的是

        1. 更新数据
        2. 更新bar
        3. 更新策略状态
        4. 推送事件

        Arguments:
            a {[type]} -- [description]
            b {[type]} -- [description]
            c {[type]} -- [description]
            body {[type]} -- [description]
        """
        #print(body)

        new_data = json.loads(str(body, encoding='utf-8'))
        self.newbar(new_data)

    def newbar(self, new_data):

        for item in new_data:

            new_dict = self.format_stock_data(item)
            self.new_data = new_dict
            # 只接受5min
            if (not self.new_data or self.new_data['type'] != '5min'):
                # print('new_data 不存在', new_data)
                continue



            self.running_time = self.new_data['datetime']
            code = self.new_data['code']
            if code not in self.dtcode or self.dtcode[code] != str(self.new_data['datetime'])[0:16]:
                # [0:16]是分钟线位数
                print('update!!!!!!!!!!!! dt:')
                self.dtcode[code] = str(self.new_data['datetime'])[0:16]
                self.isupdate = True

        


            self.latest_price[self.new_data['code']] = self.new_data['close']
            self.acc.on_price_change(self.new_data['code'], self.new_data['close'])
            bar = pd.DataFrame([self.new_data]).set_index(['datetime', 'code'])
            #bar = QA_DataStruct_Stock_min(bar)
            bar = bar.loc[:, ['open', 'high', 'low', 'close', 'volume']]
            #print('bar:.......',bar)
            self.upcoming_data(bar)

    def format_stock_data(self, item):
        code = item.get('code')
        new_code = code[-6:]

        d = {}
        #and item.get('frequence') == '1min'
        if new_code in self.stock_pool_list:
            d['code'] = new_code
            d['open'] = item.get('open')
            d['high'] = item.get('high')
            d['close'] = item.get('close')
            d['low'] = item.get('low')
            d['vol'] = item.get('volume')
            d['volume'] = item.get('volume')
            d['type'] = item.get('frequence')
            # d['amount'] = item.get('volume')
            d['date_stamp'] = QA_util_date_stamp(item.get('datetime'))
            d['time_stamp'] = QA_util_time_stamp(item.get('datetime'))
            d['date'] = item.get('datetime')[0:10]  # 2020-10-12
            d['datetime'] = pd.to_datetime(item.get('datetime'))  # 2020-10-12 10:02:00
            d['tradetime'] = item.get('datetime')[0:16]  # 2020-10-12 10:02

        return d

    def _debug_sim(self):
        self.running_mode = 'sim'

        self._old_data = QA.QA_fetch_stock_min(self.code, QA.QA_util_get_last_day(
            QA.QA_util_get_real_date(str(datetime.date.today())), 5), str(datetime.datetime.now()), format='pd',
                                               frequence=self.frequence).set_index(['datetime', 'code'])
        #当天启动前行情数据
        sim_stock_df = self.getSimStock(self.code, self.frequence)
        self._old_data = pd.concat([self._old_data, sim_stock_df])

        self._old_data = self._old_data.loc[:, ['open', 'high', 'low', 'close', 'volume']]
        #print('old_data ;:::::::', self._old_data)
        self.database = pymongo.MongoClient(mongo_ip).QAREALTIME

        self.client = self.database.account
        self.subscriber_client = self.database.subscribe
        self.acc = QIFI_Account(
            username=self.strategy_id, password=self.strategy_id, trade_host=mongo_ip)
        self.acc.initial()
        self.pub = publisher_routing(exchange='QAORDER_ROUTER', host=self.trade_host,
                                     port=self.trade_port, user=self.trade_user, password=self.trade_password)

        self.subscribe_data(self.code, self.frequence, self.data_host,
                            self.data_port, self.data_user, self.data_password)

        self.database.strategy_schedule.job_control.update(
            {'strategy_id': self.strategy_id},
            {'strategy_id': self.strategy_id, 'taskid': self.taskid,
             'filepath': os.path.abspath(__file__), 'status': 200}, upsert=True)

        # threading.Thread(target=, daemon=True).start()
        # print(self.subscribe_data)
        # self.sub.start()
        #self.debug_callback()
        self.subscriber.start()

    def debug_callback(self):
        new_data = [{'datetime': '2021-01-27 18:05:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000338', 'open': 5.24,
             'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min', 'pctchange': 0.0}]
        self.newbar(new_data)

        # 另一个code
        new_data = [{'datetime': '2021-01-27 18:05:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000545',
                     'open': 5.24,
                     'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min',
                     'pctchange': 0.0}]
        self.newbar(new_data)

        new_data = [{'datetime': '2021-01-27 18:10:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000338', 'open': 5.24,
             'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min', 'pctchange': 0.0}]
        self.newbar(new_data)

        new_data = [{'datetime': '2021-01-27 18:10:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000545',
                     'open': 5.24,
                     'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min',
                     'pctchange': 0.0}]
        self.newbar(new_data)

        new_data = [
            {'datetime': '2021-01-27 18:15:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000338', 'open': 5.24,
             'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min', 'pctchange': 0.0}]
        self.newbar(new_data)
        new_data = [{'datetime': '2021-01-27 18:15:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000545',
                     'open': 5.24,
                     'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min',
                     'pctchange': 0.0}]
        self.newbar(new_data)

        '''
        new_data = [{'datetime': '2021-01-27 18:15:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000338','open': 5.24,
                    'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min', 'pctchange': 0.0}]
        self.newbar(new_data)
        new_data = [{'datetime': '2021-01-27 18:15:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000545',
                     'open': 5.24,
                     'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min',
                     'pctchange': 0.0}]
        self.newbar(new_data)

        new_data = [{'datetime': '2021-01-27 18:20:00', 'updatetime': '2021-01-15 10:10:51', 'code': 'SZ000338',
                     'open': 5.24,
                     'high': 5.24, 'low': 5.24, 'close': 5.24, 'volume': 39378.0, 'frequence': '5min',
                     'pctchange': 0.0}]
        self.newbar(new_data)
        '''


    def run(self):
        while True:
            pass

    def debug(self):
        self.running_mode = 'backtest'
        self.database = pymongo.MongoClient(mongo_ip).QUANTAXIS
        user = QA_User(username="admin", password='admin')
        port = user.new_portfolio(self.portfolio)
        self.acc = port.new_accountpro(
            account_cookie=self.strategy_id, init_cash=self.init_cash, market_type=self.market_type)
        # self.positions = self.acc.get_position(self.code)
        print('data')
        data = QA.QA_quotation(self.code, self.start, self.end, source=QA.DATASOURCE.MONGO,
                               frequence=self.frequence, market=self.market_type, output=QA.OUTPUT_FORMAT.DATASTRUCT)
        print(data.data)
        data.data.apply(self.x1, axis=1)

    def update_account(self):
        if self.running_mode == 'sim':
            QA.QA_util_log_info('{} UPDATE ACCOUNT'.format(
                str(datetime.datetime.now())))

            self.accounts = self.acc.account_msg
            self.orders = self.acc.orders
            self.positions = self.acc.positions

            self.trades = self.acc.trades
            self.updatetime = self.acc.dtstr
        elif self.running_mode == 'backtest':
            # self.positions = self.acc.get_position(self.code)
            self.positions = self.acc.positions

    def send_order(self, direction='BUY', offset='OPEN', code=None, price=3925, volume=10, order_id='', ):

        towards = eval('ORDER_DIRECTION.{}_{}'.format(direction, offset))
        order_id = str(uuid.uuid4()) if order_id == '' else order_id

        if self.market_type == QA.MARKET_TYPE.STOCK_CN:
            """
            在此对于股票的部分做一些转换
            """
            if towards == ORDER_DIRECTION.SELL_CLOSE:
                towards = ORDER_DIRECTION.SELL
            elif towards == ORDER_DIRECTION.BUY_OPEN:
                towards = ORDER_DIRECTION.BUY

        if isinstance(price, float):
            pass
        elif isinstance(price, pd.Series):
            price = price.values[0]

        if self.running_mode == 'sim':

            QA.QA_util_log_info(
                '============ {} SEND ORDER =================='.format(order_id))
            QA.QA_util_log_info('direction{} offset {} price{} volume{}'.format(
                direction, offset, price, volume))

            if self.check_order(direction, offset):
                self.last_order_towards = {'BUY': '', 'SELL': ''}
                self.last_order_towards[direction] = offset
                now = str(datetime.datetime.now())

                order = self.acc.send_order(
                    code=code, towards=towards, price=price, amount=volume, order_id=order_id)
                order['topic'] = 'send_order'
                self.pub.pub(
                    json.dumps(order), routing_key=self.strategy_id)

                self.acc.make_deal(order)
                self.bar_order['{}_{}'.format(direction, offset)] = self.bar_id
                if self.send_wx:
                    for user in self.subscriber_list:
                        QA.QA_util_log_info(self.subscriber_list)
                        try:
                            "oL-C4w2WlfyZ1vHSAHLXb2gvqiMI"
                            """http://www.yutiansut.com/signal?user_id=oL-C4w1HjuPRqTIRcZUyYR0QcLzo&template=xiadan_report&\
                                        strategy_id=test1&realaccount=133496&code=rb1910&order_direction=BUY&\
                                        order_offset=OPEN&price=3600&volume=1&order_time=20190909
                            """

                            requests.post(
                                'http://www.yutiansut.com/signal?user_id={}&template={}&strategy_id={}&realaccount={}&code={}&order_direction={}&order_offset={}&price={}&volume={}&order_time={}'.format(
                                    user, "xiadan_report", self.strategy_id, self.acc.user_id, code, direction, offset,
                                    price, volume, now))
                        except Exception as e:
                            QA.QA_util_log_info(e)

            else:
                QA.QA_util_log_info('failed in ORDER_CHECK')

        elif self.running_mode == 'backtest':

            self.bar_order['{}_{}'.format(direction, offset)] = self.bar_id

            self.acc.receive_simpledeal(
                code=code, trade_time=self.running_time, trade_towards=towards, trade_amount=volume, trade_price=price,
                order_id=order_id)
            # self.positions = self.acc.get_position(self.code)


if __name__ == '__main__':
    QAStrategyStockBase(code=['000001', '000002']).run_sim()
