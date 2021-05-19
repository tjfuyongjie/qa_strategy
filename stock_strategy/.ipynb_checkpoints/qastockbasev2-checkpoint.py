#


"""
stock_base
"""
import uuid
import datetime
import time
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
from QAPUBSUB.consumer import subscriber_topic,  subscriber_routing
from QAPUBSUB.producer import publisher_routing
from QAStrategy.qactabase import QAStrategyCTABase
from QIFIAccount import QIFI_Account
# 自增
from QUANTAXIS.QAUtil import (
    QA_util_date_stamp,
    QA_util_time_stamp
)
import re
from QUANTAXIS.QASU.save_huobi import QA_SU_save_huobi_min,huobi_SYMBOL


class QAStrategyCoinBase(QAStrategyCTABase):

    def __init__(self, code=['000001'], frequence='1min', strategy_id='QA_STRATEGY', risk_check_gap=1, portfolio='default',
                 start='2019-01-01', end='2019-10-21', send_wx=False, market_type='stock_cn',
                 data_host=eventmq_ip, data_port=eventmq_port, data_user=eventmq_username, data_password=eventmq_password,
                 trade_host=eventmq_ip, trade_port=eventmq_port, trade_user=eventmq_username, trade_password=eventmq_password,
                 taskid=None, mongo_ip=mongo_ip):
        super().__init__(code=code, frequence=frequence, strategy_id=strategy_id, risk_check_gap=risk_check_gap, portfolio=portfolio,
                         start=start, end=end, send_wx=send_wx,
                         data_host=eventmq_ip, data_port=eventmq_port, data_user=eventmq_username, data_password=eventmq_password,
                         trade_host=eventmq_ip, trade_port=eventmq_port, trade_user=eventmq_username, trade_password=eventmq_password,
                         taskid=taskid, mongo_ip=mongo_ip)

        self.code = code
        self.dtcode = {}
        self.send_wx = send_wx
        self.frequenceInt = 0
        '''
        self.subscriber = subscriber_topic(
            host='www.yutiansut.com',
            port='5678',
            exchange='QARealtimePro_FIX',
            durable=False,
            vhost='/',
            routing_key='*')

        '''


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


    def upcoming_data(self, new_bar):
        """upcoming_bar :

        Arguments:
            new_bar {json} -- [description]
        """
        self._market_data = pd.concat([self._old_data, new_bar])
        # QA.QA_util_log_info(self._market_data)

        if self.isupdate:
            self.update()
            self.isupdate = False

        self.update_account()
        # self.positions.on_price_change(float(new_bar['close']))
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
        print(time.strftime("%H:%M:%S", time.localtime()))
        while True:
            time_now_min = int(time.strftime("%M", time.localtime()))
            time_now_s = int(time.strftime("%S", time.localtime()))
            frequence_int = self.frequenceInt
            if (time_now_min%frequence_int == 0 and (time_now_s%30 == 0 or time_now_s%59 == 0)) :
                time.sleep(1) # 延迟1s 防止上边的if重复执行
                QA.QA_util_log_info('{} while开始....'.format(
                    str(datetime.datetime.now())))

                code_list = [huobi_SYMBOL.format(x) for x in self.code]
               
                # new_data = json.loads(str(body, encoding='utf-8'))
                lastPreMinTime = (datetime.datetime.now()).strftime(
                    "%Y-%m-%d %H:%M:%S")

                preLastPreMinTime = (datetime.datetime.now() + datetime.timedelta(minutes=-4 * frequence_int)).strftime(
                    "%Y-%m-%d %H:%M:%S")

                print(preLastPreMinTime, lastPreMinTime, self.frequence)
                self._new_data = QA.QA_fetch_cryptocurrency_min(code_list, preLastPreMinTime, lastPreMinTime, format='pd',
                                                              frequence=self.frequence)

                if (self._new_data.empty):
                    continue

                self._new_data = self._new_data.loc[:, ['code','open', 'high', 'low', 'close', 'volume','datetime']]
                #self._market_data.iloc[-1:]

                print('NEW DATA:', self._new_data)


                #整理成一个code的一个frequence为一个bar
                for idx, row in  self._new_data.iterrows():
                    #print("当前row:",row)
                    code = row['code']
                    self.latest_price[row['code']] = code

                    self.running_time = row['datetime']
                    #if self.dt != str(row['datetime'])[0:16]:
                    if code not in self.dtcode or self.dtcode[code] < str(row['datetime'])[0:16]:
                        # [0:16]是分钟线位数
                        print('update!!!!!!!!!!!!')
                        self.dtcode[code] = str(row['datetime'])[0:16]
                        self.isupdate = True


                        self.acc.on_price_change(code, row['close'])
                        bar = pd.DataFrame([row]).set_index(['datetime', 'code']).loc[:, ['open', 'high', 'low', 'close', 'volume']]
                        #print(bar)
                        self.upcoming_data(bar)

    def format_stock_data(self, item):
        code = item.get('code')
        new_code = code[-6:]
        d = {}
        #print(new_code)
        #print(item)
        import pandas as pd
        stock_pool_pd = pd.read_csv("/root/sim/stock_strategy/stock_pool.csv",encoding='utf-8',converters = {'code':str});
        stock_pool_list = stock_pool_pd['code'].tolist()
        if new_code in stock_pool_list and item.get('frequence') == '5min':
            d['code'] = new_code
            d['open'] = item.get('open')
            d['high'] = item.get('high')
            d['close'] = item.get('close')
            d['low'] = item.get('low')
            d['vol'] = item.get('volume')
            d['type'] = item.get('frequence')
            #d['amount'] = item.get('volume')
            d['date_stamp'] = QA_util_date_stamp(item.get('datetime'))
            d['time_stamp'] = QA_util_time_stamp(item.get('datetime'))
            d['date'] = item.get('datetime')[0:10] #2020-10-12
            d['datetime'] = item.get('datetime') #2020-10-12 10:02:00
            d['tradetime'] = item.get('datetime')[0:16]  #2020-10-12 10:02

        return d

    def _debug_sim(self):
        self.running_mode = 'sim'
        

        QA_SU_save_huobi_min(frequency= self.frequence, fetch_range= self.code)

        frequence_int = int(re.findall("\d+", self.frequence)[0])
        self.frequenceInt = frequence_int
        start = (datetime.datetime.now() + datetime.timedelta(minutes=-500 * frequence_int)).strftime(
            "%Y-%m-%d %H:%M:%S")
        end = (datetime.datetime.now() + datetime.timedelta(minutes=-frequence_int)).strftime(
            "%Y-%m-%d %H:%M:%S")
        code_list =  [huobi_SYMBOL.format(x) for x in self.code]
        self._old_data = QA.QA_fetch_cryptocurrency_min(code_list, start, end, format='pd', frequence=self.frequence).set_index(['datetime', 'code'])

        #print(self._old_data)
        self._old_data = self._old_data.loc[:, ['open', 'high', 'low', 'close', 'volume']]

        self.database = pymongo.MongoClient(mongo_ip).QAREALTIME

        self.client = self.database.account
        self.subscriber_client = self.database.subscribe
        self.acc = QIFI_Account(
            username=self.strategy_id, password=self.strategy_id, trade_host=mongo_ip)
        self.acc.initial()
        self.pub = publisher_routing(exchange='QAORDER_ROUTER', host=self.trade_host,
                                     port=self.trade_port, user=self.trade_user, password=self.trade_password)
        '''
        self.subscribe_data(self.code, self.frequence, self.data_host,
                            self.data_port, self.data_user, self.data_password)
        '''


        self.database.strategy_schedule.job_control.update(
            {'strategy_id': self.strategy_id},
            {'strategy_id': self.strategy_id, 'taskid': self.taskid,
             'filepath': os.path.abspath(__file__), 'status': 200}, upsert=True)
        
        # threading.Thread(target=, daemon=True).start()
        #print(self.subscribe_data)
        #self.sub.start()
        #self.subscriber.start()
        self.callback(1,2,3,4)

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
        #self.positions = self.acc.get_position(self.code)


        data = QA.QA_quotation(self.code, self.start, self.end, source=QA.DATASOURCE.MONGO,
                               frequence=self.frequence, market=self.market_type, output=QA.OUTPUT_FORMAT.DATASTRUCT)
        #print(data.data)
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
            #self.positions = self.acc.get_position(self.code)
            self.positions = self.acc.positions

    def send_order(self,  direction='BUY', offset='OPEN', code=None, price=3925, volume=10, order_id='',):

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

            if self.check_order(direction, offset, code):
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

                            requests.post('http://www.yutiansut.com/signal?user_id={}&template={}&strategy_id={}&realaccount={}&code={}&order_direction={}&order_offset={}&price={}&volume={}&order_time={}'.format(
                                user, "xiadan_report", self.strategy_id, self.acc.user_id, code, direction, offset, price, volume, now))
                        except Exception as e:
                            QA.QA_util_log_info(e)

            else:
                QA.QA_util_log_info('failed in ORDER_CHECK')

        elif self.running_mode == 'backtest':

            self.bar_order['{}_{}'.format(direction, offset)] = self.bar_id

            self.acc.receive_simpledeal(
                code=code, trade_time=self.running_time, trade_towards=towards, trade_amount=volume, trade_price=price, order_id=order_id)
            #self.positions = self.acc.get_position(self.code)


if __name__ == '__main__':
    QAStrategyStockBase(code=['000001', '000002']).run_sim()
