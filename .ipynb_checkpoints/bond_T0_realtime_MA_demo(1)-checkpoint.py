import QUANTAXIS as QA
from QAStrategy.qastockbase import QAStrategyStockBase
from QAPUBSUB.consumer import subscriber, subscriber_topic, subscriber_routing
from QAPUBSUB.producer import publisher_routing
import json
import pandas as pd
import io
import sys
import os
import pymongo
from qaenv import (eventmq_ip, eventmq_password, eventmq_port,
                   eventmq_username, mongo_ip)
from QIFIAccount import QIFI_Account
import datetime

import requests
import uuid


class strategy(QAStrategyStockBase):
    def on_bar(self, data):
        code = data.iloc[-1].name[1]
        res = self.ma_cross(self.get_code_marketdata(code))
        print(res)
        print(self.market_data.sort_index())
        if res.cross_s.iloc[-2] == 1:
            print("均线指标上穿！！！！！")
            if self.get_positions(code).volume_long == 0:
                print('买入股票 {}'.format(code))
                self.send_order('BUY', 'OPEN', code,
                                price=self.latest_price[code], volume=100)
            else:
                print('已经持有股票 {}'.format(code))

        if res.cross_l.iloc[-2] == 1:
            print('均线指标下穿！！！！！')
            if self.get_positions(code).volume_long > 0:
                print('卖出股票 {}'.format(code))
                self.send_order('SELL', 'CLOSE', code,
                                price=self.latest_price[code], volume=100)
            else:
                print('没有持有股票 {}'.format(code))
        # print('---------------under is on_bar data --------------')
        # print('data:')
        # print(data)
        # # print('data.index:')
        # # print(data.index)
        # print('---------------under is 当前持仓的get_positions --------------')
        # for code in self.code:

        #     print(self.get_positions(code))

        # print('---------------under is 当前self.market_data --------------')

        # print(self.market_data.tail(20))
        # # print('data.names:')
        # # print(data.names)
        # code = data.iloc[-1].name[1]  # 此处无法通过data.name获取代码需要再思考。
        # # code = data.index[-1][1]

        # print('---------------under is 当前全市场的market_data  get_current_marketdata()--------------')

        # print(self.get_current_marketdata())
        # print('---------------under is 当前品种的market_data get_code_marketdata(code) --------------')
        # print(self.get_code_marketdata(code).tail(20))
        print('---------------under is on_bar data.name[1] --------------')
        print(code)
        print(self.running_time)
        # input()

    def ma_cross(self, data):
        ma = QA.QA_indicator_MA(data, 5, 10)
        cross_s = QA.CROSS(ma.MA5, ma.MA10)
        cross_l = QA.CROSS(ma.MA10, ma.MA5)
        # ind=pd.concat([cross_s,cross_l],axis=1)
        # print(ind)
        return pd.DataFrame({'cross_s': cross_s,
                             'cross_l': cross_l})

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

        self.new_data = json.loads(str(body, encoding='utf-8'))
        # print('self.new_data:')
        # print(self.new_data)
        # print('self.latest_price:')
        # print(self.latest_price)

        self.latest_price[self.new_data[-1]
                          ['code']] = self.new_data[-1]['close']

        self.running_time = self.new_data[-1]['datetime']
        if self.dt != str(self.new_data[-1]['datetime'])[0:16]:
            # [0:16]是分钟线位数
            print('update!!!!!!!!!!!!')
            self.dt = str(self.new_data[-1]['datetime'])[0:16]
            self.isupdate = True

        self.acc.on_price_change(
            self.new_data[-1]['code'], self.new_data[-1]['close'])

        bar = pd.DataFrame(self.new_data)
        bar.datetime = pd.to_datetime(bar.datetime)
        bar = bar.set_index(['datetime', 'code']).loc[:, [
            'open', 'high', 'low', 'close', 'volume']]
        # print('bar:')
        # print(bar)
        self.upcoming_data(bar)

    def upcoming_data(self, new_bar):
        """upcoming_bar :

        Arguments:
            new_bar {json} -- [description]
        """

        _old_data = self.old_data.reset_index()

        # print(_old_data)
        _new_bar = new_bar.reset_index()

        # print(_new_bar)
        self._market_data = pd.concat([_old_data, _new_bar], sort=False).drop_duplicates(
            ['code', 'datetime'], keep='last').sort_values(['datetime', 'code']).reset_index(drop=True).set_index(['datetime', 'code'])

        # QA.QA_util_log_info(self._market_data)

        if self.isupdate:
            self.update()
            self.isupdate = False

        self.update_account()
        # self.positions.on_price_change(float(new_bar['close']))
        self.on_bar(new_bar)

    def send_order(self,  direction='BUY', offset='OPEN', code=None, price=3925, volume=10, order_id='',):

        towards = eval('ORDER_DIRECTION.{}_{}'.format(direction, offset))
        order_id = str(uuid.uuid4()) if order_id == '' else order_id
        # 此处屏蔽对股票对优化，使用BUY_OPEN  SELL_CLOSE 进行日内交易
        # if self.market_type == QA.MARKET_TYPE.STOCK_CN:
        #     """
        #     在此对于股票的部分做一些转换
        #     """
        #     if towards == ORDER_DIRECTION.SELL_CLOSE:
        #         towards = ORDER_DIRECTION.SELL
        #     elif towards == ORDER_DIRECTION.BUY_OPEN:
        #         towards = ORDER_DIRECTION.BUY

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

                            requests.post('http://www.yutiansut.com/signal?user_id={}&template={}&strategy_id={}&realaccount={}&code={}&order_direction={}&order_offset={}&price={}&volume={}&order_time={}'.format(
                                user, "xiadan_report", self.strategy_id, self.acc.user_id, code, direction, offset, price, volume, now))
                        except Exception as e:
                            QA.QA_util_log_info(e)

            else:
                QA.QA_util_log_info('failed in ORDER_CHECK')

    '''
    def _debug_sim(self):
        """
        希望对可转债进行测试，所以对_debug_sim进行重写
        希望修改获得历史数据，
        修改手续费标准，印花税标准等内容
        """
        self.running_mode = 'sim'

        # self._old_data = QA.QA_fetch_stock_min(self.code, QA.QA_util_get_last_day(
        #     QA.QA_util_get_real_date(str(datetime.date.today()))), str(datetime.datetime.now()), format='pd', frequence=self.frequence).set_index(['datetime', 'code'])
        data = data = [QA.QA_fetch_get_bond_min(
            'tdx', code, '2020-04-01', '2020-04-01') for code in self.code]
        data = pd.concat(i for i in data)
        data = data.loc[:, ['open', 'code', 'close', 'high', 'low', 'vol']].rename(
            columns={'vol': 'volume'}).reset_index().set_index(['datetime', 'code'])
        self._old_data = data

        # self._old_data = self._old_data.loc[:, [
        #     'open', 'high', 'low', 'close', 'volume']]

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
        self.sub.start()
        '''


if __name__ == '__main__':
    codes = ['128084', '128073']
    s = strategy(code=codes, frequence='1min',
                 start='2021-01-18', end='2021-01-22', strategy_id='x_bond_Ma_cross_test1', market_type='stock_cn')
    #s.market_type = 'future_cn'
    s.debug_sim()
