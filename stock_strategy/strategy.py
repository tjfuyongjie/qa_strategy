import QUANTAXIS as QA
from qastockbase import QAStrategyStockBase
import pprint
import time
import datetime
import pandas as pd
import copy


class DMI(QAStrategyStockBase):

    def user_init(self):
        print("我是用户初始化。。。。" + str(self.code))

    def on_dailyopen(self):
        print("每日开盘运行。。。。" + str(self.running_time)[0:10])
        pass

    def on_dailyclose(self):
        print("每日收盘运行。。。。")
        pass

    def on_30min_bar(self, code, newbar):
        # dayData = self.getData(self.code, '30m')
        datadf = self.formatData(newbar, 30)
        lasttime = pd.to_datetime(datadf.index.values[-1])
        # 跳过
        if str(lasttime) != str(self.running_time):
            return {}

        # 开始策略
        dimRst = self.dimBuyOrSell(datadf)
        # print(dimRst)
        # return
        wrRst = self.wrBuyOrSell(datadf)

        if (wrRst == -1 and dimRst == -1):
            print('buy')
            self.sendWx(code, 'buy', 5)
        elif (wrRst == 1 and dimRst == 1):
            print('sell')
            self.sendWx(code, 'sell', 5)
        else:
            print('30m:keep')
        return {}

    def on_15min_bar(self, code, newbar):

        datadf = self.formatData(newbar, 15)
        lasttime = pd.to_datetime(datadf.index.values[-1])
        # 跳过
        if str(lasttime) != str(self.running_time):
            return {}

        # print(datadf)
        # 开始策略
        dimRst = self.dimBuyOrSell(datadf)
        # print(dimRst)
        # return
        wrRst = self.wrBuyOrSell(datadf)

        if (wrRst == -1 and dimRst == -1):
            print('buy')
            self.sendWx(code, 'buy', 3)
        elif (wrRst == 1 and dimRst == 1):
            print('sell')
            self.sendWx(code, 'sell', 3)
        else:
            print('15m:keep')
        return {}

    def on_5min_bar(self, code, newbar):
        datadf = self.formatData(newbar, 5)
        lasttime = pd.to_datetime(datadf.index.values[-1])
        # 跳过
        if str(lasttime) != str(self.running_time):
            return {}

        # print(datadf)
        # 开始策略
        macdRst = self.macdBuyOrSell(datadf)
        dimRst = self.dimBuyOrSell(datadf)
        wrRst = self.wrBuyOrSell(datadf)

        if (wrRst == -1 and dimRst == -1 and macdRst == -1):
            print('buy')
            self.sendWx(code, 'buy', 2)
        elif (wrRst == 1 and dimRst == 1 and macdRst == 1):
            print('sell')
            self.sendWx(code, 'sell', 2)
        else:
            print('5m:keep')
        return {}

    # 一个stock code 一个bar
    def on_bar(self, bar):
        if (self.running_mode == 'backtest'):
            code = bar.name[1]
        elif (self.running_mode == 'sim'):
            code = bar._stat_axis.values[0][1]

        newbar = self.get_code_marketdata(code)
        print('newbar.......', code, newbar.tail(20))
        # print(newbar.index)
        rst30minDict = self.on_30min_bar(code, newbar)
        rst15minDict = self.on_15min_bar(code, newbar)
        rst5minDict = self.on_5min_bar(code, newbar)
        try:
            # 当前stock code
            # code = bar.name[1]
            a = 1
        except Exception as e:
            print('异常：', newbar, e)
        # dmiDay = dayData.add_func(QA.QA_indicator_DMI,12,6)
        # print(dmiDay)
        # print(self.running_time)

        return
        res = self.dmi()
        # print('~~~~~~~~~~~~~~~~~~~~~~')
        # print(res.iloc[-1])
        # print('---------xxxxxxxxxxxx---------')
        # print(self.market_data)听听歌c v h vv
        '''
        if res.MA2[-1] > res.MA5[-1]:

            print('LONG')

            if self.positions.volume_long == 0:
                self.send_order('BUY', 'OPEN', price=bar['close'], volume=1)

            if self.positions.volume_short > 0:
                self.send_order('BUY', 'CLOSE', price=bar['close'], volume=1)

        else:
            print('SHORT')
            if self.positions.volume_short == 0:
                self.send_order('SELL', 'OPEN', price=bar['close'], volume=1)
            if self.positions.volume_long > 0:
                self.send_order('SELL', 'CLOSE', price=bar['close'], volume=1)
        '''

    def sendOrder():
        return

    def wrBuyOrSell(self, allData):
        allData['high'] = allData['high'] + 0.000000001
        wrDay = QA.QA_indicator_WR(allData, 10, 6)
        """try :
            wrDay = QA.QA_indicator_WR(allData,10,6)
        except:
            print('wr异常', allData)"""
        # wrDay = dayData.add_func(QA.QA_indicator_WR,10,6)
        # print('-------')
        #print(wrDay.tail(20))
        # print('uuuuuuu')
        # if (wrDay.WR1[-2:])
        # print(wrDay['WR1'].values[-1])
        wr2 = wrDay['WR2'].values
        wr1 = wrDay['WR1'].values
        if (len(wrDay['WR2'].values) < 2):
            return 0
        if (wrDay['WR2'].values[-1] >= 10 and wrDay['WR2'].values[-2] < 10 \
                and self.almostEquel(wrDay['WR1'].values[-2], wrDay['WR2'].values[-2]) \
                ):
            return 1
        elif (wrDay['WR2'].values[-1] <= 88 and wrDay['WR2'].values[-2] > 90 \
              and self.almostEquel(wrDay['WR1'].values[-2], wrDay['WR2'].values[-2]) \
                ):
            return -1
        elif (self.yetEquel(wr1[-1],wr2[-1]) and  self.yetEquel(wr1[-2],wr2[-2])  \
                and  wr2[-2]<15 and wr2[-1]>15 \
                ):
            return 1
        elif (self.yetEquel(wr1[-1],wr2[-1]) and  self.yetEquel(wr1[-2],wr2[-2])  \
                and wr2[-2]>85 and wr2[-1]<85 \
                ):
            return -1
        else:
            return 0

    def dimBuyOrSell(self, dayData):
        f = QA.QA_indicator_DMI(dayData, 14, 6)
        # print(f)
        pdi = f['DI1'].values
        mdi = f['DI2'].values
        adx = f['ADX'].values
        adxr = f['ADXR'].values
        if (len(pdi) < 2):
            return 0
        hold = 20
        if (adx[-1] >= adx[-2] and adx[-2] >= adx[-3] and adx[-2] > hold \
                and pdi[-1] > mdi[-1] and pdi[-1] < adx[-1] and pdi[-1] < adxr[-1] \
                and mdi[-1] < adx[-1] and mdi[-1] < adxr[-1]

        ):
            return 1
        elif (adx[-1] >= adx[-2] and adx[-2] > hold \
              and mdi[-1] > pdi[-1] and mdi[-1] < adx[-1] and mdi[-1] < adxr[-1] \
              and pdi[-1] < adx[-1] and pdi[-1] < adxr[-1]

        ):
            return -1
        else:
            return 0

    def macdBuyOrSell(self, dayData):
        f = QA.QA_indicator_MACD(dayData, 12, 26, 9)
        # print(f)
        dif = f['DIF'].values
        dea = f['DEA'].values
        macd = f['MACD'].values
        if (len(dif) < 1):
            return 0
        if (macd[-1] < 0 and dea[-1] < macd[-1] and dea[-2] < macd[-2]):
            return -1
        elif (macd[-1] > 0 and dea[-1] > macd[-1] and dea[-2] > macd[-2]):
            return 1
        else:
            return 0
        return 0

    def almostEquel(self, d1, d2):
        if (d1 - d2 < 4):
            return 1
    def yetEquel(self, d1, d2):
        if (d1 - d2 < 0.01 or d1 - d2 > 0.01):
            return 1
    def dmi(self):
        return QA.QA_indicator_DMI(self.market_data, 14, 6)

    def risk_check(self):
        pass
        # pprint.pprint(self.qifiacc.message)

    def sendWx(self, code, target, position):
        import requests
        ordertime = str(self.running_time)
        requests.post(
            "http://www.yutiansut.com/signal?user_id=oL-C4w2cSApfgeB6Uy9028RomZp4&template=xiadan_report&strategy_id=test1&realaccount=133496&code=" + str(
                code) + "&order_direction=" + target + "&order_offset=OPEN&price=xxx&volume=" + str(
                position) + "成仓&order_time=" + ordertime)

    def getData(self, stock, frequence):
        end = self.running_time  # str(datetime.datetime.now())
        if (frequence == "day"):
            start = QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())), 30)
            data = QA.QA_fetch_stock_day_adv(stock, start=start, end=end).to_qfq()
        elif (frequence == "60m"):
            start = QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())), 8)
            data = QA.QA_fetch_stock_min(stock, start=start, end=end, format='pd', frequence=frequence)
        elif (frequence == "30m"):
            start = QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())), 4)
            data = QA.QA_fetch_stock_min(stock, start=start, end=end, format='pd', frequence=frequence)
        elif (frequence == "15m"):
            start = QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())), 2)
            data = QA.QA_fetch_stock_min(stock, start=start, end=end, format='pd', frequence=frequence)
        return data
        # print(self.market_data.to_qfq().add_func(QA.QA_indicator_DMI,12,6))
        # .add_func(QA.QA_indicator_DMI,12,6)

    # frequenceInt 5,15,30...
    def formatData(self, mydata, frequenceInt):
        # 获取复合索引里的某列值
        code = mydata._stat_axis.values[0][1]
        # 将某个索引变为列，并给列名
        # mydata = pd.DataFrame(mydata).rename_axis(['code'], axis=1)
        # print(pd1['code'])

        # 将复合索引的第二个变为列
        mydata = mydata.reset_index(1)
        # mydata.index = mydata.index.swaplevel()
        # mydata.index = mydata.index.droplevel(1)

        # return
        period = str(frequenceInt) + "T"
        frequence = str(frequenceInt) + "min"
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            # 'amount': 'sum'

        }
        period = str(frequenceInt) + "T"
        data = mydata.resample(period, closed='right', label='right').apply(ohlc_dict)
        data['code'] = code
        data['type'] = frequence
        return data.dropna()


if __name__ == '__main__':
    # QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())),), str(datetime.datetime.now()),
    print(datetime.datetime.now())
    start = QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())), 10)
    end = str(datetime.datetime.now())

    stock_pool_pd = pd.read_csv("/root/sim/stock_strategy/stock_pool.csv", encoding='utf-8', converters={'code': str});
    stock_pool_list = stock_pool_pd['code'].tolist()
    #stock_pool_list=['000338','000545']
    #DMI = DMI(code=stock_pool_list, frequence='5min',strategy_id='x', start=start, end=end)
    #DMI.run_backtest()
    #stock_pool_list = ['etcusdt']
    DMI = DMI(code=stock_pool_list, frequence='5min', strategy_id='stock_sim',start=start, end=end, send_wx=True)
    DMI.debug_sim()
    DMI.add_subscriber(qaproid="oL-C4w2cSApfgeB6Uy9028RomZp4")