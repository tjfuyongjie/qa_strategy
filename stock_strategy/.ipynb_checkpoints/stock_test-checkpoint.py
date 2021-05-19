import QUANTAXIS as QA
import pprint
import time
import datetime
import pandas as pd
import copy
# 第三方函数库
import numpy as np
import urllib.parse
from QUANTAXIS.QASU.save_huobi import QA_SU_save_huobi_min,huobi_SYMBOL
from zhibiao import *
import requests
import re
from qastockbase import QAStrategyStockBase


class DMI(QAStrategyStockBase):
    
    
    def user_init(self):
        print("我是用户初始化。。。。" + str(self.code))

    def on_dailyopen(self):
        print("每日开盘运行。。。。" + str(self.running_time)[0:10])
        pass

    def on_dailyclose(self):
        print("每日收盘运行。。。。")
        pass

    def on_min_bar(self, code, newbar, frequenceInt):
        # dayData = self.getData(self.code, '30m')
        datadf = self.formatData(newbar, frequenceInt)
        lasttime = pd.to_datetime(datadf.index.values[-1])
        # 跳过
        if str(lasttime) != str(self.running_time):
            return {}

        # 开始策略
        dimRst = self.dimBuyOrSell(datadf)
        # print(dimRst)
        # return
        wrRst = self.wrBuyOrSell(datadf)
        macdRst = self.macdBuyOrSell(datadf)
        
        '''
        # rsi 高抛低吸
        rsibuyselldf = datadf.add_func(rsi.RSI)
        rsibuysell=rsibuyselldf.tail(1).values
        print("rsi", rsibuysell)
        return 
        '''
        volBig = self.volBig(datadf)
        if (frequenceInt > 1):
            if (wrRst == -1 and dimRst == -1 and macdRst == -1 and volBig == 1):
                print('buy')
                self.sendWx(code, 'buy-wr-dmi-macd', frequenceInt)
            elif (wrRst == 1 and dimRst == 1 and macdRst == 1 and volBig == 1):
                print('sell')
                self.sendWx(code, 'sell-wr-dmi-macd', frequenceInt)
            else:
                print(str(frequenceInt)+'m:keep')
            
           
        
            
        isdev = self.check_deviating(datadf)
        if (isdev == -1 and volBig == 1):
            self.sendWx(code, 'buy-macd', frequenceInt)
        elif (isdev == 1 and volBig == 1):
            self.sendWx(code, 'sellmacd', frequenceInt)
        else:
            print("nomal")
            
        bollRst = self.bollBuyOrSell(datadf)
        if (bollRst == -1):
            self.sendWx(code, 'buy-boll', frequenceInt)
        elif (bollRst == 1):
            self.sendWx(code, 'sell-boll', frequenceInt)
        else:
            print(str(frequenceInt)+'m:keep')
        
        return {}

    

    # 一个stock code 一个bar
    def on_bar(self, bar):
        if (self.running_mode == 'backtest'):
            code = bar.name[1]
        elif (self.running_mode == 'sim'):
            code = bar._stat_axis.values[0][1]

        allBarDf = self.get_code_marketdata(code)
        print('newbar.......', code, allBarDf.tail(20))
        datadf = self.formatData(allBarDf, 30)
        print(datadf.tail(20))
        # print(newbar.index)
        #self.on_min_bar(code, allBarDf,1)
        
        self.on_min_bar(code, allBarDf,5)
        
        self.on_min_bar(code, allBarDf,15)
        
        self.on_min_bar(code, allBarDf,30)

        self.on_min_bar(code, allBarDf,60)
        
        #self.on_min_bar(code, allBarDf,240)

        try:
            # 当前stock code
            # code = bar.name[1]
            a = 1
        except Exception as e:
            print('异常：', allBarDf, e)
        # dmiDay = dayData.add_func(QA.QA_indicator_DMI,12,6)
        # print(dmiDay)
        # print(self.running_time)

        return
        res = self.dmi()
        # print('~~~~~~~~~~~~~~~~~~~~~~')
        # print(res.iloc[-1])
        # print('---------xxxxxxxxxxxx---------')
        # print(self.market_data)
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

   
    def volBig(self, dayData):
        return 1
        volData = QA.QA_indicator_MA_VOL(dayData, 10)
        vol10 = volData['MA_VOL10'].values
        vol = dayData['volume'].values
       
        if ((vol[-1] > 1.5 * vol10[-2]) or \
            (vol[-2] > 1.5 * vol10[-3]) or \
            (vol[-3] > 1.5 * vol10[-4]) ):
            return 1
        else:
            return 0
  
    
    
    def bollBuyOrSell(self, dayData):
        bollData = QA.QA_indicator_BOLL(dayData)
        bollup = bollData['UB'].values
        bolllb = bollData['LB'].values
        boll = bollData['BOLL'].values
        closepc = dayData['close'].values
        openpc = dayData['open'].values
        lowpc = dayData['low'].values
        highpc = dayData['high'].values
        volData = QA.QA_indicator_MA_VOL(dayData, 10)
        vol10 = volData['MA_VOL10'].values
        vol = dayData['volume'].values
        # -1: buy
        if (closepc[-1] >= bollup[-1] and openpc[-1] <= bollup[-1] and lowpc[-2]<boll[-2]  and vol[-1] > 3 * vol10[-2]):
            return -1
        if (closepc[-1] < bolllb[-1] and vol[-1] > 3 * vol10[-2] and vol[-1]> 4*vol[-2]):
            return -1
        elif (closepc[-1] <= bolllb[-1] and highpc[-1] >= boll[-1] and vol[-1] > 3 * vol10[-2]):
            return 1
        else:
            return 0

    def wrBuyOrSell(self, allData):
        allData['high'] = allData['high'] + 0.000000001
        wrDay = QA.QA_indicator_WR(allData, 10, 6)
        """try :
            wrDay = QA.QA_indicator_WR(allData,10,6)
        except:
            print('wr异常', allData)"""
        # wrDay = dayData.add_func(QA.QA_indicator_WR,10,6)
        # print('-------')
        # print(wrDay.tail(20))
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
        elif (self.yetEquel(wr1[-1], wr2[-1]) and self.yetEquel(wr1[-2], wr2[-2]) \
              and wr2[-2] < 15 and wr2[-1] > 15 \
                ):
            return 1
        elif (self.yetEquel(wr1[-1], wr2[-1]) and self.yetEquel(wr1[-2], wr2[-2]) \
              and wr2[-2] > 85 and wr2[-1] < 85 \
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

    '''
    https://www.joinquant.com/view/community/detail/78026357495c3f8acbb29ce0a73a4677?type=1
    '''

    def check_deviating(self, barsdf, fastperiod=11, slowperiod=26, signalperiod=9):
        #
        # 日线级别，计算昨天收盘是否发生顶或底背离，利用快慢线金、死叉判断
        # scode，证券代码
        # fastperiod，fastperiod，signalperiod：MACD参数，默认为11,26,9
        # 返回 dev_type, 0：没有背离，1：发生顶背离，-1：发生底背离

        # rows = (fastperiod + slowperiod + signalperiod) * 5
        # close = attribute_history(security=scode, count=rows, unit='1d', fields=['close']).dropna()
        close = barsdf.loc[:, 'close'].values
        grid = QA.QA_indicator_MACD(barsdf, fastperiod, slowperiod, signalperiod)
        # print(close)

        dif = grid['DIF'].values
        dea = grid['DEA'].values
        macd = grid['MACD'].values
        # dif, dea, macd = talib.MACD(close.values, fastperiod, slowperiod, signalperiod)
        dev_type = 0

        if macd[-1] > 0 > macd[-2]:
            # 底背离
            # 昨天金叉
            # idx_gold: 各次金叉出现的位置
            idx_gold = np.where((macd[:-1] < 0) & (macd[1:] > 0))[0] + 1  # type: np.ndarray
            print('idx---->',idx_gold)
            print(close)
            if len(idx_gold) > 1:
                if close[idx_gold[-1]] < close[idx_gold[-2]] and dif[idx_gold[-1]] > dif[idx_gold[-2]]:
                    dev_type = -1

        elif macd[-1] < 0 < macd[-2]:
            # 顶背离
            # 昨天死叉
            # idx_dead: 各次死叉出现的位置
            idx_dead = np.where((macd[:-1] > 0) & (macd[1:] < 0))[0] + 1  # type: np.ndarray
            print('idx---->', idx_dead)
            print(close)
            if len(idx_dead) > 1:
                if close[idx_dead[-1]] > close[idx_dead[-2]] and dif[idx_dead[-1]] < dif[idx_dead[-2]]:
                    dev_type = 1
        else:
            # 不发生背离
            dev_type = 0

        return dev_type

    def macdBL():
        fast = 12
        slow = 26
        sign = 9
        rows = (fast + slow + sign) * 5
        suit = {'dif': 0, 'dea': 0, 'macd': 0, 'gold': False, 'dead': False}
        grid = attribute_history(stock, rows, '1d', fields=['close'])

        # print(f)
        dif = f['DIF'].values
        dea = f['DEA'].values
        macd = f['MACD'].values

        try:
            grid = QA.QA_indicator_MACD(dayData, 12, 26, 9)
            grid = grid.dropna()
            # 底背离----------------------------------------------------------------
            mask = grid['MACD'] > 0
            mask = mask[mask == True][mask.shift(1) == False]

            # key3 = mask.keys()[-3]
            key2 = mask.keys()[-2]
            key1 = mask.keys()[-1]
            if grid.vol[key2] > grid.vol[key1] and \
                    grid.dif[key2] < grid.dif[key1] < 0 and \
                    grid.macd[-2] < 0 < grid.macd[-1]:
                returnStock.append(stock);
        except:
            pass

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
       
        ordertime = str(self.running_time)
        start = datetime.datetime.now().strftime("%H:%M:%S")
        target = target+"   时间:"+start
       
        ordertimeInt = datetime.datetime.strptime(ordertime, '%Y-%m-%d %H:%M:%S')
        now = datetime.datetime.now()
        delay = int((now - ordertimeInt).total_seconds()-self.frequenceInt*60)
        dayDataNew = self.get_code_marketdata(code).tail(20)
        
       
        requests.post(
            "http://www.yutiansut.com/signal?user_id=oL-C4w2cSApfgeB6Uy9028RomZp4&template=xiadan_report&strategy_id=test1&realaccount=133496&code=" + str(
                code) + "&order_direction=" + target + "&order_offset=OPEN&price=xxx&volume=" + str(
                position) + "级别&order_time=" + ordertime)
        
        
        
       
       
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
        frequenceBaseInt =  int(re.findall("\d+", self.frequence)[0])
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
        frequence = str(frequenceInt) + "min"
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            # 'amount': 'sum'

        }
        #periodInt = frequenceInt/frequenceBaseInt
        period = str(frequenceInt) + "T"
        data = mydata.resample(period, closed='right', label='right').apply(ohlc_dict)
        data['code'] = code
        data['type'] = frequence
        return data.dropna()


if __name__ == '__main__':
    
    # QA.QA_util_get_last_day(QA.QA_util_get_real_date(str(datetime.date.today())),), str(datetime.datetime.now()),
    print(datetime.datetime.now())
    frequence = '5min'
    frequence_int = int(re.findall("\d+", frequence)[0])
    start = (datetime.datetime.now() + datetime.timedelta(minutes=-50 * frequence_int)).strftime("%Y-%m-%d %H:%M:%S")
    end = (datetime.datetime.now() + datetime.timedelta(minutes=-frequence_int)).strftime(
        "%Y-%m-%d %H:%M:%S")
    stock_pool_pd = pd.read_csv("/root/qa_strategy/stock_strategy/stock_pool.csv", encoding='utf-8', converters={'code': str});
    stock_pool_list = stock_pool_pd['code'].tolist()
    # print(stock_pool_list)
    print(start, end)
    # stock_pool_list=['000338','000545']
    
    DMI = DMI(code=stock_pool_list, frequence=frequence, strategy_id='stock_sim', send_wx=True)
    #DMI.send_order(self,  'BUY', offset='OPEN', code='1', price=100, volume=100, order_id='',)
    DMI.debug_sim()
    DMI.add_subscriber(qaproid="oL-C4w2cSApfgeB6Uy9028RomZp4")