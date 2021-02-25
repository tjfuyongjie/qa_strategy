#!/usr/local/bin/python
# -*- coding:UTF-8 -*-
# coding :utf-8
#
# The MIT License (MIT)
#
# Copyright (c) 2016-2020 yutiansut/QUANTAXIS
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
import datetime
import time

"""对应于save all"""
try:
    from QUANTAXIS.QASU.main import (QA_SU_save_etf_day, QA_SU_save_etf_min,
                                     QA_SU_save_financialfiles,
                                     QA_SU_save_index_day, QA_SU_save_index_min,
                                     QA_SU_save_stock_block, QA_SU_save_stock_day,
                                     QA_SU_save_stock_info,
                                     QA_SU_save_stock_info_tushare,
                                     QA_SU_save_stock_list, QA_SU_save_stock_min,
                                     QA_SU_save_stock_xdxr)
    from QUANTAXIS.QASU.save_binance import (QA_SU_save_binance_symbol,
                                             QA_SU_save_binance_1hour,
                                             QA_SU_save_binance_1day,
                                             QA_SU_save_binance_1min,
                                             QA_SU_save_binance)
    from QUANTAXIS.QASU.save_bitmex import (QA_SU_save_bitmex_symbol,
                                            QA_SU_save_bitmex_day)
    from QUANTAXIS.QASU.save_huobi import (QA_SU_save_huobi_symbol,
                                           QA_SU_save_huobi_1hour,
                                           QA_SU_save_huobi_1day,
                                           QA_SU_save_huobi_1min,
                                           QA_SU_save_huobi)

    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_day' + '\n')
    QA_SU_save_stock_day('tdx')
    sfile.write(nowtime + ': ' + 'save_stock_day finished' + '\n')
    sfile.close()

    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_xdxr' + '\n')
    QA_SU_save_stock_xdxr('tdx')
    sfile.write(nowtime + ': ' + 'save_stock_xdxr finished' + '\n')
    sfile.close()


    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_index_day' + '\n')
    QA_SU_save_index_day('tdx')
    sfile.write(nowtime + ': ' + 'save_index_day finished' + '\n')
    sfile.close()



    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_etf_day' + '\n')
    QA_SU_save_etf_day('tdx')
    sfile.write(nowtime + ': ' + 'save_etf_day finished' + '\n')
    sfile.close()



    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_list' + '\n')
    QA_SU_save_stock_list('tdx')
    sfile.write(nowtime + ': ' + 'save_stock_list finished' + '\n')
    sfile.close()

    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_block' + '\n')
    QA_SU_save_stock_block('tdx')
    sfile.write(nowtime + ': ' + 'save_stock_block finished' + '\n')
    sfile.close()

    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_info' + '\n')
    QA_SU_save_stock_info('tdx')
    sfile.close()
    '''
    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_min' + '\n')
    QA_SU_save_stock_min('tdx')
    sfile.write(nowtime + ': ' + 'save_stock_min finished' + '\n')
    sfile.close()
    
    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_index_min' + '\n')
    QA_SU_save_index_min('tdx')
    sfile.write(nowtime + ': ' + 'save_index_min finished' + '\n')
    sfile.close()
    
    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_etf_min' + '\n')
    QA_SU_save_etf_min('tdx')
    sfile.write(nowtime + ': ' + 'save_etf_min finished' + '\n')
    sfile.close()
    '''
    sfile = open('/root/datalog.txt', 'a+')
    nowtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    sfile.write(nowtime + ': ' + 'save_stock_info finished' + '\n')
    sfile.close()


except Exception as err:
    file = open('/root/log.txt', 'w')
    file.write(str(err))
    file.close()
