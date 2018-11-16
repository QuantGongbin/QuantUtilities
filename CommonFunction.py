# -*- coding: utf-8 -*-
"""
Created on Sat Apr  1 13:55:55 2017

@author: user
"""

'''
函数接口输入尽量使用本地的数据输入
对于下载量较小，但可能经常调用的数据，尽量采用不限流量的接口
数据方面，尽量兼容wind和RQData的数据形式
'''
#倒入 rqdatac
import pandas as pd 
import datetime
import numpy as np 
from dateutil.parser import parse
import platform


##获取文件地址

sys_info = platform.system()
if 'Windows' in sys_info:
    trade_day_path = r'H:\QuantData\BaseData\TradeDay.pkl'
elif 'Darwin' in sys_info:
    trade_day_path = r'/Volumes/Quant/QuantData/BaseData/TradeDay.pkl'



#rqdatac.init("slf","8slf8") 
#%%交易日期处理函数，基于本地数据库
def GetTradeDates(StartDate,EndDate):
    '''
    获取区间的所有交易日
    Parameters
    ----------
        StartDate: str, 如 '20100101'
        EndDate  :str, 如 '20110101'
    return
    ---------
         y : list, 交易日日期
    '''
    tradedates = pd.read_pickle(trade_day_path).tolist()
    y = [x for x in tradedates if x>=StartDate and x<=EndDate]
    return y 

def GetTradeWeeks(StartDate,EndDate):
    '''
    获取区间的所有交易周
    '''
    tradeweeks = pd.read_pickle(r"E:\DataBase\WindApiData\TradeDates\TradeWeek.pkl").tolist()
    y = [x for x in tradeweeks if x>=StartDate and x<=EndDate]
    return y 

def GetTradeMonthes(StartDate,EndDate):
    '''
    获取区间的所有交易月份
    '''
    trademonthes = pd.read_pickle(r"E:\DataBase\WindApiData\TradeDates\TradeMonth.pkl").tolist()
    y = [x for x in trademonthes if x>=StartDate and x<=EndDate]
    return y 


def GetPreTradeDate(date,preday=1):
    '''
    获取date上nextday天的交易日
    Parameters
    ----------
        date: str, 如 '20100101'
    return
    ---------
        PreTradeDate : str, 上个交易日日期
    '''
    TradeDates = pd.read_pickle(trade_day_path).tolist()
    if date is None:
        PreTradeDate = np.nan
    elif date < TradeDates[0]:
        PreTradeDate = np.nan
    else:
        PreTradeDate = [x for x in TradeDates if x<date][-preday] 
    return PreTradeDate


def GetNextTradeDate(date,nextday=1):
    '''
    获取date下nextday天的交易日
    Parameters
    ----------
        date: str, 如 '20100101'
    return
    ---------
         y : str, 上个交易日日期
    '''
    TradeDates = pd.read_pickle(trade_day_path).tolist()
    NextTradeDate = [x for x in TradeDates if x>date][nextday-1]
    return NextTradeDate



#%% 股票代码转换函数
def StockCodeTransform(x):
    '''
    将Wind证券代码转为优矿(米矿)证券代码，或者反向
    
    Parameters
    ----------
        x:str or list
    return
    ---------
        y :str or list 
        
    '''
    #XSHG表示上海证券交易所，XSHE表示深圳证券交易所
    #SH表示上海证券交易所，SZ表示深圳证券交易所
    transform={'SZ':'XSHE',
               'SH':'XSHG',
               'XSHG':'SH',
               'XSHE':'SZ'}   
    if isinstance(x,str):
        y = x.split('.')[0]+'.'+transform[x.split('.')[-1]]
        return y 
    elif isinstance(x,list):
        y = [code.split('.')[0]+'.'+transform[code.split('.')[-1]] for code in x]
        return y
    else:
        raise ValueError("x must be str or list")
        
def TickerToWindID(ticker):
    '''
    给定股票的ticker如 000001或者1，转换成wind的代码
    '''
    add_wind_id = lambda x : x+".SZ" if x<"400000" else x+".SH"
    add_zero = lambda x: "0"*(6-len(x))+x 
    if isinstance(ticker,str):
        ticker = add_zero(ticker) 
        y = add_wind_id(ticker)
    elif isinstance(ticker,list):
        y = [add_wind_id(add_zero(code)) for code in ticker]
    else:
        raise ValueError("x must be str or list")
    return y 
        