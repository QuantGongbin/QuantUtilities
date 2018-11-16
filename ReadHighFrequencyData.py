# -*- coding: utf-8 -*-
"""
Created on Mon Oct 23 13:57:52 2017

@author: user
"""

import h5py
import os 
import numpy as np 
import pandas as pd
import platform

sys_info = platform.system()

if 'Windows' in sys_info:
    trans_path = r'H:\QuantData\AShareHighFrequencyData\Transaction'
    tick_path = r'H:\QuantData\AShareHighFrequencyData\Tick'
    order_path = r'H:\QuantData\AShareHighFrequencyData\Order'

elif 'Mac' in sys_info:
    trans_path = r'H:\QuantData\AShareHighFrequencyData\Transaction'
    tick_path = r'H:\QuantData\AShareHighFrequencyData\Tick'
    order_path = r'H:\QuantData\AShareHighFrequencyData\Order'

def get_file_path(path, trade_date):
    '''
    本函数用于
    :param path: 原始路径
    :param trade_date: 交易日期
    :return: 生成的文件路径

    '''
    if 'Windows' in sys_info:
        file_path = path + "\\" + trade_date[0:4] + "\\" + trade_date
    elif 'Darwin' in sys_info:
        file_path = path + "/" + trade_date[0:4] + "/" + trade_date
    return file_path
    ##file_path = tick_path + "\\" + TradeDate[0:4] + "\\" + TradeDate


'''
高频数据存在的问题：
缺失，比如最新的数据列有数据，之前的数据列没有
重复，因此需要drop_duplicates()
'''
#%% 按照文件路径，获取股票数据
def ToBSFlag(numb):
    if numb==83:
        return "S"
    elif numb==66:
        return "B"
    else:
        return "0"
        
def ReadTransactionData(filepath):
    '''
    读入matlab文件的TransactionData
    '''
    f = h5py.File(filepath,"r")
    data = f["r1"]
    namelist = ['AskOrder','BSFlag','BidOrder','Date','FunctionCode','Index','OrderKind','Price','Time','TradeVolume']
    df = pd.DataFrame()
    for name in namelist:
        df[name] = data[name][:][0]
    df["WindCode"] = filepath.split(".")[0][-6:]+"."+filepath.split(".")[1][0:2]
    df = df[['WindCode','Date','Time','Index','FunctionCode','OrderKind','BSFlag','Price','TradeVolume','AskOrder','BidOrder']]
    #对读入代码的问题进行调整
    df["FunctionCode"] = df["FunctionCode"].map(lambda x:"C" if x==67 else "0")
    df["BSFlag"] = df["BSFlag"].map(ToBSFlag)
    return df.drop_duplicates()
  

def ReadTickData(filepath):
    '''
    读入matlab文件的TickData，按照3秒切片
    数据可能存在问题，不交易的空值(多一些),存在重复值，导致df和df2有差异！
    不一定是稳定的3秒切片，有些是没有切片的！
    '''
    f = h5py.File(filepath,"r")
    data = f["r1"]
    #正常高开低收等数据
    namelist1 = ["Date","Time","Price","Volume","Turover","MatchItems","TradeFlag","BSFlag","AccVolume","AccTurover","High","Low","Open","PreClose"]
    #买卖盘字段，namelist2包含十档，namelist3
    namelist2 = ["AskPrice","AskVolume","BidPrice","BidVolume"]
    namelist3 = ["Date","Time","AskAvPrice","BidAvPrice","TotalAskVolume","TotalBidVolume"]
    
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    
    for name in namelist1:
        df1[name] = data[name][:][0]
    df1["WindCode"] = filepath.split(".")[0][-6:]+"."+filepath.split(".")[1][0:2]
    
    for name in namelist3:
        df3[name] = data[name][:][0]
    df3["WindCode"] = filepath.split(".")[0][-6:]+"."+filepath.split(".")[1][0:2]
    
    for name in namelist2:
        df2_temp = pd.DataFrame(columns=[name+str(i) for i in range(1,11)],data = np.array(data[name][:]).T)
        if df2.empty:
            df2 = df2_temp.copy()
        else:
            df2 = pd.merge(df2,df2_temp,left_index=True,right_index=True)
            
    df2["WindCode"] = filepath.split(".")[0][-6:]+"."+filepath.split(".")[1][0:2]
    df2["Date"] = data["Date"][:][0]
    df2["Time"] = data["Time"][:][0]
    
    df = pd.merge(df1,df3,on=["WindCode","Date","Time"])
    df = df[['WindCode','Date', 'Time', 'Price', 'Volume', 'Turover', 'MatchItems', 'TradeFlag',
       'BSFlag', 'AccVolume', 'AccTurover', 'High', 'Low', 'Open', 'PreClose','AskAvPrice',
       'BidAvPrice', 'TotalAskVolume','TotalBidVolume']]
    
    df2 = df2[df2.columns[-3:].tolist()+df2.columns[:-3].tolist()]
    df = pd.merge(df,df2,on=["WindCode","Date","Time"])
    df["BSFlag"] = df["BSFlag"].map(ToBSFlag)
    return df.drop_duplicates()


def ReadOrderData(filepath):
    '''
    获取Order数据!
    '''
    f = h5py.File(filepath,"r")
    data = f["r1"]
    namelist = ['Date','Time','Index','Order','OrderKind','FunctionCode','Price','OrderVolume']
    df = pd.DataFrame()
    for name in namelist:
        df[name] = data[name][:][0]
    df["WindCode"] = filepath.split(".")[0][-6:]+"."+filepath.split(".")[1][0:2]
    df = df[['WindCode','Date','Time','Index','Order','OrderKind','FunctionCode','Price','OrderVolume']]
    return df.drop_duplicates()


#%% 按照日期获取股票
def GetTick(TradeDate):
    '''
    按照交易日获取到tick数据
    '''
    ##path = r"H:\AShareHighFrequencyData\Tick"
    ##file_path = tick_path +"\\"+TradeDate[0:4]+"\\"+TradeDate
    file_path = get_file_path(tick_path, TradeDate)
    final = pd.concat([ReadTickData(os.path.join(file_path,file)) for file in os.listdir(file_path)],axis=0)
    return final.reset_index().drop("index",axis=1)


def GetTransaction(TradeDate):
    '''
    按照交易日获取到tick数据
    '''
    #path = r"H:\AShareHighFrequencyData\Transaction"
    #file_path = trans_path + "\\"+TradeDate[0:4]+"\\"+TradeDate
    file_path = get_file_path(trans_path, TradeDate)
    final = pd.concat([ReadTransactionData(os.path.join(file_path,file)) for file in os.listdir(file_path)],axis=0)
    return final.reset_index().drop("index",axis=1)


def GetOrder(TradeDate):
    '''
    按照交易日获取到tick数据
    '''
    ##path = r"H:\AShareHighFrequencyData\Order"
    ##file_path = order_path +"\\"+TradeDate[0:4]+"\\"+TradeDate
    file_path = get_file_path(order_path, TradeDate)
    final = pd.concat([ReadOrderData(os.path.join(file_path,file)) for file in os.listdir(file_path)],axis=0)
    return final.reset_index().drop("index",axis=1)

