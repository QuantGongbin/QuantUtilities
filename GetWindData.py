# -*- coding: utf-8 -*-
"""
Created on Tue Apr 18 09:56:25 2017

@author: user
"""

import pandas as pd 
import os 

##import sys
##sys.path.append("E:\MyWork\BaseFunction")

from CommonFunction import GetTradeDates,GetTradeWeeks,GetTradeMonthes


class GetAShareCashFlow(object):
    '''
    获取wind底库中A股现金流量表,仅保留合并报表数据
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_cash_flow\data\\"
        
    def GetAllData(self):
        '''
        获取所有财报数据
        '''
        data = pd.read_pickle(os.path.join(self.path,"data.pkl"))
        report_types = ["408001000","408002000","408003000","408004000","408005000"]
        data = data[data["STATEMENT_TYPE"].isin(report_types)]
        data = data[data["S_INFO_WINDCODE"].map(lambda x:x[0:1])!="A"]
        return data
      
    def GetSpecificData(self,field):
        '''
        获取指定科目,field，相关科目
        '''
        data = pd.read_pickle(os.path.join(self.path,"data.pkl"))
        report_types = ["408001000","408004000","408005000","408050000"]
        data = data[data["STATEMENT_TYPE"].isin(report_types)]
        columns = ["S_INFO_WINDCODE","ANN_DT","REPORT_PERIOD","STATEMENT_TYPE","ACTUAL_ANN_DT"]
        data = data[columns+field].dropna()
        data = data[data["S_INFO_WINDCODE"].map(lambda x:x[0:1])!="A"]
        return data





#%% 获取量价数据
def GetAdjClose(StartDate,EndDate):
    '''
    获取后复权价格
    '''
    path = r"E:\DataBase\WindData\ashare_eod_prices\data\\"
    trading_dates = GetTradeDates(StartDate,EndDate)
    data = pd.concat([pd.read_pickle(path+date+".pkl") for date in trading_dates],axis=0) 
    prices = data.pivot(index="TRADE_DT",columns="S_INFO_WINDCODE",values="S_DQ_ADJCLOSE")
    prices.index.name = "date"
    prices.columns.name = "asset"
    prices.index = pd.to_datetime(prices.index)
    return prices


   
class GetAshareEodPrices(object):
    '''
    获取wind底库中行情数据
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_eod_prices\data\\"
        
    def GetDailyStocksData(self,TradeDate):
        '''
        TradeDate : [str] 交易日期,如 "20080131"
        '''
        df = pd.read_pickle(self.path+TradeDate+".pkl")
        df.sort_values(["S_INFO_WINDCODE"],inplace=True)
        df = df.reset_index().drop("index",axis=1)
        return df 
    
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''获取区间股票数据
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)

        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df
    
    def GetPreClose(self,StartDate,EndDate):
        '''获取前开盘价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_PRECLOSE" )
        return final 
      
    def GetOpen(self,StartDate,EndDate):
        '''获取开盘价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_OPEN" )
        return final 

    def GetClose(self,StartDate,EndDate):
        '''获取收盘价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_CLOSE" )
        return final 
    
    def GetHigh(self,StartDate,EndDate):
        '''获取最高价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_HIGH" )
        return final 
      
    def GetLow(self,StartDate,EndDate):
        '''获取最低价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_LOW" )
        return final 

    def GetVwap(self,StartDate,EndDate):
        '''获取交易均价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_AVGPRICE" )
        return final 
    
    def GetAdjClose(self,StartDate,EndDate):
        '''获取调整后的收盘价
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_ADJCLOSE" )
        return final
      
    def GetPctChange(self,StartDate,EndDate):
        '''获取日涨跌幅
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_PCTCHANGE" )
        return final 
    
    def GetVolume(self,StartDate,EndDate):
        '''获取成交量，手数
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_VOLUME" )
        return final
    
    def GetAmount(self,StartDate,EndDate):
        '''获取成交金额，千元
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_AMOUNT" )
        return final     
    
    def GetTradeStatus(self,StartDate,EndDate):
        '''获取交易状态
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_TRADESTATUS" )
        return final 
    
      
class GetAshareMoneyFlow(object):
    '''
    获取wind底库中行情数据
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_money_flow\data\\"
        
    def GetDailyMoneyFlow(self,TradeDate):
        '''
        TradeDate : [str] 交易日期,如 "20080131"
        '''
        df = pd.read_pickle(self.path+TradeDate+".pkl")
        df.sort_values(["S_INFO_WINDCODE"],inplace=True)
        df = df.reset_index().drop("index",axis=1)
        return df 
    
    def GetPeriodMoneyFlow(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)

        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df


class GetAshareWeeklyYield(object):
    '''
    获取wind底库中周行情数据
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_weekly_yield\data\\"
        
    def GetWeeklyStocksData(self,TradeDate):
        '''
        TradeDate : [str] 交易周日期, 如 "20170421"
        '''
        df = pd.read_pickle(self.path+TradeDate+".pkl")
        df.sort_values(["S_INFO_WINDCODE"],inplace=True)
        return df 
    
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期
        EndDate   : [str] 截止日期
        #用windapi提取的周度数据有误，因此直接采用本地的周度数据进行提取
        '''
        trading_weeks = [file.split(".")[0] for file in os.listdir(self.path) if file.split(".")[0]>=StartDate and file.split(".")[0]<=EndDate]
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_weeks],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df


class GetAshareMonthlyYield(object):
    '''
    获取wind底库中周行情数据
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_monthly_yield\data\\"
        
    def GetWeeklyStocksData(self,TradeDate):
        '''
        TradeDate : [str] 交易周日期, 如 "20170421"
        '''
        df = pd.read_pickle(self.path+TradeDate+".pkl")
        df.sort_values(["S_INFO_WINDCODE"],inplace=True)
        return df 
    
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期
        EndDate   : [str] 截止日期
        '''
        trading_monthes = GetTradeMonthes(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_monthes],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df


class GetAindexEodPrice(object):
    '''
    获取wind底库中的指数数据
    '''
    def __init__(self):
        '''
        定义数据库
        '''
        self.path = r"E:\DataBase\WindData\aindex_eod_prices\data\\"
        
    def GetPeriodIndexData(self,IndexCode,StartDate,EndDate):
        '''
        获取某个指数一段时间内的数据
        Parameters
        ----------
        IndexCode : [str] 指数代码,如 "000001.SH"
        StartDate : [str] 开始日期,如 "20100101"
        EndDate   : [str] 截止日期,如 "20100131"
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df = final_df[final_df["S_INFO_WINDCODE"]==IndexCode]
        return final_df

class GetAshareEodDerivativeIndicator(object):
    
    '''
    获取wind数据库中衍生数据
    '''
    def __init__(self):
        '''
        定义数据库
        '''
        self.path = r"E:\DataBase\WindData\ashare_eod_derivative_indicator\data\\"
        
    def GetPeriodMarketValue(self,StartDate,EndDate):
        '''
        获取一段时间所有股票的市值
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df = final_df[["S_INFO_WINDCODE","TRADE_DT","S_VAL_MV","S_DQ_MV"]]   
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df
    
    def GetPeriodTurnover(self,StartDate,EndDate):
        '''
        获取一段时间所有股票的换手率
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df = final_df[["S_INFO_WINDCODE","TRADE_DT","S_DQ_TURN","S_DQ_FREETURNOVER"]]     
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df  
    
    def GetPeriodDerivativeData(self,StartDate,EndDate):
        '''
        获取一段时间所有股票的衍生数据
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0)  
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df  
    
    def GetFreeTurnover(self,StartDate,EndDate):
        '''
        获取一段时间所有股票的自由换手率
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        data = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        data.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        final = data.pivot(index = "TRADE_DT" ,columns = "S_INFO_WINDCODE" ,values = "S_DQ_FREETURNOVER" )
        return final
    
class GetAshareIntensityTrendADJ(object):
    '''
    获取wind底库中A股强弱与趋向技术指标（后复权）
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_intensity_trend_adj\data\\"
        
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期
        EndDate   : [str] 截止日期
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df
      
class GetAshareIntensityTrend(object):
    '''
    获取wind底库中A股强弱与趋向技术指标（后复权）
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_intensity_trend\data\\"
        
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期
        EndDate   : [str] 截止日期
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df
      
class GetAshareMarginTrade(object):
    '''
    获取wind底库中A股强弱与趋向技术指标（后复权）
    '''
    def __init__(self):
        '''
        定义数据存储的路径
        '''
        self.path = r"E:\DataBase\WindData\ashare_margin_trade\data\\"
        
    def GetPeriodStocksData(self,StartDate,EndDate):
        '''
        StartDate : [str] 开始日期
        EndDate   : [str] 截止日期
        '''
        trading_dates = GetTradeDates(StartDate,EndDate)
        final_df = pd.concat([pd.read_pickle(self.path+date+".pkl") for date in trading_dates],axis=0) 
        final_df.sort_values(["TRADE_DT","S_INFO_WINDCODE"],inplace=True)
        return final_df
      
