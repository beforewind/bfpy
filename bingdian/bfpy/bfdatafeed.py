# coding=utf-8
# -*- coding: utf-8 -*-
import datetime
import random
import numpy as np
import pandas as pd
from pb2.bfdatafeed_pb2 import *
from google.protobuf.any_pb2 import *

from grpc.beta import implementations
from grpc.beta import interfaces

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_TIMEOUT_SECONDS = 1

class bfDataFeed(object):
    
    def __init__(self,client):
        self.symbol=client.symbol
        self.exchange=client.exchange
        self.clientMT=client.clientMT
        self.datafeed_channel = implementations.insecure_channel('localhost',50052)
        self.datafeed = beta_create_BfDatafeedService_stub(self.datafeed_channel)
        
        
   #----------------------------------------------------------------------
   #功能:取得tickNum个Tick,并转换成period的bar
   #----------------------------------------------------------------------
    def getTick2BarLast(self,tickNum,period,toDate="20991231",toTime="00:00:00"):
        req = BfGetTickReq(symbol=self.symbol,exchange=self.exchange,toDate=toDate,toTime=toTime,count=tickNum)
        responses = self.datafeed.GetTick(req,timeout=_ONE_DAY_IN_SECONDS,metadata=self.clientMT)
        #responses=self.getTick(tickNum)
        tickList=[(resp.actionDate+' '+resp.tickTime,resp.lastPrice,resp.lastVolume) for resp in responses]
        #tickDataNp
        tickdataType=np.dtype({'names':['time','price','volume'],'formats':['S68','f','i']}) #datetime64[ns]
        tickNp=np.array(tickList,dtype=tickdataType)
        ts_index=pd.to_datetime(tickNp['time'])
        ts_price=pd.Series(tickNp['price'],name='price',index=ts_index)
        ts_volume=pd.Series(tickNp['volume'],name='volume',index=ts_index)
        #print ts_volume
        bar_ohlc=ts_price.resample(period['TALIB_NAME']).ohlc()
        bar_volume=ts_volume.resample(period['TALIB_NAME']).sum()
        bar=pd.concat([bar_ohlc,bar_volume],axis=1)
        #print bar
        bar.dropna(how='any',inplace=True)
        #print bar
        return bar
   #----------------------------------------------------------------------
   #功能:取得从start到end时间内的Tick,并转换成period的bar
   #ToDo:datefeed增加获取tick方向后,简化
   #----------------------------------------------------------------------
    def getTick2BarFrmTo(self,start,end,period):
        delta=end-start
        seconds=delta.total_seconds()
        #print 'lastbarTime:',start ,'lastTickTime:',end, 'seconds:',seconds,'Tick Num',seconds*period['ticksOfSecond']
        
        #count=int(self.bar.tail(1).index.values-pd.to_datetime(tickTime))*2+240 #多取2分钟
        tickNum=abs(int(seconds*period['ticksOfSecond']))##abs:7x24服务器传过来的tick时间倒流
        #print tickNum
        step=period['secondsOfPeriod']*period['ticksOfSecond'] #每次取得用于生成bar的tick数
        temp_bar=pd.DataFrame()
        temp_datetime=end
        temp_deltime=datetime.timedelta(microseconds=1000)
        for i in range(step,tickNum+step,step):
            temp_date=temp_datetime.strftime('%Y%m%d')
            temp_time=temp_datetime.strftime('%H:%M:%S.%f')[:-3] #去除字符串后三位 保留到小数点后3位
            #bar2=self.dataserver.getTick2BarLast(tickNum,period) 
            #print 'lastbar time:',start,'get tick to',temp_date , temp_time
            bar1=self.getTick2BarLast(step+10,period,toDate=temp_date,toTime=temp_time) #每次返回 1.5个bar
            #print 'bar1=====================:\n',bar1
            temp_bar=pd.concat([bar1[1:],temp_bar])#bar1[0]为不完整的bar,丢掉. temp_bar中 为完整的历史bar+当前bar
            #print 'temp_bar==================:\n',temp_bar
            if start in temp_bar.index:
                #print '====================start in temp_bar.index=========='
                temp_bar=temp_bar[start:] #结束
                #print '***********tempbar*******exit loop******\n',temp_bar
                break
            temp_datetime=temp_bar.index[0]-temp_deltime
            #print 'update temp_datetime for next.....',temp_datetime
          
        return temp_bar  
               
        
       