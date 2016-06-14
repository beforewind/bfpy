# -*- coding: utf-8 -*-
import time
import datetime
import random
import numpy as np
import pandas as pd
import PrintColor
import bfpy.bfGateway as GW
import bfpy.bfdatafeed as DF
from bfpy.bfDefine import *

_TIMEOUT_SECONDS = 1
_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class bfCtaTemplate(object):
    gatewayserver=EMPTY_CLASS()
    dataserver=EMPTY_CLASS()
    reconnect=True
    clrPrint = PrintColor.Color() 
    # 策略变量
    orderData = {}  # 订单
    pos = {'long': EMPTY_INT,'yd_long':EMPTY_INT, 'short': EMPTY_INT,'yd_short': EMPTY_INT}  # 仓位
    bidvol=EMPTY_INT
    bidprice=EMPTY_FLOAT
    askvol=EMPTY_INT
    askprice=EMPTY_FLOAT
    lastprice=EMPTY_FLOAT
    lastvol=EMPTY_INT
    
        
    #策略参数,在算法实例中初始化
    clientID=EMPTY_STRING 
    clientMT=[("clientid",clientID)]
    symbol=EMPTY_STRING 
    exchange='SHFE'
    
    leastBars=EMPTY_INT #计算需要的bar数
    '''
    TALIB_NAME=EMPTY_STRING #tablib 周期: 1MIN 5MIN 15MIN 60MIN 1D 1W 1M
    secondsOfPeriod=60 #每周期秒数
    #ticksOfSecond=2.0 #tick/秒，用于计算 补数据的条数
    '''
    period={'TALIB_NAME':EMPTY_STRING,'secondsOfPeriod':EMPTY_INT,'ticksOfSecond':2}
    offsetNum=EMPTY_INT #每次交易手数
    #----------------------------------------------------------------------
    def __init__(self):
        print "----------------------------init bfCtaTemplate"
                 
    #----------------------------------------------------------------------    
    def datafeedBarCount(self,count,period):
        self.bar=self.dataserver.getTick2BarLast(count,period)#保证最少有2个均线值
        print self.bar
    #----------------------------------------------------------------------    
    def datafeedBarFrmDateTime(self,tickDateTime,period):
        #处理,补数据时收到新的Tick
        #ToDo:datefeed增加获取tick方向后,简化
        start=pd.to_datetime(self.bar.tail(1).index.values[0])#从最后一个bar的开始取,倒序
        end=tickDateTime
        #step=self.secondsOfPeriod*self.ticksOfSecond
        temp_bar=self.dataserver.getTick2BarFrmTo(start,end,period)
        #print 'before update:.......\n',self.bar[start:]  #.tail(3)
        self.bar=pd.concat([self.bar[:-1],temp_bar])
        #print 'finish update, result:................\n',self.bar[start:]
        
   #----------------------------------------------------------------------
    def buy(self, price, volume):
        """买开"""
        
        self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_OPEN)
    
    #----------------------------------------------------------------------
    def sell(self, price, volume):
        """卖平"""
        # 只有上期所才要考虑平今平昨
        if self.exchange!='SHFE':
            self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                  PRICETYPE_LIMITPRICE, DIRECTION_SHORT,OFFSET_CLOSE)  
        
        else: 
            #昨仓优先平
            if volume>self.pos['yd_long']:  #数量大于昨仓，昨仓不足的部分；昨仓为0，所有数量都是今仓；数量小于昨仓，不平今
                dif_volume=volume-self.pos['yd_long'] 
                
                self.gateway.sendOrder(self.symbol,self.exchange,price,dif_volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_SHORT,OFFSET_CLOSETODAY)
                volume=self.pos['yd_long']#若昨仓为0， 不执行平昨
            if volume>0:#平昨
                self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_SHORT,OFFSET_CLOSE)
        '''
        else: 
            #今仓优先平
            if volume>self.pos['long']:#数量大于今仓，今仓不足的部分；今仓为0，所有数量都是今仓；数量小于今仓，不平昨
                dif_volume=volume-self.pos['long'] 
                
                self.gateway.sendOrder(self.symbol,self.exchange,price,dif_volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_SHORT,OFFSET_CLOSET)
                volume=self.pos['long']#若今仓为0， 不执行平今
            if volume>0:
                self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_SHORT,OFFSET_CLOSETODAY)
         '''
            

    #----------------------------------------------------------------------
    def short(self, price, volume):
        """卖开"""
        self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                           PRICETYPE_LIMITPRICE,DIRECTION_SHORT,OFFSET_OPEN)          
 
    #----------------------------------------------------------------------
    def cover(self, price, volume):
        """买平"""
         # 只有上期所才要考虑平今平昨
        if self.exchange!='SHFE':
            self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_CLOSE)
             
        else:
            #昨仓优先平
            if volume>self.pos['yd_short']: #数量大于昨仓，昨仓不足的部分；昨仓为0，所有数量都是今仓；数量小于昨仓，不平今
                 dif_volume=volume-self.pos['yd_short'] 
                 self.gateway.sendOrder(self.symbol,self.exchange,price,dif_volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_CLOSETODAY)
                 volume=self.pos['yd_short']#若昨仓为0， 不执行平昨
            if volume>0:#平昨
                 self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_CLOSE)
        '''
        else:
            #今仓优先平
            if volume>self.pos['short']:#数量大于今仓，今仓不足的部分；今仓为0，所有数量都是今仓；数量小于今仓，不平昨
                dif_volume=volume-self.pos['short'] 
                
                self.gateway.sendOrder(self.symbol,self.exchange,price,dif_volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_CLOSET)
                volume=self.pos['short']#若今仓为0， 不执行平今
            if volume>0:#平今
                self.gateway.sendOrder(self.symbol,self.exchange,price,volume,
                                    PRICETYPE_LIMITPRICE, DIRECTION_LONG,OFFSET_CLOSETODAY)
         '''  
                
            
    #-------------------------------------------------------------------
   
    def SP_BK(self,price,volume):
        toltal_pos=self.pos['short']+self.pos['yd_short']
        #有反向仓位,先全平
        if toltal_pos>0:
            self.cover(price,toltal_pos)
        self.buy(price,volume) 
    #----------------------------------------------------------------------       
    def BP_SK(self,price,volume):
            
        #有反向仓位,先全平
        toltal_pos=self.pos['long']+self.pos['yd_long']
        #有反向仓位,先全平
        if toltal_pos>0:
            self.sell(price,toltal_pos)
        self.short(price,volume) 
    #----------------------------------------------------------------------   
    def singalOnTick(self):
        raise NotImplementedError
    #----------------------------------------------------------------------
    def singalOnBarOpen(self):
        raise NotImplementedError  
    #----------------------------------------------------------------------
    def singalOnBarclose(self):
        raise NotImplementedError            
        
    #----------------------------------------------------------------------        
    def OnInit(self):
        # load tick
        # tick-rb1609-SHFE-20160429-14:39:07.000
        print "OnInit-->loadTick"
        self.gateway=GW.GatewayConnector(self)
        #self.gateway=self.gatewayConnector.gateway
        self.dataserver=DF.bfDataFeed(self)
        self.datafeedBarCount(self.leastBars*self.period['ticksOfSecond']*self.period['secondsOfPeriod'],self.period)#
        if self.bar.shape[0]<self.leastBars:
            self.clrPrint.print_red_text_oneline( 'bars is not enough---->("q" to exit):')
            cmd=raw_input()
            if cmd =='q':exit()
            return
        print 'finish OnInit'
    #----------------------------------------------------------------------    
    def OnTradeWillBegin(self, request):
        print "OnTradeWillBegin"
        print request        
    #----------------------------------------------------------------------
    def OnGotContracts(self, request):
        print "OnGotContracts"
        print request
        # GetContract
        for i in range(1,1000):
            resp=self.gateway.getContract(i)
            if (resp.symbol):
                print resp.name.encode("GBK")
                print resp
            else:
                break
        # QueryPosition
        resp = self.gateway.queryPosition()
        print resp
        # QueryAccount
        resp = self.gateway.queryAccount()
        print resp
    #----------------------------------------------------------------------        
    def OnPing(self, request,):
        #print "OnPing"
        #print request
        pass
    #----------------------------------------------------------------------
    def OnTick(self, tick):
        #if request.symbol != self.symbol:return
        #print 'TickTime________________________',request.actionDate,request.tickTime
        #tick=request
        #处理实盘"0" 值tick
        self.bidvol=tick.bidVolume1 if tick.bidVolume1>0 else self.bidvol
        self.askvol=tick.askVolume1 if tick.askVolume1 >0 else self.askvol
        self.bidprice=tick.bidPrice1 if tick.bidPrice1>0 else  self.bidprice
        self.askprice=tick.askPrice1 if tick.askPrice1>0 else self.askprice
        self.lastprice=tick.lastPrice if tick.lastPrice>0 else self.lastprice
        self.lastvol=tick.lastVolume if tick.lastVolume >0 else 0
        self.ticktime=tick.tickTime 
        
        start=time.clock()
        tickDateTime=pd.to_datetime(tick.actionDate+' '+tick.tickTime)
        tickPrice=self.lastprice
        tickVolume=self.lastvol
        #tickMinute = tickdatetime.minute
        #print 'tickTime_________________________________________',tickDateTime
        if self.reconnect==True :
            print 'Reconnect:get tick  from datafeed,and update bar '
            self.datafeedBarFrmDateTime(tickDateTime,self.period)
            self.reconnect=False
            self.tickInbar=1
            # QueryPosition
            self.gateway.queryPosition()
            #print '===================onTick==reconnect====tail(1)======\n',self.bar.tail(1)
        else:
            lastbar=self.bar.tail(1)
            lastbarDateTime=pd.to_datetime(lastbar.index.values[0])
            #print 'lastbarDateTime__________________',lastbarDateTime ,(tickDateTime-lastbarDateTime).total_seconds()
            tickSecondsOfLastbar=(tickDateTime-lastbarDateTime).total_seconds()
            if tickSecondsOfLastbar<self.period['secondsOfPeriod']:
                self.tickInbar=self.tickInbar+1
                lastbarOpen=lastbar.open[0]
                lastbarHigh=max(lastbar.high[0],tickPrice)
                lastbarLow=min(lastbar.low[0],tickPrice)
                lastbarClose=tickPrice
                
                lastbarVolume=lastbar.volume[0]+tickVolume
                #print lastbar.volume[0],tickVolume,lastbarVolume
                #print self.bar.iloc[-1]
                self.bar.iloc[-1]=(lastbarOpen,lastbarHigh,lastbarLow,lastbarClose,lastbarVolume)#直接修改原始对象
                print lastbarDateTime,'',lastbarOpen,'',lastbarHigh,'',lastbarLow,'',
                if lastbarClose>self.bidprice: 
                    #self.clrPrint.print_red_text_oneline('')
                    self.clrPrint.print_red_text_oneline(lastbarClose)
                    print '',lastbarVolume,
                    #self.clrPrint.print_red_text_oneline(':')
                    self.clrPrint.print_red_text_oneline(self.lastvol)
                else:
                    #self.clrPrint.print_green_text_oneline('')
                    self.clrPrint.print_green_text_oneline(lastbarClose)
                    print '',lastbarVolume,
                    #self.clrPrint.print_green_text_oneline(':')
                    self.clrPrint.print_green_text_oneline(self.lastvol)
                print '        \r',
                self.singalOnTick()
            else:
                #new bar
                
                self.tickInbar=1
                #self.clrPrint.print_red_text(lastbarDateTime,tickSecondsOfLastbar,self.secondsOfPeriod)
                #不用tickDateTime:防止bar的开始时间点没有的tick, 若周期内无tick(小节间),则不生成bar
                newBarDateTime=lastbarDateTime+pd.Timedelta(seconds=self.period['secondsOfPeriod']*int(tickSecondsOfLastbar/self.period['secondsOfPeriod'])) 
                newbar=pd.DataFrame([(tickPrice,tickPrice,tickPrice,tickPrice,tickVolume)],columns=['open','high','low','close','volume'],index=[newBarDateTime])
                #print('\n')
                #print 'last bar******************\n',lastbar
                #print 'newbar**********************\n',newbar
                #上根bar收盘,计算交易信号
                self.singalOnBarclose()
                self.bar=pd.concat([self.bar,newbar])
                print '\n concatbar**********************\n',self.bar
                #上根bar收盘,计算交易信号
                self.singalOnBarOpen()
        #print 'ontick Do time:',time.clock()-start,self.tickInbar
           
            
    #----------------------------------------------------------------------
    def OnError(self, request):
        print "OnError"
        print request.message.encode("GBK")
    #----------------------------------------------------------------------        
    def OnLog(self, request):
        print "OnLog"
        print request.message.encode("GBK")
    #----------------------------------------------------------------------  
    def OnTrade(self, tradeData):
        #print "OnTrade"
        #print tradeData
        if tradeData.symbol == self.symbol:
            if tradeData.direction == DIRECTION_LONG and tradeData.offset == OFFSET_OPEN:
                self.pos['long'] += tradeData.volume
            elif tradeData.direction == DIRECTION_SHORT and tradeData.offset == OFFSET_OPEN:
                self.pos['short'] += tradeData.volume
            elif tradeData.direction == DIRECTION_LONG and tradeData.offset == OFFSET_CLOSE :
                self.pos['yd_short'] -= tradeData.volume
                if self.pos['yd_short']<0 : #非上期所,平仓数是昨仓和今仓的和 , 大于昨仓
                    self.pos['short'] +=self.pos['yd_short']
                    self.pos['yd_short']=0
            elif tradeData.direction == DIRECTION_SHORT and tradeData.offset == OFFSET_CLOSE :
                self.pos['yd_long'] -= tradeData.volume
                if self.pos['yd_long']<0 : #非上期所,平仓数是昨仓和今仓的和 , 大于昨仓
                    self.pos['long'] +=self.pos['yd_long']
                    self.pos['yd_long']=0
            elif tradeData.direction == DIRECTION_LONG and tradeData.offset == OFFSET_CLOSETODAY:
                self.pos['short'] -= tradeData.volume
            elif tradeData.direction == DIRECTION_SHORT and tradeData.offset == OFFSET_CLOSETODAY:
                self.pos['long'] -= tradeData.volume
            print tradeData.symbol,TR_Direction[tradeData.direction],TR_Offset[tradeData.offset],tradeData.volume
            self.clrPrint.print_blue_text(self.pos)
        
    #----------------------------------------------------------------------    
    def OnOrder(self, orderData):
        #print "OnOrder"
        #print orderData
        if orderData.symbol == self.symbol:
            self.orderData[orderData.bfOrderId] = orderData
            #print self.orderData
    #----------------------------------------------------------------------    
    def OnPosition(self, posData):
        #print "OnPosition",posData.symbol
        #print posData
        # 昨仓和今仓的数据更新是分在两条记录里的，因此需要判断检查该条记录对应仓位
        # 注意将bfgateway_pb2:_BFPOSITIONDATA 对应的默认值修改为None,否则,无法区分仓位0和无仓位信息. 
        if posData.symbol == self.symbol:
           
            if posData.direction==DIRECTION_LONG and posData.position != None:
                self.pos['long'] = posData.position
            if posData.direction==DIRECTION_LONG and posData.ydPosition != None:
                print 'long yd:',posData.ydPosition
                self.pos['yd_long']=posData.ydPosition
                
            if posData.direction==DIRECTION_SHORT and posData.position != None:
                
                self.pos['short'] = posData.position
            if posData.direction==DIRECTION_SHORT and posData.ydPosition != None:
                self.pos['yd_short']=posData.ydPosition
            #print posData
            self.clrPrint.print_blue_text(self.pos)
            
         
    #----------------------------------------------------------------------
    def OnAccount(self, request):
        print "OnAccount"
        print request
    #----------------------------------------------------------------------
    def run(self):
        print u'注意将bfgateway_pb2:_BFPOSITIONDATA 对应的默认值修改为None.\n否则,无法区分仓位0和无仓位信息.' 
        cmd=raw_input('\n ---->("q" to exit):')
        if cmd =='q':exit()
        self.gateway.run()
