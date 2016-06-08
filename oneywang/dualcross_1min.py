# coding=utf-8

#################Readme#########################
#1.请手工保证帐号上的钱够！
#2.本策略还不支持多实例等复杂场景。
#3.策略退出是会清除所有挂单。

from  datetime  import  *
import time
import random

from bfgateway_pb2 import *
from bfdatafeed_pb2 import *
from google.protobuf.any_pb2 import *

from grpc.beta import implementations
from grpc.beta import interfaces

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_TIMEOUT_SECONDS = 1
_CLIENT_ID = "dualcross"
_MT = [("clientid",_CLIENT_ID)]

_PING_TYPE = BfPingData().DESCRIPTOR
_ACCOUNT_TYPE = BfAccountData().DESCRIPTOR
_POSITION_TYPE = BfPositionData().DESCRIPTOR
_TICK_TYPE = BfTickData().DESCRIPTOR
_TRADE_TYPE = BfTradeData().DESCRIPTOR
_ORDER_TYPE = BfOrderData().DESCRIPTOR
_LOG_TYPE = BfLogData().DESCRIPTOR
_ERROR_TYPE = BfErrorData().DESCRIPTOR
_NOTIFICATION_TYPE = BfNotificationData().DESCRIPTOR

# 默认空值
EMPTY_STRING = ''
EMPTY_UNICODE = u''
EMPTY_INT = 0
EMPTY_FLOAT = 0.0

#################要与BfBarData返回结果一致#########################
class BarData(object):
    # K线数据

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.symbol = EMPTY_STRING          # 代码
        self.exchange = EMPTY_STRING        # 交易所
    
        self.open = EMPTY_FLOAT             # OHLC
        self.high = EMPTY_FLOAT
        self.low = EMPTY_FLOAT
        self.close = EMPTY_FLOAT
        
        self.date = EMPTY_STRING            # bar开始的时间，日期
        self.time = EMPTY_STRING            # 时间
        #self.datetime = None                # python的datetime时间对象
        
        self.volume = EMPTY_INT             # 成交量
        self.openInterest = EMPTY_INT       # 持仓量


class DualCross(object):
    # 策略参数
    TRADE_VOLUME = 1    # 每次交易的手数
    VOLUME_LIMIT = 5      # 多仓或空仓的最大手数
    FAST_K_NUM = 15     # 快速均线
    SLOW_K_NUM = 60     # 慢速均线
    
    # 策略变量
    #bar = None
    inited = 0
    barCount = 0
    barMinute = EMPTY_STRING
    
    fastMa = []             # 15均线数组
    fastMa0 = EMPTY_FLOAT   # 当前最新的快均线
    fastMa1 = EMPTY_FLOAT   # 上一根的快均线

    slowMa = []             # 60均线数组
    slowMa0 = EMPTY_FLOAT   # 当前最新的慢均线
    slowMa1 = EMPTY_FLOAT   # 上一根的慢均线

    # 关心的品种，持有仓位
    symbol = "rb1610"
    exchange = "SHFE"
    period = PERIOD_M01
    pos_long = 0    #多仓
    pos_short = 0   #空仓
    pending_orders = []

    def __init__(self):
        print "init dualcross"
        self.gateway_channel = implementations.insecure_channel('localhost', 50051)
        self.gateway = beta_create_BfGatewayService_stub(self.gateway_channel)
        self.datafeed_channel = implementations.insecure_channel('localhost',50052)
        self.datafeed = beta_create_BfDatafeedService_stub(self.datafeed_channel)
        self.connectivity = interfaces.ChannelConnectivity.IDLE
    
    def update(self,connectivity):
        '''C:\projects\grpc\src\python\grpcio\tests\unit\beta\_connectivity_channel_test.py'''
        print connectivity
        self.connectivity = connectivity
        
    def subscribe(self):
        self.gateway_channel.subscribe(self.update,try_to_connect=True)
    
    def unsubscribe(self):
        self.gateway_channel.unsubscribe(self.update)

    def initPosition(self, position):
        if position.direction == DIRECTION_LONG:
            self.pos_long += position.position
        elif position.direction == DIRECTION_SHORT:
            self.pos_short -= position.position
    
    def OnInit(self):
        print "OnInit-->QueryPosition"
        # 获取当前仓位
        #positions = self.gateway.QueryPosition(BfVoid(),_TIMEOUT_SECONDS,metadata=_MT)
        #for pos in positions:
        #    print pos
        #    if pos.symbol == self.symbol and pos.exchange == self.exchange:
        #        initPosition(pos)

    def OnTick(self, tick):
        # 收到行情TICK推送
        tickMinute = datetime.strptime(tick.tickTime,"%H:%M:%S.%f").minute

        # 初始得到K线
        if not self.inited:
            self.inited = 1
            print "Init: load history Bar"
            now = datetime.now()
            req = BfGetBarReq(symbol=self.symbol,exchange=self.exchange,period=self.period,toDate=tick.actionDate,toTime=tick.tickTime,count=self.SLOW_K_NUM)
            bars = self.datafeed.GetBar(req,timeout=_ONE_DAY_IN_SECONDS,metadata=_MT)
            for bar in bars:
                self.onBar(bar.closePrice)
            
            self.barMinute = tickMinute
            return
        
        # 每一新分钟得到K线
        if tickMinute != self.barMinute:
            print tick.tickTime + " got a new bar"
            # 因为只用到了bar.closePrice，所以不必再去datafeed取上一K线
            # TODO，如果需要去datafeed取，记得稍微延迟几个tick以防datafeed还没准备好。
            self.onBar(tick.lastPrice)
            self.barMinute = tickMinute    

    def onBar(self, closePrice):
        # 计算快慢均线
        if not self.fastMa0:        
            self.fastMa0 = closePrice
            self.fastMa.append(self.fastMa0)
        else:
            self.fastMa1 = self.fastMa0
            self.fastMa0 = ( closePrice + self.fastMa0 * (self.FAST_K_NUM - 1)) / self.FAST_K_NUM
            self.fastMa.append(self.fastMa0)
            
        if not self.slowMa0:
            self.slowMa0 = closePrice
            self.slowMa.append(self.slowMa0)
        else:
            self.slowMa1 = self.slowMa0
            self.slowMa0 = ( closePrice + self.slowMa0 * (self.SLOW_K_NUM -1) ) / self.SLOW_K_NUM
            self.slowMa.append(self.slowMa0)
        
        # 判断是否足够bar--初始化时会去历史，如果历史不够，会积累到至少  SLOW_K_NUM 数量的bar才会交易
        self.barCount += 1
        print self.barCount
        if self.barCount < self.SLOW_K_NUM:
            return

        # 判断买卖
        print self.fastMa0
        print self.slowMa0
        crossOver = self.fastMa0>self.slowMa0 and self.fastMa1<self.slowMa1     # 金叉上穿
        crossBelow = self.fastMa0<self.slowMa0 and self.fastMa1>self.slowMa1    # 死叉下穿
        
        # 金叉
        if crossOver:
            # 1.如果有空头持仓，则先平仓
            if self.pos_short > 0:
                self.cover(closePrice, self.pos_short)
            # 2.持仓未到上限，则继续做多
            if self.pos_long < self.VOLUME_LIMIT:
                self.buy(closePrice, self.TRADE_VOLUME)
        # 死叉
        elif crossBelow:
            # 1.如果有多头持仓，则先平仓
            if self.pos_long > 0:
                self.sell(closePrice, self.pos_long)
            # 2.持仓未到上限，则继续做空
            if self.pos_short < self.VOLUME_LIMIT:
                self.short(closePrice, self.TRADE_VOLUME)
    
    def buy(self, price, volume):
        print time.strftime("%Y-%m-%d %H:%M:%S")
        print ("buy: price=%10.3f vol=%d" %(price, volume))
        req = BfSendOrderReq(symbol=self.symbol,exchange=self.exchange,price=price,volume=volume,priceType=PRICETYPE_LIMITPRICE,direction=DIRECTION_LONG,offset=OFFSET_OPEN)
        resp = self.gateway.SendOrder(req,_TIMEOUT_SECONDS,metadata=_MT)
        self.pending_orders.append(resp.bfOrderId)
        print resp

    def sell(self, price, volume):
        print time.strftime("%Y-%m-%d %H:%M:%S")
        print ("sell: price=%10.3f vol=%d" %(price, volume))
        req = BfSendOrderReq(symbol=self.symbol,exchange=self.exchange,price=price,volume=volume,priceType=PRICETYPE_LIMITPRICE,direction=DIRECTION_LONG,offset=OFFSET_CLOSE)
        resp = self.gateway.SendOrder(req,_TIMEOUT_SECONDS,metadata=_MT)
        self.pending_orders.append(resp.bfOrderId)
        print resp

    def short(self, price, volume):
        print time.strftime("%Y-%m-%d %H:%M:%S")
        print ("short: price=%10.3f vol=%d" %(price, volume))
        req = BfSendOrderReq(symbol=self.symbol,exchange=self.exchange,price=price,volume=volume,priceType=PRICETYPE_LIMITPRICE,direction=DIRECTION_SHORT,offset=OFFSET_OPEN)
        resp = self.gateway.SendOrder(req,_TIMEOUT_SECONDS,metadata=_MT)
        self.pending_orders.append(resp.bfOrderId)
        print resp

    def cover(self, price, volume):
        print time.strftime("%Y-%m-%d %H:%M:%S")
        print ("cover: price=%10.3f vol=%d" %(price, volume))
        req = BfSendOrderReq(symbol=self.symbol,exchange=self.exchange,price=price,volume=volume,priceType=PRICETYPE_LIMITPRICE,direction=DIRECTION_SHORT,offset=OFFSET_CLOSE)
        resp = self.gateway.SendOrder(req,_TIMEOUT_SECONDS,metadata=_MT)
        self.pending_orders.append(resp.bfOrderId)
        print resp

    def OnTradeWillBegin(self, request):
        # 盘前启动策略，能收到这个消息，而且是第一个消息
        # TODO：这里是做初始化的一个时机
        print "OnTradeWillBegin"
        print request        

    def OnGotContracts(self, request):
        # 盘前启动策略，能收到这个消息，是第二个消息
        # TODO：这里是做初始化的一个时机
        print "OnGotContracts"
        print request
            
    def OnPing(self, request,):
        pass

    def OnError(self, request):
        print "OnError"
        print request
            
    def OnLog(self, request):
        print "OnLog"
        print request

    def updatePosition(self, direction, offset, volume):
        if (direction == DIRECTION_LONG and offset == OFFSET_OPEN):
            self.pos_long += volume
        elif (direction == DIRECTION_LONG and offset == OFFSET_CLOSE):
            self.pos_long -= volume
        elif (direction == DIRECTION_SHORT and offset == OFFSET_OPEN):
            self.pos_short += volume
        elif (direction == DIRECTION_SHORT and offset == OFFSET_CLOSE):
            self.pos_short -= volume
    
    def OnTrade(self, request):
        # 挂单的成交
        print "OnTrade"
        print request
        # 按最新结果更新当前仓位
        if request.bfOrderId not in self.pending_orders:
            return;
        if request.symbol != self.symbol or request.exchange != self.exchange:
            return;
        
        self.pending_orders.remove(request.bfOrderId)
        updatePosition(pos)
        
    
    def OnOrder(self, request):
        # 挂单的中间状态，一般只需要在OnTrade里面处理。
        print "OnOrder"
        print request
            
    def OnPosition(self, request):
        print "OnPosition"
        print request

    def OnAccount(self, request):
        print "OnAccount"
        print request
    
def dispatchPush(client,resp):
    if resp.Is(_TICK_TYPE):
        resp_data = BfTickData()
        resp.Unpack(resp_data)
        client.OnTick(resp_data)
    elif resp.Is(_PING_TYPE):
        resp_data = BfPingData()
        resp.Unpack(resp_data)
        client.OnPing(resp_data)
    elif resp.Is(_ACCOUNT_TYPE):
        resp_data = BfAccountData()
        resp.Unpack(resp_data)
        client.OnAccount(resp_data)
    elif resp.Is(_POSITION_TYPE):
        resp_data = BfPositionData()
        resp.Unpack(resp_data)
        client.OnPosition(resp_data)
    elif resp.Is(_TRADE_TYPE):
        resp_data = BfTradeData()
        resp.Unpack(resp_data)
        client.OnTrade(resp_data)
    elif resp.Is(_ORDER_TYPE):
        resp_data = BfOrderData()
        resp.Unpack(resp_data)
        client.OnOrder(resp_data)
    elif resp.Is(_LOG_TYPE):
        resp_data = BfLogData()
        resp.Unpack(resp_data)
        client.OnLog(resp_data)
    elif resp.Is(_ERROR_TYPE):
        resp_data = BfErrorData()
        resp.Unpack(resp_data)
        client.OnError(resp_data)
    elif resp.Is(_NOTIFICATION_TYPE):
        resp_data = BfNotificationData()
        resp.Unpack(resp_data)
        if resp_data.type == NOTIFICATION_GOTCONTRACTS:
            client.OnGotContracts(resp_data)
        elif resp_data.type == NOTIFICATION_TRADEWILLBEGIN:
            client.OnTradeWillBegin(resp_data)
        else:
            print "invliad notification type"
    else:
        print "invalid push type"        
    
def connect(client):
    print "connect gateway"
    req = BfConnectReq(clientId=_CLIENT_ID,tickHandler=True,tradeHandler=True,logHandler=True,symbol=client.symbol,exchange=client.exchange)
    responses = client.gateway.Connect(req,timeout=_ONE_DAY_IN_SECONDS)
    for resp in responses:
        dispatchPush(client,resp)            
    print "connect quit"

def onClose(client):
    # 退出前，把挂单都撤了
    print "cancel all pending orders"
    req = BfCancelOrderReq(symbol=client.symbol,exchange=client.exchange)
    for id in client.pending_orders:
        req.bfOrderId = id
        client.gateway.CancelOrder(req)

def disconnect(client):
    print "disconnect gateway"
    onClose(client)
    req = BfVoid()
    resp = client.gateway.Disconnect(req,_TIMEOUT_SECONDS,metadata=_MT)
    
def tryconnect(client):
    '''subscribe dont tryconnect after server shutdown. so unsubscrible and subscrible again'''
    print "sleep 5s,try reconnect..."
    time.sleep(_TIMEOUT_SECONDS)
    client.unsubscribe()
    time.sleep(_TIMEOUT_SECONDS)
    client.subscribe()            
    time.sleep(_TIMEOUT_SECONDS)
    time.sleep(_TIMEOUT_SECONDS)
    time.sleep(_TIMEOUT_SECONDS)
    
def run():
    print "start dualcross"
    client = DualCross()
    client.subscribe()
    client.OnInit()

    try:
        while True:
            if client.connectivity == interfaces.ChannelConnectivity.READY:
                connect(client)
            tryconnect(client)
    except KeyboardInterrupt:
        print "ctrl+c"        
    
    if client.connectivity == interfaces.ChannelConnectivity.READY:
        disconnect(client)
    
    print "stop dualcross"
    client.unsubscribe()
    
if __name__ == '__main__':
    run()
