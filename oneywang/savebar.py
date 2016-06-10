# coding=utf-8

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
_CLIENT_ID = "bar saver"
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

def Tick2Bar(tick, bar, period):
    bar.symbol = tick.symbol
    bar.exchange = tick.exchange
    bar.period = period
    
    bar.actionDate = tick.actionDate
    bar.barTime = datetime.strftime(datetime.strptime(tick.tickTime,"%H:%M:%S.%f"),"%H:%M:%S")
    bar.volume = tick.volume
    bar.openInterest = tick.openInterest
    bar.lastVolume = tick.lastVolume
    
    bar.openPrice = tick.lastPrice
    bar.highPrice = tick.lastPrice
    bar.lowPrice = tick.lastPrice
    bar.closePrice = tick.lastPrice

    return bar

class DataRecorder(object):
    # 变量
    bars = {}
    _contract_inited = 0

    def __init__(self):
        print "init datarecorder"
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
        
    def OnTradeWillBegin(self, request):
        print "OnTradeWillBegin"
        print request        

    def insertContracts(self):
        # GetContract
        req = BfGetContractReq(symbol="*",exchange="*")
        resps = self.gateway.GetContract(req,_TIMEOUT_SECONDS,metadata=_MT)
        for resp in resps:
            print resp
            df = self.datafeed.InsertContract(resp,_TIMEOUT_SECONDS,metadata=_MT)
        
    def OnGotContracts(self, request):
        print "OnGotContracts"
        print request

        # GetContract
        self._contract_inited = 1
        self.insertContracts()
        
        # QueryPosition
        req = BfVoid()
        resp = self.gateway.QueryPosition(req,_TIMEOUT_SECONDS,metadata=_MT)
        print resp
        
        # QueryAccount
        req = BfVoid()
        resp = self.gateway.QueryAccount(req,_TIMEOUT_SECONDS,metadata=_MT)
        print resp
    
    def OnPing(self, request):
        pass


    def OnTick(self, tick):
        df = self.datafeed.InsertTick(tick,_TIMEOUT_SECONDS,metadata=_MT)

        # 要把contract保存到datafeed里面才会看到数据
        # ongotcontracts只有ctpgateway连接上ctp时候才发，所有盘中策略连接ctpgateway时候，是没有这个信息的。可以把ctpgateway ctp-stop然后ctp-start以下，就可以得到这个消息。
        # 这里在判断如果没有调用则主动调用一次。
        if self._contract_inited == 0 :
            self._contract_inited = 1
            self.insertContracts()
        
        # 计算K线
        id = tick.symbol + '@' + tick.exchange
        # tickDatetime = datetime.strptime(tick.actionDate+tick.tickTime,"%Y%m%d%H:%M:%S.%f")
        
        if not self.bars.has_key(id):
            bar = BfBarData()              
            Tick2Bar(tick, bar, PERIOD_M01)
            self.bars[id] = bar
            return

        #print "update bar for: " + id
        bar = self.bars[id]
        if datetime.strptime(tick.tickTime,"%H:%M:%S.%f").minute != datetime.strptime(bar.barTime,"%H:%M:%S").minute:
            # 存入datafeed
            print "Insert bar" 
            print tick.tickTime
            print bar   
            self.datafeed.InsertBar(bar,_TIMEOUT_SECONDS,metadata=_MT)
            
            # 新的k线
            Tick2Bar(tick, bar, PERIOD_M01)

            # TESTONLY: 得到最近的10个bar
            #print "get bar for: " + id
            #now = datetime.now()
            #req = BfGetBarReq(symbol=bar.symbol,exchange=bar.exchange,period=bar.period,toDate=now.strftime("%Y%m%d"),toTime=now.strftime("%H:%M:%S.000"),count=10)
            #allbars = self.datafeed.GetBar(req,timeout=_ONE_DAY_IN_SECONDS,metadata=_MT)
            #for b in allbars: print b

        else:
            # 继续累加当前K线
            bar.highPrice = max(bar.highPrice, tick.lastPrice)
            bar.lowPrice = min(bar.lowPrice, tick.lastPrice)
            bar.closePrice = tick.lastPrice
            bar.volume = tick.volume
            bar.openInterest = tick.openInterest
            bar.lastVolume += tick.lastVolume
        
    def OnError(self, request):
        print "OnError"
        print request
            
    def OnLog(self, request):
        print "OnLog"
        print request
    
    def OnTrade(self, request):
        print "OnTrade"
        print request
    
    def OnOrder(self, request):
        print "OnOrder"
        print request
            
    def OnPosition(self, request):
        print "OnPosition"
        print request

    def OnAccount(self, request):
        print "OnAccount"
        print request

    def OnGotContracts(self, request):
        # 盘前启动策略，能收到这个消息，是第二个消息
        # TODO：这里是做初始化的一个时机
        print "OnGotContracts"
        print request

        # GetContract
        for i in range(1,1000):
            req = BfGetContractReq(index=i,subscribled=True)
            resp = self.gateway.GetContract(req,_TIMEOUT_SECONDS,metadata=_MT)
            if (resp.symbol):
                print resp
            else:
                break
        
        # QueryPosition
        req = BfVoid()
        resp = self.gateway.QueryPosition(req,_TIMEOUT_SECONDS,metadata=_MT)
        print resp
        
        # QueryAccount
        req = BfVoid()
        resp = self.gateway.QueryAccount(req,_TIMEOUT_SECONDS,metadata=_MT)
        print resp
            
    
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
    req = BfConnectPushReq(clientId=_CLIENT_ID,tickHandler=True,tradeHandler=True,logHandler=True,symbol="rb1610",exchange="SHFE")
    responses = client.gateway.ConnectPush(req,timeout=_ONE_DAY_IN_SECONDS)
    for resp in responses:
        dispatchPush(client,resp)            
    print "connect quit"
    
def disconnect(client):
    print "disconnect gateway"
    req = BfVoid()
    resp = client.gateway.DisconnectPush(req,_TIMEOUT_SECONDS,metadata=_MT)
    
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
    print "start DataRecorder"
    client = DataRecorder()
    client.subscribe()

    try:
        while True:
            if client.connectivity == interfaces.ChannelConnectivity.READY:
                connect(client)
            tryconnect(client)
    except KeyboardInterrupt:
        print "ctrl+c"        
    
    if client.connectivity == interfaces.ChannelConnectivity.READY:
        disconnect(client)
    
    print "stop DataRecorder"
    client.unsubscribe()
    
if __name__ == '__main__':
    run()
