# -*- coding: utf-8 -*-
import time
import PrintColor
from pb2.bfgateway_pb2 import *
from google.protobuf.any_pb2 import *

from grpc.beta import implementations
from grpc.beta import interfaces

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_TIMEOUT_SECONDS = 1

_PING_TYPE = BfPingData().DESCRIPTOR
_ACCOUNT_TYPE = BfAccountData().DESCRIPTOR
_POSITION_TYPE = BfPositionData().DESCRIPTOR
_TICK_TYPE = BfTickData().DESCRIPTOR
_TRADE_TYPE = BfTradeData().DESCRIPTOR
_ORDER_TYPE = BfOrderData().DESCRIPTOR
_LOG_TYPE = BfLogData().DESCRIPTOR
_ERROR_TYPE = BfErrorData().DESCRIPTOR
_NOTIFICATION_TYPE = BfNotificationData().DESCRIPTOR

class GatewayConnector(object):   

    def __init__(self,client):
        self.client=client
        self.gateway_channel = implementations.insecure_channel('localhost', 50051)
        self.gateway = beta_create_BfGatewayService_stub(self.gateway_channel)
        self.connectivity = interfaces.ChannelConnectivity.IDLE
        #client.gateway=self.gateway
        self.clrPrint = PrintColor.Color() 
    #-----------------------------------------------------------
    #封装的发单函数,功外部调用
    #-----------------------------------------------------------
    def sendOrder(self,symbol,exchange,price,volume,priceType,direction,offset):
        req = BfSendOrderReq(symbol=symbol, exchange=exchange, price=price, volume=volume,
                                  priceType=priceType, direction=direction, offset=offset)
        resp=self.gateway.SendOrder(req, _TIMEOUT_SECONDS, metadata=self.client.clientMT)
        print resp.bfOrderId
        return resp.bfOrderId
    def getContract(self,index):
        req = BfGetContractReq(index=index,subscribled=True)
        resp = self.gateway.GetContract(req,_TIMEOUT_SECONDS,metadata=self.client.clientMT)
        return resp
    def queryPosition(self):
        req = BfVoid()
        resp= self.gateway.QueryPosition(req,_TIMEOUT_SECONDS,metadata=self.client.clientMT)
        return resp
    def queryAccount(self):
        req = BfVoid()
        resp= self.gateway.QueryAccount(req,_TIMEOUT_SECONDS,metadata=self.client.clientMT)
        return
    def update(self,connectivity):
        '''C:\projects\grpc\src\python\grpcio\tests\unit\beta\_connectivity_channel_test.py'''
        print connectivity
        self.connectivity = connectivity
  
    def subscribe(self):
        print 'subsccribe'
        self.gateway_channel.subscribe(self.update,try_to_connect=True)
    
    def unsubscribe(self):
        self.gateway_channel.unsubscribe(self.update)
        
    def dispatchPush(self,client,resp):
        #self.type_url.split('/')[-1] == descriptor.full_name   
        if resp.Is(_TICK_TYPE):
            start=time.clock()
            resp_data = BfTickData()
            resp.Unpack(resp_data)
            client.OnTick(resp_data)
            time_dowith_tick=time.clock()-start
            if time_dowith_tick>0.2:
                self.clrPrint.print_red_text("=================time to Do tick=============",time_dowith_tick)
            if time_dowith_tick>0.4:#(1/self.client.ticksOfSecond):
                self.client.reconnect=True #重连则从datafeed补tick
            
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
        
    def connect(self):
        print "connect gateway"
        #req = BfConnectReq(clientId=_CLIENT_ID,tickHandler=True,tradeHandler=True,logHandler=True,symbol="*",exchange="*")
        req = BfConnectReq(clientId=self.client.clientID,tickHandler=True,tradeHandler=True,logHandler=True,symbol=self.client.symbol,exchange=self.client.exchange)
        responses = self.gateway.Connect(req,timeout=_ONE_DAY_IN_SECONDS)
        #循环:接收数据流,并调用处理函数  
        for resp in responses:
            #print resp
            self.dispatchPush(self.client,resp)            
        print "connect quit"
        
    def disconnect(self):
        print "disconnect gateway"
        req = BfVoid()
        resp = self.gateway.Disconnect(req,_TIMEOUT_SECONDS,metadata=self.client.clientMT)
        
    def tryconnect(self):
        '''subscribe dont tryconnect after server shutdown. so unsubscrible and subscrible again'''
        self.client.reconnect=True #重连则从datafeed补tick
        print "sleep 5s,try reconnect..."
        time.sleep(_TIMEOUT_SECONDS)
        self.unsubscribe()
        time.sleep(_TIMEOUT_SECONDS)
        self.subscribe()            
        time.sleep(_TIMEOUT_SECONDS)
        time.sleep(_TIMEOUT_SECONDS)
        time.sleep(_TIMEOUT_SECONDS)
        
    def run(self):
        print "start GateWay"
        #client=self.client
        try:
            while True:
                if self.connectivity == interfaces.ChannelConnectivity.READY:
                    self.connect()
                self.tryconnect()
        except KeyboardInterrupt:
            print "ctrl+c"        
        
        if self.connectivity == interfaces.ChannelConnectivity.READY:
            self.disconnect()
        
        print "stop GateWay"
        self.unsubscribe()
 