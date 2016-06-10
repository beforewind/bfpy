# coding=utf-8

import bftraderclient as bf
from bfgateway_pb2 import *
from bfdatafeed_pb2 import *

class Demo(bf.BfTraderClient):
    def __init__(self):
        bf.BfTraderClient.__init__(self)
        self.clientid = "demo";
        self.tickHandler = True
        self.tradeHandler = True
        self.logHandler = True
        self.symbol = "*"
        self.exchange = "*"
        
    def OnStart(self):
        print "OnStart"
        
    def OnTradeWillBegin(self, response):
        print "OnTradeWillBegin"
        print response        

    def OnGotContracts(self, response):
        print "OnGotContracts"
        print response
            
    def OnPing(self, response):
        print "OnPing"
        print response

    def OnTick(self, response):
        print "OnTick"
        print response
        
    def OnError(self, response):
        print "OnError"
        print response
            
    def OnLog(self, response):
        print "OnLog"
        print response
    
    def OnTrade(self, response):
        print "OnTrade"
        print response
    
    def OnOrder(self, response):
        print "OnOrder"
        print response
            
    def OnPosition(self, response):
        print "OnPosition"
        print response

    def OnAccount(self, response):
        print "OnAccount"
        print response
        
    def OnStop(self):
        print "OnStop"

if __name__ == '__main__':
    client = Demo()
    bf.BfRun(client,clientId=client.clientid,tickHandler=client.tickHandler,tradeHandler=client.tradeHandler,logHandler=client.logHandler,symbol=client.symbol,exchange=client.exchange)
