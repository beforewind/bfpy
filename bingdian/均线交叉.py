# coding=utf-8
from bfpy.bfCtaTemplate import *
import talib as ta
import bfpy.quickFuncion as qf
import sys
class ma_cross_algo(bfCtaTemplate):
    def __init__(self):
        print "macross__init__"
        self.clientID='ma_cross'
        self.clientMT=[("clientid",self.clientID)]
        self.symbol='rb1610'
        self.exchange='SHFE'
        
        self.period={'TALIB_NAME':'5MIN','secondsOfPeriod':300,'ticksOfSecond':2}
        '''
        TALIB_NAME='2MIN' #talib周期 : 1MIN 5MIN 15MIN 60MIN 1D 1W 1M
        secondsOfPeriod=120#每周期秒数
        ticksOfSecond=2 #每秒2Tick
        '''
        self.ma1_p=5
        self.ma2_p=10
        self.leastBars=10+3#self.ma2_p+3 #cross需要3根k判断
        self.offsetNum=2 #每单2手
        
        
    def singalOnTick(self):
        pass
    def singalOnBarOpen(self):
        pass        
    def singalOnBarclose(self):
        ma1=ta.SMA(self.bar['close'].values.astype(np.float),self.ma1_p)[-3:]
        ma2=ta.SMA(self.bar['close'].values.astype(np.float),self.ma2_p)[-3:]
        print '\n singalOnBarclose:'
        print ma1
        print ma2
        if qf.cross(ma1,ma2):self.SP_BK(self.askprice,self.offsetNum)
        if qf.cross(ma2,ma1):self.BP_SK(self.bidprice,self.offsetNum) 

if __name__ == '__main__':
    
   
    client = ma_cross_algo()
    #bfgw = GW.bfCtaGateway(client)
    print u'正在生成bar,请稍候....'
    client.OnInit()
    client.run()
            
            