# coding=utf-8

from bfpy.bfCtaTemplate import *
import talib as ta
import bfpy.quickFuncion as qf

class ma_cross_algo(bfCtaTemplate):
    def __init__(self):
        print "macd_dif dea cross__init__"
        self.clientID='macd_dif_cross'
        self.clientMT=[("clientid",self.clientID)]
        self.symbol='rb1610'
        self.exchange='SHFE'
        
        self.period={'TALIB_NAME':'1MIN','secondsOfPeriod':60,'ticksOfSecond':2}
        '''
        TALIB_NAME='2MIN' #talib周期 : 1MIN 5MIN 15MIN 60MIN 1D 1W 1M
        secondsOfPeriod=120#每周期秒数
        ticksOfSecond=2 #每秒2Tick
        '''
        self.fastperiod=5
        self.slowperiod=8
        self.signalperiod=3
        self.leastBars=8+3#self.ma2_p+3 #cross需要3根k判断
        self.offsetNum=2 #每单2手
        
        
    def singalOnTick(self):
        
        pass
        
    def singalOnBarOpen(self):
        pass        
    def singalOnBarclose(self):
        np_close=np.array(self.bar['close'],dtype=np.float) #TA-lib wants numpy arrays of "double" floats as inputs
        DIFF, DEA, MACD = ta.MACD(np_close, self.fastperiod, self.slowperiod, self.signalperiod)
        
        #DIFF与DEA交叉交易
        if qf.cross(DIFF[-3:],DEA[-3:]):self.SP_BK(self.askprice,self.offsetNum)
        if qf.cross(DEA[-3:],DIFF[-3:]):self.BP_SK(self.bidprice,self.offsetNum) 
        

if __name__ == '__main__':
    
   
    client = ma_cross_algo()
    #bfgw = GW.bfCtaGateway(client)
    print u'正在生成bar,请稍候....'
    client.OnInit()
    client.run()
            
 