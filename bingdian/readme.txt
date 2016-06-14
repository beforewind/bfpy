基于bftrader 0.0.97 
使用TALIB计算指标
文件说明:
pb2:bftrader原有的API
bfpy:封装的类. 
     周期bar,通过Tick数据在线生成,暂未使用都周期数据
     支持短网重连.


注意:将bfgateway_pb2:_BFPOSITIONDATA 对应的默认值修改为None.
     否则,无法区分仓位为0或无相应仓位信息.
 
