# -*- coding: utf-8 -*- #
import sys, os, datetime, signal, time
import redis
from ctp import MdApi, TraderApi, ApiStruct
from ConfigParser import ConfigParser


class MdSpi(MdApi):
    """ 实现MdApi的方法 """
    def __init__(self, config, on_mdf):
        
        self.broker_id   = config.get('CTP','broker_id')
        self.investor_id = config.get('CTP','investor_id')
        self.password    = config.get('CTP','password')
        self.on_mdf      = on_mdf
        
        self.requestid   = 0
        self.instruments = []

    def isErrorRspInfo(self, info):
        if info.ErrorID !=0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID !=0

    def OnFrontConnected(self):
        print('OnFrontConnected: Login...')
        requestid = self.get_requestid()
        req = ApiStruct.ReqUserLogin(BrokerID=self.broker_id, \
                                     UserID=self.investor_id, \
                                     Password=self.password)
        self.ReqUserLogin(req, requestid)

    def OnFrontDisconnected(self, nReason):
        print('OnFrontDisconnected:', nReason)

    def OnHeartBeatWarning(self, nTimeLapse):
        print('OnHeartBeatWarning:', nTimeLapse)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        print('OnRspUserLogin:', pRspInfo)
        if pRspInfo.ErrorID == 0: # Success
            print('GetTradingDay:', self.GetTradingDay())
            print('SubMarketData')
            self.SubscribeMarketData(self.instruments)
        else:
            print('login failed:',pRspInfo.ErrorID)
            sys.exit(-1)

    def OnRspSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        print 'OnRspSubMarketData:', pSpecificInstrument, pRspInfo, nRequestID, bIsLast

    def OnRspUnSubMarketData(self, spec_instrument, info, requestid, islast):
        print "OnRspUnSubMarketData", spec_instrument, info, requestid, islast

    def OnRtnDepthMarketData(self, depth_market_data):
        self.on_mdf(depth_market_data)

    def get_requestid(self):
        self.requestid += 1
        return self.requestid
    
    def instruments_add(self, tkr):
        self.instruments.append(tkr)

        
class TraderSpi(TraderApi):
    """ 实现TraderApi的方法 """
    def __init__(self, config, on_tkr):

        self.broker_id   = config.get('CTP','broker_id')
        self.investor_id = config.get('CTP','investor_id')
        self.password    = config.get('CTP','password')
        self.on_tkr      = on_tkr
        
        self.requestid = 0
        self._qry_lock = True 

    def isErrorRspInfo(self, info):
        if info.ErrorID !=0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID !=0

    def OnFrontConnected(self):
        print('OnFrontConnected: Login...')
        requestid = self.get_requestid()
        req = ApiStruct.ReqUserLogin(BrokerID=self.broker_id, \
                                     UserID=self.investor_id, \
                                     Password=self.password)
        self.ReqUserLogin(req, requestid)

    def OnFrontDisconnected(self, nReason):
        print('OnFrontDisconnected:', nReason)

    def OnHeartBeatWarning(self, nTimeLapse):
        print('OnHeartBeatWarning:', nTimeLapse)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        print('OnRspUserLogin:', pRspInfo)
        if pRspInfo.ErrorID == 0: # Success
            print('GetTradingDay:', self.GetTradingDay())
            print('ReqQryInstrument')
            requestid = self.get_requestid()
            self.ReqQryInstrument(ApiStruct.Instrument(''), requestid)
        else:
            print('login failed:',pRspInfo.ErrorID)
            sys.exit(-1)

    def OnRspQryInstrument(self, pInstrument, pRspInfo, nRequestID, bIsLast):
        self.on_tkr(pInstrument.InstrumentID)
        if bIsLast:
            self._qry_lock = False

    def get_requestid(self):
        self.requestid += 1
        return self.requestid

    def wait_qry_finish(self, timeout=5):
        while self._qry_lock and timeout > 0:
            timeout -= 1
            time.sleep(1)
        if self._qry_lock:
            print >>sys.stderr, "query timeout, exit."
            self._qry_lock = False
            sys.exit(-1)
            
        
class ctp_collector(object):
    """ ctp collector """
    def __init__(self, dte, config, flag):
        self.dte    = dte
        self.config = config
        self.flag   = flag

        self.md_front     = config.get('CTP','md_front')
        self.trader_front = config.get('CTP','trader_front')

        self.mdapi     = MdSpi(config, self.on_mdf)
        self.traderapi = TraderSpi(config, self.on_tkr)

        self.running = True

    def on_tkr(self, tkr):
        self.mdapi.instruments_add(tkr)

    def on_mdf(self, mdf):
        print mdf

    def is_tradetme(self, tme):
        if self.flag == 'day':
            return tme < 153000
        if self.flag == 'night':
            return tme > 200000 or tme <= 23000
        
    def run(self):
        print "start trader api", datetime.datetime.now()
        self.traderapi.Create('stream/')
        self.traderapi.RegisterFront(self.trader_front)
        self.traderapi.Init()
        self.traderapi.wait_qry_finish()

        print "start md api", datetime.datetime.now()
        self.mdapi.Create('stream/')
        self.mdapi.RegisterFront(self.md_front)
        self.mdapi.Init()

        print "wait for market close"
        while self.running:
            time.sleep(15)
            tme = int(datetime.datetime.now().strftime('%H%M%S'))
            if not self.is_tradetme(tme):
                self.running = False

    def signal_handler(self, signal, frame):
        print "reveice signal", signal, ", exit."
        self.running = False
        

if __name__ == '__main__':

    dte = int(datetime.datetime.today().strftime('%Y%m%d'))

    if len(sys.argv) < 3:
        print >>sys.stderr, 'Usage: python %s <config> <day|night>' % sys.argv[0]
        sys.exit(-1)

    if not os.path.exists(sys.argv[1]):
        print >>sys.stderr, 'Error: config file not fild at %s' % sys.argv[1]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    if sys.argv[2] not in ['day','night']:
        print >>sys.stderr, 'Error: expect flag [day|night], input is %s' % sys.argv[2]
        sys.exit(-1)
        
    flag = sys.argv[2]
    
    p = ctp_collector(dte, config, flag)
    
    signal.signal(signal.SIGINT, p.signal_handler)
    
    p.run()
    
