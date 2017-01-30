# -*- coding: utf-8 -*- #
import sys, os, datetime, signal, time, redis
from ctp import MdApi, TraderApi, ApiStruct
from ConfigParser import ConfigParser


class MdSpi(MdApi):
    """ 实现MdApi的方法 """
    def __init__(self, broker_id, investor_id, password, on_mdf):

        self.broker_id   = broker_id
        self.investor_id = investor_id
        self.password    = password
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

    def OnRspUnSubMarketData(self, pSpecificInstrument, pRspInfo, nRequestID, bIsLast):
        print "OnRspUnSubMarketData", pSpecificInstrument, pRspInfo, nRequestID, bIsLast

    def OnRtnDepthMarketData(self, pDepthMarketData):
        mdf = (pDepthMarketData.TradingDay, pDepthMarketData.UpdateTime, pDepthMarketData.UpdateMillisec, \
               pDepthMarketData.InstrumentID, pDepthMarketData.ExchangeID, pDepthMarketData.ExchangeInstID, \
               pDepthMarketData.PreDelta, pDepthMarketData.CurrDelta, \
               pDepthMarketData.AveragePrice, pDepthMarketData.ActionDay, \
               pDepthMarketData.PreSettlementPrice, pDepthMarketData.PreClosePrice, pDepthMarketData.PreOpenInterest, \
               pDepthMarketData.OpenPrice, pDepthMarketData.HighestPrice, \
               pDepthMarketData.LowestPrice, pDepthMarketData.LastPrice, \
               pDepthMarketData.Volume, pDepthMarketData.Turnover, pDepthMarketData.OpenInterest, \
               pDepthMarketData.ClosePrice, pDepthMarketData.SettlementPrice, \
               pDepthMarketData.UpperLimitPrice, pDepthMarketData.LowerLimitPrice, \
               (pDepthMarketData.BidPrice1, pDepthMarketData.BidPrice2, pDepthMarketData.BidPrice3, \
                pDepthMarketData.BidPrice4, pDepthMarketData.BidPrice5), \
               (pDepthMarketData.BidVolume1, pDepthMarketData.BidVolume2, pDepthMarketData.BidVolume3, \
                pDepthMarketData.BidVolume4, pDepthMarketData.BidVolume5), \
               (pDepthMarketData.AskPrice1, pDepthMarketData.AskPrice2, pDepthMarketData.AskPrice3, \
                pDepthMarketData.AskPrice4, pDepthMarketData.AskPrice5), \
               (pDepthMarketData.AskVolume1, pDepthMarketData.AskVolume2, pDepthMarketData.AskVolume3, \
                pDepthMarketData.AskVolume4, pDepthMarketData.AskVolume5))
        self.on_mdf(mdf)

    def get_requestid(self):
        self.requestid += 1
        return self.requestid

    def on_tkr(self, tkr):
        self.instruments.append(tkr)


class TraderSpi(TraderApi):
    """ 实现TraderApi的方法 """
    def __init__(self, broker_id, investor_id, password, on_tkr):

        self.broker_id   = broker_id
        self.investor_id = investor_id
        self.password    = password
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

    def wait_qry_finish(self, timeout=5, message=''):
        while self._qry_lock and timeout > 0:
            time.sleep(1)
            timeout -= 1
        if self._qry_lock:
            print(message + ' Error: timeout {}s'.format(timeout))
            self._qry_lock = False


class ctp_collector(object):
    """ ctp collector """
    def __init__(self, dte, config, flag):
        self.dte    = dte
        self.config = config
        self.flag   = flag

        # ctp config
        md_front       = config.get('CTP','md_front')
        trader_front   = config.get('CTP','trader_front')
        broker_id      = config.get('CTP','broker_id')
        investor_id    = config.get('CTP','investor_id')
        password       = config.get('CTP','password')

        self.mdapi     = MdSpi(broker_id, investor_id, password, self.on_mdf)
        self.traderapi = TraderSpi(broker_id, investor_id, password, self.mdapi.on_tkr)
        self.md_front  = md_front
        self.trader_front = trader_front

        # redis config
        redis_host     = config.get('redis','host')
        redis_port     = config.get('redis','port')
        redis_password = config.get('redis','password')

        self.r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)

        # flow path
        self.md_flowpath   = config.get('common','md_flowpath')

        # prefix define
        self.db_prefix     = config.get('define','db_prefix')
        self.stream_prefix = config.get('define','stream_prefix')
        self.quotes_prefix = config.get('define','quotes_prefix')

        # general settings
        self.running = True

    def data_filter(self, data):
        if isinstance(data, float) and data == sys.float_info.max:
            return 0
        if isinstance(data, tuple):
            return tuple(self.data_filter(x) for x in data)
        return data

    def on_mdf(self, mdf):
        mdf = self.data_filter(mdf)
        # self.r.push(self.db_prefix + '_CTP', mdf)
        self.r.publish(self.stream_prefix + '_CTP', mdf)
        dte, tme, msc, tkr = mdf[:4]
        updtme = datetime.datetime.strptime('{} {}.{}'.format(dte, tme, msc),
                                            '%Y%m%d %H:%M:%S.%f')
        fields = ['echid','echtkr','predelta','delta','avgprc','actday',\
                  'presettle','preclse','preopi','opn','high','low','lastprc',\
                  'shr','val','opi','clse','settle','ulmtprc','llmtprc','bid',\
                  'bsize','ask','asize']

        obj = dict(zip(fields, mdf[4:]))
        obj['dte']     = dte
        obj['tkr']     = tkr
        obj['updtme']  = updtme
        obj['opi']     = obj['preopi']    if obj['opi']  == 0 else obj['opi']
        obj['opn']     = obj['presettle'] if obj['opn']  == 0 else obj['opn']
        obj['high']    = obj['presettle'] if obj['high'] == 0 else obj['high']
        obj['low']     = obj['presettle'] if obj['low']  == 0 else obj['low']
        obj['lastprc'] = obj['presettle'] if obj['lastprc'] == 0 else obj['lastprc']
        obj['settle']  = obj['presettle'] if obj['settle']  == 0 else obj['settle']
        obj['ulmtprc'] = obj['high']      if obj['ulmtprc'] == 0 else obj['ulmtprc']
        obj['llmtprc'] = obj['low']       if obj['llmtprc'] == 0 else obj['llmtprc']

        self.r.set(self.quotes_prefix + '_' + tkr, obj)

    def is_tradetme(self, tme):
        if self.flag == 'day':
            return 90000 < tme < 153000
        if self.flag == 'night':
            return tme > 200000 or tme <= 23000

    def run(self):
        # create md api
        if not os.path.exists(self.md_flowpath):
            os.makedirs(self.md_flowpath)
        self.traderapi.Create(self.md_flowpath)
        self.traderapi.RegisterFront(self.trader_front)
        self.traderapi.Init()
        self.traderapi.wait_qry_finish()

        # create trader api
        self.mdapi.Create(self.md_flowpath)
        self.mdapi.RegisterFront(self.md_front)
        self.mdapi.Init()

        # wait for market close
        while self.running:
            time.sleep(15)
            tme = int(datetime.datetime.now().strftime('%H%M%S'))
            if not self.is_tradetme(tme):
                self.running = False

    def signal_handler(self, signal, frame):
        print "reveice signal", signal, ", exit."
        self.running = False


if __name__ == '__main__':

    if len(sys.argv) < 3:
        print >>sys.stderr, 'Usage: python %s <config> <day|night>' % sys.argv[0]
        sys.exit(-1)

    if not os.path.exists(sys.argv[1]):
        print >>sys.stderr, 'Error: config file not fild at %s' % sys.argv[1]
        sys.exit(-1)

    if sys.argv[2] not in ['day','night']:
        print >>sys.stderr, 'Error: expect flag [day|night], input is %s' % sys.argv[2]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    flag = sys.argv[2]

    dte  = int(datetime.datetime.today().strftime('%Y%m%d'))

    # 生产环境一般是配置每天启动，需要下面的部分判断是否是交易日
    # holidays = [int(x[0]) for x in [line.strip().split() for line in open(config.get('common','calendar'))] if len(x) < 6]

    # if dte in holidays:
    #     print >>sys.stderr, 'not trade day, exit.'
    #     sys.exit(0)

    p = ctp_collector(dte, config, flag)

    signal.signal(signal.SIGINT, p.signal_handler)

    p.run()
