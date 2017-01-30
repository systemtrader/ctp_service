# -*- coding: utf-8 -*- #
import sys, os, datetime, signal, time, redis
from ctp import MdApi, TraderApi, ApiStruct
from ConfigParser import ConfigParser
from threading import Thread
from Queue import PriorityQueue


class TraderSpi(TraderApi):
    """ 实现TraderApi的方法 """
    def __init__(self, broker_id, investor_id, password):

        self.broker_id   = broker_id
        self.investor_id = investor_id
        self.password    = password

        self._requestid   = 0
        self._front_id    = None
        self._session_id  = None
        self._orderref    = None
        self._login_status= 0       # 0-未登录；1-登录成功；-1-登录失败
        self._qry_lock    = True

        self.order_status = dict()  # 记录订单状态
        self.order_eqty   = dict()  # 记录订单完成数量

    def isErrorRspInfo(self, info):
        if info.ErrorID !=0:
            print "ErrorID=", info.ErrorID, ", ErrorMsg=", info.ErrorMsg
        return info.ErrorID !=0

    def OnFrontConnected(self):
        print('OnFrontConnected: Login...')
        reqid = self.get_requestid()
        req = ApiStruct.ReqUserLogin(BrokerID=self.broker_id, \
                                     UserID=self.investor_id, \
                                     Password=self.password)
        self.ReqUserLogin(req, reqid)

    def OnFrontDisconnected(self, nReason):
        print('OnFrontDisconnected:', nReason)

    def OnHeartBeatWarning(self, nTimeLapse):
        print('OnHeartBeatWarning:', nTimeLapse)

    def OnRspUserLogin(self, pRspUserLogin, pRspInfo, nRequestID, bIsLast):
        print('OnRspUserLogin:', pRspInfo)
        if bIsLast: self._qry_lock = False
        if pRspInfo.ErrorID == 0: # Success
            self.front_id   = pRspUserLogin.FrontID
            self.session_id = pRspUserLogin.SessionID
            self._orderref  = int(pRspUserLogin.MaxOrderRef or '0')
            self.login_status = 1
            self.tradingday = self.GetTradingDay()
        else:
            self.login_status = -1

    def OnRspQryInvestorPosition(self, pInvestorPosition, pRspInfo, nRequestID, bIsLast):
        print('OnRspQryInvestorPosition: ', pInvestorPosition, pRspInfo, nRequestID, bIsLast)
        if bIsLast: self._qry_lock = False

    def OnRspQryTradingAccount(self, pTradingAccount, pRspInfo, nRequestID, bIsLast):
        print('OnRspQryTradingAccount: ', pTradingAccount, pRspInfo, nRequestID, bIsLast)
        if bIsLast: self._qry_lock = False

    def OnRspQrySettlementInfo(self, pSettlementInfo, pRspInfo, nRequestID, bIsLast):
        print('OnRspQrySettlementInfo: ', pSettlementInfo, pRspInfo, nRequestID, bIsLast)
        if bIsLast: self._qry_lock = False

    def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print('OnRspSettlementInfoConfirm: ', pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast)
        if bIsLast: self._qry_lock = False

    def OnRspQrySettlementInfoConfirm(self, pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast):
        print('OnRspQrySettlementInfoConfirm: ', pSettlementInfoConfirm, pRspInfo, nRequestID, bIsLast)
        if bIsLast: self._qry_lock = False

    def OnRspOrderInsert(self, pInputOrder, pRspInfo, nRequestID, bIsLast):
        print('OnRspOrderInsert: ', pInputOrder, pRspInfo, nRequestID, bIsLast)

    def OnRtnOrder(self, pOrder):
        print('OnRtnOrder: ', pOrder)
        self.order_status[pOrder.OrderRef] = pOrder.OrderStatus

    def OnRtnTrade(self, pTrade):
        print('OnRtnTrade: ', pTrade)
        self.order_eqty[pTrade.OrderRef] += pTrade.Volume

    def OnErrRtnOrderInsert(self, pInputOrder, pRspInfo):
        print('OnErrRtnOrderInsert: ', pInputOrder, pRspInfo)

    def OnRspOrderAction(self, pInputOrderAction, pRspInfo, nRequestID, bIsLast):
        print('OnRspOrderAction: ', pInputOrderAction, pRspInfo, nRequestID, bIsLast)

    def OnErrRtnOrderAction(self, pOrderAction, pRspInfo):
        print('OnErrRtnOrderAction: ', pOrderAction, pRspInfo)

    def get_requestid(self):
        self._requestid += 1
        return self._requestid

    @property
    def orderref(self):
        return self._orderref

    def query_order_status(self, orderref):
        return self.order_status.get(orderref, '')

    def query_order_eqty(self, orderref):
        return self.order_eqty.get(orderref, 0)

    def wait_qry_finish(self, timeout=5, message=''):
        while self._qry_lock and timeout > 0:
            time.sleep(1)
            timeout -= 1
        if self._qry_lock:
            print(message + ' Error: timeout {}s'.format(timeout))
            self._qry_lock = False

    def check_login(self):
        """ 检查登陆状态和接口初始化状态 """
        print('CheckLoginStatus')
        self.wait_qry_finish(timeout = 5, message = 'ReqUserLogin') # 登录时间限制10s，否则报超时错误
        if self.login_status != 1: # 登陆不成功，退出
            sys.exit(-1)

    def qry_settlement_info(self):
        """ 查询结算单 """
        print('ReqQrySettlementInfo')
        self._qry_lock = True
        reqid = self.get_requestid()
        req   = ApiStruct.QrySettlementInfo(BroketID=self.broker_id, InvestorID=self.investor_id, TradingDay=self.tradingday)
        self.ReqQrySettlementInfo(req, reqid)
        self.wait_qry_finish(message = 'ReqQrySettlementInfo')

    def settlement_confirm(self):
        """ 结算单确认 """
        print('ReqSettlementInfoConfirm')
        self._qry_lock = True
        reqid = self.get_requestid()
        req   = ApiStruct.SettlementInfoConfirm(BroketID=self.broker_id, InvestorID=self.investor_id)
        self.ReqSettlementInfoConfirm(req, reqid)
        self.wait_qry_finish(message = 'ReqSettlementInfoConfirm')

    def qry_settlement_confirm(self):
        """ 查询结算单确认 """
        print('ReqQrySettlementConfirm')
        self._qry_lock = True
        reqid = self.get_requestid()
        req   = ApiStruct.QrySettlementInfo(BroketID=self.broker_id, InvestorID=self.investor_id)
        self.ReqQrySettlementInfoConfirm(req, reqid)
        self.wait_qry_finish(message = 'ReqQrySettlementInfoConfirm')

    def qry_accounts(self):
        """ 查询账户 """
        print('ReqQryTradingAccount')
        self._qry_lock = True
        reqid = self.get_requestid()
        req   = ApiStruct.QryTradingAccount(BrokerID=self.broker_id, InvestorID=self.investor_id)
        self.ReqQryTradingAccount(req, reqid)
        self.wait_qry_finish(message = 'ReqQryTradingAccount')

    def qry_holdings(self):
        """ 查询持仓 """
        print('ReqQryInvestorPosition')
        self._qry_lock = True
        reqid = self.get_requestid()
        req   = ApiStruct.QryInvestorPosition(BrokerID=self.broker_id, InvestorID=self.investor_id, InstrumentID='')
        self.ReqQryInvestorPosition(req, reqid)
        self.wait_qry_finish(message = 'ReqQryInvestorPosition')

    def order_insert(self, orderref, tkr, side, ocflag, qty, prc, hedging, pct=1):
        reqid   = self.get_requestid()
        qty     = int(max(qty * pct, 1))
        ocflag  = ApiStruct.OF_Open  if ocflag  == 'O'       else  (ApiStruct.OF_Close if ocflag == 'C' else ApiStruct.OF_CloseToday)
        side    = ApiStruct.D_Buy    if side    == 'B'       else ApiStruct.D_Sell
        hedging = ApiStruct.HF_Hedge if hedging == 'hedging' else ApiStruct.HF_Speculation
        req = ApiStruct.InputOrder(BrokerID=self.broker_id, InvestorID=self.investor_id, \
                                   InstrumentID=tkr, \
                                   OrderRef=orderref, \
                                   OrderPriceType=ApiStruct.OPT_AnyPrice, \
                                   Direction=side, \
                                   CombOffsetFlag=ocflag, \
                                   LimitPrice=0.0, \
                                   VolumeTotalOriginal=qty, \
                                   TimeCondition=ApiStruct.TC_IOC, \
                                   MinVolume=1, \
                                   CombHedgeFlag=hedging, \
                                   RequestID=reqid)
        self.order_status[orderref] = ''
        self.order_eqty[orderref]   = 0
        print("OrderInsert: ", req)
        self.ReqOrderInsert(req, reqid)

    def order_action(self, orderref):
        reqid = self.get_requestid()
        req   = ApiStruct.InputOrderAction(BrokerID=self.broker_id,\
                                           InvestorID=self.investor_id,\
                                           OrderRef=orderref,\
                                           ActionFlag=ApiStruct.AF_Delete,\
                                           RequestID=reqid)
        print("OrderAction: ", req)
        self.ReqOrderAction(req, reqid)


class ctp_trade(object):
    """ ctp trade """
    def __init__(self, dte, config):
        self.dte    = dte
        self.config = config

        # ctp config
        md_front       = config.get('CTP','md_front')
        trader_front   = config.get('CTP','trader_front')
        broker_id      = config.get('CTP','broker_id')
        investor_id    = config.get('CTP','investor_id')
        password       = config.get('CTP','password')

        self.traderapi = TraderSpi(broker_id, investor_id, password)
        self.trader_front = trader_front

        # redis config
        redis_host     = config.get('redis','host')
        redis_port     = config.get('redis','port')
        redis_password = config.get('redis','password')

        self.r = redis.Redis(host=redis_host, port=redis_port, password=redis_password)
        self.ps = self.r.pubsub()

        # flow path
        self.trade_flowpath= config.get('common','trade_flowpath')

        # prefix define
        self.db_prefix     = config.get('define','db_prefix')
        self.stream_prefix = config.get('define','stream_prefix')
        self.quotes_prefix = config.get('define','quotes_prefix')

        # parameters
        self.order_insert_pct   = float(config.get('param','order_insert_pct'))  # 单次委托比例
        self.order_insert_span  = int(config.get('param','order_insert_span'))   # 委托时间
        self.order_cancel_span  = int(config.get('param','order_cancel_span'))   # 撤单时间

        # private variable
        self.running  = True
        self.orderref = None
        self.bsktid_visited  = set()             # 防止重复下单
        self.order_queue     = PriorityQueue()   # 订单优先队列

    def get_orderref(self):
        self.orderref += 1
        return str(self.orderref)

    def start_bskt_listener(self):
        self.ps.subscribe(self.stream_prefix + '_STRT')
        for msg in self.ps.listen():
            if msg['type'] != 'message':
                continue
            bsktid, runmode, orders = eval(msg['data'])
            if bsktid in self.bsktid_visited:
                continue
            self.bsktid_visited.add(bsktid)
            for order in orders:
                orderref = self.get_orderref()
                self.order_queue.put((runmode, 'init', orderref, datetime.datetime.now(), order))

    def order_init_action(self, orderref, tme, order, runmode):
        tkr, side, ocflag, qty, prc, hedging = order
        if qty > 0:
            self.traderapi.order_insert(orderref, tkr, side, ocflag, qty, prc, hedging, self.order_insert_pct)
            self.order_queue.put((runmode, 'waiting', orderref, datetime.datetime.now(), order))

    def order_waiting_action(self, orderref, tme, order, runmode):
        status = self.traderapi.query_order_status(orderref)
        if status == '0' or status == '5': # 完全成交或已撤单
            tkr, side, ocflag, qty, prc, hedging = order
            eqty  = self.traderapi.query_order_eqty(orderref)
            order = (tkr, side, ocflag, qty - eqty, prc, hedging)
            orderref = self.get_orderref()
            self.order_queue.put((runmode, 'init', orderref, datetime.datetime.now(), order))
        else:
            now = datetime.datetime.now()
            if (now - tme).total_seconds() > self.order_insert_span:
                self.traderapi.order_action(orderref)
                self.order_queue.put((runmode, 'cancel', orderref, datetime.datetime.now(), order))
            else:
                self.order_queue.put((runmode, 'waiting', orderref, tme, order))

    def order_cancel_action(self, orderref, tme, order, runmode):
        status = self.traderapi.query_order_status(orderref)
        if status == '0' and status == '5': # 完全成交或已撤单
            tkr, side, ocflag, qty, prc, hedging = order
            eqty  = self.traderapi.query_order_eqty(orderref)
            order = (tkr, side, ocflag, qty - eqty, prc, hedging)
            orderref = self.get_orderref()
            self.order_queue.put((runmode, 'init', orderref, datetime.datetime.now(), order))
        else:
            now = datetime.datetime.now()
            if (now - tme).total_seconds() > self.order_cancel_span:
                self.traderapi.order_action(orderref)
                tkr, side, ocflag, qty, prc, hedging = order
                eqty  = self.traderapi.query_order_eqty(orderref)
                order = (tkr, side, ocflag, qty - eqty, prc, hedging)
                orderref = self.get_orderref()
                self.order_queue.put((runmode, 'init', orderref, datetime.datetime.now(), order))
            else:
                self.order_queue.put((runmode, 'cancel', orderref, tme, order))

    def run(self):
        # create trader api
        if not os.path.exists(self.trade_flowpath):
            os.makedirs(self.trade_flowpath)
        self.traderapi.Create(self.trade_flowpath)
        self.traderapi.SubscribePrivateTopic(ApiStruct.TERT_QUICK)
        self.traderapi.SubscribePublicTopic(ApiStruct.TERT_QUICK)
        self.traderapi.RegisterFront(self.trader_front)
        self.traderapi.Init()

        # check login status and wait for api initialization
        self.traderapi.check_login()
        self.orderref = self.traderapi.orderref

        # confirm settlement info
        self.traderapi.qry_settlement_info()
        self.traderapi.settlement_confirm()
        self.traderapi.qry_settlement_confirm()

        # query accounts
        self.traderapi.qry_accounts()

        # query holdings
        self.traderapi.qry_holdings()

        # start basket listener daemon
        t = Thread(target = self.start_bskt_listener)
        t.daemon = True
        t.start()

        # start order processing
        while self.running:
            runmode, action, orderref, tme, order = self.order_queue.get(block=True)
            if action == 'init':
                self.order_init_action(orderref, tme, order, runmode)
            elif action == 'waiting':
                self.order_waiting_action(orderref, tme, order, runmode)
            elif action == 'cancel':
                self.order_cancel_action(orderref, tme, order, runmode)

    def signal_handler(self, signal, frame):
        print "reveice signal", signal, ", exit."
        self.running = False


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print >>sys.stderr, 'Usage: python %s <config>' % sys.argv[0]
        sys.exit(-1)

    if not os.path.exists(sys.argv[1]):
        print >>sys.stderr, 'Error: config file not fild at %s' % sys.argv[1]
        sys.exit(-1)

    config = ConfigParser()
    config.read(sys.argv[1])

    dte  = int(datetime.datetime.today().strftime('%Y%m%d'))

    # 生产环境一般是配置每天启动，需要下面的部分判断是否是交易日
    # holidays = [int(x[0]) for x in [line.strip().split() for line in open(config.get('common','calendar'))] if len(x) < 6]

    # if dte in holidays:
    #     print >>sys.stderr, 'not trade day, exit.'
    #     sys.exit(0)

    p = ctp_trade(dte, config)

    signal.signal(signal.SIGINT, p.signal_handler)

    p.run()
