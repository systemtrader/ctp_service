# -*- coding: utf-8 -*- #
import sys, os, datetime, time, redis

r = redis.Redis('localhost')


print "1st group order", datetime.datetime.now()
orders = []
#tkr, side, ocflag, qty, prc, hedging
orders.append(('TF1703', 'B', 'O', 10, 0.0, '0'))
orders.append(('TF1706', 'B', 'O', 6, 0.0, '0'))
orders.append(('TF1709', 'S', 'O', 8, 0.0, '0'))

bsktid  = 'bskt001'
runmode = 10 # 任务优先级，越小越优先执行

r.publish('STREAM_STRT',(bsktid, runmode, orders))

time.sleep(5*60)

print "2nd group order", datetime.datetime.now()
orders = []
orders.append(('AL1703', 'B', 'O', 10, 0.0, '0'))
orders.append(('CU1706', 'B', 'O', 6, 0.0, '0'))
orders.append(('RB1709', 'S', 'O', 8, 0.0, '0'))

bsktid  = 'bskt002'
runmode = 4

r.publish('STREAM_STRT',(bsktid, runmode, orders))

time.sleep(5*60)

print "3rd group order", datetime.datetime.now()
orders = []
orders.append(('IC1703', 'B', 'O', 10, 0.0, '0'))
orders.append(('IF1706', 'B', 'O', 6, 0.0, '0'))
orders.append(('IH1709', 'S', 'O', 8, 0.0, '0'))

bsktid  = 'bskt001' # 测试重复篮子
runmode = 1

r.publish('STREAM_STRT',(bsktid, runmode, orders))
