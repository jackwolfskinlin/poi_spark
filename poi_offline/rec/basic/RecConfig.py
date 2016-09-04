# -*- coding: utf-8 -*-

import random
import redis

#PORT = 3000
#PORT = 6379  #测试用
PORT = 6271  #测试用
EXPIRE_SECONDS = 3600*24*30

PORT_POI = 3000
#PORT_POI = 6271
REDIS_SERVER_POI = ['10.120.110.81']
#REDIS_SERVER_POI = ['10.231.145.99']
CONNS_POI = []
for ip in REDIS_SERVER_POI:
  CONNS_POI.append(redis.Redis(host=ip,port=PORT_POI))
print 'port poi:',PORT_POI,'Redis server poi:',REDIS_SERVER_POI

#REDIS_SERVER_HIST = ["10.121.84.217", "10.121.85.213", "10.121.104.93", "10.121.86.22"]
#REDIS_SERVER_HIST = ['127.0.0.1']  #测试用
REDIS_SERVER_HIST = ['10.231.145.99']  #测试用
CONNS_HIST = []
for ip in REDIS_SERVER_HIST:
  CONNS_HIST.append(redis.Redis(host=ip,port=PORT))

#REDIS_SERVER_RECENT = ["10.121.103.156", "10.121.103.218"]
#REDIS_SERVER_RECENT = ['127.0.0.1']  #测试用
REDIS_SERVER_RECENT = ['10.231.145.99']  #测试用
CONNS_RECENT = []
for ip in REDIS_SERVER_RECENT:
  CONNS_RECENT.append(redis.Redis(host=ip,port=PORT))

def getRandomConn(redisType):
  typeSet = set(['hist','recent','poi'])
  assert redisType in typeSet
  if redisType == 'hist':
    return CONNS_HIST[random.randint(0,len(CONNS_HIST)-1)]
  if redisType == 'poi':
    return CONNS_POI[random.randint(0,len(CONNS_POI)-1)]
  return CONNS_RECENT[random.randint(0,len(CONNS_RECENT)-1)]