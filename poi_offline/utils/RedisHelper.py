# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
#由于bigdata机器上不允许安装软件，所以直接使用egg包
sys.path.append(os.path.abspath('.')+'/'+'redis-2.10.3-py2.7.egg')
import redis

def getRedisConn(host,port):
  return redis.Redis(host=host,port=port)

def writeToRedis(localPath,expairTime,host,port,stageSize=10000):
  ff = open(localPath)
  lines = ff.readlines()
  ff.close()

  stageCount = 0
  print 'total stage:',len(lines)/stageSize
  conn = getRedisConn(host,port)
  for line in lines:
    try:
      key, val = line.strip().split('\t')
    except:
      continue
    stageCount += 1
    if stageCount % stageSize == 0:
      print 'stage:',stageCount/stageSize
      print 'sample kv:',key,val
    conn.setex(key,val,expairTime)
  print 'over'