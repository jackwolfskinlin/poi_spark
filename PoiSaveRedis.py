# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
#由于bigdata机器上不允许安装软件，所以直接使用egg包
sys.path.append(os.path.abspath('.')+'/'+'redis-2.10.3-py2.7.egg')
from poi_offline.rec.basic import RecConfig

EXPIRE_SECONDS = 3600*24*90

def main(localPath):
  ff = open(localPath)
  lines = ff.readlines()
  ff.close()

  kvs = {}
  count = 0
  stage = 0
  stageSize = 10000
  print 'total stage:',len(lines)/stageSize
  for line in lines:
    try:
      key, val = line.strip().split('\t')
      kvs[key] = val
    except:
      continue
    count += 1
    stage += 1
    if stage % stageSize == 0:
      print 'stage :',stage/stageSize
      print 'test kv',key,val
    if count >= 180:
      redis = RecConfig.getRandomConn("poi")
      redis.mset(kvs)
      items = kvs.items()
      for k,v in items:
        redis.expire(k,EXPIRE_SECONDS)
      kvs.clear()
      count = 0
  if count > 0:
    redis = RecConfig.getRandomConn("poi")
    redis.mset(kvs)
    for k,v in kvs.items():
      redis.expire(k,EXPIRE_SECONDS)
    kvs.clear()
  print "over"

if __name__ == '__main__':
  if len(sys.argv) != 2:
    print 'Usage cmd localPath'
    exit(1)
  main(sys.argv[1])