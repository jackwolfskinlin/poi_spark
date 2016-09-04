# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext
from poi_offline.utils import IoHelper

POI_DAILY_ROOT = '/user/strategy/rec'+'/poi_daily'

def poiIsValid(poi):
  if poi['displayname'].find('\r') > -1:
    return False
  if poi['displayname'].find('\n') > -1:
    return False
  if poi['address'].find('\r') > -1:
    return False
  if poi['address'].find('\n') > -1:
    return False
  if poi['oldaddr'].find('\r') > -1:
    return False
  if poi['oldaddr'].find('\n') > -1:
    return False
  return True

def format(row):
  arr = []
  try:
    d = json.loads(row.json_str)
    for poi in d['result']:
      if poiIsValid(poi) == False:
        continue
      arr.append(('POI_'+str(poi['area'])+'_'+poi['displayname'],json.dumps(poi,ensure_ascii=False)))
  except Exception,ex:
    pass
  return arr

def main():
  conf = SparkConf()
  dateStr = conf.get('spark.date')
  sc = SparkContext(appName='get sug poi of today from log table',conf=conf)
  hc = HiveContext(sc)

  sql = '''select log.param['json_str'] as json_str from pbs_dw.ods_log_ws_addrsuggestion as log
    where concat(year,month,day)=dateStr'''
  sql = sql.replace('dateStr',dateStr)
  print sql

  df = hc.sql(sql)
  rdd = df.flatMap(lambda x:format(x)).reduceByKey(lambda x,y:x,200).map(lambda x:x[0]+'\t'+x[1])
  fPath = POI_DAILY_ROOT+'/'+'poilist.'+dateStr
  print fPath
  print IoHelper.deleteFileInHDFS(fPath)
  rdd.saveAsTextFile(fPath)

  sc.stop()
  print 'over'

if __name__ == '__main__':
  main()