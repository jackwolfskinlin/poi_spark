# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from poi_offline.utils import UtilHelper,IoHelper
from poi_offline.entity import POI, CityLoc
from poi_offline.basic import Global

NAME_BLACK_LIST = set(["当前位置","所选位置","测试出发地","测试目的地"])

def prepareSql(dateStr):
  sqls = {}
  sqls["taxi"] = '''
    SELECT
    area,
    address AS start_name,
    srclng AS start_lng,
    srclat AS start_lat,
    destination AS dest_name,
    destlng AS dest_lng,
    destlat AS dest_lat
    FROM
    pdw.dw_order
    WHERE
    CONCAT(year,month,day)='dateStr'
    '''
  sqls["gulfstream"] = '''
    SELECT
    area,
    starting_name AS start_name,
    starting_lng AS start_lng,
    starting_lat AS start_lat,
    dest_name,
    dest_lng,
    dest_lat
    FROM
    gulfstream_ods.g_order
    WHERE
    CONCAT(year,month,day)='dateStr'
    '''
  sqls["beatles"] = '''
    SELECT
    from_area_id AS area,
    if(trim(from_address)='', from_name, concat_ws('|', from_address, from_name)) AS start_name,
    from_lng AS start_lng,
    from_lat AS start_lat,
    if(trim(to_address)='', to_name, concat_ws('|', to_address, to_name)) AS dest_name,
    to_lng AS dest_lng,
    to_lat AS dest_lat
    FROM
    beatles_ods.order_info
    WHERE
    CONCAT(year,month,day)='dateStr'
    '''
  for k,v in sqls.items():
    sqls[k] = v.replace('dateStr',dateStr)
  return sqls

def main():
  conf = SparkConf()
  dateStr = conf.get('spark.date')
  sc = SparkContext(conf=conf, appName='Loc City Data Prepare, '+dateStr)
  hc = HiveContext(sc)
  
  sqlDict = prepareSql(dateStr)
  #mergedRdd = sc.emptyRDD()
  mergedRdd = sc.parallelize([])
  for prod,sql in sqlDict.items():
    print sql
    df = hc.sql(sql)
    #print 'df count:', df.count()
    rdd = df.map(lambda x:toCityLoc(x, prod))
    rdd = rdd.filter(lambda x: x[0] is not None)
    rdd = rdd.map(lambda x:x[0])
    mergedRdd = mergedRdd.union(rdd)
    #break
  
  mergedRdd.cache()
  print 'mergedRdd count:', mergedRdd.count()
  fromRdd = mergedRdd.map(lambda cityLoc:((cityLoc.area, cityLoc.fromPoi.displayName), (cityLoc.fromPoi, 1L)))
  toRdd = mergedRdd.map(lambda cityLoc:((cityLoc.area, cityLoc.toPoi.displayName), (cityLoc.toPoi, 1L)))
  count(fromRdd, dateStr, 'from')
  count(toRdd, dateStr, 'to')
  print 'success'
  sc.stop()

def count(rdd, dateStr, type):
  fPath = Global.CITY_DAILY_ROOT+'/'+dateStr+'.'+type
  print IoHelper.deleteFileInHDFS(fPath)
  rdd = rdd.filter(lambda x:x[1][0].lng > 10 and x[1][0].lat > 10 and \
    x[1][0].displayName not in NAME_BLACK_LIST)
  print 'rdd count:', rdd.count()
  rdd = rdd.reduceByKey(lambda x,y:(y[0],x[1]+y[1]))
  rdd = rdd.map(lambda x:(x[0][0],x[1])).groupByKey(100)
  rdd.map(sortCityLoc).saveAsTextFile(fPath)
  print 'City',type,'file save to ',fPath
  
def sortCityLoc(row):
  area, poiList = row
  top = sorted(poiList,key=lambda x:x[1],reverse=True)[:1000]
  poiList = ['|'.join([str(poi[0]),str(poi[1])]) for poi in top]
  return str(area)+'\t'+'\t'.join(poiList)
  
def toCityLoc(row,prod):
  try:
    area = row.area
    if prod == 'taxi':
      area = int(row.area)
    fromName = row.start_name
    fromLng, fromLat = (float(row.start_lng),float(row.start_lat))
    if prod == 'taxi':
      fromLng, fromLat = UtilHelper.BDtoGCJ(float(row.start_lng), float(row.start_lat))
    toName = row.dest_name
    toLng, toLat = (float(row.dest_lng), float(row.dest_lat))
    if prod == 'taxi':
      toLng, toLat = UtilHelper.BDtoGCJ(float(row.dest_lng), float(row.dest_lat))
    fromDisplayName, fromAddress = UtilHelper.addSplit(fromName)
    toDisplayName, toAddress = UtilHelper.addSplit(toName)
    fromPoi = POI(area, fromDisplayName, fromAddress, toLng, toLat)
    toPoi = POI(area, toDisplayName, toAddress, toLng, toLat)
    return CityLoc(area,fromPoi,toPoi),None
  except Exception,ex:
    return None,ex

if __name__ == '__main__':
  main()