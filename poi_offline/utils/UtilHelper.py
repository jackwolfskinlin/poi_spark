# -*- coding: utf-8 -*-

import math

X_PI = 3000 * 0.0174532925194
MILL = 10000000

# only taxi need transfrom
def BDtoGCJ(lng, lat):
  if lng < 1 or lat < 1:
    return (0.0, 0.0)
  else:
    x = lng - 0.0065
    y = lat - 0.006
    z = math.sqrt(x * x + y * y) - 0.00002 * math.sin(y * X_PI)
    theta = math.atan2(y, x) - 0.000003 * math.cos(x * X_PI)
    gg_lng = round(z * math.cos(theta) * MILL) / MILL
    gg_lat = round(z * math.sin(theta) * MILL) / MILL
    return (gg_lng, gg_lat)

def toStr(x):
  return '%.6f' % x

def distinct(iterObj,getKey):
  d = {}
  for v in iterObj:
    key = getKey(v)
    if key not in d:
      d[key] = v
  return d.items()

def groupBy(iterObj,getKey):
  d = {}
  for v in iterObj:
    key = getKey(v)
    if key not in d:
      d[key] = [v]
    else:
      d[key].append(v)
  return d.items()

def getPoiWhiteList(filePath):
  file = open(filePath,'r')
  tups = []
  for line in file.readlines():
    ll = line.split('\t')
    try:
      areaId = int(ll[1])
      poiName = ll[2]
      lngC = float(ll[4])
      latC = float(ll[5])
      tups.append(((areaId,poiName),(lngC,latC),None))
    except Exception,ex:
      tups.append((None,None,ex))
  file.close()

  poi2Loc = {}
  for tup in tups:
    if tup[0] is None:
      print tup[2]
    else:
      poi2Loc[tup[0]] = tup[1]
  return poi2Loc

def addSplit(s, sep='|'):
  res = s.split(sep)
  l = len(res)
  if l == 0: return "",""
  elif l==1: return res[0],""
  elif l==2: return res[1],res[0]
  else: raise Exception(s+" format is incorrect")
