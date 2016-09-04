# -*- coding: utf-8 -*-

from datetime import datetime
from POI import POI
import json

class Trip:
  def __init__(self,area,hour,from_,to,orderType,prodId,time,current):
    self.area = area
    self.hour = hour
    self.from_ = from_
    self.to = to
    self.orderType = orderType
    self.prodId = prodId
    self.time = time
    self.current = current

  def format(self):
    res = str(self.area)+'|'
    res += str(self.hour)+'|'
    res += self.from_.displayName+'|'+self.from_.address+'|'+str(self.from_.lng)+'|'+str(self.from_.lat)+'|'
    res += self.to.displayName+'|'+self.to.address+'|'+str(self.to.lng)+'|'+str(self.to.lat)+'|'
    res += self.orderType+'|'
    res += str(self.prodId)+'|'
    res += self.time.strftime('%Y-%m-%d %H:%M:%S')+'|'
    res += str(self.current[0])+'|'
    res += str(self.current[1])
    return res

  @staticmethod
  def parse(paraStr):
    paras = paraStr.split('|')
    area = int(paras[0])
    hour = int(paras[1])
    from_ = POI(area,paras[2],paras[3],float(paras[4]),float(paras[5]))
    to = POI(area,paras[6],paras[7],float(paras[8]),float(paras[9]))
    orderType = paras[10]
    prodId = int(paras[11])
    dt = datetime.strptime(paras[12],'%Y-%m-%d %H:%M:%S')
    current = (0.0,0.0)
    try:
      current = (float(paras[13]),float(paras[14]))
    except:
      pass
    return Trip(area,hour,from_,to,orderType,prodId,dt,current)

  def toJson(self,whiteListMap):
    to_lng,to_lat = (self.to.lng,self.to.lat)
    if (self.area, self.to.displayName) in whiteListMap:
      to_lng,to_lat = whiteListMap[(self.area, self.to.displayName)]
    d = {}
    d['time'] = self.time.strftime('%Y-%m-%d %H:%M:%S')
    d['productId'] = str(self.prodId)
    d['orderType'] = str(self.orderType)
    d['from_displayname'] = self.from_.displayName
    d['from_address'] = self.from_.address
    d['from_lng'] = str(self.from_.lng)
    d['from_lat'] = str(self.from_.lat)
    d['to_displayname'] = self.to.displayName
    d['to_address'] = self.to.address
    d['to_lng'] = str(to_lng)
    d['to_lat'] = str(to_lat)
    #必须得按这个顺序拼凑json串，因为rec使用的时候有顺序限制
    val = '{'
    val += '"time":"'+d['time']+'",'
    val += '"productId":"'+d['productId']+'",'
    val += '"orderType":"'+d['orderType']+'",'
    val += '"from_displayname":"'+d['from_displayname']+'",'
    val += '"from_address":"'+d['from_address']+'",'
    val += '"from_lng":"'+d['from_lng']+'",'
    val += '"from_lat":"'+d['from_lat']+'",'
    val += '"to_displayname":"'+d['to_displayname']+'",'
    val += '"to_address":"'+d['to_address']+'",'
    val += '"to_lng":"'+d['to_lng']+'",'
    val += '"to_lat":"'+d['to_lat']+'"'
    val += '}'
    return val
