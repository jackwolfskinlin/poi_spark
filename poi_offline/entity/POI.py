# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
from poi_offline.utils import UtilHelper

class POI:
  def __init__(self,areaId,displayName,address,lng,lat):
    self.areaId = areaId
    self.displayName = displayName
    self.address = address
    self.lng = lng
    self.lat = lat

  def __str__(self):
    return '|'.join([self.displayName,self.address,UtilHelper.toStr(self.lng),UtilHelper.toStr(self.lat)])

