# -*- coding: utf-8 -*-

import subprocess

def deleteFileInHDFS(filePath):
  ret = subprocess.call(['hdfs','dfs','-rm','-r',filePath])
  if ret == 0:
    return True
  return False
