#!/usr/bin/env bash

FWDIR="$(cd `dirname $0`; pwd)"
remoteTar=all.tgz
remotePoiList=poilist.txt
tarFile=${FWDIR}/${remoteTar}
poi05workspace=/data/xiaoju/yangjiecloud/rec_offline_workspace/

tar -cvf ${tarFile} ./

tee tt.exp <<-EOF
set timeout 300
spawn scp -P 36000 ${tarFile} xiaoju@10.231.135.81:${poi05workspace}
  expect {
    "*yes/no*" {send "yes\r"; exp_continue}
    "*password:*" {send "MhxzKhl\r" }
  }
  expect EOF
  exit
EOF
expect tt.exp

if [ ${?} -ne 0 ];then
  exit 1
fi

tee tt.exp <<-EOF
set timeout 259200
spawn ssh xiaoju@10.231.135.81
  expect {
    "*yes/no*" {send "yes\r"; exp_continue}
    "*password:*" {send "MhxzKhl\r" }
  }
  expect "~\$*"
  send "cd ${poi05workspace}\r"
  expect "rec_offline_workspace\$*"
  send "tar -xvf ${remoteTar}\r"
  expect "rec_offline_workspace\$*"
  send "python PoiSaveRedis.py ${remotePoiList}\r"
  expect {
    "rec_offline_workspace\$*" { exit }
    "*Traceback*" { exit 1 }
  }
EOF
expect tt.exp

if [ ${?} -ne 0 ];then
  exit 1
fi