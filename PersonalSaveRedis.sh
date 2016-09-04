#!/usr/bin/env bash
FWDIR="$(cd `dirname $0`; pwd)"

prodDataRoot=$1
date=$2
index=$3

python ${FWDIR}/PersonalSaveRedis.py $prodDataRoot $date $index