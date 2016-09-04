#!/usr/bin/env bash

export SPARK_HOME=/home/xiaoju/spark-1.6.0
export PATH=$SPARK_HOME/bin:$PATH

FWDIR="$(cd `dirname $0`; pwd)"
date=$1

spark-submit -v \
    --name LOC_REC \
    --driver-memory 4g \
    --queue strategy \
    --conf "spark.dynamicAllocation.minExecutors=10" \
    --conf "spark.dynamicAllocation.maxExecutors=40" \
    --conf spark.date=${date} \
    --py-files ${FWDIR}/dist/poi_offline-0.1.0-py2.6.egg \
    ${FWDIR}/PrepareDailyCityHot.py