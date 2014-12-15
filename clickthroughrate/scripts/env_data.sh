#!/bin/bash

#--- modify parameters here
export HDFS_DIR_ROOT=/projects
export HDFS_DIR=${HDFS_DIR_ROOT}/clickthroughrate/prod
export HDFS_DIR_IMP_CLK=${HDFS_DIR_ROOT}/clickthroughrate/prod
#export HDFS_DIR_PC=${HDFS_DIR_ROOT}/clickthroughrate/prod
export HDFS_DIR_RTB=/projects/campaign_analytics/prod
export HDFS_DIR_RTB_IMP=${HDFS_DIR_RTB}
export HDFS_DIR_RTB_CLK=${HDFS_DIR_RTB}
export HDFS_DIR_RTB_CLK_SVR=${HDFS_DIR_RTB}
export HDFS_DIR_PV=${HDFS_DIR_ROOT}/pricevolume/prod
export HDFS_DIR_PC=${HDFS_DIR_PV}
export LOG_TMP_DIR=/tmp/ctr
export S3KEY_DIR=/root/keys
export QUEUENAME_DATA_HOURLY=bt.hourly
export QUEUENAME_DATA_DAILY=bt.daily
export NUM_HOUR=24
export HOUR_WD=1
export NUM_SLEEP_HOUR=1
export NUM_SLEEP_HOUR_COPY=1
export HOUR_DELAY_DATA=6
export LOGS_FILE_PATH=${LOGS_DIR}
export NUM_OF_REDUCERS=30
export NUM_OF_HOURS_CLICK=5
export NUM_OF_HOURS_BID=3

#--- end of modification

###ENV
STOP_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/stop-mapred.sh"
START_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/start-mapred.sh"
HADOOP="/usr/bin/hadoop"

#########Price-Volume/Inventory properties ###########
email_address="insights@sharethis.com"
