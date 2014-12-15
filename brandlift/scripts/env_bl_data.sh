#!/bin/bash

#--- modify parameters here
export HDFS_DIR_ROOT=/projects
export HDFS_DIR=${HDFS_DIR_ROOT}/brandlift/prod
export HDFS_DIR_HOURLY=${HDFS_DIR}
export HDFS_DIR_RTB=/projects/campaign_analytics/prod
export HDFS_DIR_RET=${HDFS_DIR_RTB}
export HDFS_DIR_ALL=${HDFS_DIR_ROOT}/clickthroughrate/prod
export HDFS_DIR_BL=${HDFS_DIR}
export LOG_TMP_DIR=/tmp/bl
export S3KEY_DIR=/root/keys
export QUEUENAME_DATA_HOURLY=bt.hourly
export QUEUENAME_DATA_DAILY=bt.daily
export NUM_HOUR=24
export HOUR_WD=1
export HOUR_BL_DAILY=14
export NUM_SLEEP_DAILY=24
export NUM_SLEEP_HOUR=1
export NUM_SLEEP_HOUR_COPY=1
export HOUR_DELAY_DATA_BL=3
export HOUR_DELAY_DATA_BL_AGG=8
export LOGS_FILE_PATH=${LOGS_DIR}
export NUM_OF_REDUCERS=40
export LAST_HOUR=06

#--- end of modification

###ENV
STOP_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/stop-mapred.sh"
START_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/start-mapred.sh"
HADOOP="/usr/bin/hadoop"

#########Price-Volume/Inventory properties ###########
email_address="insights@sharethis.com"
