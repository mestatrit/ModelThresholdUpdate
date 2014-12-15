#!/bin/bash

#--- modify parameters here
#export HDFS_DIR_ROOT=/projects
export HDFS_DIR_ROOT=/user/btdev/projects
export HDFS_DIR=${HDFS_DIR_ROOT}/clickthroughrate/prod
export HDFS_DIR_ROOT_HOURLY=/projects
export HDFS_DIR_HOURLY=${HDFS_DIR_ROOT_HOURLY}/clickthroughrate/prod
#export HDFS_DIR_PV=${HDFS_DIR_ROOT}/pricevolume/prod
export HDFS_DIR_PV=/projects/pricevolume/prod
#export HDFS_DIR_ICP=${HDFS_DIR}
#export HDFS_DIR_ICP=${HDFS_DIR_ROOT}/clickthroughrate/prod
export HDFS_DIR_ICP=/projects/clickthroughrate/prod
export HDFS_DIR_ALL=/projects/clickthroughrate/prod
export HDFS_DIR_RTB=/projects/campaign_analytics/prod
export HDFS_DIR_RTB_IMP=${HDFS_DIR_RTB}
export HDFS_DIR_RTB_CLK=${HDFS_DIR_RTB}
export LOG_TMP_DIR=/tmp/ctr
export S3KEY_DIR=/root/keys
export QUEUENAME_CTR_HOURLY=bt.hourly
export QUEUENAME_CTR_DAILY=bt.daily
export NUM_HOUR=8
export HOUR_WD=1
export NUM_SLEEP_HOUR=5
export NUM_SLEEP_HOUR_COPY=1
export LAST_HOUR=06
export HOUR_CTR_DAILY=14
export HOUR_CTR_MODEL_DAILY=20
export HOUR_CTR_MV_DAILY=23
export HOUR_CTR_BKUP_DAILY=23
export HOUR_DELAY_CTR=7
export HOUR_DELAY_CTR_MV=8
export NUM_SLEEP_DAILY=24
export LOGS_FILE_PATH=${LOGS_DIR}
#--- end of modification

###ENV
STOP_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/stop-mapred.sh"
START_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/start-mapred.sh"
HADOOP="/usr/bin/hadoop"

#########Price-Volume/Inventory properties ###########
email_address="xibin@sharethis.com"
