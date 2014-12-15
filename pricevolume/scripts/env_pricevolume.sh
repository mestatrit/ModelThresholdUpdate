#!/bin/bash

#--- modify parameters here
export HDFS_DIR=/projects/pricevolume/prod
export HDFS_DIR_RTB_BD=${HDFS_DIR}
export HDFS_DIR_RTB_PC=${HDFS_DIR}
export HDFS_DIR_RTB_NB=${HDFS_DIR}
export LOG_TMP_DIR=/tmp/pricevolume
export S3KEY_DIR=/root/keys
export QUEUENAME_PV_HOURLY=bt.hourly
export QUEUENAME_PV_DAILY=bt.daily
export QUEUENAME_INV_HOURLY=bt.hourly
export QUEUENAME_INV_DAILY=bt.daily
export NUM_SLEEP_HOUR=2
export LAST_HOUR=07
export NUM_HOUR_PV=24
export HOUR_WD_PV=1
export NUM_HOUR_INV=2
export HOUR_WD_INV=1
export HOUR_INV_DAILY=10
export HOUR_PV_DAILY=11
export HOUR_PV_BKUP_DAILY=14
export HOUR_INV_BKUP_DAILY=15
export HOUR_DELAY_DATA=2
export HOUR_DELAY_INV=3
export HOUR_DELAY_PV=4
export NUM_SLEEP_DAILY=24
export LOGS_FILE_PATH=${LOGS_DIR}
#--- end of modification

###ENV
#HADOOP_HOME=`ls -d /usr/local/hadoop-*`
STOP_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/stop-mapred.sh"
START_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/start-mapred.sh"
HADOOP="/usr/bin/hadoop"

#########Price-Volume/Inventory properties ###########
email_address="insights@sharethis.com"
