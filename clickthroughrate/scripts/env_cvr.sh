#!/bin/bash

#--- modify parameters here
export HDFS_DIR_ROOT_MOD=/projects
export HDFS_DIR_MOD=${HDFS_DIR_ROOT_MOD}/cpa/prod
export HDFS_DIR_CVR_DATA=/user/btdev/projects/visitrate/prod
export LOG_TMP_DIR=/tmp/cvr
export S3KEY_DIR=/root/keys
export QUEUENAME_CTR_HOURLY=bt.hourly
export QUEUENAME_CTR_DAILY=bt.daily
export LOGS_FILE_PATH=${LOGS_DIR}
#--- end of modification

###ENV
STOP_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/stop-mapred.sh"
START_HADOOP_MAPREDUCE="/usr/lib/hadoop-0.20-mapreduce/bin/start-mapred.sh"
HADOOP="/usr/bin/hadoop"

#########Price-Volume/Inventory properties ###########
email_address="insights@sharethis.com"
