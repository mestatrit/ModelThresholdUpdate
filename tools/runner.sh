#!/bin/bash

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo "install root is $install_root"

PROJ_DIR=$install_root
RUN_DIR=${PROJ_DIR}/run
CONF_DIR=${PROJ_DIR}/conf

if [ ! -d ${RUN_DIR} ]; then
    mkdir -p ${RUN_DIR}
fi

job=$1
today=`date`
todayL=`date +%s`
currentdate=`date --d="$today" +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
currentdateago=`date --d="$today 10 days ago" +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
todayH=${currentdate:0:4}${currentdate:5:2}${currentdate:8:2}${currentdate:11:2}
todayHago=${currentdateago:0:4}${currentdateago:5:2}${currentdateago:8:2}${currentdateago:11:2}

echo ${currentdate}' -- start runnning...'

database=$2
table=$3
#rtbDelivery.UserSegmentMapping
column=$4
#createDate
timethreshold=$5
#day

sendalert() {

subject="Insufficient data insertion from job $job"
maillist="bli@sharethis.com,prasanta@sharethis.com,Xibin@sharethis.com,changyi@sharethis.com,markku@sharethis.com"

echo " Error stack as follows :: $msg" >> /tmp/mailbody_$$.txt

cat - /tmp/mailbody_$$.txt <<EOF | /usr/sbin/sendmail -t
To:$maillist
From:Watchdog<watchdog@sharethis.com>
Subject:${subject}

EOF
rm /tmp/mailbody_$$.txt
}

. ${CONF_DIR}/env.sh

if [ $database = 'rtbDelivery' ]
then 
  host=$rtbDelivery_ip
  user=$rtbDelivery_user
  password=$rtbDelivery_pw
elif [ $database = 'rtbStats' ]
then
  host=$rtbStats_ip
  user=$rtbStats_user
  password=$rtbStats_pw
else
  msg='Error -- wrong db in argument. force to exit'
  echo $msg 
  exit 1
fi

IFS="/" read -ra job_array <<< "$job" 
job_array_len=${#job_array[@]}
job_child=${job_array[(${job_array_len}-1)]}
IFS=" " read -ra job_child_array <<< $job_child
job_child_head=${job_child_array[0]}
progressfile=${RUN_DIR}/${job_child_head}_${database}_${table}

currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
if ! [ -a $progressfile ]
then
  echo "${currentdate} touching $progressfile..."
  touch $progressfile  
  . ${job}
  rm $progressfile
  currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
  echo "${currentdate} removed $progressfile..."
else
  msg=${currentdate}' -- Error -- previous job is still running. force to exit'
  echo $msg
  sendalert 
  exit 1
fi

currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
echo ${currentdate}' -- Finished runnning.'

if [ $timethreshold = 'day' ]
then
  thresholddate=${todayH:0:4}'-'${todayH:4:2}'-'${todayH:6:2}' 00:00:00'
elif [ $timethreshold = 'long' ]
then
  thresholddate=${todayL}'000'
elif [ $timethreshold -ge 0 ]
then
  thresholddate=`date --d="$today $timethreshold minutes ago" +%Y-%m-%d' '\%H\:\%M\:00`
else
  msg='No right threshold is set to cross check data existence. Force to exit' 
  echo $msg
  exit 0  
fi

cntstring=`mysql -h${host} -u${user} -p${password} -e "SELECT COUNT(1) FROM ${database}.${table} WHERE ${column} >= '${thresholddate}'" mysql`
if [ $? -ne 0 ]
then
  currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
  msg=${currentdate}' -- Error in mysql when checking data: connecting db and fetching data' 
  echo $msg
  sendalert $msg 
  exit 1
fi

let cnt=`echo $cntstring | sed s/COUNT\(1\)\ //`
if ! [[ "$cnt" =~ ^[0-9]+$ ]] 
then
  currentdate=`date +\%Y\-\%m\-\%d' '\%H\:\%M\:\%S`
  msg=${currentdate}' -- Error in mysql -- wrong fetched result='$cntstring 
  echo $msg
  sendalert $msg
  exit 1
else
  if [ $cnt -le 0 ]
  then
    msg='Error at thresholddate='${thresholddate}' -- Not enough rows in table '${database}'.'${table}' -- count='$cnt
    echo $msg
    sendalert $msg
    exit 1
  else
    echo 'at thresholddate='${thresholddate}' -- '$cnt' number of rows in table '${database}'.'${table} ' -- Success run'
    exit 0
  fi
fi

