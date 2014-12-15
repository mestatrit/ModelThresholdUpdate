#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

export BIN_DIR=${PROJ_DIR}/bin
export JAR_DIR=${PROJ_DIR}/jar
export CONF_DIR=${PROJ_DIR}/conf
export LOGS_DIR=${PROJ_DIR}/logs
export TMP_DIR=${PROJ_DIR}/tmp

if [ ! -d ${LOGS_DIR} ]; then
        mkdir -p ${LOGS_DIR}
fi
if [ ! -d ${TMP_DIR} ]; then
        mkdir -p ${TMP_DIR}
fi

#check to see if all arguments passed

if [ $1 == -1 ]
then
    stringZ=`date -u --d "1 day ago" +%Y%m%d`
else
    stringZ=$1
fi

#stringZ=`date -u +%Y%m%d -d  "24 hour ago"`

echo "The date and time of the processing day is ${stringZ}"  
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
v_date=${stringZ:0:8}
v_date_hour=${stringZ:0:10}
pDate="${yyyy}-${mm}-${dd}"

hour_sec=`date --date=$pDate +"%s"`
let hour_24=3600*24
let newZS=$hour_sec+$hour_24
stringAfter=`date -d @$newZS +"%Y%m%d"`

echo "The date and time of the current day is ${stringAfter}"
yyyyAf=${stringAfter:0:4}
mmAf=${stringAfter:4:2}
ddAf=${stringAfter:6:2}
v_date_after=${stringAfter:0:8}
v_date_after_hour=${stringAfter:0:10}
pDateAf="${yyyyAf}-${mmAf}-${ddAf}"

. ${CONF_DIR}/env_pricevolume.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][PV Daily] -- The daily pv job failed while processing for day: $pDate"
maillist=${email_address}

echo "The following step was failed while the job progressing" >> /tmp/mailbody_$$.txt
echo "Warning message :: $message " >> /tmp/mailbody_$$.txt

cat - /tmp/mailbody_$$.txt <<EOF | /usr/sbin/sendmail -t
To:$maillist
From:Watchdog<watchdog@sharethis.com>
Subject:${subject}

EOF

rm /tmp/mailbody_$$.txt
#exit 1
}

PV_BASE_DIR_HOURLY="${HDFS_DIR}/price_volume_hourly"
PV_BASE_DIR_DAILY="${HDFS_DIR}/price_volume_daily"
PV_DATE_DIR="${PV_BASE_DIR_DAILY}/${v_date}"
#PV_HOUR_DIR="${PV_DATE_DIR}/${hh}"
PV_HOUR_DIR="${PV_BASE_DIR_HOURLY}/${v_date_hour}"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_pv_dir_daily=${s3_bucket_dir}/price_volume_daily/${v_date}

qName="-Dmapred.job.queue.name=${QUEUENAME_PV_DAILY}"

pv_jar=${JAR_DIR}/PriceVolumeRunner.jar
pv_main="com.sharethis.adoptimization.pricevolume.PriceVolumeMain" 
parm_date="ProcessingDate=${pDate}"
parm_out_hourly="OutFilePathHourly=${PV_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${PV_BASE_DIR_DAILY}/"
parm_res="${CONF_DIR}/pv.properties"
parm_q_name="QueueName=${QUEUENAME_PV_DAILY}"

pv_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_out_hourly} ${parm_out_daily}"

${HADOOP} fs -test -e ${PV_DATE_DIR}/_SUCCESS 
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder :${PV_DATE_DIR}. Please clean this folder in order to reprocess this again"
   #exit 0
else

PV_LAST_HOUR_DIR="${PV_BASE_DIR_HOURLY}/${v_date_after}${LAST_HOUR}"

let num_sleeps=${NUM_HOUR_PV}*6+${NUM_SLEEP_DAILY}
cnt_sleep_pv_d=0

while [ $cnt_sleep_pv_d -le $num_sleeps ]
do
  ${HADOOP} fs -test -e ${PV_LAST_HOUR_DIR}/_SUCCESS 
  if [ $? -eq 0 ] || [ $cnt_sleep_pv_d -eq $num_sleeps ]
  then
      echo "${HADOOP} jar ${pv_jar} ${pv_main} ${pv_parm}"
      ${HADOOP} jar ${pv_jar} ${pv_main} ${pv_parm}
      if [ $? -eq 0 ]
      then
          ${HADOOP} fs -touchz ${PV_DATE_DIR}/_SUCCESS
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${PV_DATE_DIR} ${s3_pv_dir_daily}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${PV_DATE_DIR} ${s3_pv_dir_daily}
          ${HADOOP} fs ${CONF} -rm -r ${s3_pv_dir_daily}/_distcp*
          #if [ $? -eq 0 ]
          #then
          #    ${HADOOP} fs -touchz ${PV_DATE_DIR}/_SUCCESS_BACKUP
          #fi
      else
          msg="PV daily job failed...please investigate"
          sendalert $msg
      fi
      break
  else
      echo "PV Last Hour Data: ${PV_LAST_HOUR_DIR} is not ready!"
      sleep 10m
      let cnt_sleep_pv_d=${cnt_sleep_pv_d}+1
      echo "The number of sleeps: ${cnt_sleep_pv_d}"
      continue
  fi
done

if [ $cnt_sleep_pv_d -gt $num_sleeps ]
then
    msg="PV daily job failed due to that ${PRICE_LAST_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

echo "Delete the daily data older than 10 days"
days_ago=`date --date="10 day ago" +%Y%m%d`
${HADOOP} fs -rm -r ${PV_BASE_DIR_DAILY}/${days_ago}

/usr/bin/find ${LOGS_DIR}/*.log -type f -mtime +3 -delete

fi

#exit 0
