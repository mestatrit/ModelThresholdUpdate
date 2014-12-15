#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

export s3_bucket_dir=s3n://sharethis-insights-backup
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

if [ $1 == -1 ]
then
    stringZ=`date -u --d " 1 day ago" +%Y%m%d`
else
    stringZ=$1
fi

echo "The date and time is ${stringZ}"  
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
v_date=${stringZ:0:8}
pDate="${yyyy}-${mm}-${dd}"

hour_sec=`date --date=$pDate +"%s"`
let hour_24=3600*24
let newZS=$hour_sec+$hour_24
stringAfter=`date -d @$newZS +"%Y%m%d"`

echo "The date and time of the processing day is ${stringAfter}"
yyyyAf=${stringAfter:0:4}
mmAf=${stringAfter:4:2}
ddAf=${stringAfter:6:2}
v_date_after=${stringAfter:0:8}
pDateAf="${yyyyAf}-${mmAf}-${ddAf}"

. ${CONF_DIR}/env_pricevolume.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][Inventory Backup] -- The daily inventory backup job failed while processing for day: $pDate"
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


CONF="-conf ${CONF_DIR}/core-insights.xml"

NOBID_BASE_DIR="${HDFS_DIR}/rtb_nb"
INV_BASE_DIR_HOURLY="${HDFS_DIR}/inventory_hourly"
INV_BASE_DIR_DAILY="${HDFS_DIR}/inventory_daily"
INV_DATE_DIR="${INV_BASE_DIR_DAILY}/${v_date}"


s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_inv_dir_daily=${s3_model_dir}/inventory_daily

qName="-Dmapred.job.queue.name=${QUEUENAME_INV_DAILY}"

log_dir_in=${LOG_TMP_DIR}/distcp_log_inv_${v_date}
log="-log ${log_dir_in}"

${HADOOP} fs -test -e ${INV_DATE_DIR}/_SUCCESS_BACKUP
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS_BACKUP Marker file in the partition folder: ${INV_DATE_DIR}. Backup for is already done!"
   #exit 0
else

let num_sleeps=${NUM_HOUR_INV}*6+${NUM_SLEEP_DAILY}

cnt_sleep_inv_bk=0
while [ $cnt_sleep_inv_bk -le $num_sleeps ]
do
  ${HADOOP} fs -test -e ${INV_DATE_DIR}/_SUCCESS
  if [ $? -eq 0 ]
  then
      echo "rm -rf ${log_dir_in}"
      rm -rf ${log_dir_in}
      echo "${HADOOP} distcp ${CONF} ${qName} ${INV_DATE_DIR} ${s3_inv_dir}"
      ${HADOOP} distcp ${CONF} ${qName} ${INV_DATE_DIR} ${s3_inv_dir_daily}
      if [ $? -eq 0 ]
      then
          ${HADOOP} fs -touchz ${INV_DATE_DIR}/_SUCCESS_BACKUP
      fi
      break
  else
      echo "INV Data Folder: ${INV_DATE_DIR} is not ready!"
      sleep 10m
      let cnt_sleep_inv_bk=${cnt_sleep_inv_bk}+1
      echo "The number of sleeps: ${cnt_sleep_inv_bk}"
      continue
  fi
done

if [ $cnt_sleep_inv_bk -gt $num_sleeps ]
then
    msg="Daily backup inventory job failed due to that ${INV_DATE_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

echo "delete data older than 10 days"
week_ago=`date --date="10 day ago" +%Y%m%d`
${HADOOP} fs -rm -r ${INV_BASE_DIR_DAILY}/${week_ago}
week_ago=`date --date="4 day ago" +%Y%m%d%H`
${HADOOP} fs -rm -r ${NOBID_BASE_DIR}/${week_ago}

/usr/bin/find ${LOGS_DIR}/*.log -type f -mtime +3 -delete
fi

#exit 0

