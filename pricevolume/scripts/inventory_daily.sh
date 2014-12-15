#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

#export s3_bucket_dir=s3n://sharethis-insights-backup
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
pDateAf="${yyyyAf}-${mmAf}-${ddAf}"

. ${CONF_DIR}/env_pricevolume.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][Inventory Daily] -- The daily inventory job failed while processing for $pDate"
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


#CONF="-conf ${PROD_CONF}"
CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

INV_BASE_DIR_HOURLY="${HDFS_DIR}/inventory_hourly"
INV_BASE_DIR_DAILY="${HDFS_DIR}/inventory_daily"
INV_DATE_DIR="${INV_BASE_DIR_DAILY}/${v_date}"
INV_HOUR_DIR="${INV_BASE_DIR_HOURLY}/${v_date_hour}"

INV_LAST_HOUR_DIR="${INV_BASE_DIR_HOURLY}/${v_date_after}${LAST_HOUR}"


s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_inv_dir_daily=${s3_bucket_dir}/inventory_daily/${v_date}

qName="-Dmapred.job.queue.name=${QUEUENAME_INV_DAILY}"

inv_jar=${JAR_DIR}/PriceVolumeRunner.jar
inv_main="com.sharethis.adoptimization.inventory.PeriodicalInventoryMain" 
parm_date="ProcessingDate=${pDate}"
parm_out_hourly="OutFilePathHourly=${INV_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${INV_BASE_DIR_DAILY}/"
#parm_upload="UploadingInvFlag=false"
parm_res="${CONF_DIR}/inv.properties"
parm_q_name="QueueName=${QUEUENAME_INV_DAILY}"

inv_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_out_hourly} ${parm_out_daily}"


echo "Starting the inventory daily aggregation task ..."

${HADOOP} fs -test -e ${INV_DATE_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder :${INV_DATE_DIR}. Please clean this folder in order to reprocess this again"
#   exit 0
else

let num_sleeps=${NUM_HOUR_INV}*6+${NUM_SLEEP_DAILY}
cnt_sleep_inv_d=0

while [ $cnt_sleep_inv_d -le $num_sleeps ]
do
  ${HADOOP} fs -test -e ${INV_LAST_HOUR_DIR}/_SUCCESS
  if [ $? -eq 0 ] || [ $cnt_sleep_inv_d -eq $num_sleeps ]
  then
      echo "${HADOOP} jar ${inv_jar} ${inv_main} ${inv_parm}"
      ${HADOOP} jar ${inv_jar} ${inv_main} ${inv_parm}
      if [ $? -eq 0 ]
      then
          ${HADOOP} fs -touchz ${INV_DATE_DIR}/_SUCCESS
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${INV_DATE_DIR} ${s3_inv_dir_daily}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${INV_DATE_DIR} ${s3_inv_dir_daily}
          ${HADOOP} fs ${CONF} -rm -r ${s3_inv_dir_daily}/_distcp*
          #if [ $? -eq 0 ]
          #then
          #    ${HADOOP} fs -touchz ${INV_DATE_DIR}/_SUCCESS_BACKUP
          #fi
      else    
          msg="Hadoop MapReduce daily job failed for inventory. Please investigate!"
          sendalert $msg
      fi
      break
  else
      echo "The inventory hourly data under ${INV_LAST_HOUR_DIR} is not ready."
      sleep 10m
      let cnt_sleep_inv_d=${cnt_sleep_inv_d}+1
      echo "The number of sleeps: ${cnt_sleep_inv_d}"
 
      continue
  fi
done

if [ $cnt_sleep_inv_d -gt $num_sleeps ]
then 
    msg="Inventory daily job failed due to that ${INV_LAST_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

echo "Delete the daily data older than 10 days"
days_ago=`date --date="10 day ago" +%Y%m%d`
${HADOOP} fs -rm -r ${INV_BASE_DIR_DAILY}/${days_ago}

/usr/bin/find ${LOGS_DIR}/*.log -type f -mtime +3 -delete

fi

#exit 0

