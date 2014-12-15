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
#echo "$1"

if [ $1 == -1 ]
then
    let delay_hour=2
    stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
else
    stringZ=$1
fi

#let delay_hour=2
#stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`

echo "The date and time of the processing hour is ${stringZ}"	
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
hh=${stringZ:8:2}
v_date=${stringZ:0:8}
v_date_hour=${stringZ:0:10}
pDate="${yyyy}-${mm}-${dd}"

. ${CONF_DIR}/env_pricevolume.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][Inventory Hourly] -- The hourly inventory job failed while processing for hour: $hh on $pDate"
##maillist="insights@sharethis.com"
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

NOBID_BASE_DIR="${HDFS_DIR_RTB_NB}/rtb_nb"
NOBID_DATE_DIR="${NOBID_BASE_DIR}/${v_date}"
#NOBID_HOUR_DIR="${NOBID_DATE_DIR}/${hh}"
NOBID_HOUR_DIR="${NOBID_BASE_DIR}/${v_date_hour}"

INV_BASE_DIR_HOURLY="${HDFS_DIR}/inventory_hourly"
INV_BASE_DIR_DAILY="${HDFS_DIR}/inventory_daily"
INV_DATE_DIR="${INV_BASE_DIR_DAILY}/${v_date}"
INV_HOUR_DIR="${INV_BASE_DIR_HOURLY}/${v_date_hour}"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_inv_dir_hourly=${s3_bucket_dir}/inventory_hourly

qName="-Dmapred.job.queue.name=${QUEUENAME_INV_HOURLY}"

inv_jar=${JAR_DIR}/PriceVolumeRunner.jar
inv_main="com.sharethis.adoptimization.inventory.HourlyInventoryMain" 
parm_date="ProcessingDate=${pDate}"
parm_hour="HourOfDay=${hh}"
parm_nobid="DataFilePath=${NOBID_BASE_DIR}/"
parm_out_hourly="OutFilePathHourly=${INV_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${INV_BASE_DIR_DAILY}/"
parm_res="${CONF_DIR}/inv.properties"
parm_q_name="QueueName=${QUEUENAME_INV_HOURLY}"

inv_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_hour} ${parm_nobid} ${parm_out_hourly} ${parm_out_daily}"

echo "Starting the inventory hourly aggregation task ..."

${HADOOP} fs -test -e ${INV_HOUR_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder :${INV_HOUR_DIR}. Please clean this folder in order to reprocess this again"
#   exit 0
else

echo "${HADOOP} fs -rm -r ${INV_HOUR_DIR}"
${HADOOP} fs -rm -r ${INV_HOUR_DIR} 

cnt_sleep_inv=0
while [ $cnt_sleep_inv -le $NUM_SLEEP_HOUR ]
do
  ${HADOOP} fs -test -e ${NOBID_HOUR_DIR}/_SUCCESS 
  if [ $? -eq 0 ]
  then
      echo "${HADOOP} jar ${inv_jar} ${inv_main} ${inv_parm}"
      ${HADOOP} jar ${inv_jar} ${inv_main} ${inv_parm} 
      if [ $? -eq 0 ]
      then
          echo "${HADOOP} fs -touchz ${INV_HOUR_DIR}/_SUCCESS"
          ${HADOOP} fs -touchz ${INV_HOUR_DIR}/_SUCCESS

          ${HADOOP} fs ${CONF} -test -e ${s3_inv_dir_hourly}
          if [ $? -ne 0 ]
          then
              ${HADOOP} fs ${CONF} -mkdir ${s3_inv_dir_hourly}/
          fi
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${INV_HOUR_DIR} ${s3_inv_dir_hourly}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${INV_HOUR_DIR} ${s3_inv_dir_hourly}
          ${HADOOP} fs ${CONF} -rm -r ${s3_inv_dir_hourly}/_distcp*
          #if [ $? -eq 0 ]
          #then
          #    ${HADOOP} fs -touchz ${INV_DATE_DIR}/_SUCCESS_BACKUP
          #fi
      else
          msg="Inventory hourly job failed ...please investigate"
          sendalert $msg
      fi
      break
  else
      echo "The no bid data under ${NOBID_HOUR_DIR} is not ready."
      sleep 5m 
      let cnt_sleep_inv=${cnt_sleep_inv}+1
      echo "The number of sleeps: ${cnt_sleep_inv}"
      continue
  fi
done

if [ $cnt_sleep_inv -gt $NUM_SLEEP_HOUR ]
then 
    msg="Inventory hourly job failed due to that ${NOBID_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

hours_ago=`date --date="4 day ago" +%Y%m%d%H`
${HADOOP} fs -rm -r ${INV_BASE_DIR_HOURLY}/${hours_ago}
${HADOOP} fs -rm -r ${NOBID_BASE_DIR}/${hours_ago}

fi

#exit 0

