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
    stringZ=`date -u --d "1 day ago" +%Y%m%d`
else
    stringZ=$1
fi

echo "The date and time of the processing day is ${stringZ}"  
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
v_date=${stringZ:0:8}
v_date_hour=${stringZ:0:10}
pDate="${yyyy}-${mm}-${dd}"

hour_sec=`date --date="$pDate" +%s`
let hour_24=3600*24
let newZS=$hour_sec+$hour_24
stringAfter=`date -d @$newZS +"%Y%m%d"`

let newBefore=$hour_sec-$hour_24
stringBefore=`date -d @$newBefore +"%Y%m%d"`
v_date_before=${stringBefore:0:8}

echo "The date and time of the current day is ${stringAfter}"
yyyyAf=${stringAfter:0:4}
mmAf=${stringAfter:4:2}
ddAf=${stringAfter:6:2}
v_date_after=${stringAfter:0:8}
pDateAf="${yyyyAf}-${mmAf}-${ddAf}"

day_sec=`date --date=$stringZ +"%s"`
let week_12=3600*24*12*7
let newZS_w12=$day_sec-$week_12
string12w=`date -d @$newZS_w12 +"%Y%m%d"`

let week_4=3600*24*9*7
let newZS_w4=$day_sec-$week_4
string4w=`date -d @$newZS_w4 +"%Y%m%d"`

v_date_12w=${string12w:0:8}
v_date_4w=${string4w:0:8}

. ${CONF_DIR}/env_ctr.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][CTR Daily] -- The daily ctr job failed while processing for day: $pDate"
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


CTR_BASE_DIR_HOURLY="${HDFS_DIR_HOURLY}/ctr_hourly"
CTR_BASE_DIR_DAILY="${HDFS_DIR}/ctr_daily"
CTR_DATE_DIR="${CTR_BASE_DIR_DAILY}/${v_date}"
CTR_HOUR_DIR="${CTR_BASE_DIR_HOURLY}/${v_date_hour}"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=102400"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_ctr_dir=${s3_model_dir}/ctr_daily/

s3_ctr_daily_dir=${s3_bucket_dir}/ctr_daily/${v_date}

ctr_jar=${JAR_DIR}/ClickThroughRateRunner.jar
ctr_main="com.sharethis.adoptimization.clickthroughrate.DailyGeneratingCTREachTaskMain"
parm_date="ProcessingDate=${pDate}"
parm_out_hourly="OutFilePathHourly=${CTR_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${CTR_BASE_DIR_DAILY}/"

parm_res="${CONF_DIR}/ctr.properties"
parm_q_name="QueueName=${QUEUENAME_CTR_DAILY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_DAILY}"

ctr_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_out_hourly} ${parm_out_daily}"

echo "Checking whether job is running for the day which has already processed in which case User has to clean up the folder first"

${HADOOP} fs -test -e ${CTR_DATE_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder :${CTR_DATE_DIR}. Please clean this folder in order to reprocess this again"
   #exit 0
else

CTR_LAST_HOUR_DIR="${CTR_BASE_DIR_HOURLY}/${v_date_after}${LAST_HOUR}"

let num_sleeps=${NUM_HOUR}*6+${NUM_SLEEP_DAILY}

cnt_sleep_ctr_d=0
while [ $cnt_sleep_ctr_d -le $num_sleeps ]
do
  ${HADOOP} fs -test -e ${CTR_LAST_HOUR_DIR}/_SUCCESS
  if [ $? -eq 0 ] || [ $cnt_sleep_ctr_d -eq $num_sleeps ]
  then
      #echo "${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_12w}/_SUCCESS"
      #${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_12w}/_SUCCESS
      #if [ $? -ne 0 ] 
      #then
      #    echo "${BIN_DIR}/data_ctr_daily.sh ${v_date_12w} >> ${LOGS_FILE_PATH}/data_ctr_daily_${string12w}.log 2>&1"
      #    ${BIN_DIR}/data_ctr_daily.sh ${v_date_12w} >> ${LOGS_FILE_PATH}/data_ctr_daily_${string12w}.log 2>&1 
      #fi
      #echo "${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_4w}/_SUCCESS"
      #${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_4w}/_SUCCESS
      #if [ $? -ne 0 ] 
      #then
      #    echo "${BIN_DIR}/data_ctr_daily.sh ${v_date_4w} >> ${LOGS_FILE_PATH}/data_ctr_daily_${string4w}.log 2>&1"
      #    ${BIN_DIR}/data_ctr_daily.sh ${v_date_4w} >> ${LOGS_FILE_PATH}/data_ctr_daily_${string4w}.log 2>&1
      #fi

      #echo "${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_before}/_SUCCESS"
      #${HADOOP} fs -test -e ${CTR_BASE_DIR_DAILY}/${v_date_before}/_SUCCESS
      #if [ $? -ne 0 ] 
      #then
      #    echo "${BIN_DIR}/data_ctr_daily.sh ${v_date_before} >> ${LOGS_FILE_PATH}/data_ctr_daily_${stringBefore}.log 2>&1"
      #    ${BIN_DIR}/data_ctr_daily.sh ${v_date_before} >> ${LOGS_FILE_PATH}/data_ctr_daily_${stringBefore}.log 2>&1
      #fi

      echo "${HADOOP} jar ${ctr_jar} ${ctr_main} ${ctr_parm}"
      ${HADOOP} jar ${ctr_jar} ${ctr_main} ${ctr_parm}
      if [ $? -eq 0 ]
      then
          echo "${HADOOP} fs -touchz ${CTR_DATE_DIR}/_SUCCESS"
          ${HADOOP} fs -touchz ${CTR_DATE_DIR}/_SUCCESS
          #${HADOOP} fs -rm -r ${CTR_BASE_DIR_DAILY}/${v_date_12w}
          #${HADOOP} fs -rm -r ${CTR_BASE_DIR_DAILY}/${v_date_4w} 
          #echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_DATE_DIR} ${s3_ctr_dir}"
          #${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_DATE_DIR} ${s3_ctr_dir}
          #${HADOOP} fs ${CONF} -rm -r ${s3_ctr_dir}/_distcp*

          #${HADOOP} fs ${CONF} -test -e ${s3_ctr_daily_dir}
          #if [ $? -ne 0 ]
          #then
          #    ${HADOOP} fs ${CONF} -mkdir ${s3_ctr_daily_dir}
          #fi

          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_DATE_DIR} ${s3_ctr_daily_dir}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_DATE_DIR} ${s3_ctr_daily_dir}
          ${HADOOP} fs ${CONF} -rm -r ${s3_ctr_daily_dir}/_distcp*
      else
          msg="Hadoop MapReduce job failed for CTR daily ..please investigate"
          sendalert $msg
      fi
      break
  else
      echo "CTR Last Hour Data: ${CTR_LAST_HOUR_DIR} is not ready!"
      sleep 10m
      let cnt_sleep_ctr_d=${cnt_sleep_ctr_d}+1
      echo "The number of sleeps: ${cnt_sleep_ctr_d}"
      continue
  fi
done

if [ $cnt_sleep_ctr_d -gt $num_sleeps ]
then
    msg="Hadoop MapReduce job failed for CTR-daily due to that ${CLK_LAST_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

#echo "delete data older than 10 days"
#months_ago=`date --date="10 day ago" +%Y%m%d`
#${HADOOP} fs -rm -r ${CTR_BASE_DIR_DAILY}/${months_ago}

#/usr/bin/find ${LOGS_DIR}/*.log -type f -mtime +3 -delete

fi

#exit 0
