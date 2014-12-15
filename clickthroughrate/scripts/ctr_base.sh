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
export DATA_DIR=${PROJ_DIR}/data

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
subject="[WARNING][CTR Base Daily] -- The daily ctr base job failed while processing for day: $pDate"
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


CTR_BASE_DIR="${HDFS_DIR_DAILY}/ctr_daily"
CTR_DATE_DIR="${CTR_BASE_DIR}/${v_date}"
BAS_BASE_DIR="${HDFS_DIR_MOD}/ctr_base"
BAS_DATE_DIR="${BAS_BASE_DIR}/${v_date}"
MOD_BASE_DIR="${HDFS_DIR_MOD}/ctr_model"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_base_daily_dir=${s3_bucket_dir}/ctr_base/${v_date}

base_jar=${JAR_DIR}/ClickThroughRateRunner.jar
base_main="com.sharethis.adoptimization.clickthroughrate.ctrmodel.GeneratingBaseDataMain"
parm_date="ProcessingDate=${pDate}"
parm_in="CTRFilePath=${CTR_BASE_DIR}/"
parm_out="ModelFilePath=${BAS_BASE_DIR}/"

parm_res="${CONF_DIR}/ctr_base.properties"
parm_q_name="QueueName=${QUEUENAME_CTR_DAILY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_DAILY}"
setHeap="-Dmapred.child.java.opts=-Xmx3000m"

base_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_in} ${parm_out} ${setHeap}"

echo "Checking whether job is running for the day which has already processed in which case User has to clean up the folder first"

${HADOOP} fs -test -e ${BAS_DATE_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder: ${BAS_DATE_DIR}. Please clean this folder in order to reprocess this again"
else


let num_sleeps=${NUM_SLEEP_DAILY}

cnt_sleep_base_d=0
while [ $cnt_sleep_base_d -le 1 ]
do
  ${HADOOP} fs -test -e ${CTR_DATE_DIR}/_SUCCESS
  if [ $? -eq 0 ]
  then
      echo "${HADOOP} jar ${base_jar} ${base_main} ${base_parm}"
      ${HADOOP} jar ${base_jar} ${base_main} ${base_parm}
      if [ $? -eq 0 ]
      then
          #Copy the outside domain ctr data into rtb folder
          echo "${HADOOP} fs -mkdir ${BAS_DATE_DIR}/rtb/rtb_anx_base"
          ${HADOOP} fs -mkdir ${BAS_DATE_DIR}/rtb/rtb_anx_base
          echo "${HADOOP} fs -put ${DATA_DIR}/* ${BAS_DATE_DIR}/rtb/rtb_anx_base/"
          ${HADOOP} fs -put ${DATA_DIR}/anx_* ${BAS_DATE_DIR}/rtb/rtb_anx_base/
          if [ $? -eq 0 ]
          then
              echo "${HADOOP} fs -touchz ${BAS_DATE_DIR}/rtb/rtb_anx_base/_SUCCESS"
              ${HADOOP} fs -touchz ${BAS_DATE_DIR}/rtb/rtb_anx_base/_SUCCESS
          fi

          echo "${HADOOP} fs -touchz ${BAS_DATE_DIR}/_SUCCESS"
          ${HADOOP} fs -touchz ${BAS_DATE_DIR}/_SUCCESS
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${BAS_DATE_DIR} ${s3_base_daily_dir}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${BAS_DATE_DIR} ${s3_base_daily_dir}
          ${HADOOP} fs ${CONF} -rm -r ${s3_base_daily_dir}/_distcp*
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${BAS_DATE_DIR} ${MOD_BASE_DIR}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${BAS_DATE_DIR} ${MOD_BASE_DIR}
          ${HADOOP} fs ${CONF} -rm -r ${MOD_BASE_DIR}/_distcp*
      else
          msg="Hadoop MapReduce job failed for CTR base data. Please investigate!"
          sendalert $msg
      fi
      break
  else
      echo "CTR Daily Data: ${CTR_DATE_DIR} is not ready!"
      sleep 10m
      let cnt_sleep_mod_d=${cnt_sleep_base_d}+1
      echo "The number of sleeps: ${cnt_sleep_base_d}"
      continue
  fi
done

if [ $cnt_sleep_base_d -gt 1 ]
then
    msg="Hadoop MapReduce job failed for CTR base data due to that ${CTR_DATE_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi
fi

#exit 0
