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
subject="[WARNING][CTR Model Daily] -- The daily ctr model job failed while processing for day: $pDate"
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
MOD_BASE_DIR="${HDFS_DIR_MOD}/ctr_model"
MOD_DATE_DIR="${MOD_BASE_DIR}/${v_date}"
BAS_BASE_DIR="${HDFS_DIR_MOD}/ctr_base"
BAS_DATE_DIR="${BAS_BASE_DIR}/${v_date}"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_mod_dir=${s3_model_dir}/ctr_model

s3_mod_daily_dir=${s3_bucket_dir}/ctr_model/${v_date}

mod_jar=${JAR_DIR}/ClickThroughRateRunner.jar
mod_main="com.sharethis.adoptimization.clickthroughrate.ctrmodel.GeneratingModelsMain"
parm_date="ProcessingDate=${pDate}"
parm_in="CTRFilePath=${CTR_BASE_DIR}/"
parm_out="ModelFilePath=${MOD_BASE_DIR}/"
parm_base="CTRBaseFilePath=${BAS_BASE_DIR}/"

parm_res="${CONF_DIR}/ctr_model.properties"
parm_q_name="QueueName=${QUEUENAME_CTR_DAILY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_DAILY}"
setHeap="-Dmapred.child.java.opts=-Xmx3000m"

mod_parm="${parm_res} ${parm_q_name} ${parm_date} ${parm_in} ${parm_out} ${parm_base} ${setHeap}"

echo "Checking whether job is running for the day which has already processed in which case User has to clean up the folder first"

${HADOOP} fs -test -e ${MOD_DATE_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder: ${MOD_DATE_DIR}. Please clean this folder in order to reprocess this again"
else


let num_sleeps=${NUM_SLEEP_DAILY}

cnt_sleep_mod_d=0
while [ $cnt_sleep_mod_d -le 10 ]
do
  ${HADOOP} fs -test -e ${CTR_DATE_DIR}/_SUCCESS
  if [ $? -eq 0 ]
  then
      ${HADOOP} fs -test -e ${BAS_DATE_DIR}/_SUCCESS
      if [ $? -eq 0 ]
      then

      echo "${HADOOP} jar ${mod_jar} ${mod_main} ${mod_parm}"
      ${HADOOP} jar ${mod_jar} ${mod_main} ${mod_parm}
      if [ $? -eq 0 ]
      then
          ##Copy the outside domain ctr data into rtb folder
          #echo "${HADOOP} fs -mkdir ${MOD_DATE_DIR}/rtb/rtb_anx_base"
          #${HADOOP} fs -mkdir ${MOD_DATE_DIR}/rtb/rtb_anx_base
          #echo "${HADOOP} fs -put ${DATA_DIR}/* ${MOD_DATE_DIR}/rtb/rtb_anx_base/"
          #${HADOOP} fs -put ${DATA_DIR}/anx_* ${MOD_DATE_DIR}/rtb/rtb_anx_base/
          #if [ $? -eq 0 ]
          #then
          #    echo "${HADOOP} fs -touchz ${MOD_DATE_DIR}/rtb/rtb_anx_base/_SUCCESS"
          #    ${HADOOP} fs -touchz ${MOD_DATE_DIR}/rtb/rtb_anx_base/_SUCCESS
          #fi

          echo "${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS"
          ${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS
          echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${MOD_DATE_DIR} ${s3_mod_daily_dir}"
          ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${MOD_DATE_DIR} ${s3_mod_daily_dir}
          ${HADOOP} fs ${CONF} -rm -r ${s3_mod_daily_dir}/_distcp*
      else
          msg="Hadoop MapReduce job failed for CTR model. Please investigate!"
          sendalert $msg
      fi
      break
      else
          echo "CTR Daily Base Data: ${BAS_DATE_DIR} is not ready!"
          sleep 10m
          let cnt_sleep_mod_d=${cnt_sleep_mod_d}+1
          echo "The number of sleeps: ${cnt_sleep_mod_d}"
          continue
      fi    
  else
      echo "CTR Daily Data: ${CTR_DATE_DIR} is not ready!"
      sleep 10m
      let cnt_sleep_mod_d=${cnt_sleep_mod_d}+1
      echo "The number of sleeps: ${cnt_sleep_mod_d}"
      continue
  fi
done

if [ $cnt_sleep_mod_d -gt 10 ]
then
    msg="Hadoop MapReduce job failed for CTR model due to that ${CTR_DATE_DIR} or ${BAS_DATE_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi
fi

#exit 0