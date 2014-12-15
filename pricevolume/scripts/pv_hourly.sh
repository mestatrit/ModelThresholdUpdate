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

#let delay_hour=3
#let delay_hour_p1=${delay_hour}-1

#stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
#stringP1=`date -u +%Y%m%d%H -d "${delay_hour_p1} hour ago"`

#check to see if all arguments passed
#echo "$1"

if [ $1 == -1 ]
then
    let delay_hour_pv=3
    stringZ=`date -u +%Y%m%d%H -d "${delay_hour_pv} hour ago"`
else
    stringZ=$1
fi

echo "The date and time of the processing hour is ${stringZ}"	
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
hh=${stringZ:8:2}
v_date=${stringZ:0:8}
v_date_hour=${stringZ:0:10}
pDate="${yyyy}-${mm}-${dd}"

pDate_hour="${pDate} ${hh}:00:00"
hour_sec=`date --date="${pDate_hour}" +%s`
#date -d @$hour_sec +"%Y%m%d%H"
#echo $hh
let newZS=$hour_sec+3600
#echo $newZS
stringP1=`date -d @$newZS +"%Y%m%d%H"`
#echo $stringZ
#echo $stringP1

v_date_p1=${stringP1:0:8}
v_date_hour_p1=${stringP1:0:10}
hhp1=${stringP1:8:2}

. ${CONF_DIR}/env_pricevolume.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][PV Hourly] -- The hourly pv job failed while processing for hour: $hh on $pDate"
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

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_pv_dir_hourly=${s3_bucket_dir}/price_volume_hourly

qName="-Dmapred.job.queue.name=${QUEUENAME_PV_HOURLY}"

BID_BASE_DIR="${HDFS_DIR_RTB_BD}/rtb_bid"
BID_DATE_DIR="${BID_BASE_DIR}/${v_date}"
#BID_HOUR_DIR="${BID_DATE_DIR}/${hh}"
BID_HOUR_DIR="${BID_BASE_DIR}/${v_date_hour}"
PRICE_BASE_DIR="${HDFS_DIR_RTB_PC}/rtb_pc"
PRICE_DATE_DIR="${PRICE_BASE_DIR}/${v_date}"
#PRICE_HOUR_DIR="${PRICE_DATE_DIR}/${hh}"
#PRICE_HOUR_P1_DIR="${PRICE_BASE_DIR}/${v_date_p1}/${hhp1}"
PRICE_HOUR_DIR="${PRICE_BASE_DIR}/${v_date_hour}"
PRICE_HOUR_P1_DIR="${PRICE_BASE_DIR}/${v_date_hour_p1}"
PRICE_BASE_DIR_TMP="${HDFS_DIR_RTB_PC}/rtb_pc_tmp"

PV_BASE_DIR_HOURLY="${HDFS_DIR}/price_volume_hourly"
PV_BASE_DIR_DAILY="${HDFS_DIR}/price_volume_daily"
#PV_DATE_DIR="${PV_BASE_DIR}/${v_date}"
#PV_HOUR_DIR="${PV_DATE_DIR}/${hh}"
PV_HOUR_DIR="${PV_BASE_DIR_HOURLY}/${v_date_hour}"

pv_jar=${JAR_DIR}/PriceVolumeRunner.jar
pv_main="com.sharethis.adoptimization.pricevolume.HourlyGeneratingPVTaskMain" 
parm_date="ProcessingDate=${pDate}"
parm_hour="HourOfDay=${hh}"
parm_bid="BidFilePath=${BID_BASE_DIR}/"
parm_price="PriceFilePath=${PRICE_BASE_DIR}/"
parm_out_hourly="OutFilePathHourly=${PV_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${PV_BASE_DIR_DAILY}/"
parm_source="DataSource=1"
parm_res="${CONF_DIR}/pv.properties"
parm_q_name="QueueName=${QUEUENAME_PV_HOURLY}"

pv_parm="${parm_res} ${parm_q_name} ${parm_source} ${parm_date} ${parm_hour} ${parm_bid} ${parm_price} ${parm_out_hourly} ${parm_out_dily}"

${HADOOP} fs -test -e ${PV_HOUR_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
   echo "Found the _SUCCESS Marker file in the partition folder :${PV_HOUR_DIR}. Please clean this folder in order to reprocess this again"
   #exit 0
else

echo "${HADOOP} fs -rm -r ${PV_HOUR_DIR}"
${HADOOP} fs -rm -r ${PV_HOUR_DIR}

cnt_sleep_pv=0
while [ $cnt_sleep_pv -le $NUM_SLEEP_HOUR ]
do
  ${HADOOP} fs -test -e ${BID_HOUR_DIR}/_SUCCESS
  if [ $? -eq 0 ]
  then
      ${HADOOP} fs -test -e ${PRICE_HOUR_P1_DIR}/_SUCCESS
      if [ $? -eq 0 ]
      then
          echo "${HADOOP} jar ${pv_jar} ${pv_main} ${pv_parm}"
          ${HADOOP} jar ${pv_jar} ${pv_main} ${pv_parm} 
	  if [ $? -eq 0 ]
	  then
              echo "${HADOOP} fs -touchz ${PV_HOUR_DIR}/_SUCCESS"
              ${HADOOP} fs -touchz ${PV_HOUR_DIR}/_SUCCESS
              ${HADOOP} fs ${CONF} -test -e ${s3_pv_dir_hourly}
              if [ $? -ne 0 ]
              then
                  ${HADOOP} fs ${CONF} -mkdir ${s3_pv_dir_hourly}
              fi    
              echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${PV_HOUR_DIR} ${s3_pv_dir_hourly}"
              ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${PV_HOUR_DIR} ${s3_pv_dir_hourly}
              ${HADOOP} fs ${CONF} -rm -r ${s3_pv_dir_hourly}/_distcp*
              #if [ $? -eq 0 ]
              #then
              #    ${HADOOP} fs -touchz ${PV_DATE_DIR}/_SUCCESS_BACKUP
              #fi
          else
              msg="Hadoop MapReduce job failed for PV..please investigate"
              sendalert $msg
          fi
          break
      else
          echo "The priceconf data under ${PRICE_HOUR_P1_DIR} is not ready."
          sleep 5m 
          let cnt_sleep_pv=${cnt_sleep_pv}+1
          echo "The number of sleeps: ${cnt_sleep_pv}"
          continue
      fi
  else
      echo "The bidder data under ${BID_HOUR_DIR} is not ready."
      sleep 5m 
      let cnt_sleep_pv=${cnt_sleep_pv}+1
      echo "The number of sleeps: ${cnt_sleep_pv}"
      continue
  fi
done

if [ $cnt_sleep_pv -gt $NUM_SLEEP_HOUR ]
then 
    msg=" PV hourly job failed due to that ${BID_HOUR_DIR} or ${PRICE_HOUR_P1_DIR} is not ready at `date -u +%Y%m%d%H`."
    sendalert $msg
fi

#hours_ago=`date --date="4 day ago" +%Y%m%d%H`
#${HADOOP} fs -rm -r ${PV_BASE_DIR_HOURLY}/${hours_ago}
#${HADOOP} fs -rm -r ${BID_BASE_DIR}/${hours_ago}
#${HADOOP} fs -rm -r ${PRICE_BASE_DIR}/${hours_ago}
#${HADOOP} fs -rm -r ${PRICE_BASE_DIR_TMP}/${hours_ago}

fi

#exit 0
