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
    let delay_hour_pv=4
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
hour_sec=`date --date="$pDate_hour" +%s`
#date -d @$hour_sec +"%Y%m%d%H"
let newZS=${hour_sec}+$((3600))
stringP1=`date -d @$newZS +"%Y%m%d%H"`
let newZS2=$hour_sec+$((7200))
stringP2=`date -d @$newZS2 +"%Y%m%d%H"`

v_date_p1=${stringP1:0:8}
v_date_p2=${stringP2:0:8}
v_date_hour_p1=${stringP1:0:10}
v_date_hour_p2=${stringP2:0:10}
hhp1=${stringP1:8:2}
hhp2=${stringP2:8:2}

. ${CONF_DIR}/env_ctr.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][CTR Hourly] -- The hourly ctr job failed while processing for hour: $hh on $pDate "
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

#CONF="-conf ${CONF_DIR}/core-prod.xml"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

DATA_BASE_DIR="${HDFS_DIR}"
IMP_BASE_DIR="${HDFS_DIR_RTB_IMP}/rtb_impression"
#IMP_BASE_DIR_TMP="${HDFS_DIR_RTB_IMP}/rtb_imp_tmp"
IMP_DATE_DIR="${IMP_BASE_DIR}/${v_date}"
#IMP_HOUR_DIR="${IMP_DATE_DIR}/${hh}"
IMP_HOUR_DIR="${IMP_BASE_DIR}/${v_date_hour}"
CLK_BASE_DIR="${HDFS_DIR_RTB_CLK}/rtb_click"
#CLK_BASE_DIR_TMP="${HDFS_DIR_RTB_CLK}/rtb_click_tmp"
CLK_DATE_DIR="${CLK_BASE_DIR}/${v_date}"
CLK_HOUR_P2_DIR="${CLK_BASE_DIR}/${v_date_hour_p2}"

IMP_CLK_BASE_DIR="${HDFS_DIR_ICP}/camp_data_hourly"
IMP_CLK_DATE_DIR="${IMP_CLK_BASE_DIR}/${v_date}"
IMP_CLK_HOUR_DIR="${IMP_CLK_BASE_DIR}/${v_date_hour}"
ALL_BASE_DIR="${HDFS_DIR_ALL}/camp_data_hourly"
ALL_DATE_DIR="${ALL_BASE_DIR}/${v_date}"
ALL_HOUR_DIR="${ALL_BASE_DIR}/${v_date_hour}"

PV_BASE_DIR="${HDFS_DIR_PV}/price_volume_hourly"
PV_DATE_DIR="${PV_BASE_DIR}/${v_date}"
PV_HOUR_P1_DIR="${PV_BASE_DIR}/${v_date_hour_p1}"
PV_HOUR_DIR="${PV_BASE_DIR}/${v_date_hour}"

CTR_BASE_DIR_HOURLY="${DATA_BASE_DIR}/ctr_hourly"
#CTR_DATE_DIR="${CTR_BASE_DIR}/${v_date}"
CTR_HOUR_DIR="${CTR_BASE_DIR_HOURLY}/${v_date_hour}"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_ctr_dir=${s3_model_dir}/ctr_hourly

s3_ctr_hourly_dir=${s3_bucket_dir}/ctr_hourly

ICP_FILE_NAME=icp_data_hourly
ALL_FILE_NAME=all_data_hourly

ctr_jar=${JAR_DIR}/ClickThroughRateRunner.jar
ctr_main="com.sharethis.adoptimization.clickthroughrate.HourlyGeneratingCTRTaskMain" 
parm_date="ProcessingDate=${pDate}"
parm_hour="HourOfDay=${hh}"
parm_out_hourly="OutFilePathHourly=${CTR_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${CTR_BASE_DIR_DAILY}/"
parm_imp="ImpFilePath=${IMP_BASE_DIR}/"
parm_click="ClickFilePath=${CLK_BASE_DIR}/"
parm_pv="PbFilePath=${PV_BASE_DIR}/"
parm_icp_path="IcpFilePath=${IMP_CLK_BASE_DIR}/"
parm_icp_name="IcpFileName=${ICP_FILE_NAME}"
parm_all_path="AllFilePath=${ALL_BASE_DIR}/"
parm_all_name="AllFileName=${ALL_FILE_NAME}"

parm_res="${CONF_DIR}/ctr.properties"
parm_q_name="QueueName=${QUEUENAME_CTR_HOURLY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_HOURLY}"

ctr_parm="${parm_res} ${parm_q_name} ${parm_all_path} ${parm_all_name} ${parm_date} ${parm_hour} ${parm_out_hourly} ${parm_out_daily} ${parm_icp_path} ${parm_icp_name} ${parm_imp} ${parm_click} ${parm_pv}"

echo "Checking whether job is running for the hour which has already processed in which case User has to clean up the folder first"

${HADOOP} fs -test -e ${CTR_HOUR_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
    echo "Found the _SUCCESS Marker file in the partition folder :${CTR_HOUR_DIR}. Please clean this folder in order to reprocess this again"
#   exit 0
else

    echo "${HADOOP} fs -rm -r ${CTR_HOUR_DIR}"
    ${HADOOP} fs -rm -r ${CTR_HOUR_DIR}

    cnt_sleep_ctr=0
    while [ $cnt_sleep_ctr -lt $NUM_SLEEP_HOUR ]
    do
       ${HADOOP} fs -test -e ${ALL_HOUR_DIR}/_SUCCESS
       if [ $? -eq 0 ]
       then       
           echo "${HADOOP} jar ${ctr_jar} ${ctr_main} ${ctr_parm}"        
           ${HADOOP} jar ${ctr_jar} ${ctr_main} ${ctr_parm}
           if [ $? -eq 0 ]
           then
               ${HADOOP} fs -touchz ${CTR_HOUR_DIR}/_SUCCESS
               ${HADOOP} fs ${CONF} -test -e ${s3_ctr_hourly_dir}
               if [ $? -ne 0 ]
               then
                   ${HADOOP} fs ${CONF} -mkdir ${s3_ctr_hourly_dir}
               fi

               echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_HOUR_DIR} ${s3_ctr_hourly_dir}"
               ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${CTR_HOUR_DIR} ${s3_ctr_hourly_dir}
               ${HADOOP} fs ${CONF} -rm -r ${s3_ctr_hourly_dir}/_distcp*        
           else
       	       msg="Hadoop MapReduce job failed for CTR..please investigate"
               sendalert $msg
           fi
           break
       else
           echo "The all data under ${IMP_CLK_HOUR_DIR} is not ready."
           sleep 2m
           let cnt_sleep_ctr=${cnt_sleep_ctr}+1
           echo "The number of sleeps: ${cnt_sleep_ctr}"
           continue
       fi
    done
    if [ $cnt_sleep_ctr -ge $NUM_SLEEP_HOUR ]
    then
        msg="Hadoop MapReduce job failed for CTR-hourly due to that ${IMP_CLK_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
        sendalert $msg
    fi
fi

