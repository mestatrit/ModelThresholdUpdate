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
    let delay_hour=3
    stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
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

. ${CONF_DIR}/env_data.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][Data Hourly] -- The hourly data joining job failed while processing for hour: $hh on $pDate "
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

CONF_PROD="-conf ${CONF_DIR}/core-prod.xml"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

DATA_BASE_DIR="${HDFS_DIR}"
IMP_BASE_DIR="${HDFS_DIR_RTB_IMP}/rtb_impression"
IMP_DATE_DIR="${IMP_BASE_DIR}/${v_date}"
IMP_HOUR_DIR="${IMP_BASE_DIR}/${v_date_hour}"
CLK_BASE_DIR="${HDFS_DIR_RTB_CLK}/rtb_click"
CLK_DATE_DIR="${CLK_BASE_DIR}/${v_date}"
CLK_HOUR_P2_DIR="${CLK_BASE_DIR}/${v_date_hour_p2}"
PC_BASE_DIR="${HDFS_DIR_PC}/rtb_pc"
PC_DATE_DIR="${PC_BASE_DIR}/${v_date}"
PC_HOUR_P1_DIR="${PC_BASE_DIR}/${v_date_hour_p1}"
PC_HOUR_DIR="${PC_BASE_DIR}/${v_date_hour}"

CLK_BASE_DIR_SVR="${HDFS_DIR_RTB_CLK_SVR}/svr_anx_click"
CLK_DATE_DIR_SVR="${CLK_BASE_DIR_SVR}/${v_date}"
CLK_HOUR_P2_DIR_SVR="${CLK_BASE_DIR_SVR}/${v_date_hour_p2}"

PV_BASE_DIR="${HDFS_DIR_PV}/price_volume_hourly"
PV_HOUR_DIR="${PV_BASE_DIR}/${v_date_hour}"
PV_HOUR_P1_DIR="${PV_BASE_DIR}/${v_date_hour_p1}"

SB_FILE_NAME=rtb_success_bid_hourly

#PC_HOUR_P1_DIR="${PC_BASE_DIR}/${v_date}/${hhp1}"
#PC_HOUR_DIR="${PC_BASE_DIR}/${v_date}/${hh}"

DATA_BASE_DIR_HOURLY="${DATA_BASE_DIR}/camp_data_hourly"
DATA_HOUR_DIR="${DATA_BASE_DIR_HOURLY}/${v_date_hour}"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_data_dir=${s3_model_dir}/camp_data_hourly

s3_data_hourly_dir=${s3_bucket_dir}/camp_data_hourly

data_jar=${JAR_DIR}/ClickThroughRateRunner.jar
data_main="com.sharethis.adoptimization.campaigndata.HourlyGeneratingDataTaskMain" 
parm_date="ProcessingDate=${pDate}"
parm_hour="HourOfDay=${hh}"
parm_out_hourly="OutFilePathHourly=${DATA_BASE_DIR_HOURLY}/"
parm_out_daily="OutFilePathDaily=${DATA_BASE_DIR_DAILY}/"
parm_imp="ImpFilePath=${IMP_BASE_DIR}/"
parm_click="ClickFilePath=${CLK_BASE_DIR}/"
parm_pc="PriceFilePath=${PC_BASE_DIR}/"
parm_reducer="NumOfReducers=${NUM_OF_REDUCERS}"
parm_num_click="NumOfHoursClick=${NUM_OF_HOURS_CLICK}"

parm_sb_path="SbFilePath=${PV_BASE_DIR}/"
parm_sb_name="SbFileName=${SB_FILE_NAME}"
parm_num_bid="NumOfHoursBid=${NUM_OF_HOURS_BID}"

parm_click_svr="ClickFilePathSVR=${CLK_BASE_DIR_SVR}/"

parm_q_name="QueueName=${QUEUENAME_DATA_HOURLY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_DATA_HOURLY}"

data_parm="${parm_sb_path} ${parm_sb_name} ${parm_num_bid} ${parm_num_click} ${parm_reducer} ${parm_q_name} ${parm_date} ${parm_hour} ${parm_out_hourly} ${parm_out_daily} ${parm_imp} ${parm_click} ${parm_pc} ${parm_click_svr}"

echo "Checking whether job is running for the hour which has already processed in which case User has to clean up the folder first"

${HADOOP} fs -test -e ${DATA_HOUR_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
    echo "Found the _SUCCESS Marker file in the partition folder: ${DATA_HOUR_DIR}. Please clean this folder in order to reprocess this again"
else
    echo "${HADOOP} fs -rm -r ${DATA_HOUR_DIR}"
    ${HADOOP} fs -rm -r ${DATA_HOUR_DIR}

    cnt_sleep_data=0
    while [ $cnt_sleep_data -lt $NUM_SLEEP_HOUR ]
    do
        ${HADOOP} fs -test -e ${IMP_HOUR_DIR}/_SUCCESS
        if [ $? -eq 0 ]
        then
            ${HADOOP} fs -test -e ${CLK_HOUR_P2_DIR}/_SUCCESS
            if [ $? -eq 0 ]
            then
                ${HADOOP} fs -test -e ${PC_HOUR_P1_DIR}/_SUCCESS
                if [ $? -eq 0 ]
                then
                    ${HADOOP} fs -test -e ${PC_HOUR_DIR}/_SUCCESS
                    if [ $? -eq 0 ]
                    then
                ${HADOOP} fs -test -e ${PV_HOUR_P1_DIR}/_SUCCESS
                if [ $? -eq 0 ]
                then
                    ${HADOOP} fs -test -e ${PV_HOUR_DIR}/_SUCCESS
                    if [ $? -eq 0 ]
                    then
                        echo "${HADOOP} jar ${data_jar} ${data_main} ${data_parm}"
                        ${HADOOP} jar ${data_jar} ${data_main} ${data_parm}
	                if [ $? -eq 0 ]
	                then
        	            ${HADOOP} fs -touchz ${DATA_HOUR_DIR}/_SUCCESS
		            #${HADOOP} fs ${CONF} -test -e ${s3_data_dir}
                            #if [ $? -ne 0 ]
		            #then
		            #    ${HADOOP} fs ${CONF} -mkdir ${s3_data_dir}     
                            #fi
                            #echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${DATA_HOUR_DIR} ${s3_data_dir}"
                            #${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${DATA_HOUR_DIR} ${s3_data_dir}
		            #${HADOOP} fs ${CONF} -rm -r ${s3_data_dir}/_distcp*

                            ${HADOOP} fs ${CONF} -test -e ${s3_data_hourly_dir}
                            if [ $? -ne 0 ]
                            then
                                ${HADOOP} fs ${CONF} -mkdir ${s3_data_hourly_dir}
                            fi
                            echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${DATA_HOUR_DIR} ${s3_data_hourly_dir}"
                            ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${DATA_HOUR_DIR} ${s3_data_hourly_dir}
                            ${HADOOP} fs ${CONF} -rm -r ${s3_data_hourly_dir}/_distcp*

                        else
       	                    msg="Hadoop MapReduce job failed for data. Please investigate!"
                            sendalert $msg
	               fi
                       break
                   else
                       echo "The success-bid data under ${PV_HOUR_DIR} is not ready."
                       sleep 2m
                       let cnt_sleep_data=${cnt_sleep_data}+1
                       echo "The number of sleeps: ${cnt_sleep_data}"
                       continue
                   fi
               else
                   echo "The success-bid data under ${PV_HOUR_P1_DIR} is not ready."
                   sleep 2m
                   let cnt_sleep_data=${cnt_sleep_data}+1
                   echo "The number of sleeps: ${cnt_sleep_data}"
                   continue
               fi
                   else
                       echo "The priceconf data under ${PC_HOUR_DIR} is not ready."
                       sleep 2m
                       let cnt_sleep_data=${cnt_sleep_data}+1
                       echo "The number of sleeps: ${cnt_sleep_data}"
                       continue
                   fi
               else
                   echo "The priceconf data under ${PC_HOUR_P1_DIR} is not ready."
                   sleep 2m
                   let cnt_sleep_data=${cnt_sleep_data}+1
                   echo "The number of sleeps: ${cnt_sleep_data}"
                   continue
               fi
           else
               echo "The click data under ${CLK_HOUR_P2_DIR} is not ready."
               sleep 2m
               let cnt_sleep_data=${cnt_sleep_data}+1
               echo "The number of sleeps: ${cnt_sleep_data}"
               continue
           fi
       else
           echo "The impression data under ${IMP_HOUR_DIR} is not ready."
           sleep 2m
           let cnt_sleep_data=${cnt_sleep_data}+1
           echo "The number of sleeps: ${cnt_sleep_data}"
           continue
       fi
   done

   if [ $cnt_sleep_data -gt $NUM_SLEEP_HOUR ]
   then
        msg="Hadoop MapReduce job failed for Data-hourly due to that ${IMP_HOUR_DIR} or ${CLK_HOUR_P2_DIR} or ${PC_HOUR_P1_DIR} or ${PV_HOUR_P1_DIR} is not ready at `date -u +%Y%m%d%H`."
        sendalert $msg
   fi
fi

