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

. ${CONF_DIR}/env_bl_data.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][BL Data Hourly] -- The hourly data aggregating job failed while processing for hour: $hh on $pDate "
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
DATA_BASE_DIR_HOURLY="${DATA_BASE_DIR}/bl_hourly"
DATA_HOUR_DIR="${DATA_BASE_DIR_HOURLY}/${v_date_hour}"

BL_BASE_DIR="${HDFS_DIR_BL}/bl_parsed_hourly"
BL_HOUR_DIR="${BL_BASE_DIR}/${v_date_hour}"
BL_FILE_NAME=bl_parsed_hourly

ALL_DATA_PATH="${HDFS_DIR_ALL}/camp_data_hourly"
ALL_DATA_HOUR_DIR="${ALL_DATA_PATH}/${v_date_hour}"
ALL_DATA_NAME=all_data_hourly

s3_data_hourly_dir=${s3_bucket_dir}/bl_hourly

data_jar=${JAR_DIR}/BrandLiftRunner.jar
data_main="com.sharethis.adoptimization.brandlift.aggregating.HourlyAggregatingBLDataTaskMain" 
parm_date="ProcessingDate=${pDate}"
parm_hour="HourOfDay=${hh}"
parm_out_hourly="OutFilePathHourly=${DATA_BASE_DIR_HOURLY}/"
parm_bl_path="BLFilePath=${BL_BASE_DIR}/"
parm_bl_name="BLFileName=${BL_FILE_NAME}"
parm_all_path="AllFilePath=${ALL_DATA_PATH}/"
parm_all_name="AllFileName=${ALL_DATA_NAME}"

parm_reducer="NumOfReducers=${NUM_OF_REDUCERS}"

parm_res="${CONF_DIR}/bl.properties"

parm_q_name="QueueName=${QUEUENAME_DATA_HOURLY}"
qName="-Dmapred.job.queue.name=${QUEUENAME_DATA_HOURLY}"

data_parm="${parm_res} ${parm_reducer} ${parm_q_name} ${parm_date} ${parm_hour} ${parm_out_hourly} ${parm_bl_path} ${parm_bl_name} ${parm_all_path} ${parm_all_name}"

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
        ${HADOOP} fs -test -e ${ALL_DATA_HOUR_DIR}/_SUCCESS
        if [ $? -eq 0 ]
        then
            ${HADOOP} fs -test -e ${BL_HOUR_DIR}/_SUCCESS
            if [ $? -eq 0 ]
            then

                echo "${HADOOP} jar ${data_jar} ${data_main} ${data_parm}"
                ${HADOOP} jar ${data_jar} ${data_main} ${data_parm}
	        if [ $? -eq 0 ]
	        then
        	    ${HADOOP} fs -touchz ${DATA_HOUR_DIR}/_SUCCESS

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
               echo "The retarg data under ${BL_HOUR_DIR} is not ready."
               sleep 2m
               let cnt_sleep_data=${cnt_sleep_data}+1
               echo "The number of sleeps: ${cnt_sleep_data}"
               continue
           fi
       else
           echo "The all data under ${ALL_DATA_HOUR_DIR} is not ready."
           sleep 2m
           let cnt_sleep_data=${cnt_sleep_data}+1
           echo "The number of sleeps: ${cnt_sleep_data}"
           continue
       fi
   done

   if [ $cnt_sleep_data -gt $NUM_SLEEP_HOUR ]
   then
        msg="Hadoop MapReduce job failed for aggregating BL Data-hourly due to that ${ALL_DATA_HOUR_DIR} or ${BL_HOUR_DIR} is not ready at `date -u +%Y%m%d%H`."
        sendalert $msg
   fi
fi

