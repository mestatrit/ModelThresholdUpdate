bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

export s3_rtb_dir=s3n://sharethis-logs-rtb
export s3_pc_dir=s3n://sharethis-logs-rtb-pc
export s3_nb_dir=s3n://sharethis-logs-rtb-nb
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

. ${CONF_DIR}/env_data.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][PV Hourly] -- Hourly copying pc data job failed while processing for hour: $hh on $pDate"
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

CONF_PROD="-conf ${CONF_DIR}/core-prod.xml"

if [ $1 == -1 ]
then
    let delay_hour_pc=2
    stringZ=`date -u +%Y%m%d%H -d "${delay_hour_pc} hour ago"`
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

PC_BASE_DIR_TMP="${HDFS_DIR}/rtb_pc_tmp"
PC_DATE_DIR_TMP="${PC_BASE_DIR_TMP}/${v_date}"

PC_LOCAL_TMP="${TMP_DIR}"

PC_BASE_DIR="${HDFS_DIR}/rtb_pc"
PC_DATE_DIR="${PC_BASE_DIR}/${v_date}"
#PC_HOUR_DIR="${PC_DATE_DIR}/${hh}"
PC_HOUR_DIR="${PC_BASE_DIR}/${v_date_hour}"

qName="-Dmapred.job.queue.name=${QUEUENAME_PV_HOURLY}"

s3_pc_date_dir=${s3_pc_dir}/${v_date}
s3_pc_hour_dir=${s3_pc_date_dir}/${hh}

#log_dir_pc=${LOG_TMP_DIR}/distcp_log_pc_${stringZ}
#log="-log ${log_dir_pc}"

${HADOOP} fs -test -e ${PC_HOUR_DIR}/_SUCCESS 
if [ $? -eq 0 ]
then
    echo "Found the _SUCCESS file in the partition folder :${PC_HOUR_DIR}. No work is required!"
else
    #echo "rm -rf ${log_dir_pc}" 
    #rm -rf ${log_dir_pc} 

    echo "Before copying the pc data from s3n, ${HADOOP} fs -rm -r ${PC_HOUR_DIR}" 
    ${HADOOP} fs -rm -r ${PC_HOUR_DIR} 

    echo "${HADOOP} distcp ${CONF_PROD} ${qName} -Ddistcp.bytes.per.map=102400 ${s3_pc_hour_dir} ${PC_HOUR_DIR}" 
    ${HADOOP} distcp ${CONF_PROD} ${qName} -Ddistcp.bytes.per.map=102400 ${s3_pc_hour_dir} ${PC_HOUR_DIR} 
    if [ $? -eq 0 ]
    then
        ${HADOOP} fs -rm -r ${PC_HOUR_DIR}/NOTE*
        ${HADOOP} fs -touchz ${PC_HOUR_DIR}/_SUCCESS
        ${HADOOP} fs -rm -r ${PC_BASE_DIR}/_distcp_logs_*
        echo "Copying the pc data from ${s3_pc_hour_dir} to ${PC_HOUR_DIR} is done."
    else
        msg="Copying the pc data job failed. Please investigate!"
        sendalert $msg
    fi
fi

#exit 0

