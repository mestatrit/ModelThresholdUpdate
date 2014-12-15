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

. ${CONF_DIR}/env_pricevolume.sh

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
#PC_HOUR_DIR_TMP="${PC_DATE_DIR_TMP}/${hh}"
PC_HOUR_DIR_TMP="${PC_BASE_DIR_TMP}/${v_date_hour}"

PC_LOCAL_TMP="${TMP_DIR}"

PC_BASE_DIR="${HDFS_DIR_RTB_PC}/rtb_pc"
PC_DATE_DIR="${PC_BASE_DIR}/${v_date}"
#PC_HOUR_DIR="${PC_DATE_DIR}/${hh}"
PC_HOUR_DIR="${PC_BASE_DIR}/${v_date_hour}"

qName="-Dmapred.job.queue.name=${QUEUENAME_PV_HOURLY}"

s3_pc_date_dir=${s3_pc_dir}/${v_date}
s3_pc_hour_dir=${s3_pc_date_dir}/${hh}

log_dir_pc=${LOG_TMP_DIR}/distcp_log_pc_${stringZ}
log="-log ${log_dir_pc}"

${HADOOP} fs -test -e ${PC_HOUR_DIR}/_SUCCESS 
if [ $? -eq 0 ]
then
    echo "Found the _SUCCESS file in the partition folder :${PC_HOUR_DIR}. No work is required!"
else
    ${HADOOP} fs -test -e ${PC_HOUR_DIR_TMP}/_SUCCESS 
    if [ $? -eq 0 ]
    then
        echo "Found the _SUCCESS file in the partition folder :${PC_HOUR_DIR_TMP}. No copy is required!"
    else
        echo "rm -rf ${log_dir_pc}" 
        rm -rf ${log_dir_pc} 

        echo "Before copying the pc data from s3n, ${HADOOP} fs -rm -r ${PC_HOUR_DIR_TMP}" 
        ${HADOOP} fs -rm -r ${PC_HOUR_DIR_TMP} 

        echo "${HADOOP} distcp ${CONF_PROD} ${qName} -Ddistcp.bytes.per.map=102400 ${s3_pc_hour_dir} ${PC_HOUR_DIR_TMP}" 
        ${HADOOP} distcp ${CONF_PROD} ${qName} -Ddistcp.bytes.per.map=102400 ${s3_pc_hour_dir} ${PC_HOUR_DIR_TMP} 
        if [ $? -eq 0 ]
        then
            ${HADOOP} fs -touchz ${PC_HOUR_DIR_TMP}/_SUCCESS
            ${HADOOP} fs -rm -r ${PC_BASE_DIR_TMP}/_distcp_logs_*
            echo "Copying the pc data from ${s3_pc_hour_dir} to ${PC_HOUR_DIR_TMP} is done."
        else
            msg="Copying the pc data job failed. Please investigate!"
            sendalert $msg
        fi
    fi

    echo "Combining all files under ${PC_HOUR_DIR_TMP} into one gzipped file under ${PC_HOUR_DIR}"
    test -e ${PC_LOCAL_TMP}
    if [ $? -eq 0 ]
    then
        echo "The local folder: ${PC_LOCAL_TMP} exists!"
    else
        mkdir ${PC_LOCAL_TMP}
    fi

    echo "Before generating the data, ${HADOOP} fs -rm -r ${PC_HOUR_DIR}" 
    ${HADOOP} fs -rm -r ${PC_HOUR_DIR} 

    file_name=${PC_LOCAL_TMP}/pc_${stringZ}
    cnt_sleep_pc=0
    while [ $cnt_sleep_pc -le $NUM_SLEEP_HOUR ]
    do
        ${HADOOP} fs -test -e ${PC_HOUR_DIR_TMP}/_SUCCESS 
        if [ $? -eq 0 ]
        then
            echo "${HADOOP} fs -cat ${PC_HOUR_DIR_TMP}/*.gz | zcat > ${file_name}"
            ${HADOOP} fs -cat ${PC_HOUR_DIR_TMP}/*.gz | zcat > ${file_name} 
            if [ $? -eq 0 ]
            then
                ${HADOOP} fs -test -e ${PC_HOUR_DIR} 
                if [ $? -ne 0 ]
                then
                    ${HADOOP} fs -mkdir ${PC_HOUR_DIR}
                fi
                echo "${HADOOP} fs -put ${file_name} ${PC_HOUR_DIR}"
                ${HADOOP} fs -put ${file_name} ${PC_HOUR_DIR} 
                if [ $? -eq 0 ]
                then
                    ${HADOOP} fs -touchz ${PC_HOUR_DIR}/_SUCCESS
                    rm ${file_name} 
                    echo "${HADOOP} fs -rm -r ${PC_HOUR_DIR_TMP}" 
                    ${HADOOP} fs -rm -r ${PC_HOUR_DIR_TMP}
                else
                    msg="Put the gziped file to HDFS failed for PC...please investigate"
                    let cnt_sleep_pc=${cnt_sleep_pc}+1
                    sendalert $msg
                fi
                break
            else
                msg="Gzipped file to the local folder failed for PC...please investigate"
                let cnt_sleep_pc=${cnt_sleep_pc}+1
                sendalert $msg
            fi
        else
            echo "The raw pc data under ${PC_HOUR_DIR_TMP} is not ready."
            sleep 5m
            let cnt_sleep_pc=${cnt_sleep_pc}+1
            echo "The number of sleeps: ${cnt_sleep_pc}"
            continue
        fi
    done

    if [ $cnt_sleep_pc -gt $NUM_SLEEP_HOUR ]
    then
        msg="Gzipped PC data job failed due to that ${PC_HOUR_DIR_TMP} is not ready at `date -u +%Y%m%d%H`."
        sendalert $msg
    fi
fi

#exit 0

