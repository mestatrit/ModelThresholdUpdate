bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

#export s3_bucket_dir=s3n://sharethis-insights-backup
export s3_par_dir=s3n://sharethis-campaign-logs-parsed
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
    let delay_hour=2
    stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
else
    stringZ=$1
fi

#echo "The date and time is ${stringZ}" 
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
hh=${stringZ:8:2}
v_date=${stringZ:0:8}
v_date_hour=${stringZ:0:10}
pDate="${yyyy}-${mm}-${dd}"

. ${CONF_DIR}/env_ctr.sh


sendalert() {

message="$*"
echo $message
subject="[ERROR] -- Hourly copying impression data job failed while processing for hour: $hh on $pDate"
##maillist="insights@sharethis.com"
maillist=${email_address}


echo "The following step was failed while the job progressing" >> /tmp/mailbody_$$.txt
echo " Error message :: $message " >> /tmp/mailbody_$$.txt

cat - /tmp/mailbody_$$.txt <<EOF | /usr/sbin/sendmail -t
To:$maillist
From:Watchdog<watchdog@sharethis.com>
Subject:${subject}

EOF

rm /tmp/mailbody_$$.txt
exit 1
}


IMP_BASE_DIR_TMP="${HDFS_DIR_RTB_IMP}/rtb_imp_tmp"
IMP_DATE_DIR_TMP="${IMP_BASE_DIR_TMP}/${v_date}"
#IMP_HOUR_DIR_TMP="${IMP_DATE_DIR_TMP}/${hh}"
IMP_HOUR_DIR_TMP="${IMP_BASE_DIR_TMP}/${v_date_hour}"

IMP_LOCAL_TMP="${TMP_DIR}"

IMP_BASE_DIR="${HDFS_DIR_RTB_IMP}/rtb_impression"
IMP_DATE_DIR="${IMP_BASE_DIR}/${v_date}"
#IMP_HOUR_DIR="${IMP_DATE_DIR}/${hh}"
IMP_HOUR_DIR="${IMP_BASE_DIR}/${v_date_hour}"

qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_HOURLY}"

CONF_PROD="-conf ${CONF_DIR}/core-prod.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_imp_date_dir=${s3_par_dir}/rtb_impression/${v_date}
#s3_imp_hour_dir=${s3_imp_date_dir}/${hh}
s3_imp_hour_dir=${s3_par_dir}/rtb_impression/${v_date_hour}/data

log_dir_imp="${LOG_TMP_DIR}/distcp_log_imp_${stringZ}"
log="-log ${log_dir_imp}"

${HADOOP} fs -test -e ${IMP_HOUR_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
    echo "Found the _SUCCESS file in the partition folder :${IMP_HOUR_DIR}. No work is required!"
else
    ${HADOOP} fs -test -e ${IMP_HOUR_DIR_TMP}/_SUCCESS
    if [ $? -eq 0 ]
    then
        echo "Found the _SUCCESS file in the partition folder :${IMP_HOUR_DIR_TMP}. No copy is required!"
    else
        cnt_sleep_imp=0
        while [ $cnt_sleep_imp -le $NUM_SLEEP_HOUR ]
        do
            ${HADOOP} fs  ${CONF_PROD} -test -e ${s3_imp_hour_dir}/_SUCCESS
            if [ $? -eq 0 ]
            then 
                echo "rm -rf ${log_dir_imp}"
                rm -rf ${log_dir_imp}

                echo "Before downloading the data from s3n, ${HADOOP} fs -rm -r ${IMP_HOUR_DIR_TMP}"
                ${HADOOP} fs -rm -r ${IMP_HOUR_DIR_TMP}

                echo "${HADOOP} distcp ${CONF_PROD} ${DISTCP_OPT} ${qName} ${log} -i ${s3_imp_hour_dir} ${IMP_HOUR_DIR_TMP}"
                ${HADOOP} distcp ${CONF_PROD} ${DISTCP_OPT} ${qName} ${log} -i ${s3_imp_hour_dir} ${IMP_HOUR_DIR_TMP}
                if [ $? -eq 0 ]
                then
                    ${HADOOP} fs -test -e ${IMP_HOUR_DIR_TMP}
                    if [ $? -eq 0 ]
                    then
                        ${HADOOP} fs -touchz ${IMP_HOUR_DIR_TMP}/_SUCCESS
                        #${HADOOP} fs -rm -r ${IMP_BASE_DIR_TMP}/_distcp_logs_*
                        echo "Copying the impression data from ${s3_imp_hour_dir} to ${IMP_HOUR_DIR_TMP} is done."
                    else
                        msg="Copying the impression data folder is empty. Please investigate!"
                        sendalert $msg
                    fi
                else
                    msg="Copying the impression data job failed. Please investigate!"
                    sendalert $msg
                fi
                break
            else
                echo "The Hour Data: ${s3_imp_hour_dir} is not ready at `date -u +%Y%m%d%H` !";
                sleep 5m
                let cnt_sleep_imp=${cnt_sleep_imp}+1
                echo "The number of sleeps: ${cnt_sleep_imp}"
                continue
            fi
        done
    fi

    echo "Combining all files under ${IMP_HOUR_DIR_TMP} into one gzipped file under ${IMP_HOUR_DIR}"
    test -e ${IMP_LOCAL_TMP}
    if [ $? -eq 0 ]
    then
        echo "The local folder: ${IMP_LOCAL_TMP} exists!"
    else
        mkdir ${IMP_LOCAL_TMP}
    fi

    echo "Before generating the data, ${HADOOP} fs -rm -r ${IMP_HOUR_DIR}"
    ${HADOOP} fs -rm -r ${IMP_HOUR_DIR}

    file_name=${IMP_LOCAL_TMP}/imp_${stringZ}
#    cnt_sleep_imp=0
#    while [ $cnt_sleep_imp -le 0 ]
#    do
        ${HADOOP} fs -test -e ${IMP_HOUR_DIR_TMP}/_SUCCESS
        if [ $? -eq 0 ]
        then
            echo "${HADOOP} fs -cat ${IMP_HOUR_DIR_TMP}/*.gz | zcat > ${file_name}"
            ${HADOOP} fs -cat ${IMP_HOUR_DIR_TMP}/*.gz | zcat > ${file_name}
            if [ $? -eq 0 ]
            then
                ${HADOOP} fs -test -e ${IMP_HOUR_DIR}
                if [ $? -ne 0 ]
                then
                    ${HADOOP} fs -mkdir ${IMP_HOUR_DIR}
                fi
                echo "${HADOOP} fs -put ${file_name} ${IMP_HOUR_DIR}"
                ${HADOOP} fs -put ${file_name} ${IMP_HOUR_DIR}
                if [ $? -eq 0 ]
                then
                    ${HADOOP} fs -touchz ${IMP_HOUR_DIR}/_SUCCESS
                    rm ${file_name}
                    echo "${HADOOP} fs -rm -r ${IMP_HOUR_DIR_TMP}"
                    ${HADOOP} fs -rm -r ${IMP_HOUR_DIR_TMP}
                else
                    msg="Put the gziped file to HDFS failed for impression... please investigate"
                    sendalert $msg
                fi
                break
            else
                msg="Gzipped file to the local folder failed for impression... please investigate"
                sendalert $msg
            fi
        else
            echo "The raw impression data under ${IMP_HOUR_DIR_TMP} is not ready at `date -u +%Y%m%d%H`."
 #           sleep 5m
 #           let cnt_sleep_imp=${cnt_sleep_imp}+1
 #           echo "The number of sleeps: ${cnt_sleep_imp}"
 #           continue
        fi
 #   done

    if [ $cnt_sleep_imp -gt $NUM_SLEEP_HOUR ]
    then
        msg="Gzipped impression data job failed due to that ${IMP_HOUR_DIR_TMP} is not ready at `date -u +%Y%m%d%H`."
        sendalert $msg
    fi
fi

#exit 0

