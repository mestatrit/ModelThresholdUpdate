#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
#echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

#export s3_par_dir=s3n://sharethis-logs-parsed-rtblog
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

#check to see if all arguments passed

#let delay_hour=4

#if [ $1 == -1 ]
#then
#    stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
#else
#    stringZ=$1
#fi


delay_day=1
while [ ${delay_day} -le 1 ]
do
stringZ=`date -u +%Y%m%d -d  "${delay_day} day ago"`

echo "The date and time is ${stringZ}"  
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
v_date=${stringZ:0:8}
pDate="${yyyy}-${mm}-${dd}"

. ${CONF_DIR}/env_ctr.sh

sendalert() {

message="$*"
echo $message
subject="[ERROR] -- Daily Copying Data: The daily copying job failed while processing the data on $pDate"
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

CONF="-conf ${CONF_DIR}/core-insights.xml"

#. ${BIN_DIR}/data_ctr_daily.sh >> ${LOGS_DIR}/data_ctr_daily_${stringZ}.log 2>&1

CTR_BASE_DIR="${HDFS_DIR}/ctr"
CTR_DATE_DIR="${CTR_BASE_DIR}/${v_date}"

qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_DAILY}"

s3_backup=s3n://sharethis-insights-backup/model/
s3_ctr_date_dir=${s3_backup}/${v_date}/ctr

log_dir_cr="${LOG_TMP_DIR}/distcp_log_cr_${v_date}"
log="-log ${log_dir_cr}"

${HADOOP} fs ${CONF} -test -e ${s3_ctr_date_dir}
if [ $? -eq 0 ]
then
    echo "rm -rf ${log_dir_cr}"
    rm -rf ${log_dir_cr}

    echo "Before downloading the data from s3n, ${HADOOP} fs -rm -r ${CTR_DATE_DIR}"
    ${HADOOP} fs -rm -r ${CTR_DATE_DIR}

    echo "${HADOOP} distcp ${CONF} ${qName} ${log} ${s3_ctr_date_dir} ${CTR_DATE_DIR}"
    ${HADOOP} distcp ${CONF} ${qName} ${log} ${s3_ctr_date_dir} ${CTR_DATE_DIR}
    if [ $? -eq 0 ]
    then
        ${HADOOP} fs -test -e ${CTR_DATE_DIR}
        if [ $? -eq 0 ]
        then
            #${HADOOP} fs -rm -r ${CTR_DATE_DIR}/_distcp_logs_*
            echo "Copying the ctr data from ${s3_ctr_date_dir} to ${CTR_DATE_DIR} is done."
        else
            msg="Copying the ctr data folder is empty. Please investigate!"
            sendalert $msg
        fi
    else
        msg="Copying the ctr data job failed. Please investigate!"
        sendalert $msg
    fi
else
    echo "The Hour Data: ${s3_ctr_date_dir} is not ready!";
fi

let delay_day=${delay_day}+1
done

#exit 0

