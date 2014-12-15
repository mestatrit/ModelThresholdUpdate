#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

#export s3_bucket_dir=s3n://sharethis-insights-backup
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

stringZ_data=$1

echo "The date and time is ${stringZ_data}"
yyyy_old=${stringZ_data:0:4}
mm_old=${stringZ_data:4:2}
dd_old=${stringZ_data:6:2}
v_date_old=${stringZ_data:0:8}
pDate_old="${yyyy_old}-${mm_old}-${dd_old}"

. ${CONF_DIR}/env_ctr.sh

sendalert() {

message="$*"
echo $message
subject="[WARNING][CTR Prediction Daily] -- The daily copying predicted ctr old data failed while processing for day: $pDate_old"
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

CTR_BASE_DIR="${HDFS_DIR}/ctr_mv_daily"
CTR_DATE_DIR_OLD="${CTR_BASE_DIR}/${v_date_old}"

qName="-Dmapred.job.queue.name=${QUEUENAME_CTR_DAILY}"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_backup=s3n://sharethis-insights-backup/model
s3_ctr_date_dir=${s3_backup}/${v_date_old}/ctr_mv_daily

echo "${HADOOP} fs ${CONF} -test -e ${s3_ctr_date_dir}"
${HADOOP} fs ${CONF} -test -e ${s3_ctr_date_dir}
if [ $? -eq 0 ]
then

log_dir_ct="${LOG_TMP_DIR}/distcp_log_ct_${v_date_old}"
log="-log ${log_dir_ct}"

#CONF_S3="-conf ${CONF_DIR}/s3cfg-insights"
#echo "s3cmd -c ${CONF_S3} ls -e ${s3_ctr_date_dir}/_SUCCESS"
#s3cmd -c ${CONF_S3} ls -e ${s3_ctr_date_dir}/_SUCCESS
#echo "${HADOOP} fs ${CONF} -test -e ${s3_ctr_date_dir}"
#${HADOOP} fs ${CONF} -test -e ${s3_ctr_date_dir}
#if [ $? -eq 0 ]
#then
    echo "rm -rf ${log_dir_ct}"
    rm -rf ${log_dir_ct}

    echo "Before downloading the data from s3n, ${HADOOP} fs -rm -r ${CTR_DATE_DIR_OLD}"
    ${HADOOP} fs -rm -r ${CTR_DATE_DIR_OLD}

    echo "${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${s3_ctr_date_dir} ${CTR_DATE_DIR_OLD}"
    ${HADOOP} distcp ${CONF} ${DISTCP_OPT} ${qName} ${s3_ctr_date_dir} ${CTR_DATE_DIR_OLD}
    if [ $? -eq 0 ]
    then
        ${HADOOP} fs -test -e ${CTR_DATE_DIR_OLD}
        if [ $? -eq 0 ]
        then
            #${HADOOP} fs -rm -r ${CTR_DATE_DIR_OLD}/_distcp_logs_*
            ${HADOOP} fs -rm -r ${CTR_BASE_DIR}/_distcp_logs_*
            echo "Copying the ctr data from ${s3_ctr_date_dir} to ${CTR_DATE_DIR_OLD} is done."
        else
            msg="Copying the ctr data folder is empty. Please investigate!"
            sendalert $msg
        fi
    else
        msg="Copying the ctr data job failed. Please investigate!"
        sendalert $msg
    fi     
else
    echo "The daily data: ${s3_ctr_date_dir} is not ready!"
fi

#exit 0

