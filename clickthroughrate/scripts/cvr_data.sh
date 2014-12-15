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

let week_before=3600*24*7
let oldZS=$hour_sec-$week_before
stringBefore=`date -d @$oldZS +"%Y%m%d"`
pDate_before="${stringBefore:0:4}-${stringBefore:4:2}-${stringBefore:6:2}"

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

. ${CONF_DIR}/env_cvr.sh

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


MOD_BASE_DIR="${HDFS_DIR_MOD}/cvr_data"
MOD_DATE_DIR="${MOD_BASE_DIR}/${v_date}"

#LAST_SUNDAY=`date -dlast-sunday +%Y%m%d`
#echo "LAST SUNSAY: ${LAST_SUNDAY}"

LAST_SUNDAY=`date -d "$pDate -$(date -d $pDate +%w) days" +%Y%m%d`
echo "pDate: ${pDate}"
echo "LAST SUNDAY: ${LAST_SUNDAY}"

LAST_SUNDAY_BEFORE=`date -d "$pDate_before -$(date -d $pDate_before +%w) days" +%Y%m%d`
echo "pDate_before: ${pDate_before}"
echo "LAST SUNDAY_BEFORE: ${LAST_SUNDAY_BEFORE}"


CVR_DATA_DATE_DIR="${HDFS_DIR_CVR_DATA}/vr/${LAST_SUNDAY}/model/data"
CVR_DATA_DATE_DIR_BEFORE="${HDFS_DIR_CVR_DATA}/vr/${LAST_SUNDAY_BEFORE}/model/data"

CONF="-conf ${CONF_DIR}/core-insights.xml"

DISTCP_OPT="-Ddistcp.bytes.per.map=51200"

s3_model_dir=${s3_bucket_dir}/model/${v_date}
s3_mod_dir=${s3_model_dir}/cvr_data

s3_mod_daily_dir=${s3_bucket_dir}/cvr_data/${v_date}

echo "Checking whether job is running for the day which has already processed in which case User has to clean up the folder first"

echo "${HADOOP} fs -test -e ${MOD_DATE_DIR}/_SUCCESS"

${HADOOP} fs -test -e ${MOD_DATE_DIR}/_SUCCESS
if [ $? -eq 0 ]
then
    echo "The job is done."
else
    echo "${HADOOP} fs -mkdir ${MOD_DATE_DIR}/rtb"
    ${HADOOP} fs -mkdir ${MOD_DATE_DIR}/rtb

    echo "${HADOOP} fs -test -e ${CVR_DATA_DATE_DIR}/_SUCCESS"
    ${HADOOP} fs -test -e ${CVR_DATA_DATE_DIR}/_SUCCESS
    if [ $? -eq 0 ]
    then
        echo "${HADOOP} fs -cp ${CVR_DATA_DATE_DIR}/* ${MOD_DATE_DIR}/rtb/"
        ${HADOOP} fs -cp ${CVR_DATA_DATE_DIR}/* ${MOD_DATE_DIR}/rtb/
        if [ $? -eq 0 ]
        then
            echo "${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS"
            ${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS
        fi 
    else
        echo "${HADOOP} fs -cp ${CVR_DATA_DATE_DIR_BEFORE}/* ${MOD_DATE_DIR}/rtb/"
        ${HADOOP} fs -cp ${CVR_DATA_DATE_DIR_BEFORE}/* ${MOD_DATE_DIR}/rtb/
        if [ $? -eq 0 ]
        then
            echo "${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS"
            ${HADOOP} fs -touchz ${MOD_DATE_DIR}/_SUCCESS
        fi
    fi
fi

