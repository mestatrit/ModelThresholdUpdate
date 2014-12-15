#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
#echo $install_root

# Use this for everything else

export PROJ_DIR=$install_root

export s3_par_dir=s3n://sharethis-logs-parsed-rtblog
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

let delay_hour=2

stringZ=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`

#echo "The date and time is ${stringZ}"	
yyyy=${stringZ:0:4}
mm=${stringZ:4:2}
dd=${stringZ:6:2}
hh=${stringZ:8:2}
v_date=${stringZ:0:8}
pDate="${yyyy}-${mm}-${dd}"

. ${CONF_DIR}/env_ctr.sh

sendalert() {

message="$*"
echo $message
subject="[ERROR] -- Hourly copying data job failed while processing for hour: $hh on $pDate"
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

CONF="-conf ${CONF_DIR}/core-prod.xml"

( ${BIN_DIR}/data_imp_hourly.sh ${stringZ} >> ${LOGS_FILE_PATH}/data_imp_hourly_${stringZ}.log 2>&1 ) &
( ${BIN_DIR}/data_click_hourly.sh ${stringZ} >> ${LOGS_FILE_PATH}/data_click_hourly_${stringZ}.log 2>&1 ) &

exit 0

