#!/bin/bash
#. ~/.bash_profile

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`
echo $install_root

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
if [ ! -d ${LOGS_FILE_PATH} ]; then
        mkdir -p ${LOGS_FILE_PATH}
fi

. ${CONF_DIR}/env_bl_data.sh

echo "The time to start the bl main daily script: `date -u`"
let delay_hour=${HOUR_DELAY_DATA_BL_AGG}
let delay_hour_after=${delay_hour}-2
string_inc=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
string_inc_after=`date -u +%Y%m%d%H -d "${delay_hour_after} hour ago"` 
while [ 1 -eq 1 ]
do
   startingTime=`date -u +%s`
   echo "The current time: `date -u`"
   string_inc=`date -u +%Y%m%d%H`
   yyyy_inc=${string_inc:0:4}
   mm_inc=${string_inc:4:2}
   dd_inc=${string_inc:6:2}
   hh_inc=${string_inc:8:2}
   vDate_inc=${string_inc:0:8}
   pDate_inc="${yyyy_inc}-${mm_inc}-${dd_inc}"

   pDate_inc_hour="${pDate_inc} ${hh_inc}:00:00"
   hour_inc=`date --date="${pDate_inc_hour}" +%s`
   let newZS_inc=${hour_inc}+3600
   string_inc=`date -d @$newZS_inc +"%Y%m%d%H"`
   #let newZS_inc_after=$hour_inc+10800
   #string_inc_after=`date -d @$newZS_inc_after +"%Y%m%d%H"`


   #######################################################################################################################################
   #Catch-up the previous day's daily task
   let dayZS_inc_2=${hour_inc}-3600*48
   vDate_inc_before_2=`date -d @$dayZS_inc_2 +"%Y%m%d"`

   echo "`date -u` - The daily bl job of ${vDate_inc_before_2} started."
   echo "${BIN_DIR}/brand_lift_daily.sh ${vDate_inc_before_2} >> ${LOGS_FILE_PATH}/brand_lift_daily_${vDate_inc_before_2}.log 2>&1"
   ${BIN_DIR}/brand_lift_daily.sh ${vDate_inc_before_2} >> ${LOGS_FILE_PATH}/brand_lift_daily_${vDate_inc_before_2}.log 2>&1
   #######################################################################################################################################

   let dayZS_inc=${hour_inc}-3600*24
   vDate_inc_before=`date -d @$dayZS_inc +"%Y%m%d"`

   echo "The current processing hour: $hh_inc"
   echo "The hour to start the daily bl job: $HOUR_BL_DAILY"
   typeset -i hh_inc_int
   typeset -i hour_daily_int
   hh_inc_int=$hh_inc
   hour_daily_int=$HOUR_BL_DAILY
   if [ $hh_inc_int -ge $hour_daily_int ]
   then
       echo "`date -u` - The daily bl job of ${vDate_inc_before} started."
       echo "${BIN_DIR}/brand_lift_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/brand_lift_daily_${vDate_inc_before}.log 2>&1"
       ${BIN_DIR}/brand_lift_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/brand_lift_daily_${vDate_inc_before}.log 2>&1 
   fi

   endingTime=`date -u +%s`
   diffTime=$(( $endingTime - $startingTime ))
   delayTime=$((3600 - $diffTime))
   if [ ${delayTime} -gt 0 ]
   then
      echo "`date -u` - The task is sleeping to wait and the sleep time is ${delayTime} seconds." 
      sleep ${delayTime}s
   fi
done

exit 0
