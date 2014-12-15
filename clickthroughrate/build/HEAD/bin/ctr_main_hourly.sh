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

. ${CONF_DIR}/env_ctr.sh

echo "The time to start the CTR main script: `date -u`"
let delay_hour=${HOUR_DELAY_CTR}
let delay_hour_after=${delay_hour}-2
string_inc=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
string_inc_after=`date -u +%Y%m%d%H -d "${delay_hour_after} hour ago"` 
while true
do
   startingTime=`date -u +%s`
   string_h=${string_inc}
   string_h_after=${string_inc_after}
   hour_cnt=0
   echo " "
   currentTime=`date -u`
   echo "${currentTime} - All ctr hourly jobs in the previous ${NUM_HOUR} hours start ..."
   while [ $hour_cnt -lt $NUM_HOUR ] 
   do   
       #echo "The time to download the rtb data is ${string_h_after}"
       #echo "The date and time for hourly copying impression and click data is ${string_h_after}"
       #echo "${BIN_DIR}/data_imp_hourly.sh ${string_h_after} >> ${LOGS_FILE_PATH}/data_imp_hourly_${string_h}_after.log 2>&1"
       #( ${BIN_DIR}/data_imp_hourly.sh ${string_h_after} >> ${LOGS_FILE_PATH}/data_imp_hourly_${string_h_after}.log 2>&1 ) &
       #pid_imp=$!
       #echo "${BIN_DIR}/data_click_hourly.sh ${string_h_after} >> ${LOGS_FILE_PATH}/data_click_hourly_${string_h_after}.log 2>&1"
       #( ${BIN_DIR}/data_click_hourly.sh ${string_h_after} >> ${LOGS_FILE_PATH}/data_click_hourly_${string_h_after}.log 2>&1 ) &
       #pid_clk=$!
       #wait
       echo "`date -u` - The date and time for ctr hourly task is ${string_h}"
       echo "${BIN_DIR}/ctr_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/clickthroughrate_hourly_${string_h}.log 2>&1"
       ( ${BIN_DIR}/ctr_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/clickthroughrate_hourly_${string_h}.log 2>&1 )
       #echo "${BIN_DIR}/ctr_mv_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/clickthroughrate_mv_hourly_${string_h}.log 2>&1"
       #( ${BIN_DIR}/ctr_mv_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/clickthroughrate_mv_hourly_${string_h}.log 2>&1 )
       #pid_ctr=$!
       #wait $pid_ctr     
       yyyy_h=${string_h:0:4}
       mm_h=${string_h:4:2}
       dd_h=${string_h:6:2}
       hh_h=${string_h:8:2}
       pDate_h="${yyyy_h}-${mm_h}-${dd_h}"
       pDate_h_hour="${pDate_h} ${hh_h}:00:00"
       hour_h=`date --date="$pDate_h_hour" +%s`
       #date -d @$hour_h +"%Y%m%d%H"
       #echo $hh_h
       #let hhS_h=${hh_h}*3600
       let newZS_h=$hour_h-${HOUR_WD}*3600
       #echo $newZS_h
       string_h=`date -d @$newZS_h +"%Y%m%d%H"`
       let newZS_h_after=$hour_h-$((${HOUR_WD}-2))*3600
       string_h_after=`date -d @$newZS_h_after +"%Y%m%d%H"`
       let hour_cnt=$hour_cnt+$HOUR_WD
   done
   #wait
   currentTime=`date -u`
   echo "${currentTime} - all ctr hourly jobs in the previous ${NUM_HOUR} hours were done."
   echo " "
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
   let newZS_inc_after=$hour_inc+10800
   string_inc_after=`date -d @$newZS_inc_after +"%Y%m%d%H"`

   #let dayZS_inc=${hour_inc}-3600*24
   #vDate_inc_before=`date -d @$dayZS_inc +"%Y%m%d"`

   endingTime=`date -u +%s`
   diffTime=$(( $endingTime - $startingTime ))
   offsetTime=$((($endingTime - $newZS_inc )/3600))
   delayTime=$((3600 - $diffTime))
   if [ ${delayTime} -gt 0 ] && [ ${offsetTime} -lt $HOUR_DELAY_CTR ]
   then
      echo "`date -u` - The task is sleeping to wait and the sleep time is ${delayTime} seconds." 
      sleep ${delayTime}s
   fi
done

exit 0
