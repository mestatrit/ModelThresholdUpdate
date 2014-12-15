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

. ${CONF_DIR}/env_pricevolume.sh

echo "The time to start the PV main script: `date -u`"
let delay_hour=${HOUR_DELAY_PV}
let delay_hour_prev=${delay_hour}+1
string_inc=`date -u +%Y%m%d%H -d "${delay_hour} hour ago"`
string_inc_prev=`date -u +%Y%m%d%H -d "${delay_hour_prev} hour ago"` 
while [ 1 -eq 1 ]
do
   echo "The date and hour to be processed is ${string_inc}"
   startingTime=`date -u +%s`
   string_h=${string_inc}
   string_h_prev=${string_inc_prev}
   hour_cnt=0
   echo " "
   currentTime=`date -u`
   echo "${currentTime} - All hourly jobs in the previous ${NUM_HOUR} hours start ..." 

   while [ $hour_cnt -lt $NUM_HOUR ] 
   do   
       #echo "`date -u` - The date and time for hourly copying raw bid and pc data is ${string_h}"
       #echo "${BIN_DIR}/data_bid_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_bid_hourly_${string_h}.log 2>&1"
       #( ${BIN_DIR}/data_bid_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_bid_hourly_${string_h}.log 2>&1 ) &
       #pid_bid=$!
       #echo "${BIN_DIR}/data_pc_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_pc_hourly_${string_h}.log 2>&1"
       #( ${BIN_DIR}/data_pc_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_pc_hourly_${string_h}.log 2>&1 ) & 
       #pid_pc=$!
       echo "`date -u` - The date and time for pv hourly task is ${string_h_prev}."
       echo "${BIN_DIR}/pv_hourly.sh ${string_h_prev} >> ${LOGS_FILE_PATH}/pricevolume_hourly_${string_h_prev}.log 2>&1"
       ( ${BIN_DIR}/pv_hourly.sh ${string_h_prev} >> ${LOGS_FILE_PATH}/pricevolume_hourly_${string_h_prev}.log 2>&1 ) &
       pid_pv=$!
       #echo "`date -u` - The date and time for hourly copying raw nobid data is ${string_h}"
       #echo "${BIN_DIR}/data_nobid_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_nobid_hourly_${string_h}.log 2>&1"
       #( ${BIN_DIR}/data_nobid_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/data_nobid_hourly_${string_h}.log 2>&1 ) &
       #pid_nb=$!
       echo "`date -u` - The date and time for inventory hourly task is ${string_h}."
       echo "${BIN_DIR}/inventory_hourly.sh ${string_h}>> ${LOGS_FILE_PATH}/inventory_hourly_${string_h}.log 2>&1"
       ( ${BIN_DIR}/inventory_hourly.sh ${string_h} >> ${LOGS_FILE_PATH}/inventory_hourly_${string_h}.log 2>&1 ) &
       pid_inv=$! 
       wait $pid_pv $pid_inv
       yyyy_h=${string_h:0:4}
       mm_h=${string_h:4:2}
       dd_h=${string_h:6:2}
       hh_h=${string_h:8:2}
       pDate_h="${yyyy_h}-${mm_h}-${dd_h}"
       pDate_h_hour="${pDate_h} ${hh_h}:00:00"
       hour_h=`date --date="${pDate_h_hour}" +%s`
       #date -d @$hour_h +"%Y%m%d%H"
       
       let newZS_h=$hour_h-${HOUR_WD}*3600
       #echo $newZS_h
       string_h=`date -d @$newZS_h +"%Y%m%d%H"`
       let HOUR_WD1=${HOUR_WD}+1
       let newZS_h_prev=${hour_h}-${HOUR_WD1}*3600
       string_h_prev=`date -d @$newZS_h_prev +"%Y%m%d%H"`
       let hour_cnt=$hour_cnt+$HOUR_WD
   done

   currentTime=`date -u`
   echo "${currentTime} - All hourly jobs in the previous ${NUM_HOUR} hours were done." 
   echo " "
   yyyy_inc=${string_inc:0:4}
   mm_inc=${string_inc:4:2}
   dd_inc=${string_inc:6:2}
   hh_inc=${string_inc:8:2}
   vDate_inc=${string_inc:0:8}
   pDate_inc="${yyyy_inc}-${mm_inc}-${dd_inc}"
   string_inc_prev=${string_inc}

   pDate_inc_hour="${pDate_inc} ${hh_inc}:00:00"
   hour_inc=`date --date="${pDate_inc_hour}" +%s`
   let newZS_inc=${hour_inc}+3600
   string_inc=`date -d @$newZS_inc +"%Y%m%d%H"`

   let dayZS_inc=${hour_inc}-3600*24
   vDate_inc_before=`date -d @$dayZS_inc +"%Y%m%d"`

   echo "The current processing hour: $hh_inc"
   echo "The hour to start the daily inventory job: $HOUR_INV_DAILY"
   if [ $hh_inc == $HOUR_INV_DAILY ]
   then
       echo "`date -u` - The daily inventory job of ${vDate_inc_before} started."
       echo "${BIN_DIR}/inventory_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/inventory_daily_${vDate_inc_before}.log 2>&1"
       ( ${BIN_DIR}/inventory_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/inventory_daily_${vDate_inc_before}.log 2>&1 ) &
   fi

   echo "The hour to start the daily pv job: $HOUR_PV_DAILY"
   if [ $hh_inc == $HOUR_PV_DAILY ]
   then
       echo "`date -u` - The daily price-volume job of ${vDate_inc_before} started."
       echo "${BIN_DIR}/pv_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/pricevolume_daily_${vDate_inc_before}.log 2>&1"
       ( ${BIN_DIR}/pv_daily.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/pricevolume_daily_${vDate_inc_before}.log 2>&1 ) &
   fi

   echo "The hour to start the daily pv and inventory backup job: $HOUR_PV_BKUP_DAILY"
   if [ $hh_inc == $HOUR_PV_BKUP_DAILY ]
   then
       echo "`date -u` - The daily backup job of ${vDate_inc_before} started."
       echo "${BIN_DIR}/pv_backup.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/pricevolume_backup_${vDate_inc_before}.log 2>&1"
       ( ${BIN_DIR}/pv_backup.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/pricevolume_backup_${vDate_inc_before}.log 2>&1 ) &
       echo "${BIN_DIR}/inventory_backup.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/inventory_backup_${vDate_inc_before}.log 2>&1"
       ( ${BIN_DIR}/inventory_backup.sh ${vDate_inc_before} >> ${LOGS_FILE_PATH}/inventory_backup_${vDate_inc_before}.log 2>&1 ) &
   fi

   endingTime=`date -u +%s`
   diffTime=$(( $endingTime - $startingTime ))
   offsetTime=$(( ($endingTime - $newZS_inc)/3600 ))
   delayTime=$((3600-$diffTime)) 
   if [ ${delayTime} -gt 0 ] && [ ${offsetTime} -lt $HOUR_DELAY_PV ]
   then
       echo "`date -u` - The task is sleeping to wait and the sleep time is ${delayTime} seconds." 
       sleep ${delayTime}s
   fi
done

exit 0
