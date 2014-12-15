#!/bin/bash

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`

export PROJ_DIR=${install_root}


BIN_DIR=${PROJ_DIR}/bin
CONF_DIR=${PROJ_DIR}/conf
JAR_DIR=${PROJ_DIR}/jar
INPUT_DATA_DIR=${PROJ_DIR}/input
OUTPUT_DATA_DIR=${PROJ_DIR}/output
LOG_DIR=${PROJ_DIR}/logs

function log {
	echo "[`date -u`] $*";
	echo "[`date -u`] $*" >> $log_file;
}

function abort {
	log "$*";
	exit 1;
}  
function waitInMins {
    mins=$1
    log "`date -u` - sleeping for ${mins} mins"
    let secs=${mins}*60
    sleep ${secs}
}
function datedelay {
   input_d=$1
   ifplus=$2
   num_day=$3
   input_d_yyyy=${input_d:0:4}
   input_d_mm=${input_d:4:2}
   input_d_dd=${input_d:6:2}
   input_date="${input_d_yyyy}-${input_d_mm}-${input_d_dd} 00:00:00"
   input_date_sec=`date --date="${input_date}" +%s`
   if [ "$ifplus" == "plus" ]
   then
     let new_input_date_sec=${input_date_sec}+${num_day}*24*60*60
   else
     let new_input_date_sec=${input_date_sec}-${num_day}*24*60*60
   fi
   echo `date -d @$new_input_date_sec +"%Y%m%d"`
}

utcdate=$(date -u +%Y%m%d%H%M)
year=${utcdate:0:4}
month=${utcdate:4:2}
day=${utcdate:6:2}
hour=${utcdate:8:2}
log_file=${LOG_DIR}/log_ds_main_${day}_${hour}.txt
echo "log_file=$log_file";
>${log_file};

log "`date -u` - starting main job..."
aggr_day=`date -u +%Y%m%d`
aggr_day=`datedelay ${aggr_day} "minus" 1`
log "`date -u` - target day: ${aggr_day}"

hour_data_ready=01

while true
do
    curr_time=`date -u +%Y%m%d%H`
    curr_day=${curr_time:0:8}
    #curr_hour=${curr_time:8:2}
    curr_hour=${curr_time:0:10}
    target_dayplusone=`datedelay ${aggr_day} "plus" 1`;
    target_hour=${target_dayplusone}${hour_data_ready}
    log "`date -u` - target day: ${aggr_day}, curr_hour=${curr_hour}, data_expected=${target_hour}"
    if [ ${curr_hour} -gt ${target_hour} ]
    then
		log "${BIN_DIR}/daily_ias_process.sh ${aggr_day}"
		${BIN_DIR}/daily_ias_process.sh ${aggr_day} 2>&1 >> $log_file
        if [ $? -eq 0 ]; then 
			aggr_day=`datedelay ${aggr_day} "plus" 1`;
			target_dayplusone=`datedelay ${aggr_day} "plus" 1`;
    		target_hour=${target_dayplusone}${hour_data_ready}
			log "daily_ias_process Success! Moving to next day ${aggr_day}";
		else
			log "Try again same day! No data for ${aggr_day}: $curr_hour, $target_hour";
		fi
        log "`date -u` - target day: ${aggr_day}, curr_hour=${curr_hour}, data_expected=${target_hour}"
    fi
    waitInMins 15
    
done

exit 0
