#!/bin/bash

bin=`which $0`
bin=`dirname ${bin}`
bin=`cd "$bin"; pwd -P`

install_root=`dirname ${bin}`

BIN_DIR=${install_root}/bin
CONF_DIR=${install_root}/conf
JAR_DIR=${install_root}/jar
RUN_DIR=${install_root}/run/iasReport
INPUT_DATA_DIR=${RUN_DIR}/input
OUTPUT_DATA_DIR=${RUN_DIR}/output
LOG_DIR=${install_root}/logs

AGGR_DATE=
AGGR_DAY=
utcdate=$(date -u +%Y%m%d%H%M)
year=${utcdate:0:4}
month=${utcdate:4:2}
day=${utcdate:6:2}
hour=${utcdate:8:2}

mkdir -p ${LOG_DIR}
log_file=${LOG_DIR}/log_daily_ias_report_${day}_${hour}.txt
#exit;
>${log_file};

function log {
	echo "[`date -u`] $*";
	echo "[`date -u`] $*" >> $log_file;
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
function cmd_a {
	cmd="$*"
	log $cmd
	eval $cmd
	ret=$?
	[ $ret -ne 0 ] && abort "Abort! Command failed($ret): $cmd";
	return $ret;
}
function cmd_m {
	cmd="$*"
	log $cmd
	eval $cmd
	ret=$?
	[ $ret -ne 0 ] && sendalert "Warn! Command failed($ret): $cmd";
	return $ret;
}

function cmd_q {
	cmd="$*"
	log $cmd
	eval $cmd
	ret=$?
	[ $ret -ne 0 ] && quit "Quit! Command failed($ret): $cmd";
	return $ret;
}
function cmd {
	cmd="$*"
	log $cmd
	eval $cmd
	ret=$?
	log "cmd return: $ret";
	return $ret
}

if [ $# -ge 1 ]; then 
	AGGR_DATE=$1
	year=${AGGR_DATE:0:4}
	month=${AGGR_DATE:4:2}
	day=${AGGR_DATE:6:2}
	AGGR_DAY=$day
else
	AGGR_DATE=$year$month$day
	AGGR_DAY=$day
fi

log "processing ${AGGR_DATE}";

force=false
[ $# -ge 2 ] && [ "$2" == "--force" ] && force=true;

#S3_INPUT_DIR=s3://sharethis-research/adquality/domain_summary
S3_INPUT_DIR=s3://sharethis-campaign-analytics/domainSummary
#S3_OUTPUT_DIR=s3://sharethis-campaign-analytics/domainSummary
S3_OUTPUT_DIR=s3://sharethis-campaign-analytics/adquality/ias_data
DATE=$year$month$day


emails=wahid@sharethis.com
scope="daily_ias_process.sh ";
sendalert() {
msg="$*";
subject=${scope}$(echo $* |awk -F: '{print $1}')
maillist="${emails}"

echo "Error stack as follows :: $msg" >> /tmp/mailbody_$$.txt

cat - /tmp/mailbody_$$.txt <<EOF | /usr/sbin/sendmail -t
To:$maillist
From:Watchdog<watchdog@sharethis.com>
Subject:${subject}

EOF
rm /tmp/mailbody_$$.txt
}

function abort {
	log "$*";
	sendalert "$*";
	exit 1;
}  
function quit {
	log "$*";
	#sendalert "$*";
	exit 0;
}  

function copyfroms3 {
  maxretry=5
  s3_base_dir=$1
	file=$2
  local_dir=$3

  echo "`date -u` copying  ${s3_base_dir}/${DATE}/$file to ${local_dir} ..."
  retry=0
	returncode=1

	while [[ ${retry} -lt $maxretry && ${returncode} -ne 0 ]]
	do
		/usr/bin/s3cmd --force get ${s3_base_dir}/${DATE}/$file $local_dir
		returncode=$?
		if [ $returncode -ne 0 ] || [ `ls $local_dir/$file|wc -l` -ne 1 ]; then
			returncode=1;
		fi
		if [ ${returncode} -ne 0 ]; then
	  		echo "retry attempt is $retry" >> $log_file
	  		sleep 2
		fi
		let retry=$retry+1
	done;
	
	if [ ${returncode} -ne 0 ]; then
		return 1
	else
		return 0
	fi
}

function copy2S3 {
  maxretry=5
  
  local_dir=$1
  file=$2
  s3_base_dir=$3

  echo "`date -u` copying  ${local_dir}/${DATE}/$file to ${s3_base_dir} ..."
  retry=0
	returncode=1

	while [[ ${retry} -lt $maxretry && ${returncode} -ne 0 ]]
	do
		/usr/bin/s3cmd put $local_dir/$file ${s3_base_dir}/
		returncode=$?
		if [ $returncode -ne 0 ] || [ `/usr/bin/s3cmd ls ${s3_base_dir}/$file|wc -l` -ne 1 ]; then
			returncode=1;
		fi
		if [ ${returncode} -ne 0 ]; then
	  		echo "retry attempt is $retry" >> $log_file
	  		sleep 2
		fi
		let retry=$retry+1
	done;
	
	if [ ${returncode} -ne 0 ]; then
		return 1
	else
		return 0
	fi
}
function checkOnS3 {
	s3file=$1
	if [ `/usr/bin/s3cmd ls $s3file|wc -l` -ne 1 ]; then
		return 1;
	fi;
	return 0;
}

function removeFromS3 {
	s3file=$1
	/usr/bin/s3cmd del $s3file
	return $?;
}

echo "log_file: $log_file";

#prepare input/output dirs
mkdir -p ${INPUT_DATA_DIR}
mkdir -p ${OUTPUT_DATA_DIR}


#2. run server to get IAS data
log "Start download ...";
CP=${JAR_DIR}/log4j-1.2.15.jar:${JAR_DIR}/gson-2.1.jar:${JAR_DIR}/commons-io-2.4.jar:${JAR_DIR}/commons-logging-1.1.1.jar:${JAR_DIR}/httpclient-4.3.4.jar:${JAR_DIR}/httpcore-4.3.2.jar:${JAR_DIR}/mysql-connector-java-5.1.18-bin.jar:${JAR_DIR}/httpcore-nio-4.3.2.jar:${JAR_DIR}/:${JAR_DIR}/sharethis.common.jar:${JAR_DIR}/AdQuality.jar
cmd_a "java -cp ${CP} com.sharethis.adquality.IASReport.ReportServer -iapp_path ${install_root} -iap $CONF_DIR/ias_downloader.properties -il4jp $CONF_DIR/log4j.properties 2>&1 >> $log_file"

[ $? -ne 0 ] && abort "Error: IASReportServer returned non-zero!";

> ${OUTPUT_DATA_DIR}/$doneFile;

#move input/ouput to dated dirs
cmd "rm -rf ${RUN_DIR}/${AGGR_DATE}";
cmd_a "mkdir -p ${RUN_DIR}/${AGGR_DATE}";
cmd_a "mv ${INPUT_DATA_DIR} ${RUN_DIR}/${AGGR_DATE}/"
cmd_a "mv ${OUTPUT_DATA_DIR} ${RUN_DIR}/${AGGR_DATE}/"

#remove data from 7 days ago
OLD_DATE=`datedelay ${AGGR_DATE} minus 7`
cmd "rm -r ${RUN_DIR}/${OLD_DATE}"

echo "log_file: $log_file";

exit 0;
