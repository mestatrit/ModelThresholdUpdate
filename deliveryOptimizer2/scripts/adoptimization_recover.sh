#!/bin/bash
set -o nounset

export TZ=America/Los_Angeles
scriptName=$(basename $0)
fileName="${scriptName%.*}"
address=$(/sbin/ifconfig | grep 'inet ' | grep -v '127.0.0.1' | head -n1 | awk '{print $2}' | sed 's/addr://g')

scriptDir=$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
scriptName=${scriptDir}/${scriptName}
. ${scriptDir}/adoptimization_env.sh

echo "$(timestamp) BEGIN: $scriptName" >> ${log_output}

echo "" >> ${log_output}
echo "$(timestamp) Transfer task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Transfer -path ${path} -dop ${do_prop} -log4jp ${do_log} -data rtb -case all >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Transfer task failed"
	senderror $msg
	exit 1
else
	echo "$(timestamp) Transfer task finished"
fi

echo "" >> ${log_output}
echo "$(timestamp) Optimize task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Optimize -path ${path} -dop ${do_prop} -log4jp ${do_log} -pv priceVolumeCurves_backup -date default >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Optimize task failed"
	senderror $msg
	exit 1
else
	echo "$(timestamp) Optimize task finished"
fi

echo "" >> ${log_output}
echo "$(timestamp) Deliver task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Deliver -path ${path} -dop ${do_prop} -log4jp ${do_log} -adx false -rtb true >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Deliver task failed"
	senderror $msg
	exit 1
else
	echo "$(timestamp) Deliver task finished"
fi

echo "" >> ${log_output}
echo "$(timestamp) Update task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Update -path ${path} -dop ${do_prop} -log4jp ${do_log} >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Update task failed"
	senderror $msg
else
	echo "$(timestamp) Update task finished"
fi

echo "" >> ${log_output}
echo "$(timestamp) COMPLETED: $scriptName" >> ${log_output}
echo "" >> ${log_output}

echo "IMPRESSION TARGETS ALLOCATED"
