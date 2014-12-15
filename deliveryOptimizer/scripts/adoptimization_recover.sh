#!/bin/bash
set -o nounset

export TZ=America/Los_Angeles
scriptName=$(basename $0)
fileName="${scriptName%.*}"
address=$(/sbin/ifconfig | grep 'inet ' | grep -v '127.0.0.1' | head -n1 | awk '{print $2}' | sed 's/addr://g')
path=/home/btdev/products/deliveryOptimizer/prod
mail_file=${path}/tmp/mailbody_do.txt

senderror() {

message="$*"
subject="[ERROR] -- Delivery Optimizer Failed"
mailto="prasanta@sharethis.com,markku@sharethis.com"
mailfrom="markku@sharethis.com"

echo "Error: ${scriptName} @ ${address} " > ${mail_file}
echo "Cause: ${message} " >> ${mail_file}
cat ${mail_file}

cat - ${mail_file} << EOF | /usr/sbin/sendmail -t
To:$mailto
From:$mailfrom
Subject:$subject

EOF

rm ${mail_file}
}

do_runner=${path}/bin/DORunner.jar
do_prop=${path}/bin/res/deliveryOptimizer.properties
do_log=${path}/bin/res/log4j.properties
log_output=${path}/logs/${fileName}_$(date +%Y%m%d).log

echo "BEGIN: $scriptName $(date '+%Y-%m-%d %H:%M:%S %Z')" >> ${log_output}

echo "" >> ${log_output}
echo "Transfer task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Transfer -dop ${do_prop} -log4jp ${do_log} -data adx -case all >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Transfer task failed ($retvalue)"
	senderror $msg
	exit 1
else
	echo "Transfer task succeeded"
fi

echo "" >> ${log_output}
echo "Optimize task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Optimize -dop ${do_prop} -log4jp ${do_log} -pv priceVolumeCurves_backup -date default >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Optimize task failed ($retvalue)"
	senderror $msg
	exit 1
else
	echo "Optimize task succeeded"
fi

echo "" >> ${log_output}
echo "Deliver task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Deliver -dop ${do_prop} -log4jp ${do_log} -adx true -rtb true >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Deliver task failed ($retvalue)"
	senderror $msg
	exit 1
else
	echo "Deliver task succeeded"
fi

echo "" >> ${log_output}
echo "Update task:" >> ${log_output}

java -cp ${do_runner} com.sharethis.delivery.job.Update -dop ${do_prop} -log4jp ${do_log} >> ${log_output}
retvalue=$?
if [ $retvalue -ne 0 ]
then
	msg="Update task failed ($retvalue)"
	senderror $msg
else
	echo "Update task succeeded"
fi

echo "" >> ${log_output}
echo "COMPLETED: $scriptName $(date '+%Y-%m-%d %H:%M:%S %Z')" >> ${log_output}
echo "" >> ${log_output}

echo "IMPRESSION TARGETS ALLOCATED"
