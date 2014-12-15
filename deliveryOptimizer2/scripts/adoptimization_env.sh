#!/bin/bash
set -o nounset

timestamp() {
	echo $(date '+%Y-%m-%d %H:%M:%S %Z')
}

path="${scriptDir%/*}"
do_runner=${path}/bin/DORunner.jar
do_prop=${path}/bin/res/deliveryOptimizer.properties
do_log=${path}/bin/res/log4j.properties
log_output=${path}/logs/${fileName}_$(date +%Y%m%d).log

missing_kpi_dates=${path}/tmp/missingKpiDates.txt
error_cause=${path}/tmp/error.txt
mail_file=${path}/tmp/mailbody_do.txt
host=$(hostname)

mailto="prasanta@sharethis.com,markku@sharethis.com"
mailfrom="markku@sharethis.com"

senderror() {

errorStage="$*"
subject="[ERROR] -- Delivery Optimizer Failed"
errorCause=$(cat ${error_cause})

echo "Script: ${scriptName} @ ${address} ${host}" > ${mail_file}
echo "Cause: ${errorStage} " >> ${mail_file}
echo "Error: ${errorCause} " >> ${mail_file}
cat ${mail_file}

cat - ${mail_file} << EOF | /usr/sbin/sendmail -t
To:$mailto
From:$mailfrom
Subject:$subject

EOF

rm ${mail_file}
}

if [ -f ${error_cause} ]
then
	rm ${error_cause}
fi