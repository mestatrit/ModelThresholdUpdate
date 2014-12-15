#!/bin/bash
set -o nounset

export TZ=America/Los_Angeles
day=$(date "+%Y-%m-%d %H:%M")
fileName=""

scriptDir=$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
. ${scriptDir}/adoptimization_env.sh

report_file=${path}/tmp/do_report_$(date "+%Y-%m-%d_%H%M").txt

java -cp ${do_runner} com.sharethis.delivery.job.Report -path ${path} -dop ${do_prop} -log4jp ${do_log} -type all -user all -time default > ${report_file}

#sed -i 's/\[exec\]//g' ${report_file}

subject="Delivery Optimizer Report: ${day}"
mailto="prasanta@sharethis.com,markku@sharethis.com"
mailfrom="markku@sharethis.com"

{
echo "From: ${mailfrom}"
echo "To: ${mailto}"
echo "Subject: ${subject}"

/usr/bin/uuencode ${report_file} ${report_file}

} | /usr/sbin/sendmail -t

rm ${report_file}

echo "$(timestamp) Report sent to: $mailto"
