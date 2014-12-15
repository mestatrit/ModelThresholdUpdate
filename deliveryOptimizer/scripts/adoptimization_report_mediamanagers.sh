#!/bin/bash
set -o nounset

export TZ=America/Los_Angeles
path=/home/btdev/products/deliveryOptimizer/prod
day=$(date "+%Y-%m-%d %H:%M")

do_runner=${path}/bin/DORunner.jar
do_prop=${path}/bin/res/deliveryOptimizer.properties
do_log=${path}/bin/res/log4j.properties
report_file=${path}/tmp/do_report_$(date "+%Y-%m-%d_%H%M").txt

java -cp ${do_runner} com.sharethis.delivery.job.Report -dop ${do_prop} -log4jp ${do_log} -type all > ${report_file}

sed -i 's/\[exec\]//g' ${report_file}

subject="Delivery Optimizer Report: ${day}"
mailto="mediamanagers@sharethis.com,markku@sharethis.com"
mailfrom="markku@sharethis.com"

{
echo "From: ${mailfrom}"
echo "To: ${mailto}"
echo "Subject: ${subject}"

/usr/bin/uuencode ${report_file} ${report_file}

} | /usr/sbin/sendmail -t

rm ${report_file}
