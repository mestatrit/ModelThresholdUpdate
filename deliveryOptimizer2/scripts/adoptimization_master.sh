#!/bin/bash
set -o nounset

lastPriceVolumeDate() {
	query="SELECT value FROM rtbDelivery2.admin_date WHERE name='PriceVolume';"
	result=`mysql --user sharethis --password=sharethis --host adopsdb1001.east.sharethis.com -s -N -e "$query"`
	echo $result
}

timestamp() {
	echo $(date '+%Y-%m-%d %H:%M:%S %Z')
}

export TZ=America/Los_Angeles
scriptName=$(basename $0)
fileName="${scriptName%.*}"

scriptDir=$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
path="${scriptDir%/*}"
log_output=${path}/logs/${fileName}_$(date +%Y%m%d).log

count=0
yesterday=$(date -d yesterday +'%Y-%m-%d 00:00:00')

echo "$(timestamp) BEGIN: $scriptName" > ${log_output}
echo "" >> ${log_output}

echo "$(timestamp) Execute: ${path}/scripts/adoptimization_optimize.sh" >> ${log_output}
${path}/scripts/adoptimization_optimize.sh

while [ "$(lastPriceVolumeDate)" != "$yesterday" ] && [ $count -lt 7 ]
do
	(( count++ ))
	echo "$(timestamp) Price-volume curves not ready: wait 15 min" >> ${log_output}
	sleep 900
done
	
if [ "$(lastPriceVolumeDate)" != "$yesterday" ]
then
	echo "$(timestamp) Execute: ${path}/scripts/adoptimization_recover.sh" >> ${log_output}
	${path}/scripts/adoptimization_recover.sh
else
	echo "$(timestamp) Execute: ${path}/scripts/adoptimization_primary.sh" >> ${log_output}
	${path}/scripts/adoptimization_primary.sh
fi

echo "" >> ${log_output}
echo "$(timestamp) COMPLETED: $scriptName" >> ${log_output}
