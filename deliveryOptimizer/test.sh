fname=$(basename $0)
fileName="${fname%.*}"
echo $fileName
echo "$fname $(date '+%Y-%m-%d %H:%M:%S %Z')"
address=$(ifconfig | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2}' | sed 's/addr://g')
echo "Error: ${fname}@${address}"
