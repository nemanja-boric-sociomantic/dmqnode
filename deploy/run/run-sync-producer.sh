#!/bin/bash
count=1

while :
do
        echo "Started Dhtsync $count time(s)"
	$PWD/dhtsync-64 -S etc/ap-dhtmemory.xml -D etc/queue.ini --qchannel test -x 14 -c purchase_history admedia_categories user_retargeting usermap_16671055276272723590 usermap_2879492446251088096 admedia_metadata admedia_related
        sleep 5
	mv core cores/core_$(date -d "today" +"%Y%m%d%H%M")
        (( count++ ))
done
