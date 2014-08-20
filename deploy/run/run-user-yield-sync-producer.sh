#!/bin/bash
count=1

while :
do
        echo "Started Dhtsync for user_yield $count time(s)"
	$PWD/dhtsync-64 -S etc/ap-dhtmemory.xml -D etc/queue.ini --qchannel user_yield -x 14 -c user_yield 
        sleep 5
	mv core cores/core_$(date -d "today" +"%Y%m%d%H%M")
        (( count++ ))
done
