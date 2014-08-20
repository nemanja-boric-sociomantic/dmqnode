#!/bin/bash
count=1

while :
do
	echo "Started Dhtsync $count time(s)"
	$PWD/dhtsync-64 -S etc/queue.ini  -D etc/dhtmemory.xml  -A --qchannel test -x 10 -X 5
	sleep 1
	(( count++ ))
done
