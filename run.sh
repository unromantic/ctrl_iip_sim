#!/bin/bash
FILE=/lsst/rsync_key
if [ -f $FILE ];
then
   sudo chmod 0400 /lsst/rsync_key
else
   echo "Missing /lsst/rsync_key"
   exit 1
fi
WAIT_TIME_SHORTEST="0.1"
WAIT_TIME_SHORT="0.25"
WAIT_TIME_MED="1"
WAIT_TIME_LONG="2"
FORWARDERS="5"
DISTRIBUTORS="5"
VALUE=0
if [ "$#" == "1" ]; then
    FORWARDERS=$1
    DISTRIBUTORS=$1
else
	if [ "$#" == "2" ]; then
		FORWARDERS=$1
		DISTRIBUTORS=$2
	fi
fi
echo [RUN] Starting or restarting RabbitMQ server...
sudo /bin/systemctl restart rabbitmq-server.service
sleep $WAIT_TIME_MED
echo [RUN] Running RabbitMQ setup script...
python simulator/RabbitMQSetup.py
sleep $WAIT_TIME_LONG
echo [RUN] Starting Foremen...
python simulator/BaseForeman.py&
sleep $WAIT_TIME_SHORTEST
#echo START BASE FOREMAN
#sleep 5
python simulator/NCSAForeman.py&
sleep $WAIT_TIME_SHORTEST
echo [RUN] Deploying $FORWARDERS forwarders...
for ((n=0;n<FORWARDERS;n++))
do
 python simulator/Forwarder.py&
 sleep $WAIT_TIME_SHORTEST
done
echo [RUN] Deploying $DISTRIBUTORS distributors...
for ((n=0;n<DISTRIBUTORS;n++))
do
 python simulator/Distributor.py&
 sleep $WAIT_TIME_SHORTEST
done
sleep $WAIT_TIME_SHORTEST
echo [RUN] Starting DMCS...
python simulator/DMCS.py
pkill -2 -f Forwarder.py
pkill -2 -f Distributor.py
for ((n=FORWARDERS+DISTRIBUTORS;n>0;n--))
do
 sleep $WAIT_TIME_SHORT
done
sleep $WAIT_TIME_MED
pkill -2 -f BaseForeman.py
pkill -2 -f NCSAForeman.py
sleep $WAIT_TIME_LONG
echo [RUN] Sending SIGKILL to any leftover scripts...
pkill -9 -f Forwarder.py
pkill -9 -f Distributor.py
pkill -9 -f BaseForeman.py
pkill -9 -f NCSAForeman.py