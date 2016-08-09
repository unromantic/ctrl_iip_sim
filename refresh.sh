#!/bin/bash
if [[ $EUID -ne 0 ]]; then
   echo "Run as root... (sudo su)"
   exit 1
fi
IP=`hostname -I | head -n 1 | xargs`
echo "[REFRESH] Redetected IP address as \"$IP\""
echo -n "DETECTED_IP_ADDRESS = \"$IP\"" > simulator/IP_Address.py
sed -i "61s/.*/bind 127.0.0.1 $IP/" config/redis.conf
# RabbitMQ refresh
echo "[REFRESH] Refreshing RabbitMQ"
rabbitmqctl stop_app
rabbitmqctl reset
service rabbitmq-server start
service rabbitmq-server restart
./config/rabbitmq_init_users.sh
# Redis refresh
echo "[REFRESH] Refreshing Redis"
pkill node
pkill redis
export PATH=$PATH:/usr/local/bin
sysctl vm.overcommit_memory=1
redis-server config/redis.conf &> /dev/null&
redis-commander --redis-db 1 --port 8081 &> /dev/null&
echo "[REFRESH] Done refreshing!"