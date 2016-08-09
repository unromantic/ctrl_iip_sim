#!/bin/bash
echo "[STOP] Sending SIGINT to all processes."
service rabbitmq-server stop
pkill -SIGINT -f redis-server
pkill -SIGINT -f redis-commander
pkill -SIGINT -f Forwarder.py
pkill -SIGINT -f Distributor.py
pkill -SIGINT -f BaseForeman.py
pkill -SIGINT -f NCSAForeman.py
sleep "2"
echo "[STOP] Sending SIGTERM to all leftover processes."
pkill -SIGTERM -f redis-server
pkill -SIGTERM -f redis-commander
pkill -SIGTERM -f Forwarder.py
pkill -SIGTERM -f Distributor.py
pkill -SIGTERM -f BaseForeman.py
pkill -SIGTERM -f NCSAForeman.py