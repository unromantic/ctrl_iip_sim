#!/bin/bash
WAIT_TIME="1"
# Only run once to set up a new machine
# Need to be root
echo "[INSTALL] Admin needs to open ports 8081 and 15672 for web management"
echo "[INSTALL] Checking if running as root"
sleep $WAIT_TIME
if [[ $EUID -ne 0 ]]; then
   echo "[INSTALL] Run as root... (sudo su)"
   exit 1
fi
# Start with a clean Fedora 22 Cloud image
# Create directories
echo "[INSTALL] Creating directories"
sleep $WAIT_TIME
mkdir /lsst
mkdir /xfer_files
chown fedora:fedora /lsst
chown fedora:fedora /xfer_files
# Get required packages
echo "[INSTALL] Updating through DNF"
sleep $WAIT_TIME
dnf -y update
echo "[INSTALL] Installing through DNF"
sleep $WAIT_TIME
dnf -y install rabbitmq-server git wget make gcc git
echo "[INSTALL] Easy installing python imports"
sleep $WAIT_TIME
easy_install pika pyyaml redis
# Assuming source was downloaded along with install.sh
echo "[INSTALL] Setting permissions"
sleep $WAIT_TIME
chmod 0766 run.sh
chmod 0766 refresh.sh
chmod 0766 stop.sh
chmod 0766 config/rabbitmq_init_users.sh
# chmod 0400 /lsst/rsync_key
# Store this machine's IP address
#  (assuming Nebula OpenStack, so this returns a single internal IP)
IP=`hostname -I | head -n 1 | xargs`
echo "[INSTALL] Detecting IP address as \"$IP\""
sleep $WAIT_TIME
echo -n "DETECTED_IP_ADDRESS = \"$IP\"" > simulator/IP_Address.py
# Can redis config just use 0.0.0.0?
sed -i "61s/.*/bind 127.0.0.1 $IP/" config/redis.conf
# Configure RabbitMQ server
#  (access at ip_addr:15672)
echo "[INSTALL] Configuring RabbitMQ"
sleep $WAIT_TIME
rabbitmq-plugins enable rabbitmq_management
mv config/rabbitmq.config /etc/rabbitmq.config
service rabbitmq-server restart
config/rabbitmq_init_users.sh
echo "[INSTALL] Downloading Redis"
sleep $WAIT_TIME
# Install Redis server
#  (worked in testing with redis 3.2.3 in case newer version has issues)
#  wget http://download.redis.io/releases/redis-3.2.3.tar.gz
#  tar xvzf redis-3.2.3.tar.gz
wget http://download.redis.io/redis-stable.tar.gz
tar xvzf redis-stable.tar.gz
echo "[INSTALL] Making Redis"
sleep $WAIT_TIME
cd redis-stable/deps
make hiredis lua jemalloc linenoise
cd ..
make distclean
make
echo "[INSTALL] Installing and configuring Redis"
sleep $WAIT_TIME
# export only applies to subshell (fix?)
export PATH=$PATH:/usr/local/bin
make install
cd ..
sysctl vm.overcommit_memory=1
# Install Redis Commander
#  (access at ip_addr:8081)
echo "[INSTALL] Installing and configuring redis-commander"
sleep $WAIT_TIME
dnf -y install npm
npm install -g redis-commander
echo "[INSTALL] Starting redis"
redis-server config/redis.conf &> /dev/null&
redis-commander --redis-db 1 --port 8081 &> /dev/null&
echo "[INSTALL] Ready to run!"
echo "(Don't forget to first copy rsync_key to /lsst/)"