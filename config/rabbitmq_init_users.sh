#!/bin/bash
# Run as root
rabbitmqctl add_user DMCS DMCS
rabbitmqctl set_user_tags DMCS administrator
rabbitmqctl add_user FM FM
rabbitmqctl set_user_tags FM administrator
rabbitmqctl add_user NCSA NCSA
rabbitmqctl add_user Machine Machine
rabbitmqctl add_vhost /bunny
rabbitmqctl set_permissions -p /bunny DMCS '.*' '.*' '.*'
rabbitmqctl set_permissions -p /bunny FM '.*' '.*' '.*'
rabbitmqctl set_permissions -p /bunny NCSA '.*' '.*' '.*'
rabbitmqctl set_permissions -p /bunny Machine '.*' '.*' '.*'