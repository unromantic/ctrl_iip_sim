import thread
import random
import datetime
import sys
import string
import time

import pika
import yaml

from Scoreboard import Scoreboard
from Consumer import Consumer
from SimplePublisher import SimplePublisher
from InternalConst import *
from const import *

PROGRAM_NAME = "FORE"
from CustomPrint import CustomPrint

custom_print = CustomPrint(PROGRAM_NAME)
printc = custom_print.printc


class Foreman:
    # Parent class for Base and NCSA Foremen.
    # Contains set up for forwarder and distributor machines
    # along with code that was common between them.
    def parent_init(self, db_num, prog_name, type):
        custom_print.define_new_name(self.PROGRAM_NAME)

        # Create machine scoreboard
        self._sb_mach = Scoreboard(db_num, prog_name, type)

        # Messaging URL (rabbitmq server IP)
        self._broker_url = "amqp://" + AMQP_BF_USER + ":" + AMQP_BF_PSWD + "@" + AMQP_BROKER_ADDR + ":" + AMQP_BROKER_PORT + "/" + AMQP_BROKER_VHOST

        # Publisher object for sending messages to rabbit
        printc("Creating publisher...")
        self._publisher = SimplePublisher(self._broker_url)

        # Machine messages
        self._msg_actions_mach = {
            'TRANSFER_DONE': self.process_transfer_done,  # Machine done with the current job
            'REGISTER': self.process_register,  # New machine wants to join
            'DEREGISTER': self.process_deregister,  # Machine is leaving
            'STATE_UPDATE': self.process_state_update  # Machine updating us on its state
        }

        # Machines register with us and let us know how they are doing
        printc("Creating machine consumer...")
        self._mach_consumer = Consumer(self._broker_url, self._machine_publish_q)
        try:
            thread.start_new_thread(self.run_mach_consumer, ())
        except:
            printc("Thread run_mach_consumer failed, quitting...")
            sys.exit()

        return

    def run_mach_consumer(self):
        # Consume messages continuously
        printc("Machine message consumer is running...")
        self._mach_consumer.run(self.on_mach_message)
        return

    def on_mach_message(self, ch, method, properties, body):
        # Callback from consumer to process machine messages
        # Load the message which came in yaml format
        msg_dict = yaml.load(body)
        # Determine which function needs to be called for this message type
        try:
            af_handler = self._msg_actions_mach.get(msg_dict[MSG_TYPE])
        except:
            printc("Bad machine message received...")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        # Call that function and provide it with the message
        af_handler(msg_dict)
        # Acknowledge that we processed the message so rabbit can remove it from the queue
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    def process_register(self, msg_params):
        # Process a request for a machine that is registering with us
        printc("Processing name request...")
        tmp_name = self.pick_name()
        while False == self._sb_mach.register_machine(tmp_name):
            tmp_name = self.pick_name()
        self._sb_mach._redis.hset(tmp_name, 'IP_ADDR', msg_params['IP_ADDR'])
        printc("%s has registered. (%s)" % (tmp_name, msg_params['IP_ADDR']))
        msg = {}
        msg[MSG_TYPE] = 'REGISTRATION'
        msg[NAME] = tmp_name
        self._publisher.publish_message(self._machine_consume_q, yaml.dump(msg))
        return

    def pick_name(self):
        # Name creation
        tmp_name = ''.join(random.choice(string.ascii_letters) for x in range(NAME_LENGTH))
        return self._machine_prefix + tmp_name

    def process_deregister(self, msg_params):
        # Machine is deregistering with us
        printc("%s has deregistered." % msg_params[NAME])
        self._sb_mach.machine_deregister(msg_params[NAME])
        return

    def process_state_update(self, msg_params):
        # Machine is updating us on something, report it in the Scoreboard
        self._sb_mach.machine_update(msg_params['KEY'], msg_params['FIELD'], msg_params['VALUE'])
        return

    def process_transfer_done(self, msg_params):
        return
