import random
import string
import thread
import time

import pika
import yaml

from Consumer import Consumer
from SimplePublisher import SimplePublisher
from InternalConst import *
from const import *

PROGRAM_NAME = "MACH"
from CustomPrint import CustomPrint

custom_print = CustomPrint(PROGRAM_NAME)
printc = custom_print.printc


class Registration:
    # Registration class that is used to register a machine as a forwarder or
    # distributor along with assigning a unique name.
    def __init__(self, publish, consume):
        printc("Sending registration request")
        self._name = "UNKNOWN"
        # Messaging setup
        self._broker_url = "amqp://" + AMQP_MACH_USER + ":" + AMQP_MACH_PSWD + "@" + AMQP_BROKER_ADDR + ":" + AMQP_BROKER_PORT + "/" + AMQP_BROKER_VHOST
        self._publisher = SimplePublisher(self._broker_url)
        self._register_msg = {}
        self._register_msg[MSG_TYPE] = 'REGISTER'
        self._register_msg['IP_ADDR'] = DETECTED_IP_ADDRESS
        self._request_publish = publish
        self._request_consume = consume
        self._connection = pika.BlockingConnection(pika.URLParameters(self._broker_url))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._request_consume)
        return

    def request_name(self):
        # Send name request message and wait for a name
        printc("Requesting name...")
        self._publisher.publish_message(self._request_publish,
                                        yaml.dump(self._register_msg))
        method_frame, header_frame, body = self._channel.basic_get(queue=self._request_consume)
        while None == method_frame:
            method_frame, header_frame, body = self._channel.basic_get(queue=self._request_consume)
        # The else clause is only executed when your while condition becomes
        # false. If you break out of the loop, or if an exception is raised,
        # it won't be executed.
        else:
            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            self._connection.close()
            msg_dict = yaml.load(body)
            self._name = msg_dict['NAME']
            printc("Got name %s" % self._name)
            return self._name


class Machine:
    # Parent class for forwarders and distributors for their shared attributes.
    def __init__(self):
        try:
            # Create a lock for critical sections (used when canceling jobs)
            self._lock = thread.allocate_lock()
            self.child_init()
            # Create a temporary name until unique name is assigned
            c_name = ''.join(random.choice(string.ascii_letters) for x in range(NAME_LENGTH))
            c_name = self._type + "_" + c_name
            custom_print.define_new_name(c_name)
            printc("Starting...")
            # Register the machine with foreman before doing anything else
            self._register = Registration(self._publish_queue,
                                          self._consume_queue)
            # Block until a unique name is received
            self._name = self._register.request_name()
        except:
            return
        try:
            # Debug print setup
            custom_print.define_new_name(self._name)
            # Internal variable of the current state
            self._current_state = IDLE
            # Messaging setup
            self._broker_url = "amqp://" + AMQP_MACH_USER + ":" + AMQP_MACH_PSWD + "@" + AMQP_BROKER_ADDR + ":" + AMQP_BROKER_PORT + "/" + AMQP_BROKER_VHOST
            self._consume_queue = self._name + "_consume"
            self._home_dir = XFER_DIR
            # Messages foreman can send to us
            self._msg_actions = {
                JOB: self.process_foreman_job,
                STANDBY: self.process_foreman_standby,
                READOUT: self.process_foreman_readout,
                CANCEL: self.process_foreman_cancel
            }
            # Publisher to send messages to foreman
            printc("Setting up publisher...")
            self._publisher = SimplePublisher(self._broker_url)
            # Consumer for getting messages from foreman
            printc("Setting up consumer...")
            self._consumer = Consumer(self._broker_url, self._consume_queue)
        except:
            pass
        # Run blocking consumer
        try:
            self.run_consumer()
        except:
            pass

        # Alert foreman this machine is shutting down
        self.deregister()
        return

    def run_consumer(self):
        # Consumer blocking function
        self._consumer.run(self.on_message)
        return

    def child_init(self):
        # Function that child can call override to add variables during init
        _type = 'UNSET'
        return

    def deregister(self):
        # Send message to foreman to deregister machine
        msg = {}
        msg[MSG_TYPE] = 'DEREGISTER'
        msg[NAME] = self._name
        self._publisher.publish_message(self._publish_queue, yaml.dump(msg))
        return

    def state_update(self, key, field, value):
        # Send message to foreman about a state update
        msg = {}
        msg[MSG_TYPE] = 'STATE_UPDATE'
        msg['KEY'] = key
        msg['FIELD'] = field
        msg['VALUE'] = value
        self._publisher.publish_message(self._publish_queue, yaml.dump(msg))
        return

    # Foreman messaging

    def on_message(self, ch, method, properties, body):
        # Consumer callback function
        printc("Processing message...")
        msg_dict = yaml.load(body)
        try:
            af_handler = self._msg_actions.get(msg_dict[MSG_TYPE])
        except KeyError:
            printc("Invalid message received, cannot process.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        af_handler(msg_dict)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # If there was an ACK_ID in this message, they want a response
        # Initial idea is ACK to be sent after the callback has been called
        if 'ACK_ID' in msg_dict:
            self.send_ack(msg_dict['ACK_ID'], msg_dict['ACK_TYPE'])
        return
        
    def send_ack(self, session_id, type = None):
        if session_id is None:
            return
        if type is None:
            type = 'UNDEFINED'
        ack_msg = {}
        ack_msg['MSG_TYPE'] = 'ACK_RECEIVED'
        ack_msg['ACK_ID'] = str(session_id)
        ack_msg['ACK_NAME'] = str(self._name)
        ack_msg['ACK_TYPE'] = str(type)
        self._publisher.publish_message(Q_ACK_PUBLISH, yaml.dump(ack_msg))
        return

    def process_foreman_job(self, msg_params):
        return

    def process_foreman_standby(self, msg_params):
        return

    def process_foreman_readout(self, msg_params):
        return

    def process_foreman_cancel(self, msg_params):
        return
