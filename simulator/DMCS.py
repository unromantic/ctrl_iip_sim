import logging
import thread
import sys
import signal

import yaml
import pika

from Consumer import Consumer
from SimplePublisher import SimplePublisher
from InternalConst import *

PROGRAM_NAME = "DMCS"
from CustomPrint import CustomPrint

custom_print = CustomPrint(PROGRAM_NAME)
printc = custom_print.printc


# Signal to enable shutting down from a shell script
def ctrlccalled(*arg):
    raise KeyboardInterrupt, "Signal handler"
    return


class DMCS:
    # This DMCS simulator is how we interact with the test system.
    # Commands can be sent manually to simulate how to DMCS might work.
    def __init__(self):
        printc("Setting up DMCS...")
        self._options = "\
        1 - (READY) Send Job Information\n\
        2 - (SET)   Send Standby Message\n\
        3 - (GO)    Send Readout Message\n\
        4 - (RESET) Cancel a Job\n\
        0 - (EXIT)  Quit DMCS Simulator\n"
        self._broker_url = 'amqp://' + AMQP_DMCS_USER + ':' + AMQP_DMCS_PSWD + '@' + AMQP_BROKER_ADDR + ':' + AMQP_BROKER_PORT + '/' + AMQP_BROKER_VHOST
        printc('Using broker url: %s' % self._broker_url)
        printc("Declaring and binding exchanges...")
        printc("Attempting to create a consumer for the '%s' queue." % (Q_DMCS_CONSUME))
        self._dmcs_consumer = Consumer(self._broker_url, Q_DMCS_CONSUME)
        try:
            printc("Attempting to start the consumer thread...")
            thread.start_new_thread(self.run_dmcs_consumer, ())
        except:
            printc("Failed to start consumer thread, quitting...")
            sys.exit()
        printc("Done setting up consumer thread.")
        printc("Setting up publisher...")
        self._publisher = SimplePublisher(self._broker_url)
        printc("Done creating publisher.")
        self._job_msg = {}
        self._job_msg['MSG_TYPE'] = 'JOB'
        self._job_msg['JOB_NUM'] = 0
        self._job_msg['RAFT_NUM'] = 1
        self._standby_msg = {}
        self._standby_msg['MSG_TYPE'] = 'STANDBY'
        self._readout_msg = {}
        self._readout_msg['MSG_TYPE'] = 'READOUT'
        self._stop_msg = {}
        self._stop_msg['MSG_TYPE'] = 'CANCEL'
        self._shutdown_msg = {}
        self._shutdown_msg['MSG_TYPE'] = 'SHUTDOWN'

    def on_dmcs_messages(self, ch, method, properties, body):
        msg_dict = yaml.load(body)
        printc("Received: %r" % msg_dict)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    def run_dmcs_consumer(self):
        self._dmcs_consumer.run(self.on_dmcs_messages)
        return

    def run(self):
        keep_running = True
        while keep_running:
            try:
                user_input = int(raw_input(self._options))
            except:
                user_input = -1
            # New Job
            if 1 == user_input:
                good_input = True
                try:
                    new_raft_num = int(raw_input("How many pairs? : "))
                except:
                    good_input = False
                    printc("Bad input...")
                if good_input:
                    self._job_msg['JOB_NUM'] = self._job_msg['JOB_NUM'] + 1
                    self._job_msg['RAFT_NUM'] = new_raft_num
                    self._publisher.publish_message(Q_DMCS_PUBLISH, yaml.dump(self._job_msg))
                pass
            # Standby
            elif 2 == user_input:
                good_input = True
                try:
                    new_job_num = int(raw_input("STANDBY which job? : "))
                except:
                    good_input = False
                    printc("Bad input...")
                if good_input:
                    self._standby_msg['JOB_NUM'] = str(new_job_num)
                    self._publisher.publish_message(Q_DMCS_PUBLISH, yaml.dump(self._standby_msg))
                pass
            # Readout
            elif 3 == user_input:
                good_input = True
                try:
                    new_job_num = int(raw_input("READOUT which job? : "))
                except:
                    good_input = False
                    printc("Bad input...")
                if good_input:
                    self._readout_msg['JOB_NUM'] = str(new_job_num)
                    self._publisher.publish_message(Q_DMCS_PUBLISH, yaml.dump(self._readout_msg))
                pass
            # Cancel
            elif 4 == user_input:
                good_input = True
                try:
                    job_cancel = int(raw_input("Cancel which job? : "))
                except:
                    good_input = False
                    printc("Bad input...")
                if good_input:
                    self._stop_msg['JOB_NUM'] = job_cancel
                    self._publisher.publish_message(Q_DMCS_PUBLISH, yaml.dump(self._stop_msg))
                pass
            # Exit
            elif 0 == user_input:
                keep_running = False
            else:
                printc("Invalid input...\n")
        return


# Run DMCS simulator
def main():
    signal.signal(signal.SIGINT, ctrlccalled)
    dmcs = DMCS()
    try:
        dmcs.run()
    except KeyboardInterrupt:
        pass
    printc("DMCS simulator quit by user.")
    return


if __name__ == "__main__": main()
