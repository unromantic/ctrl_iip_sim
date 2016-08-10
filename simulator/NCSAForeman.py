import thread
import random
import datetime
import sys
import string
import time
import signal

import pika
import yaml

from Scoreboard import Scoreboard
from Consumer import Consumer
from SimplePublisher import SimplePublisher
from InternalConst import *
from const import *
from Foreman import *

PROGRAM_NAME = "NCSA"
from CustomPrint import CustomPrint

custom_print = CustomPrint(PROGRAM_NAME)
printc = custom_print.printc


# Signal to enable shutting down from a shell script
def ctrlccalled(*arg):
    raise KeyboardInterrupt, "Signal handler"
    return


class NCSAForeman(Foreman):
    # NCSAForeman receives messages from BaseForeman
    # and coordinates with distributors
    PROGRAM_NAME = "NCSA"

    def __init__(self):
        printc("Starting...")
        custom_print.define_new_name(self.PROGRAM_NAME)
        self._sb_mach = Scoreboard
        self._machine_prefix = 'D:'
        self._machine_publish_q = Q_DIST_PUBLISH
        self._machine_consume_q = Q_DIST_CONSUME
        # Messages we can recieve from BaseForeman
        self._msg_actions_bf = {
            JOB_REQUEST: self.process_bf_job_request,
            DISTRIBUTOR_REQUEST: self.process_bf_distributor_request,
            STANDBY: self.process_bf_standby,
            READOUT: self.process_bf_readout,
            CANCEL: self.process_bf_cancel,
            TRANSFER_DONE: self.process_bf_transfer_done
        }

        # Run parent init, starts distributor scoreboard and consumer
        self.parent_init(SCOREBOARD_DB_DIST, PROGRAM_NAME, REGISTER_DISTRIBUTOR)

        # BaseForeman consumer
        printc("Creating BaseForeman consumer...")
        self._bf_consumer = Consumer(self._broker_url, Q_NCSA_CONSUME)
        try:
            thread.start_new_thread(self.run_bf_consumer, ())
        except:
            printc("Thread run_bf_consumer failed, quitting...")
            sys.exit()
        return

    def run_bf_consumer(self):
        printc("BaseForeman message consumer is running...")
        self._bf_consumer.run(self.on_bf_message)
        return

    # BaseForeman messaging

    def on_bf_message(self, ch, method, properties, body):
        # BaseForeman message consumer
        msg_dict = yaml.load(body)
        try:
            af_handler = self._msg_actions_bf.get(msg_dict[MSG_TYPE])
        except:
            printc("Bad message received...")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        af_handler(msg_dict)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    def process_bf_job_request(self, msg_params):
        printc("NCSA Foreman is Online and ready for the Job")
        return

    def process_bf_distributor_request(self, msg_params):
        # BaseForeman wants some distributors for a job
        # Get the amount of distributors requested
        dist_needed = int(msg_params[DIST_NEEDED])
        # Check how many we have available
        num_healthy_distributors = self._sb_mach.count_idle(LIST_DISTRIBUTORS)
        printc("%d pairs available, %d were requested." % (num_healthy_distributors, dist_needed))
        # If we don't have enough in IDLE, report back that we don't have enough resources
        if dist_needed > num_healthy_distributors:
            printc("Not enough distributors.")
            insuff_dist_msg = {}
            insuff_dist_msg[MSG_TYPE] = INSUFFICIENT_NCSA_RESOURCES
            insuff_dist_msg[JOB_NUM] = msg_params[JOB_NUM]
            self._publisher.publish_message(Q_NCSA_PUBLISH, yaml.dump(insuff_dist_msg))
        # Otherwise, take the list of forwarders sent to us and pair each one
        # with an IDLE distributor and then send that pairing list back
        else:
            # Get a list that is the size we need of IDLE distributors
            self._dist_list = self._sb_mach.get_idle_list(LIST_DISTRIBUTORS,
                                                          dist_needed,
                                                          msg_params[JOB_NUM])
            printc("Generating pairs list...")
            forw_list = msg_params['FORW_LIST']
            current_pairs = {}
            counter = 0
            for each in forw_list:
                current_pairs[each] = self._dist_list[counter]
                counter = counter + 1
            # Send the pairing list back
            pair_msg = {}
            pair_msg[MSG_TYPE] = ACK_RECEIVED
            pair_msg[JOB_NUM] = msg_params[JOB_NUM]
            pair_msg[MISC] = current_pairs
            pair_msg[ACK_ID] = msg_params[ACK_ID]
            pair_msg[ACK_NAME] = PAIRING
            self._publisher.publish_message(Q_ACK_PUBLISH, yaml.dump(pair_msg))
        return

    def process_bf_standby(self, msg_params):
        # A job has moved to STANDBY
        job_num_tmp = str(msg_params[JOB_NUM])
        # Get the name of the file the distributors will be receiving
        xfer_file_main = msg_params[XFER_FILE]
        # Confirm distributors are working on this job
        if self._sb_mach.machine_find_all_m_check(LIST_DISTRIBUTORS, job_num_tmp) > 0:
            # Make a list of all the distributors for this job
            distributors = self._sb_mach.machine_find_all_m(LIST_DISTRIBUTORS, job_num_tmp)
            for distributor in distributors:
                ds_msg = {}
                ds_msg[MSG_TYPE] = STANDBY
                # ds_msg[XFER_FILE] = string.replace(xfer_file_main + '_' + distributor + '.raw', "D:", "")
                # The file was not being found on the Distributor machines so I put the exact name
                ds_msg[XFER_FILE] = xfer_file_main + '_' + 'None' + '.raw'
                routing_key = distributor + "_consume"
                self._publisher.publish_message(routing_key, yaml.dump(ds_msg))
            printc("Distributors have been sent the STANDBY message.")
            # Would probably wait and after getting ACK's from Distributors
            printc("Sending the STANDBY ACK...")
            ack_msg = {}
            ack_msg[ACK_ID] = msg_params[ACK_ID]
            ack_msg[ACK_NAME] = STANDBY
            ack_msg[MSG_TYPE] = ACK_RECEIVED
            ack_msg[JOB_NUM] = msg_params[JOB_NUM]
            self._publisher.publish_message( Q_ACK_PUBLISH, yaml.dump(ack_msg) )
        else:
            printc("No distributors are assigned to job %s, no STANDBY sent." % job_num_tmp)
        return

    def process_bf_readout(self, msg_params):
        # A job has moved to READOUT
        job_num_tmp = str(msg_params[JOB_NUM])
        # Confirm there are distributors working on this job and alert them
        if self._sb_mach.machine_find_all_m_check(LIST_DISTRIBUTORS, job_num_tmp) > 0:
            distributors = self._sb_mach.machine_find_all_m(LIST_DISTRIBUTORS, job_num_tmp)
            for distributor in distributors:
                dist_start = {}
                dist_start[MSG_TYPE] = READOUT
                dist_start[JOB_NUM] = msg_params[JOB_NUM]
                routing_key = distributor + "_consume"
                self._publisher.publish_message(routing_key, yaml.dump(dist_start))
            printc("Distributors have been sent the READOUT message.")
            printc("Sending the READOUT ACK...")
            ack_msg = {}
            ack_msg[ACK_ID] = msg_params[ACK_ID]
            ack_msg[ACK_NAME] = READOUT
            ack_msg[MSG_TYPE] = ACK_RECEIVED
            ack_msg[JOB_NUM] = msg_params[JOB_NUM]
            self._publisher.publish_message( Q_ACK_PUBLISH, yaml.dump(ack_msg) )
        else:
            printc("No distributors are assigned to job %s, no READOUT sent." % job_num_tmp)
        return

    def process_bf_cancel(self, msg_params):
        # A job was canceled
        # Get the job number being canceled
        job_to_stop = int(msg_params[JOB_NUM])
        printc("Telling distributors to cancel job %d..." % job_to_stop)
        # Tell distributors to stop
        stop_msg = {}
        stop_msg[MSG_TYPE] = 'CANCEL'
        stop_msg[JOB_NUM] = str(job_to_stop)
        # Find the distributors on that job
        list_of_q = self._sb_mach.machine_find_job(LIST_DISTRIBUTORS, job_to_stop)
        for q in list_of_q:
            self._publisher.publish_message(q, yaml.dump(stop_msg))
        return

    def process_bf_transfer_done(self, msg_params):
        # Distributors should know when they are done and tell us,
        # probably no need for Base Foreman to do it
        return


# Run NCSA Foreman
def main():
    signal.signal(signal.SIGINT, ctrlccalled)
    ncsaforeman = NCSAForeman()
    try:
        while 1:
            pass
    except:
        pass
    printc("NCSAForeman quit by user.")
    return


if __name__ == "__main__": main()
