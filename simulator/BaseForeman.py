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

PROGRAM_NAME = "BASE"
from CustomPrint import CustomPrint

custom_print = CustomPrint(PROGRAM_NAME)
printc = custom_print.printc

# Global dict for ancillary data (i.e. pairings)
glb_pair_list = {}

# Signal to enable shutting down from a shell script
def ctrlccalled(*arg):
    raise KeyboardInterrupt, "Signal handler"
    return


class BaseForeman(Foreman):
    # BaseForeman receives messages from DMCS
    # and coordinates with forwarders and NCSAForeman.
    PROGRAM_NAME = "BASE"

    def __init__(self):
        printc("Starting...")

        # Job Scoreboard
        self._sb_job = Scoreboard(SCOREBOARD_DB_JOB, PROGRAM_NAME, 'NONE')
        # ACK Scoreboard
        self._sb_ack = Scoreboard(SCOREBOARD_DB_ACK, PROGRAM_NAME, 'NONE')

        # Clean redis job and ack database, done only by BaseForeman right at the start
        self._sb_job.flush_db()

        # Start internal job number at 0
        self._sb_job.reset_internal_job()

        self._machine_prefix = 'F:'
        self._machine_publish_q = Q_FORW_PUBLISH
        self._machine_consume_q = Q_FORW_CONSUME

        # Types of messages we expect to recieve from each kind of queue
        # DMCS messages
        self._msg_actions_dmcs = {
            'JOB': self.process_dmcs_job,
            'STANDBY': self.process_dmcs_standby,
            'READOUT': self.process_dmcs_readout,
            'CANCEL': self.process_dmcs_cancel
        }
        # NCSA messages
        self._msg_actions_ncsa = {
            'None'
        }
        # ACK messages
        self._msg_actions_ack = {
            'ACK_RECEIVED': self.process_ack_received
        }

        # Run parent init, starts forwarder scoreboard and consumer
        self.parent_init(SCOREBOARD_DB_FORW, PROGRAM_NAME, REGISTER_FORWARDER)

        # DMCS consumer gets messages about new jobs and changing job states
        printc("Creating DMCS consumer...")
        self._dmcs_consumer = Consumer(self._broker_url, Q_DMCS_PUBLISH)
        try:
            thread.start_new_thread(self.run_dmcs_consumer, ())
        except:
            printc("Thread run_dmcs_consumer failed, quitting...")
            sys.exit()

        # NCSA Foreman is monitoring the distributors and reports pairings
        printc("Creating NCSA consumer...")
        self._ncsa_consumer = Consumer(self._broker_url, Q_NCSA_PUBLISH)
        try:
            thread.start_new_thread(self.run_ncsa_consumer, ())
        except:
            printc("Thread run_ncsa_consumer failed, quitting...")
            sys.exit()

        # ACK consumer gets messages from the forwarders and NCSA confirming
        # if the orginial message was received
        printc("Creating ACK consumer...")
        self._ack_consumer = Consumer(self._broker_url, Q_ACK_PUBLISH)
        try:
            thread.start_new_thread(self.run_ack_consumer, ())
        except:
            printc("Thread run_ack_consumer failed, quitting...")
            sys.exit()
        return

    # Message consumer functions, each blocks while waiting for a new message

    def run_dmcs_consumer(self):
        printc("DMCS message consumer is running...")
        self._dmcs_consumer.run(self.on_dmcs_message)
        printc("Exiting dmcs consumer")
        return

    def run_ncsa_consumer(self):
        printc("NCSA message consumer is running...")
        self._ncsa_consumer.run(self.on_ncsa_message)
        return

    def run_ack_consumer(self):
        printc("ACK message consumer is running...")
        self._ack_consumer.run(self.on_ack_message)
        return

    # Forwarder-specific messaging

    def process_transfer_done(self, msg_params):
        # Forwarder is finished with its current job
        forw_finished = msg_params[NAME]
        # Only continue if the forw reported finish on the job it is on
        if (msg_params[JOB_NUM] ==
                self._sb_mach.get_machine_job_num(forw_finished)):
            self._sb_mach.change_machine_status_to_idle(forw_finished)
            cur_workers = int(self._sb_job.get_job_value(msg_params[JOB_NUM],
                                                         'ASSIGNED_WORKERS')) - 1
            self._sb_job.add_job_value(msg_params[JOB_NUM], 'ASSIGNED_WORKERS',
                                       cur_workers)
            # If this was the last working, the just is finished
            if 0 == cur_workers:
                self._sb_job.add_job_value(msg_params[JOB_NUM], 'TIME_FINISHED',
                                           self._sb_job._redis.time()[0])
                self._sb_job.add_job_value(msg_params[JOB_NUM], 'STATUS',
                                           'INACTIVE')
                self._sb_job.set_job_state(msg_params[JOB_NUM], FINISHED)
        return

    # DMCS messaging

    def on_dmcs_message(self, ch, method, properties, body):
        # Consumer callback function
        msg_dict = yaml.load(body)
        try:
            af_handler = self._msg_actions_dmcs.get(msg_dict[MSG_TYPE])
        except:
            printc("Bad DMCS message received...")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        af_handler(msg_dict)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    def process_dmcs_job(self, msg_params):
        # DMCS is sending a new job
        # DMCS's job number
        external_job_val = str(msg_params[JOB_NUM])
        # BaseForeman's internal job value
        current_job = self._sb_job.new_job()
        current_raft = int(msg_params[RAFT_NUM])

        # Add the current job to the Job Scoreboard
        self._sb_job.add_job_value(current_job, 'TIME_STARTED',
                                   self._sb_job._redis.time()[0])
        self._sb_job.add_job_value(current_job, RAFTS, current_raft)
        self._sb_job.add_job_value(current_job, 'EXT_JOB_NUM', external_job_val)
        self._sb_job.add_job_value(current_job, 'STATUS', 'ACTIVE')
        self._sb_job.set_job_state(current_job, CHECKING_RESOURCES)
        self._sb_job.add_job(current_job)

        # Check if the forwarders needed are available
        # Number of rafts is number of forwarders we need
        forwarders_needed = current_raft
        # Get the total amount of idle forwarders
        num_healthy_forwarders = self._sb_mach.count_idle(LIST_FORWARDERS)
        printc("DMCS is requesting %d pairs, have %d pairs available."
               % (forwarders_needed, num_healthy_forwarders))
        # If there are not enough forwarders then
        # tell DMCS we cannot accept this job right now
        if forwarders_needed > num_healthy_forwarders:
            self._sb_job.set_job_state(current_job, 'JOB_FAILED_INSUF_FORWDRS')
            self._sb_job.add_job_value(current_job, 'TIME_FAILED',
                                       self._sb_job._redis.time()[0])
            # Send message to DMCS that we cannot do this job yet.
            failed_msg = {}
            failed_msg[MSG_TYPE] = INSUFFICIENT_FORWARDERS
            failed_msg[JOB_NUM] = msg_params[JOB_NUM]
            failed_msg[NEEDED_WORKERS] = str(forwarders_needed)
            failed_msg[AVAILABLE_FORWARDERS] = str(num_healthy_forwarders)
            self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
            return
        # Otherwise, we have the needed forwarders.
        # Now we need to ask NCSA if they have the needed distributors
        else:
            # Get number of forwarders we need and set them to BUSY from IDLE
            forw_list = self._sb_mach.get_idle_list(LIST_FORWARDERS, forwarders_needed, current_job)
            # Update the Job Scoreboard
            self._sb_job.add_job_value(current_job, 'AVAIL_FORW', num_healthy_forwarders)
            self._sb_job.add_job_value(current_job, 'FORW_NEEDED', forwarders_needed)
            self._sb_job.set_job_state(current_job, 'WAITING_FOR_NCSA_RESP')
            # Send NCSA a Job Request
            # This is a heads up, we do not check for a response to this
            job_request = {}
            job_request[MSG_TYPE] = JOB_REQUEST
            printc("Sending Job Request to NCSA...")
            self._publisher.publish_message(Q_NCSA_CONSUME, yaml.dump(job_request))

            # Send forwarders New Job message (i.e. Health Check)
            # This is a timed event
            timer_id = 'ACK:1_Health:' + current_job
            new_job = {}
            new_job[MSG_TYPE] = JOB
            new_job[ACK_ID] = timer_id
            new_job[ACK_TYPE] = HEALTH
            new_job[JOB_NUM] = current_job
            printc("Sending New Job Request to the Forwarders...")
            for forw in forw_list:
                routing_key = forw + "_consume"
                self._publisher.publish_message(routing_key, yaml.dump(new_job))
            printc("Starting the New Job Request Timer")
            if not self.timer(3, timer_id, forw_list):
                printc("Timer Expired without all Forwarders reporting...")
                # Check Ack SB to see which ones did not report
                # Tell DMCS we cannot do this job
                failed_msg = {}
                failed_msg[MSG_TYPE] = INSUFFICIENT_FORWARDERS
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                failed_msg[NEEDED_WORKERS] = str(forwarders_needed)
                failed_msg[AVAILABLE_FORWARDERS] = str(self._sb_ack.count_ack(timer_id, forw_list))
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
            # Send NCSA a distributor request message
            # This is a timed event
            # Must reset the global dict
            timer_id = 'ACK:2_Dist_req:' + current_job
            global glb_pair_list
            glb_pair_list = None
            ncsa_dist_request = {}
            ncsa_dist_request[MSG_TYPE] = DISTRIBUTOR_REQUEST
            ncsa_dist_request[DIST_NEEDED] = forwarders_needed
            ncsa_dist_request['FORW_LIST'] = forw_list
            ncsa_dist_request[JOB_NUM] = msg_params[JOB_NUM]
            ncsa_dist_request[ACK_ID] = timer_id
            ncsa_dist_request[ACK_TYPE] = 'PAIR'
            self._publisher.publish_message(Q_NCSA_CONSUME, yaml.dump(ncsa_dist_request))
            printc("Starting Timer for NCSA Reporting...")
            if not self.timer(2, timer_id, {'PAIRING'}):
                printc("Timer Expired without NCSA reporting")
                # Tell DMCS we reject the job
                failed_msg = {}
                failed_msg[MSG_TYPE] = 'NO_NCSA_RESP_TO_DIST_REQ'
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
            # Check if pair list is still set to None
            # This means NCSA responded with ACK_BOOL as FALSE
            if glb_pair_list is None:
                printc("No pair list, NCSA didn't have enough distributors.")
                # NCSA did not have enough distributors available, reject this job for now
                current_job = msg_params[JOB_NUM]
                forw_list = self._sb_mach.machine_find_all_m(LIST_FORWARDERS, current_job)
                # Set forwarders we reserved for this job from BUSY back to IDLE
                self._sb_mach.set_list_to_idle(forw_list)
                self._sb_job.set_job_state(current_job, 'STANDBY_JOB_DENIED_INSUF_DIST')
                self._sb_job.add_job_value(current_job, 'TIME_FAILED',
                                   self._sb_job._redis.time()[0])
                self._sb_job.add_job_value(current_job, 'STATUS', 'INACTIVE')
                # Tell DMCS we reject the job
                failed_msg = {}
                failed_msg[MSG_TYPE] = 'INSUFFICIENT_DISTRIBUTORS'
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
            # Quick sanity check to make sure we
            # got back the number we asked for
            if len(glb_pair_list) != forwarders_needed:
                printc("Invalid pair list, failed to accept job.")
                # Tell DMCS we reject the job
                failed_msg = {}
                failed_msg[MSG_TYPE] = 'INVALID_PAIR_LIST'
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
            printc("Pair list is %r" % glb_pair_list)
            # Update Job scoreboard
            self._sb_job.add_job_value(current_job, 'ASSIGNED_WORKERS', len(glb_pair_list))
            self._sb_job.add_job_value(current_job, 'PAIRS', glb_pair_list)
            # Report to DMCS that we accept the job
            accept_job_msg = {}
            accept_job_msg[MSG_TYPE] = 'JOB_ACCEPTED'
            accept_job_msg[JOB_NUM] = msg_params[JOB_NUM]
            self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(accept_job_msg) )
            self._sb_job.set_job_state(current_job, 'JOB_ACCEPTED')
            return

    def process_dmcs_standby(self, msg_params):
        # A job has been moved to the STANDBY step.
        # Forwarders need to start pulling header data and
        # distributors need to be ready to catch.
        external_job_val = str(msg_params[JOB_NUM])
        # Get the internal job value that correlates with this external job
        current_job = self._sb_job.find_ext_job(external_job_val)
        if "NOT_FOUND" == current_job:
            printc("External job %s is not on the job scoreboard." % external_job_val)
            printc("STANDBY failed.")
            return

        # Generate a file name (for testing only)
        xfer_file_main = ("%04d%02d%02d-%02d%02d%02d-%06d"
                          % (datetime.datetime.today().year,
                             datetime.datetime.today().month,
                             datetime.datetime.today().day,
                             datetime.datetime.today().hour,
                             datetime.datetime.today().minute,
                             datetime.datetime.today().second,
                             datetime.datetime.today().microsecond))

        # Update job scoreboard
        self._sb_job.add_job_value(current_job, XFER_FILE, xfer_file_main)
        self._sb_job.set_job_state(current_job, 'STANDBY_FORW_DIST_ALERTING')

        # Alert NCSA Foreman this job is entering STANDBY
        # This is a timed event
        timer_id = 'ACK:3_Standby:' + current_job
        ncsa_standby_alert = {}
        ncsa_standby_alert[MSG_TYPE] = STANDBY
        ncsa_standby_alert[JOB_NUM] = current_job
        ncsa_standby_alert[XFER_FILE] = xfer_file_main
        ncsa_standby_alert[ACK_ID] = timer_id
        ncsa_standby_alert[ACK_NAME] = 'STANDBY'
        printc("Telling NCSA we are moving to STANDBY")
        self._publisher.publish_message(Q_NCSA_CONSUME, yaml.dump(ncsa_standby_alert))

        # Send STANDBY to all the forwarders in this job
        global glb_pair_list
        # pairs = self._sb_mach.machine_find_all_pairs(current_job)
        pairs = glb_pair_list
        forwarders = pairs.keys()
        for forwarder in forwarders:
            printc("Sending %s standby..." % forwarder)
            fw_msg = {}
            fw_msg[MSG_TYPE] = STANDBY
            fw_msg[MATE] = pairs[forwarder]
            fw_msg[JOB_NUM] = current_job
            fw_msg[XFER_FILE] = string.replace(xfer_file_main + '_' + pairs[forwarder] + '.raw', "D:", "")
            fw_msg[ACK_ID] = timer_id
            fw_msg[ACK_TYPE] = STANDBY
            routing_key = forwarder + "_consume"
            self._publisher.publish_message(routing_key, yaml.dump(fw_msg))
        # Append STANDBY to the expected acks list
        forwarders.append('NCSA_STANDBY')
        printc("Starting the STANDBY timer")
        if not self.timer(4, timer_id, forwarders):
            printc("Timer Expired without NCSA and FORWARDERS reporting in STANDBY")
            # Check ACK SB to see who did not report
            if not self._sb_ack.check_ack(timer_id, 'NCSA_STANDBY'):
                failed_msg = {}
                failed_msg[MSG_TYPE] = 'NO_NCSA_RESP_TO_STANDBY'
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
            else:
                failed_msg = {}
                failed_msg[MSG_TYPE] = 'MISSING_FORW_STANDBY'
                failed_msg[JOB_NUM] = msg_params[JOB_NUM]
                failed_msg['MISSING'] = str(self._sb_ack.missing_acks(timer_id, forwarders))
                self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
                return
        # Report to DMCS that the job is still good to go
        standby_job_msg = {}
        standby_job_msg[MSG_TYPE] = 'STANDBY_COMPLETE'
        standby_job_msg[JOB_NUM] = msg_params[JOB_NUM]
        self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(standby_job_msg) )
        self._sb_job.set_job_state(current_job, 'STANDBY')
        return

    def process_dmcs_readout(self, msg_params):
        # A job has been moved to the READOUT step.
        # Forwarders need to pull images from the camera buffer,
        # append the data and send the image files to the distributors
        external_job_val = str(msg_params[JOB_NUM])
        # Get the internal job value that correlates with this external job
        current_job = self._sb_job.find_ext_job(external_job_val)
        if "NOT_FOUND" == current_job:
            printc("External job %s is not on the job scoreboard." % external_job_val)
            printc("READOUT failed.")
            return
        # If job was not in STANDBY, recover by calling that function first
        if 'STANDBY' != self._sb_job.get_job_state(current_job):
            printc("READOUT without STANDBY for Job ID %s, calling STANDBY first." % current_job)
            self.process_dmcs_standby(msg_params)
        printc("READOUT processing for Job ID %s." % current_job)
        self._sb_job.set_job_state(current_job, 'READOUT')

        # Alert NCSA Foreman we are entering READOUT
        # This is a timed event
        timer_id = 'ACK:4_Readout:' + current_job
        ncsa_readout_alert = {}
        ncsa_readout_alert[MSG_TYPE] = READOUT
        ncsa_readout_alert[JOB_NUM] = current_job
        ncsa_readout_alert[ACK_ID] = timer_id
        ncsa_readout_alert[ACK_TYPE] = READOUT
        self._publisher.publish_message(Q_NCSA_CONSUME, yaml.dump(ncsa_readout_alert))
        printc("Starting the NCSA READOUT timer...")
        if not self.timer(4, timer_id, {'READOUT'} ):
            printc("Timer expired without NCSA reporting in READOUT")
            failed_msg = {}
            failed_msg[MSG_TYPE] = 'NO_NCSA_RESP_TO_READOUT'
            failed_msg[JOB_NUM] = msg_params[JOB_NUM]
            self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
            return
        # Send READOUT to forwarders
        # This is a timed event
        # In this prototype, we will wait for forwarders acks
        # instead of Condor, since we do not have that created
        timer_id = 'ACK:5_Condor:' + current_job
        pairs = self._sb_mach.machine_find_all_pairs(current_job)
        printc("%r" % pairs)
        forwarders = pairs.keys()
        for forwarder in forwarders:
            printc("Sending %s  readout..." % forwarder)
            fw_msg = {}
            fw_msg[MSG_TYPE] = READOUT
            fw_msg[JOB_NUM] = msg_params[JOB_NUM]
            fw_msg[ACK_ID] = timer_id
            fw_msg[ACK_TYPE] = READOUT
            routing_key = forwarder + "_consume"
            self._publisher.publish_message(routing_key, yaml.dump(fw_msg))

        printc("Starting the timer for Condor ACK... ")
        # For now it is just waiting for the acks from the forwarders
        if not self.timer(4, timer_id, forwarders):
            printc("Timer Expired without Condor reporting in READOUT")
            # Can check ACK_SB to see who did not report
            failed_msg = {}
            failed_msg[MSG_TYPE] = 'MISSING_CONDOR_RESP'
            failed_msg[JOB_NUM] = msg_params[JOB_NUM]
            failed_msg[NEEDED_WORKERS] = str(forwarders_needed)
            failed_msg['MISSING'] = str(self._sb_ack.missing_acks(timer_id, forwarders))
            self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(failed_msg))
            return
        # Report to DMCS that the job was completed
        completed_job_msg = {}
        completed_job_msg[MSG_TYPE] = 'JOB_COMPLETE'
        completed_job_msg[JOB_NUM] = msg_params[JOB_NUM]
        self._publisher.publish_message(Q_DMCS_CONSUME, yaml.dump(completed_job_msg) )
        self._sb_job.set_job_state(current_job, 'JOB_COMPLETE')
        return

    def process_dmcs_cancel(self, msg_params):
        # Job was canceled, attempt to stop it.
        job_to_stop = self._sb_job.find_ext_job(msg_params[JOB_NUM])
        if "NOT_FOUND" == job_to_stop:
            printc("External job %s is not on the job scoreboard." % external_job_val)
            printc("CANCEL failed.")
            return
        cur_state = self._sb_job.get_job_value(job_to_stop, 'STATE')
        if FINISHED == cur_state:
            printc("Can't cancel this job, it is already done.")
            return
        printc("Canceling external job %s (internal job %s)..." % (msg_params[JOB_NUM], job_to_stop))
        self._sb_job.set_job_state(job_to_stop, 'JOB_CANCELED')
        self._sb_job.add_job_value(job_to_stop, 'TIME_CANCELED', self._sb_job._redis.time()[0])
        self._sb_job.add_job_value(job_to_stop, 'STATUS', 'INACTIVE')

        # Tell NCSA Foreman we are canceling this job
        stop_msg = {}
        stop_msg[MSG_TYPE] = 'CANCEL'
        stop_msg[JOB_NUM] = str(job_to_stop)
        self._publisher.publish_message(Q_NCSA_CONSUME, yaml.dump(stop_msg))

        # Tell forwarders we are canceling this job
        list_of_q = self._sb_mach.machine_find_job(LIST_FORWARDERS, job_to_stop)
        for q in list_of_q:
            self._publisher.publish_message(q, yaml.dump(stop_msg))
        printc("Job canceled. (Hopefully)")
        return

    # NCSA messaging

    def on_ncsa_message(self, ch, method, properties, body):
        # Consumer callback Function
        msg_dict = yaml.load(body)
        af_handler = self._msg_actions_ncsa.get(msg_dict[MSG_TYPE])
        af_handler(msg_dict)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Acknowledgment messaging

    def on_ack_message(self, ch, method, properties, body):
        msg_dict = yaml.load(body)
        af_handler = self._msg_actions_ack.get(msg_dict.get(MSG_TYPE))
        if af_handler is not None:
            af_handler(msg_dict)
        ch.basic_ack(delivery_tag = method.delivery_tag)
        return

    def process_ack_received(self, msg_params):
        ack_id = msg_params.get(ACK_ID)
        ack_type = msg_params.get(ACK_TYPE)
        ack_name = msg_params.get(ACK_NAME)
        ack_bool = msg_params.get(ACK_BOOL)
        printc("Received ACK with name %s with ID %s" % (ack_name, ack_id))
        # Update ACK Scoreboard
        if not self._sb_ack.update_ack(ack_id, ack_name):
            printc("Unable to add the ack...")
        if PAIRS in msg_params and ack_bool:
            printc("Updating the pair list")
            global glb_pair_list
            glb_pair_list = msg_params.get(PAIRS)
        return

    def timer(self, delay, ack_id, ack_expected):
        count = delay * TIMER_PRECISION
        while (count and not self._sb_ack.check_ack(ack_id, ack_expected) ):
            time.sleep(1/float(TIMER_PRECISION))
            count = count - 1
        return self._sb_ack.check_ack(ack_id, ack_expected)

# Run BaseForeman
def main():
    signal.signal(signal.SIGINT, ctrlccalled)
    baseforeman = BaseForeman()
    try:
        while 1:
            pass
    except:
        pass
    printc("BaseForeman quit by user.")
    return


if __name__ == "__main__": main()
