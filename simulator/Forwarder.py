import thread
import os
import random
import subprocess
import sys
import time
import string
import signal

import pika
import yaml

from Consumer import Consumer
from SimplePublisher import SimplePublisher
from InternalConst import *
from const import *
from Machine import *
from CustomPrint import CustomPrint


# Signal to enable shutting down from a shell script
def ctrlccalled(*arg):
    raise KeyboardInterrupt, "Signal handler"
    return


class Forwarder(Machine):
    # Machine that creates and sends the image files
    def child_init(self):
        self._type = 'FORW'
        self._publish_queue = Q_FORW_PUBLISH
        self._consume_queue = Q_FORW_CONSUME
        # Internal variable of current job
        # a forwardershould only ever be on one job at a time
        self._current_job = "NONE"
        # Initialize the cancel checking variable
        self.update_thread_run(THREAD_NOT_RUNNING)
        return

    def update_thread_run(self, value):
        # Set the job cancel variable to passed in value
        self._lock.acquire()
        self._thread_run = value
        self._lock.release()
        return

    def cancel_thread_run(self):
        # Set the job cancel variable to canceled and return the result
        return_val = False
        self._lock.acquire()
        if THREAD_RUNNING == self._thread_run:
            self._thread_run = THREAD_CANCELED
            return_val = True
        self._lock.release()
        return return_val

    # Foreman messaging


    def process_foreman_job(self, msg_params):
        # Foreman has a new job for us
        self._current_job = str(msg_params['JOB_NUM'])
        self._folder_title = self._current_job + ":" + self._name
        # Confirm our state update with the foreman
        self.state_update(self._folder_title, PARTNER, str(msg_params['PARTNER']))
        self.state_update(self._folder_title, STATE, PAIRED)
        self._current_state = "JOB"
        printc("Ready for Job %s." % self._current_job)
        return

    def process_foreman_standby(self, msg_params):
        # Current job is moving to the STANDBY step
        if self._current_job != msg_params[JOB_NUM]:
            printc("Sent a STANDBY for a different job number, cannot proceed.")
            printc("Current job %s. Sent job %s." % (self._current_job, msg_params[JOB_NUM]))
            return
        if "STANDBY" == self._current_state:
            printc("Sent STANDBY for this job already...")
            return
        printc("Entering STANDBY state.")
        self.state_update(self._folder_title, STATE, STANDBY)
        self._current_state = "STANDBY"
        # Learn who our partner is
        self._pairmate = msg_params[MATE]
        # Learn the name of the file we are making
        self._xfer_file = msg_params[XFER_FILE]
        # Create the file header
        printc("Creating file: %s" % self._xfer_file)
        tmp_file = open(self._xfer_file, 'w')
        tmp_file.write('FILE_HEADER')
        tmp_file.write('\0' * 2869)
        tmp_file.close()
        return

    def process_foreman_readout(self, msg_params):
        # Confirm we are entering READOUT correctly
        if self._current_job != msg_params[JOB_NUM]:
            printc("Sent READOUT for a different job number...")
            printc("Current job %s. Sent job %s." % (self._current_job, msg_params[JOB_NUM]))
            return
        if "STANDBY" != self._current_state:
            printc("Sent READOUT but not in STANDBY yet!")
            return
        # No header is a non-fatal error, just alerting
        if True != os.path.isfile(self._xfer_file):
            printc("File header is missing...")
        # Set variable for checking if job is canceled later
        self.update_thread_run(THREAD_RUNNING)
        self.state_update(self._folder_title, STATE, WORKING)
        printc("Entering READOUT state.")
        self._current_state = "READOUT"
        # Read in random data, pretending it is the camera buffer 
        printc("Reading from camera buffer...")
        random_data = open("/dev/urandom", 'r')
        tmp_file = open(self._xfer_file, 'wb')
        tmp_file.write(random_data.read(TEST_FILE_SIZE))
        random_data.close()
        tmp_file.close()
        printc("Finished reading in file from buffer.")
        # Send file in a seperate thread so we can still accept a cancel message
        printc("Sending file to distributor...")
        try:
            thread.start_new_thread(self.send_file, ())
        except:
            printc("Failed to start transfer file thread...")
            self.update_thread_run(THREAD_NOT_RUNNING)
        return

    def send_file(self):
        # New thread that will send the file
        # this allows a cancel message to be received
        # Create command line for transfering the file
        cmd = 'rsync -c -e "ssh -oStrictHostKeyChecking=no -i ' + RSYNC_KEY_DIR + 'rsync_key" ' + self._xfer_file + ' fedora@' + RSYNC_IP_ADDR + ':' + self._home_dir
        try:
            # Run the command
            printc("Running: %s" % cmd)
            process = subprocess.Popen(cmd, shell=True)
            # Wait while the process runs and job is not canceled
            while process.poll() is None and 0 == self._thread_run:
                time.sleep(0.5)
            # If the job was canceled and the process is not finished then cancel the process and clean up
            if 1 == self._thread_run and process.poll() is None:
                printc("Canceling file transfer...")
                # Terminate the process gracefully
                process.terminate()
                clean_up_window = 0;
                # Wait a few moments to let it close
                while process.poll() is None and clean_up_window < 5:
                    clean_up_window = clean_up_window + 1
                    time.sleep(0.5)
                # If it still is not closed, force kill it
                if process.poll() is None:
                    process.kill()
                printc("File transfer was canceled by Foreman.")
                # Remove file
                os.remove(self._xfer_file)
                # Update various things
                self.state_update(self._folder_title, STATE, CANCELED)
                self._current_state = IDLE
                self._current_job = "NONE"
                self.update_thread_run(THREAD_NOT_RUNNING)
                return
        # If an exception occured:
        except:
            printc("File transfer failed...")
            printc("Forwarder entering IDLE state.")
            os.remove(self._xfer_file)
            self.state_update(self._folder_title, STATE, FAILED)
            self._current_state = IDLE
            self._current_job = "NONE"
            self.update_thread_run(THREAD_NOT_RUNNING)
            return
        printc("Send command returned: %r" % process.returncode)
        # On success, delete file
        printc("Removing file...")
        try:
            os.remove(self._xfer_file)
        except:
            printc("Failed to remove file: %s" % self._xfer_file)

        # Send a message to the foreman that the transfer is finished
        transfer_result = {}
        transfer_result[MSG_TYPE] = TRANSFER_DONE
        transfer_result[JOB_NUM] = self._current_job
        transfer_result[NAME] = self._name
        self._publisher.publish_message(self._publish_queue, yaml.dump(transfer_result))
        printc("Forwarder entering IDLE state.")
        self.state_update(self._folder_title, STATE, FINISHED)
        self._current_state = IDLE
        self._current_job = "NONE"
        self.update_thread_run(THREAD_NOT_RUNNING)
        return

    # Foreman sent a cancel message, quit whatever job forwarder is currently on
    def process_foreman_cancel(self, msg_params):
        if IDLE != self._current_state:
            if True == self.cancel_thread_run():
                printc("Canceling job %r..." % self._current_job)
            else:
                printc("Canceled job %r." % self._current_job)
                try:
                    os.remove(self._xfer_file)
                except:
                    pass
            self._current_state = IDLE
            self.state_update(self._folder_title, STATE, CANCELED)
            self._current_job = "NONE"
        else:
            printc("No job to cancel, idling...")
        return


# Run Forwarder
def main():
    signal.signal(signal.SIGINT, ctrlccalled)
    forwarder = Forwarder()
    printc("Forwarder quit by user.")
    return


if __name__ == "__main__": main()
