import sys
import subprocess
import os
import thread
import random
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


class Distributor(Machine):
    # Machine that receives file transfers. Right now it just checksums a file
    # once it has been received.
    def child_init(self):
        self._publish_queue = Q_DIST_PUBLISH
        self._consume_queue = Q_DIST_CONSUME
        self._type = 'DIST'
        self._jobs_going = 0
        self._cancel_signal = 0
        return

    # Foreman messaging

    def process_foreman_standby(self, msg_params):
        # Foreman moving us to STANDBY
        if "STANDBY" == self._current_state:
            printc("Sent STANDBY, was already in STANDBY...")
        self._xfer_file = msg_params[XFER_FILE]
        printc("Entering STANDBY state.")
        printc("Received name of incoming file: %s" % self._xfer_file)
        self._current_state = "STANDBY"
        return

    def process_foreman_readout(self, msg_params):
        if "STANDBY" != self._current_state:
            printc("Sent READOUT but in STANDBY...")
            return
        printc("Distributor entering READOUT state.")
        self._current_state = "READOUT"
        # Start a new thread for file checking
        self._lock.acquire()
        self._jobs_going = self._jobs_going + 1
        self._lock.release()
        file_loc = self._home_dir + self._xfer_file
        printc("Starting file checking thread...")
        try:
            thread.start_new_thread(self.wait_for_file, (file_loc,))
        except:
            printc("File check thread failed to start...")
            self._lock.acquire()
            self._jobs_going = self._jobs_going - 1
            self._lock.release()
            self._current_state = IDLE
            self.state_update(self._name, 'STATE', 'IDLE')
            self.state_update(self._name, 'CURRENT_JOB', 'NONE')
        return

    def wait_for_file(self, file_loc):
        printc("Waiting for image file...")
        while True != os.path.isfile(file_loc) and self._cancel_signal == 0:
            pass
        self._lock.acquire()
        if self._cancel_signal == 1:
            printc("Job was canceled, stopped waiting.")
            self._cancel_signal = 0
            self._lock.release()
            return
        self._jobs_going = self._jobs_going - 1
        self._lock.release()
        printc("Image file received!")
        printc("Calculating check sum...")
        cmd = 'cksum ' + file_loc
        try:
            self._cksum_output = subprocess.check_output(cmd, shell=True)
        except:
            printc("File checksum failed...")
            return
        local = self._cksum_output.split()
        printc("File received with checksum: %s %s" % (local[0], local[1]))
        self._current_state = IDLE
        self.state_update(self._name, 'STATE', 'IDLE')
        self.state_update(self._name, 'CURRENT_JOB', 'NONE')
        return

    def process_foreman_cancel(self, msg_params):
        self._lock.acquire()
        if self._jobs_going > 0:
            printc("Canceling job...")
            self._cancel_signal = 1
        elif IDLE != self._current_state:
            printc("Canceled current job.")
            self._current_state = IDLE
            self.state_update(self._name, 'STATE', 'IDLE')
            self.state_update(self._name, 'CURRENT_JOB', 'NONE')
        else:
            printc("In IDLE with no job to cancel.")
        self._lock.release()
        return


def main():
    signal.signal(signal.SIGINT, ctrlccalled)
    distributor = Distributor()
    printc("Distributor quit by user.")
    return


if __name__ == "__main__": main()
