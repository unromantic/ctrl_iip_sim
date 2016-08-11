# Get this machine's IP address for use in multiple places
from IP_Address import DETECTED_IP_ADDRESS

# RabbitMQ broker URL data
AMQP_BROKER_ADDR = DETECTED_IP_ADDRESS
AMQP_BROKER_PORT = "5672"
AMQP_BROKER_VHOST = "%2fbunny"

# AMQP login details
AMQP_DMCS_USER = "DMCS"
AMQP_DMCS_PSWD = "DMCS"
AMQP_BF_USER = "FM"
AMQP_BF_PSWD = "FM"
AMQP_NCSA_USER = "NCSA"
AMQP_NCSA_PSWD = "NCSA"
AMQP_MACH_USER = "Machine"
AMQP_MACH_PSWD = "Machine"

# RabbitMQ queues
Q_DMCS_CONSUME = "dmcs_consume"
Q_DMCS_PUBLISH = "dmcs_publish"
Q_NCSA_CONSUME = "ncsa_consume"
Q_NCSA_PUBLISH = "ncsa_publish"
Q_FORW_CONSUME = "forwarder_consume"
Q_FORW_PUBLISH = "forwarder_publish"
Q_DIST_PUBLISH = "distributor_publish"
Q_DIST_CONSUME = "distributor_consume"
Q_ACK_PUBLISH = "ack_publish"
Q_ACK_CONSUME = "ack_consume"
Q_REPORTS = "reports"

# RabbitMQ exchanges
EXCHANGE_NAME = "message"
EXCHANGE_TYPE = "direct"

# Redis URL data
REDIS_HOST = DETECTED_IP_ADDRESS
REDIS_PORT = 6379

# IP and folder for Forwarders to send their data to
# (All distributors are on the same IP right now)
RSYNC_IP_ADDR = DETECTED_IP_ADDRESS
RSYNC_KEY_DIR = "/lsst/"

# Machine constants
THREAD_RUNNING = 0
THREAD_CANCELED = 1
THREAD_NOT_RUNNING = 2
XFER_DIR = "/xfer_files/"
DISTRIBUTOR_REQUEST = 'DISTRIBUTOR_REQUEST'
DIST_NEEDED = 'DIST_NEEDED'
NAME_LENGTH = 7
TEST_FILE_SIZE = 8096  # in bytes + header size
# TEST_FILE_SIZE = 16777216

# Scoreboard constants
RAFTS = 'RAFTS'
JOBS = 'jobs'
# All scoreboards in the same database right now
SCOREBOARD_DB_JOB = 1
SCOREBOARD_DB_FORW = 1
SCOREBOARD_DB_DIST = 1
SCOREBOARD_DB_ACK = 1

CHECKING_RESOURCES = 'CHECKING_RESOURCES'
LIST_FORWARDERS = 'LIST:FORWARDERS'
REGISTER_FORWARDER = 'LIST:FORWARDERS'
LIST_DISTRIBUTORS = 'LIST:DISTRIBUTORS'
REGISTER_DISTRIBUTOR = 'LIST:DISTRIBUTORS'
PARTNER = 'PARTNER'
PAIRED = 'PAIRED'
WORKING = 'WORKING'
FINISHED = 'FINISHED'
FAILED = 'FAILED'
CANCEL = 'CANCEL'
TRANSFER_DONE = 'TRANSFER_DONE'
CANCELED = 'CANCELED'
TIMER_PRECISION = 2
ACK_BOOL = 'ACK_BOOL'