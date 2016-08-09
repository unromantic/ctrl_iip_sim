import pika
import logging
import yaml

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)


class SimplePublisher:
    EXCHANGE = 'message'

    def __init__(self, amqp_url):
        self._connection = None
        self._channel = None
        self._message_number = 0
        self._stopping = False
        self._url = amqp_url
        self._closing = False

        try:
            self.connect()
        except:
            LOGGER.error('No channel - connection channel is None')
        return

    def connect(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        if self._channel == None:
            LOGGER.error('No channel - connection channel is None')
        return

    def publish_message(self, route_key, msg):
        if self._connection.is_closed == True:
            try:
                self.connect()
            except:
                print("Detected that connection was closed, attempting to reconnect...")
                LOGGER.critical('Unable to create connection to rabbit server. Heading for exit...')
                sys.exit(105)

        LOGGER.debug("Sending msg to %s", route_key)
        try:
            self._channel.basic_publish(exchange=self.EXCHANGE, routing_key=route_key, body=msg)
        except:
            # Temporary fix for undetected closed connection
            print("Connection was closed unexpectedly, attempting to reconnect...")
            self.connect()
            self._channel.basic_publish(exchange=self.EXCHANGE, routing_key=route_key, body=msg)
        return
