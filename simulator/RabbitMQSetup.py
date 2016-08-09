import pika

from Consumer import Consumer
from InternalConst import *


class Setup:
    # Initial setup for RabbitMQ queues and exchanges
    def __init__(self):
        self._broker_url = 'amqp://' + AMQP_DMCS_USER + ':' + AMQP_DMCS_PSWD + '@' + AMQP_BROKER_ADDR + ':' + AMQP_BROKER_PORT + '/' + AMQP_BROKER_VHOST
        connection = pika.BlockingConnection(pika.URLParameters(self._broker_url))
        channel = connection.channel()

        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type=EXCHANGE_TYPE)

        channel.queue_declare(queue='forwarder_publish')
        channel.queue_declare(queue='forwarder_consume')
        channel.queue_declare(queue='distributor_publish')
        channel.queue_declare(queue='distributor_consume')
        channel.queue_declare(queue='dmcs_consume')
        channel.queue_declare(queue='dmcs_publish')
        channel.queue_declare(queue='ncsa_consume')
        channel.queue_declare(queue='ncsa_publish')
        channel.queue_declare(queue='ack_consume')
        channel.queue_declare(queue='ack_publish')
        channel.queue_declare(queue='reports')
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=Q_DMCS_CONSUME, routing_key=Q_DMCS_CONSUME)
        channel.queue_bind(exchange=EXCHANGE_NAME, queue=Q_DMCS_PUBLISH, routing_key=Q_DMCS_PUBLISH)

        connection.close()

        consumer = Consumer(self._broker_url, Q_FORW_CONSUME)
        consumer.run_2(None)

        consumer = Consumer(self._broker_url, Q_DIST_CONSUME)
        consumer.run_2(None)

        return


def main():
    setup = Setup()
    return


if __name__ == '__main__':
    main()
