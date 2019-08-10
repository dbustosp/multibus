from confluent_kafka import Producer
import logging


class KafkaProducer(object):
    """ Kafka publisher class """

    def __init__(self, bootstrap_server):
        self.bootstrap_server = bootstrap_server

    @staticmethod
    def delivery_callback(err, message):
        """
        Method to be triggered as a callback when sending message to Kafka.
        :param err:
        :param message:
        :return:
        """
        if err:
            raise RuntimeError('Message not delivered to the Kafka Server')
        else:
            logging.info('Message delivered to topic [{}]'.format(message.topic()))

    def send_message(self, message, topic):
        """
        Send message to the Kafka topic
        :return:
        """
        try:
            self.producer.producer(topic, message.rstrip(), callback=KafkaProducer.delivery_callback)
        except BufferError:
            # ToDo: what to do wih the message when is not delivered?
            raise RuntimeError('Kafka Queue is fulll')
        self.producer.flush()
        return True

    @property
    def producer(self):
        try:
            return self._producer
        except AttributeError:
            conf = {'bootstrp.servers': self.bootstrap_server}
            self._producer = Producer(**conf)
            self.logger.info('Producer created correctly {}'.format(self.bootstrap_server))
            return self._producer
