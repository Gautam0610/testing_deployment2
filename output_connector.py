import logging
import json
from confluent_kafka import Producer
from config import Config

logger = logging.getLogger(__name__)

class OutputConnector:
    def __init__(self):
        common_config = Config.get_kafka_common_config()
        self.producer = Producer(common_config)
        logger.info("Output Connector (Kafka Producer) initialized with config: %s", common_config)

    def delivery_report(self, err, msg):
        """Callback function to handle successful or failed message delivery."""
        if err is not None:
            logger.error("Message delivery failed for topic %s: %s", msg.topic(), err)
        else:
            logger.info("Message delivered to topic '%s' [partition %d] @ %o",
                        msg.topic(), msg.partition(), msg.offset())

    def send_to_output_topic(self, key, value):
        """Sends a message to the configured output Kafka topic."""
        topic = Config.OUTPUT_CONNECTOR_TOPIC
        self._produce_message(topic, key, value)

    def send_to_dlq(self, key, value):
        """Sends a message to the configured Dead Letter Queue Kafka topic."""
        topic = Config.KAFKA_DLQ_TOPIC
        self._produce_message(topic, key, value)

    def _produce_message(self, topic, key, value):
        try:
            # .poll(0) is important to trigger delivery callbacks
            self.producer.poll(0)
            self.producer.produce(topic, key=str(key), value=json.dumps(value).encode('utf-8'),
                                  callback=self.delivery_report)
            logger.debug("Message queued for topic %s, key %s", topic, key)
        except Exception as e:
            logger.error("Failed to produce message to topic %s: %s", topic, e)
            raise # Re-raise to allow upstream error handling if needed

    def flush(self, timeout=10):
        """Waits for all messages in the Producer queue to be delivered."""
        remaining_messages = self.producer.flush(timeout)
        if remaining_messages > 0:
            logger.warning("Output Connector flush timed out. %d messages still in queue.", remaining_messages)
        else:
            logger.info("Output Connector: All messages flushed successfully.")

    def close(self):
        """Closes the producer and flushes any outstanding messages."""
        self.flush()
        logger.info("Output Connector (Kafka Producer) closed.")
