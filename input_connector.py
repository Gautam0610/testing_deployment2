import logging
import json
import time
from confluent_kafka import Consumer, KafkaException
from config import Config
from output_connector import OutputConnector
from transformation import Transformation

logger = logging.getLogger(__name__)

class InputConnector:
    def __init__(self):
        common_config = Config.get_kafka_common_config()
        consumer_config = {
            'group.id': Config.INPUT_CONNECTOR_GROUP_ID,
            'auto.offset.reset': 'earliest', # Always start from earliest for input
            'enable.auto.commit': True, # Auto-commit offsets
            'auto.commit.interval.ms': 5000, # Commit every 5 seconds
            **common_config
        }
        self.consumer = Consumer(consumer_config)
        self.output_connector = OutputConnector()
        self.transformation_module = Transformation()
        logger.info("Input Connector (Kafka Consumer) initialized with config: %s", consumer_config)

    def start_consuming(self):
        topics = [Config.INPUT_CONNECTOR_TOPIC]
        self.consumer.subscribe(topics, on_assign=self.on_assign)
        logger.info("Subscribed to topics: %s", topics)

        try:
            while True:
                msg = self.consumer.poll(1.0) # Poll for messages with a timeout

                if msg is None:
                    # logger.debug("No message received within timeout.")
                    continue
                if msg.error():
                    if msg.error().is_fatal():
                        logger.critical("Fatal Kafka consumer error: %s", msg.error())
                        raise KafkaException(msg.error())
                    else:
                        logger.warning("Non-fatal Kafka consumer error: %s", msg.error())
                        continue

                self._handle_message(msg)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user.")
        except Exception as e:
            logger.critical("An unhandled exception occurred in the input connector loop: %s", e, exc_info=True)
        finally:
            self.close()

    def _handle_message(self, msg):
        message_value = msg.value().decode('utf-8')
        message_key = msg.key().decode('utf-8') if msg.key() else 'N/A'
        logger.info("Received message: topic=%s, partition=%d, offset=%d, key=%s",
                    msg.topic(), msg.partition(), msg.offset(), message_key)

        try:
            raw_order = json.loads(message_value)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON message from topic %s: %s", msg.topic(), e)
            self.output_connector.send_to_dlq(message_key, {
                "original_message": message_value,
                "error_type": "JSON_PARSING_ERROR",
                "details": str(e),
                "timestamp": time.time()
            })
            return

        try:
            processed_order = self.transformation_module.process_order(raw_order)
            if processed_order: # Transformation might return None for rejected orders
                self.output_connector.send_to_output_topic(message_key, processed_order)
            logger.info("Message processed and sent to output/DLQ if applicable for key: %s", message_key)
        except Exception as e:
            logger.error("Error during order transformation or output for key %s: %s", message_key, e)
            self.output_connector.send_to_dlq(message_key, {
                "original_message": raw_order,
                "error_type": "PROCESSING_ERROR",
                "details": str(e),
                "timestamp": time.time()
            })

    def on_assign(self, consumer, partitions):
        logger.info("Partition assignment: %s", partitions)

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("Input Connector (Kafka Consumer) closed.")
        self.output_connector.close()
