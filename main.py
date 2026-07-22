import logging
from input_connector import InputConnector
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Starting Order Processing Pipeline...")
    logger.info("Kafka Bootstrap Servers: %s", Config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info("Input Topic: %s", Config.INPUT_CONNECTOR_TOPIC)
    logger.info("Output Topic: %s", Config.OUTPUT_CONNECTOR_TOPIC)
    logger.info("DLQ Topic: %s", Config.KAFKA_DLQ_TOPIC)
    logger.info("Minimum Order Value: %s", Config.TRANSFORM_MIN_ORDER_VALUE)
    logger.info("Processing Interval: %s seconds", Config.TRANSFORM_PROCESSING_INTERVAL_SEC)

    input_connector_app = InputConnector()
    try:
        input_connector_app.start_consuming()
    except Exception as e:
        logger.critical("Application terminated due to an unhandled error: %s", e, exc_info=True)
    finally:
        logger.info("Order Processing Pipeline stopped.")
