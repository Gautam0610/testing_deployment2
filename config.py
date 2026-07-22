import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # --- Kafka Common Settings ---
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_SECURITY_PROTOCOL = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_PLAINTEXT')
    KAFKA_SASL_MECHANISM = os.getenv('KAFKA_SASL_MECHANISM', 'PLAIN')
    KAFKA_SASL_USERNAME = os.getenv('KAFKA_SASL_USERNAME')
    KAFKA_SASL_PASSWORD = os.getenv('KAFKA_SASL_PASSWORD')
    KAFKA_DLQ_TOPIC = os.getenv('KAFKA_DLQ_TOPIC', 'order-processing-dlq')

    # --- Input Connector Settings ---
    INPUT_CONNECTOR_TOPIC = os.getenv('INPUT_CONNECTOR_TOPIC', 'raw-orders')
    INPUT_CONNECTOR_GROUP_ID = os.getenv('INPUT_CONNECTOR_GROUP_ID', 'order-input-consumer-group')

    # --- Transformation Settings ---
    TRANSFORM_MIN_ORDER_VALUE = float(os.getenv('TRANSFORM_MIN_ORDER_VALUE', 10.0))
    TRANSFORM_PROCESSING_INTERVAL_SEC = int(os.getenv('TRANSFORM_PROCESSING_INTERVAL_SEC', 1))

    # --- Output Connector Settings ---
    OUTPUT_CONNECTOR_TOPIC = os.getenv('OUTPUT_CONNECTOR_TOPIC', 'processed-orders')

    @classmethod
    def get_kafka_common_config(cls):
        conf = {
            'bootstrap.servers': cls.KAFKA_BOOTSTRAP_SERVERS,
            'security.protocol': cls.KAFKA_SECURITY_PROTOCOL
        }
        if cls.KAFKA_SECURITY_PROTOCOL.startswith('SASL'):
            conf.update({
                'sasl.mechanism': cls.KAFKA_SASL_MECHANISM,
                'sasl.username': cls.KAFKA_SASL_USERNAME,
                'sasl.password': cls.KAFKA_SASL_PASSWORD
            })
        return conf
