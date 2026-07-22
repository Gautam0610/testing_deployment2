import pytest
import os
import json
import time
from unittest.mock import MagicMock, patch, call

# Import modules to be tested
from config import Config
from output_connector import OutputConnector
from transformation import Transformation
from input_connector import InputConnector

# --- Fixtures for environment variables ---
@pytest.fixture
def set_env_vars():
    # Set minimal required environment variables for testing
    os.environ['KAFKA_BOOTSTRAP_SERVERS'] = 'test_bootstrap:9092'
    os.environ['KAFKA_SECURITY_PROTOCOL'] = 'PLAINTEXT'
    os.environ['KAFKA_SASL_MECHANISM'] = 'PLAIN'
    os.environ['KAFKA_SASL_USERNAME'] = 'test_user'
    os.environ['KAFKA_SASL_PASSWORD'] = 'test_password'
    os.environ['KAFKA_DLQ_TOPIC'] = 'test-dlq-topic'
    
    os.environ['INPUT_CONNECTOR_TOPIC'] = 'test-input-topic'
    os.environ['INPUT_CONNECTOR_GROUP_ID'] = 'test-input-group'
    
    os.environ['TRANSFORM_MIN_ORDER_VALUE'] = '50.0'
    os.environ['TRANSFORM_PROCESSING_INTERVAL_SEC'] = '0.01' # Small interval for tests
    
    os.environ['OUTPUT_CONNECTOR_TOPIC'] = 'test-output-topic'

    yield # Let the test run

    # Clean up environment variables after test
    del os.environ['KAFKA_BOOTSTRAP_SERVERS']
    del os.environ['KAFKA_SECURITY_PROTOCOL']
    del os.environ['KAFKA_SASL_MECHANISM']
    del os.environ['KAFKA_SASL_USERNAME']
    del os.environ['KAFKA_SASL_PASSWORD']
    del os.environ['KAFKA_DLQ_TOPIC']
    del os.environ['INPUT_CONNECTOR_TOPIC']
    del os.environ['INPUT_CONNECTOR_GROUP_ID']
    del os.environ['TRANSFORM_MIN_ORDER_VALUE']
    del os.environ['TRANSFORM_PROCESSING_INTERVAL_SEC']
    del os.environ['OUTPUT_CONNECTOR_TOPIC']

# --- Tests for config.py ---

def test_config_loading(set_env_vars):
    assert Config.KAFKA_BOOTSTRAP_SERVERS == 'test_bootstrap:9092'
    assert Config.INPUT_CONNECTOR_TOPIC == 'test-input-topic'
    assert Config.TRANSFORM_MIN_ORDER_VALUE == 50.0
    assert Config.OUTPUT_CONNECTOR_TOPIC == 'test-output-topic'
    assert Config.KAFKA_DLQ_TOPIC == 'test-dlq-topic'

def test_config_kafka_common_config(set_env_vars):
    common_conf = Config.get_kafka_common_config()
    assert common_conf['bootstrap.servers'] == 'test_bootstrap:9092'
    assert common_conf['security.protocol'] == 'PLAINTEXT'
    assert 'sasl.username' in common_conf

# --- Tests for output_connector.py ---

@pytest.fixture
def mock_confluent_producer():
    with patch('confluent_kafka.Producer') as MockProducer:
        yield MockProducer

def test_output_connector_init(mock_confluent_producer, set_env_vars):
    connector = OutputConnector()
    mock_confluent_producer.assert_called_once()

def test_output_connector_send_to_output_topic(mock_confluent_producer, set_env_vars):
    mock_producer_instance = mock_confluent_producer.return_value
    connector = OutputConnector()
    key = "test-key"
    value = {"data": "test-value"}

    connector.send_to_output_topic(key, value)

    mock_producer_instance.poll.assert_called_once_with(0)
    mock_producer_instance.produce.assert_called_once_with(
        Config.OUTPUT_CONNECTOR_TOPIC, key=key, value=json.dumps(value).encode('utf-8'), callback=connector.delivery_report
    )

def test_output_connector_send_to_dlq(mock_confluent_producer, set_env_vars):
    mock_producer_instance = mock_confluent_producer.return_value
    connector = OutputConnector()
    key = "dlq-key"
    value = {"error": "failed"}

    connector.send_to_dlq(key, value)

    mock_producer_instance.poll.assert_called_once_with(0)
    mock_producer_instance.produce.assert_called_once_with(
        Config.KAFKA_DLQ_TOPIC, key=key, value=json.dumps(value).encode('utf-8'), callback=connector.delivery_report
    )

# --- Tests for transformation.py ---

def test_transformation_init(set_env_vars):
    transform = Transformation()
    assert transform.min_order_value == 50.0
    assert transform.processing_interval_sec == 0.01

def test_transformation_process_order_accepted(set_env_vars):
    transform = Transformation()
    raw_order = {"order_id": "1", "total_amount": 100.0, "items": []}
    processed_order = transform.process_order(raw_order)
    assert processed_order['status'] == "accepted"
    assert processed_order['order_id'] == "1"
    assert "processing_timestamp" in processed_order

def test_transformation_process_order_rejected_min_value(set_env_vars):
    transform = Transformation()
    raw_order = {"order_id": "2", "total_amount": 40.0, "items": []}
    processed_order = transform.process_order(raw_order)
    assert processed_order['status'] == "rejected_below_min_value"
    assert processed_order['order_id'] == "2"
    assert processed_order['original_total_amount'] == 40.0
    assert processed_order['min_order_value_threshold'] == 50.0

def test_transformation_process_order_invalid_format(set_env_vars):
    transform = Transformation()
    with pytest.raises(ValueError, match="Invalid order format"):
        transform.process_order("not a dict")

def test_transformation_process_order_invalid_amount(set_env_vars):
    transform = Transformation()
    with pytest.raises(ValueError, match="Invalid total_amount"):
        transform.process_order({"order_id": "3", "total_amount": "abc"})

# --- Tests for input_connector.py ---

@pytest.fixture
def mock_confluent_consumer():
    with patch('confluent_kafka.Consumer') as MockConsumer:
        yield MockConsumer

@pytest.fixture
def mock_output_connector():
    with patch('input_connector.OutputConnector') as MockOutputConnector:
        yield MockOutputConnector.return_value

@pytest.fixture
def mock_transformation_module():
    with patch('input_connector.Transformation') as MockTransformation:
        yield MockTransformation.return_value

def create_mock_message(value, key=None, topic="test-input", partition=0, offset=0, error=None):
    msg = MagicMock()
    msg.value.return_value = value.encode('utf-8')
    msg.key.return_value = key.encode('utf-8') if key else None
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.error.return_value = error
    return msg

def test_input_connector_init(mock_confluent_consumer, mock_output_connector, mock_transformation_module, set_env_vars):
    connector = InputConnector()
    mock_confluent_consumer.assert_called_once()
    mock_output_connector.assert_called_once()
    mock_transformation_module.assert_called_once()

def test_input_connector_json_decode_error(mock_confluent_consumer, mock_output_connector, mock_transformation_module, set_env_vars):
    bad_json_msg = create_mock_message(value="not a json", key="bad-key", topic=Config.INPUT_CONNECTOR_TOPIC)

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [bad_json_msg, None, KeyboardInterrupt]

    connector = InputConnector()
    with patch('logging.Logger.error') as mock_log_error:
        try:
            connector.start_consuming()
        except KeyboardInterrupt:
            pass

        mock_log_error.assert_called_once()
        mock_output_connector.send_to_dlq.assert_called_once()
        args, kwargs = mock_output_connector.send_to_dlq.call_args
        assert args[0] == "bad-key"
        assert "JSON_PARSING_ERROR" in args[1]['error_type']
        mock_transformation_module.process_order.assert_not_called()

def test_input_connector_transformation_error(mock_confluent_consumer, mock_output_connector, mock_transformation_module, set_env_vars):
    valid_json_msg = create_mock_message(value=json.dumps({"order_id": "1", "total_amount": 100}), key="valid-key", topic=Config.INPUT_CONNECTOR_TOPIC)

    mock_transformation_module.process_order.side_effect = ValueError("Simulated transformation error")

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [valid_json_msg, None, KeyboardInterrupt]

    connector = InputConnector()
    with patch('logging.Logger.error') as mock_log_error:
        try:
            connector.start_consuming()
        except KeyboardInterrupt:
            pass

        mock_log_error.assert_called_once()
        mock_transformation_module.process_order.assert_called_once()
        mock_output_connector.send_to_dlq.assert_called_once()
        args, kwargs = mock_output_connector.send_to_dlq.call_args
        assert args[0] == "valid-key"
        assert "PROCESSING_ERROR" in args[1]['error_type']
        mock_output_connector.send_to_output_topic.assert_not_called()

def test_input_connector_successful_flow(mock_confluent_consumer, mock_output_connector, mock_transformation_module, set_env_vars):
    valid_json_msg = create_mock_message(value=json.dumps({"order_id": "1", "total_amount": 100}), key="success-key", topic=Config.INPUT_CONNECTOR_TOPIC)

    mock_transformation_module.process_order.return_value = {"order_id": "1", "status": "processed"}

    mock_consumer_instance = mock_confluent_consumer.return_value
    mock_consumer_instance.poll.side_effect = [valid_json_msg, None, KeyboardInterrupt]

    connector = InputConnector()
    try:
        connector.start_consuming()
    except KeyboardInterrupt:
        pass

    mock_transformation_module.process_order.assert_called_once()
    mock_output_connector.send_to_output_topic.assert_called_once()
    args, kwargs = mock_output_connector.send_to_output_topic.call_args
    assert args[0] == "success-key"
    assert args[1]['status'] == "processed"
    mock_output_connector.send_to_dlq.assert_not_called()

def test_input_connector_close(mock_confluent_consumer, mock_output_connector, mock_transformation_module, set_env_vars):
    connector = InputConnector()
    connector.close()
    mock_confluent_consumer.return_value.close.assert_called_once()
    mock_output_connector.close.assert_called_once()
