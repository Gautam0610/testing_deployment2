import logging
import time
import json
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class Transformation:
    def __init__(self):
        self.min_order_value = Config.TRANSFORM_MIN_ORDER_VALUE
        self.processing_interval_sec = Config.TRANSFORM_PROCESSING_INTERVAL_SEC
        logger.info("Transformation module initialized with min_order_value=%s, processing_interval_sec=%s",
                    self.min_order_value, self.processing_interval_sec)

    def process_order(self, raw_order_data):
        """Applies business logic to the raw order data."""
        order_id = raw_order_data.get('order_id', 'N/A')
        logger.info("Transforming order: %s", order_id)

        # Simulate processing time
        time.sleep(self.processing_interval_sec)

        # Basic validation
        if not isinstance(raw_order_data, dict):
            raise ValueError("Invalid order format: Expected a dictionary.")

        total_amount = raw_order_data.get('total_amount')
        if not isinstance(total_amount, (int, float)):
            raise ValueError(f"Invalid total_amount for order {order_id}: {total_amount}")

        # Apply business rule: minimum order value
        if total_amount < self.min_order_value:
            logger.warning("Order %s rejected: Total amount %s is below minimum %s",
                           order_id, total_amount, self.min_order_value)
            return {
                "order_id": order_id,
                "status": "rejected_below_min_value",
                "original_total_amount": total_amount,
                "min_order_value_threshold": self.min_order_value,
                "processing_timestamp": datetime.now().isoformat() + 'Z'
            }
        
        # Simulate a complex transformation or enrichment
        processed_order = raw_order_data.copy()
        processed_order['processed_total_amount'] = total_amount # Could be a calculated value
        processed_order['status'] = "accepted"
        processed_order['processing_timestamp'] = datetime.now().isoformat() + 'Z'

        logger.info("Order %s transformed successfully. Status: %s", order_id, processed_order['status'])
        return processed_order
