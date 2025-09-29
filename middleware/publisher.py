import logging
import time
import pika
from common.config import MiddlewareConfig


class RabbitMQPublisher:
    """
    Lightweight RabbitMQ publisher interface for threads.
    Each thread creates its own publisher with its own channel.
    """

    MAX_RETRIES = 5

    def __init__(self, rabbitmq_connection):
        """
        Initialize publisher with a RabbitMQ connection

        Args:
            rabbitmq_connection: Shared RabbitMQ connection from middleware
        """
        self.connection = rabbitmq_connection
        self.channel = None
        self.confirms_enabled = False
        self.logger = logging.getLogger(__name__)
        self._setup_channel()

    def _setup_channel(self):
        """Setup this publisher's dedicated channel with confirmations and QoS"""
        try:
            self.channel = self.connection.channel()

            # Enable publisher confirmations
            self.channel.confirm_delivery()
            self.confirms_enabled = True

            # Set QoS - prefetch one message at a time
            self.channel.basic_qos(prefetch_count=1)

            self.logger.debug("action: publisher_channel_setup | result: success")

        except Exception as e:
            self.logger.error(
                f"action: publisher_channel_setup | result: fail | error: {e}"
            )
            raise

    def publish(self, routing_key, message, exchange_name="", max_retries=None):
        """Publish a message to an exchange with retries and confirmation"""
        if max_retries is None:
            max_retries = self.MAX_RETRIES

        for attempt in range(1, max_retries + 1):
            try:
                # Prepare message properties
                properties = pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                )

                # Publish message
                published = self.channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=properties,
                    mandatory=False,
                )

                if self.confirms_enabled and not published:
                    self.logger.error(
                        f"action: publish | result: fail | attempt: {attempt} | "
                        f"exchange: {exchange_name} | routing_key: {routing_key} | "
                        f"error: message not confirmed"
                    )
                    if attempt < max_retries:
                        time.sleep(0.1 * attempt)  # Exponential backoff
                        continue
                else:
                    self.logger.debug(
                        f"action: publish | result: success | "
                        f"exchange: {exchange_name} | routing_key: {routing_key}"
                    )
                    return True

            except Exception as e:
                self.logger.error(
                    f"action: publish | result: fail | attempt: {attempt} | "
                    f"exchange: {exchange_name} | routing_key: {routing_key} | error: {e}"
                )
                if attempt < max_retries:
                    time.sleep(0.1 * attempt)  # Exponential backoff
                    continue

        error_msg = f"Failed to publish message to exchange {exchange_name} after {max_retries} attempts"
        self.logger.error(f"action: publish | result: fail | error: {error_msg}")
        return False

    def close(self):
        """Close this publisher's channel"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                self.logger.debug("action: publisher_channel_close | result: success")

        except Exception as e:
            self.logger.error(
                f"action: publisher_channel_close | result: fail | error: {e}"
            )

    def is_connected(self):
        """Check if this publisher's channel is active"""
        return (
            self.channel
            and not self.channel.is_closed
            and self.connection
            and not self.connection.is_closed
        )
