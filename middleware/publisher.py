import logging
import time
import pika
from common.utils import HEARTBEAT

class RabbitMQPublisher:
    """
    Lightweight RabbitMQ publisher interface for threads.
    Each thread creates its own publisher with its own connection.
    """

    MAX_RETRIES = 5

    def __init__(self, middleware_config):
        """
        Initialize publisher with its own RabbitMQ connection

        Args:
            middleware_config: Configuration for RabbitMQ connection
        """
        self.config = middleware_config
        self.connection = None
        self.channel = None
        self.confirms_enabled = False
        self.logger = logging.getLogger(__name__)
        self._setup_connection_and_channel()

    def _setup_connection_and_channel(self):
        """Setup this publisher's own connection and dedicated channel"""
        try:
            # Create own connection (thread-safe)
            credentials = pika.PlainCredentials(
                self.config.username, self.config.password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                credentials=credentials,
                heartbeat=HEARTBEAT,
                blocked_connection_timeout=300,
            )
            self.connection = pika.BlockingConnection(parameters)
            
            # Create channel
            self.channel = self.connection.channel()

            # Enable publisher confirmations
            self.channel.confirm_delivery()
            self.confirms_enabled = True

            # Set QoS - prefetch one message at a time
            #self.channel.basic_qos(prefetch_count=1)

            self.logger.debug("action: publisher_setup | result: success")

        except Exception as e:
            self.logger.error(
                f"action: publisher_setup | result: fail | error: {e}"
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
                # With BlockingConnection + confirm_delivery(), basic_publish returns None
                # and raises exception (like UnroutableError) if message not confirmed
                result = self.channel.basic_publish(
                    exchange=exchange_name,
                    routing_key=routing_key,
                    body=message,
                    properties=properties,
                    mandatory=False,
                )

                # If we reach here without exception, message was confirmed
                # (result is always None with BlockingConnection)
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
        """Close this publisher's channel and connection"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                self.logger.debug("action: publisher_channel_close | result: success")
                
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.debug("action: publisher_connection_close | result: success")

        except Exception as e:
            self.logger.error(
                f"action: publisher_close | result: fail | error: {e}"
            )

    def is_connected(self):
        """Check if this publisher's channel is active"""
        return (
            self.channel
            and not self.channel.is_closed
            and self.connection
            and not self.connection.is_closed
        )
