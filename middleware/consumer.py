import logging
import threading
import pika
from middleware.publisher import RabbitMQPublisher


class RabbitMQConsumer:
    """
    Lightweight RabbitMQ consumer interface for threads.
    Each consumer thread creates its own consumer with its own connection.
    """

    def __init__(self, middleware_config, queue_name, callback_function):
        """
        Initialize consumer with its own RabbitMQ connection

        Args:
            middleware_config: Configuration for RabbitMQ connection
            queue_name: Name of the queue to consume from
            callback_function: Function to call when messages are received
        """
        self.config = middleware_config
        self.connection = None
        self.queue_name = queue_name
        self.callback_function = callback_function
        self.channel = None
        self.logger = logging.getLogger(__name__)
        self._setup_connection_and_channel()

    def _setup_connection_and_channel(self):
        """Setup this consumer's own connection and dedicated channel"""
        try:
            # Create own connection (thread-safe)
            credentials = pika.PlainCredentials(
                self.config.username, self.config.password
            )
            parameters = pika.ConnectionParameters(
                host=self.config.host,
                port=self.config.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
            )
            self.connection = pika.BlockingConnection(parameters)
            
            # Create channel
            self.channel = self.connection.channel()

            # Set QoS - prefetch one message at a time
            self.channel.basic_qos(prefetch_count=1)

            self.logger.debug("action: consumer_setup | result: success")

        except Exception as e:
            self.logger.error(
                f"action: consumer_setup | result: fail | error: {e}"
            )
            raise

    def start_consuming(self):
        """Start consuming messages from the queue (blocking call)"""
        try:
            # Setup consumer
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=self.callback_function,
                auto_ack=False,
            )

            self.logger.info(
                f"action: start_consuming | result: in_progress | queue: {self.queue_name}"
            )

            # Start consuming (blocking)
            self.channel.start_consuming()

        except KeyboardInterrupt:
            self.logger.info("action: start_consuming | result: interrupted")
            self.stop_consuming()
        except Exception as e:
            self.logger.error(f"action: start_consuming | result: fail | error: {e}")

    def stop_consuming(self):
        """Stop consuming messages"""
        try:
            if self.channel:
                self.channel.stop_consuming()
                self.logger.info("action: stop_consuming | result: success")

        except Exception as e:
            self.logger.error(f"action: stop_consuming | result: fail | error: {e}")

    def close(self):
        """Close this consumer's channel and connection"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                self.logger.debug("action: consumer_channel_close | result: success")
                
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.debug("action: consumer_connection_close | result: success")

        except Exception as e:
            self.logger.error(
                f"action: consumer_close | result: fail | error: {e}"
            )

    def is_connected(self):
        """Check if this consumer's channel is active"""
        return (
            self.channel
            and not self.channel.is_closed
            and self.connection
            and not self.connection.is_closed
        )
