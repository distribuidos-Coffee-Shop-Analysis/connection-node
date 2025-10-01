# pylint: disable=broad-exception-caught
import logging
import pika
from common.utils import REPLIES_EXCHANGE, REQUIRED_EXCHANGES
from common.config import MiddlewareConfig


class Middleware:
    """
    Manages RabbitMQ connection and initial setup (exchanges/queues declaration).

    Follows standard pattern:
    1. Create connection
    2. Create init channel for setup
    3. Declare exchanges and queues
    4. Provide connection for threads to create their own channels
    """

    def __init__(self, middleware_config: MiddlewareConfig):
        self.config = middleware_config
        self.connection = None
        self.init_channel = (
            None  # Channel used for initialization (declaring exchanges/queues)
        )
        self.logger = logging.getLogger(__name__)
        self.is_started = False

    def start(self):
        """Start middleware: connect to RabbitMQ and declare exchanges/queues usando patrón estándar"""
        try:
            self.logger.info(
                "action: middleware_start | result: in_progress | msg: starting middleware"
            )

            # 1. Open TCP connection
            self._connect()

            # 2. Open initialization channel
            self._setup_init_channel()

            # 3. Declare required exchanges and queues with the initialization channel
            if not self.declare_required_exchanges():
                self.logger.error(
                    "action: middleware_start | result: fail | msg: failed to declare exchanges"
                )
                return False

            # 4. Declare replies queue
            if not self.declare_queue("replies_queue", durable=False):
                self.logger.error(
                    "action: middleware_start | result: fail | msg: failed to declare replies queue"
                )
                return False

            # 5. Bind "replies" queue to "replies" exchange with routing key
            if not self.bind_queue("replies_queue", REPLIES_EXCHANGE, ""):
                self.logger.error(
                    "action: middleware_start | result: fail | msg: failed to bind replies queue"
                )
                return False

            self.is_started = True
            self.logger.info("action: middleware_start | result: success")
            return True

        except Exception as e:
            self.logger.error(f"action: middleware_start | result: fail | error: {e}")
            return False

    def _connect(self):
        """Establish connection to RabbitMQ"""
        try:
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

        except Exception as e:
            self.logger.error(f"action: rabbitmq_connect | result: fail | error: {e}")
            raise

    def _setup_init_channel(self):
        """Setup channel de inicialización para declarar exchanges/queues"""
        try:
            self.init_channel = self.connection.channel()
            self.logger.debug("action: init_channel_setup | result: success")

        except Exception as e:
            self.logger.error(f"action: init_channel_setup | result: fail | error: {e}")
            raise

    def declare_queue(
        self, queue_name, durable=True, exclusive=False, auto_delete=False
    ):
        """Declare a queue usando el channel de inicialización"""
        try:
            if not self.init_channel:
                raise RuntimeError("Init channel not available - call start() first")

            self.init_channel.queue_declare(
                queue=queue_name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
            )
            self.logger.info(
                "action: declare_queue | result: success | queue: %s", queue_name
            )
            return True

        except Exception as e:
            self.logger.error(
                f"action: declare_queue | result: fail | queue: {queue_name} | error: {e}"
            )
            return False

    def declare_exchange(self, exchange_name, exchange_type="direct", durable=False):
        """Declare an exchange usando el channel de inicialización"""
        try:
            if not self.init_channel:
                raise RuntimeError("Init channel not available - call start() first")

            self.init_channel.exchange_declare(
                exchange=exchange_name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=False,
                internal=False,
            )
            self.logger.info(
                f"action: declare_exchange | result: success | exchange: {exchange_name}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"action: declare_exchange | result: fail | exchange: {exchange_name} | error: {e}"
            )
            return False

    def bind_queue(self, queue_name, exchange_name, routing_key):
        """Bind a queue to an exchange usando el channel de inicialización"""
        try:
            if not self.init_channel:
                raise RuntimeError("Init channel not available - call start() first")

            self.init_channel.queue_bind(
                queue=queue_name, exchange=exchange_name, routing_key=routing_key
            )
            self.logger.info(
                f"action: bind_queue | result: success | "
                f"queue: {queue_name} | exchange: {exchange_name} | routing_key: {routing_key}"
            )
            return True

        except Exception as e:
            self.logger.error(
                f"action: bind_queue | result: fail | "
                f"queue: {queue_name} | exchange: {exchange_name} | routing_key: {routing_key} | error: {e}"
            )
            return False

    def declare_required_exchanges(self):
        """Declare all required exchanges from REQUIRED_EXCHANGES"""
        try:
            self.logger.info("action: declare_required_exchanges | result: in_progress")

            for exchange_name in REQUIRED_EXCHANGES:
                if not self.declare_exchange(exchange_name, "direct", durable=False):
                    return False

            self.logger.info("action: declare_required_exchanges | result: success")
            return True

        except Exception as e:
            self.logger.error(
                f"action: declare_required_exchanges | result: fail | error: {e}"
            )
            return False

    def stop(self):
        """Stop middleware and close connections"""
        try:
            self.logger.info("action: middleware_stop | result: in_progress")
            self.close()
            self.is_started = False
            self.logger.info("action: middleware_stop | result: success")
        except Exception as e:
            self.logger.error(f"action: middleware_stop | result: fail | error: {e}")

    def close(self):
        """Close initialization channel (if still open) and connection"""
        try:
            # Close init channel if still open
            if self.init_channel and not self.init_channel.is_closed:
                self.init_channel.close()
                self.init_channel = None
                self.logger.debug("action: init_channel_close | result: success")

            # Close connection
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                self.logger.debug("action: connection_close | result: success")

            self.logger.info("action: middleware_close | result: success")

        except Exception as e:
            self.logger.error(f"action: middleware_close | result: fail | error: {e}")

    def is_connected(self):
        """Check if connection is active"""
        return self.connection and not self.connection.is_closed

    def get_connection(self):
        """Get the RabbitMQ connection for threads to create their own channels"""
        return self.connection
