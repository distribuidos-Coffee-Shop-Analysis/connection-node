import socket
import logging
import threading
import signal
import sys
import queue
from server.listener import Listener
from server.query_replies_handler.main import QueryRepliesHandler
from middleware.middleware import Middleware
from protocol.messages import DatasetType
from protocol.protocol import serialize_batch_message
from common.utils import TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE
import pika


class Server:
    def __init__(self, server_config, middleware_config):
        # Store configurations
        self.server_config = server_config
        self.middleware_config = middleware_config

        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", server_config.port))
        self._server_socket.listen(server_config.listen_backlog)

        # Initialize middleware manager
        self._middleware = Middleware(middleware_config)

        # Query replies handler management
        self._query_replies_handler = None
        self._shutdown_requested = False
        self._shutdown_event = threading.Event()
        self._query_handler_shutdown_queue = queue.Queue(maxsize=5)

        # Server callbacks for handlers
        self._server_callbacks = {
            "add_client": self._add_client,
            "remove_client": self._remove_client,
            "handle_batch_message": self._handle_batch_message,
        }

        self._listener = None

        # Set up signal handler for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)

        logging.info(
            "action: server_init | result: success | msg: server initialized on port %s",
            server_config.port,
        )

    def _signal_handler(self):
        """Handle SIGTERM signal for graceful shutdown"""
        logging.info(
            "action: shutdown | result: in_progress | msg: received shutdown signal"
        )
        self._query_handler_shutdown_queue.put_nowait(
            None
        )  # Put shutdown signal to QueryRepliesHandler
        self._shutdown_event.set()  # Signal shutdown event for listener
        self._wait_for_handlers()  # Wait for both threads to complete
        self._middleware.close()  # Close middleware connection

    def run(self):
        """Main server entry point - start middleware, start query handler, start listener"""
        try:
            # Start middleware (connect to RabbitMQ and declare exchanges)
            if not self._middleware.start():
                logging.error(
                    "action: server_startup | result: fail | msg: failed to start middleware"
                )
                return

            # Create the QueryRepliesHandler
            # Create and start QueryRepliesHandler with shutdown queue
            self._query_replies_handler = QueryRepliesHandler(
                middleware=self._middleware,
                shutdown_queue=self._query_handler_shutdown_queue,
            )

            # Create the listener with server socket and callbacks
            # Also pass shutdown event for graceful shutdown and RabbitMQ connection
            self._listener = Listener(
                server_socket=self._server_socket,
                server_callbacks=self._server_callbacks,
                shutdown_event=self._shutdown_event,
                rabbitmq_connection=self._middleware.get_connection(),
            )

            self._query_replies_handler.start()
            self._listener.start()
            logging.info(
                "action: server_run | result: success | msg: server is running"
            )

        finally:
            # Wait for both threads to complete (keep main thread alive)
            logging.info(
                "action: wait for handlers | result: in_progress | msg: waiting for handlers to complete"
            )
            self._wait_for_handlers()
            self._middleware.close()  # Close middleware connection
    def _wait_for_handlers(self):
        """Wait for listener and query replies handler threads to complete"""
        try:
            # Wait for both threads if they were created and started
            if self._listener and self._listener.is_alive():
                self._listener.join()
            if self._query_replies_handler and self._query_replies_handler.is_alive():
                self._query_replies_handler.join()
        except Exception as e:
            logging.error(
                "action: shutdown | result: fail | msg: error waiting for handlers | error: %s",
                e,
            )

    def _add_client(self, client_id, client_queue):
        """Add client to QueryRepliesHandler queue management"""
        if self._query_replies_handler.is_alive():
            self._query_replies_handler.add_client(client_id, client_queue)
            logging.debug(
                "action: add_client | result: success | msg: client added to QueryRepliesHandler | client_id: %s",
                client_id,
            )

    def _handle_batch_message(self, batch, channel):
        """Handle batch message and publish to appropriate exchange using provided channel"""
        try:
            logging.debug(
                "action: handle_batch_message | result: in_progress | dataset_type: %s | records: %d | eof: %s",
                batch.dataset_type,
                len(batch.records),
                batch.eof,
            )

            if batch.dataset_type in [DatasetType.TRANSACTIONS, DatasetType.TRANSACTION_ITEMS]:
                serialized_message = serialize_batch_message(
                    dataset_type=batch.dataset_type,
                    records=batch.records,
                    eof=batch.eof
                )
                
                # Publish directly using the provided channel instead of middleware
                success = self._publish_with_channel(
                    channel=channel,
                    routing_key="",
                    message=serialized_message,
                    exchange_name=TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE
                )
                
                if success:
                    logging.info(
                        "action: publish_batch | result: success | dataset_type: %s | exchange: %s | records: %d",
                        batch.dataset_type,
                        TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                        len(batch.records),
                    )
                else:
                    logging.error(
                        "action: publish_batch | result: fail | dataset_type: %s | exchange: %s",
                        batch.dataset_type,
                        TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                    )
            else:
                logging.debug(
                    "action: handle_batch_message | result: skipped | reason: not_transaction_dataset | dataset_type: %s",
                    batch.dataset_type,
                )
                
        except Exception as e:
            logging.error(
                "action: handle_batch_message | result: fail | error: %s",
                e,
            )

    def _publish_with_channel(self, channel, routing_key, message, exchange_name=""):
        """Publish a message using the provided channel directly"""
        try:
            if not channel or channel.is_closed:
                logging.error(
                    "action: publish_with_channel | result: fail | error: channel is closed or None"
                )
                return False
            
            # Prepare message properties
            properties = pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            )

            # Publish message
            published = channel.basic_publish(
                exchange=exchange_name,
                routing_key=routing_key,
                body=message,
                properties=properties,
                mandatory=False,
            )

            logging.debug(
                "action: publish_with_channel | result: success | exchange: %s | routing_key: %s",
                exchange_name,
                routing_key,
            )
            return True

        except Exception as e:
            logging.error(
                "action: publish_with_channel | result: fail | exchange: %s | routing_key: %s | error: %s",
                exchange_name,
                routing_key,
                e,
            )
            return False

    def _remove_client(self, client_id):
        """Remove client from QueryRepliesHandler queue management"""
        if self._query_replies_handler.is_alive():
            self._query_replies_handler.remove_client(client_id)
            logging.debug(
                "action: remove_client | result: success | msg: client removed from QueryRepliesHandler | client_id: %s",
                client_id,
            )
