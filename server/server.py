import socket
import logging
import threading
from threading import Thread
from protocol.protocol import send_batch_message
from protocol.messages import BatchMessage
from common.queue_manager import QueueManager
from .listener import Listener


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(("", port))
        self._server_socket.listen(listen_backlog)

        # Initialize queue manager for RabbitMQ communication
        self._queue_manager = QueueManager()

        # Client connections management
        self._client_connections_lock = threading.Lock()
        self._client_connections = (
            []
        )  # Store active client connections for sending replies

        # Reply handler thread
        self._reply_handler_thread = None
        self._shutdown_requested = False

        logging.info(f"action: server_init | result: success | port: {port}")

    def run(self):
        """Main server entry point - setup queues and start listener"""
        try:
            # Connect to RabbitMQ
            if not self._queue_manager.connect():
                logging.error(
                    "action: server_startup | result: fail | error: Could not connect to RabbitMQ"
                )
                return

            # Start reply handler thread to process query responses
            self._reply_handler_thread = Thread(
                target=self._handle_replies, daemon=True
            )
            self._reply_handler_thread.start()

            # Create server callbacks for the client handlers
            server_callbacks = {
                "handle_batch_message": self._handle_batch_message,
            }

            # Create and start the listener
            listener = Listener(
                server_socket=self._server_socket,
                server_callbacks=server_callbacks,
                server_instance=self,  # Pass reference to self for connection management
            )

            # Start listening for connections
            listener.run()

        except KeyboardInterrupt:
            logging.info("action: server_shutdown | result: in_progress")
            self._shutdown()
        except Exception as e:
            logging.error(f"action: server_run | result: fail | error: {e}")
            self._shutdown()

    def _handle_batch_message(self, batch_message):
        """Handle dataset batch message - route to appropriate queue"""
        try:
            # Send the batch to the appropriate queue via RabbitMQ
            success = self._queue_manager.send_dataset_batch(batch_message)

            if success:
                logging.info(
                    f"action: dataset_received | result: success | "
                    f"dataset_type: {batch_message.dataset_type} | "
                    f"record_count: {len(batch_message.records)} | "
                    f"eof: {batch_message.eof}"
                )
            else:
                logging.error(
                    f"action: dataset_received | result: fail | dataset_type: {batch_message.dataset_type}"
                )

        except Exception as e:
            logging.error(f"action: handle_batch_message | result: fail | error: {e}")

    # TODO: This responds to all clients the replies from the queue, we need to filter the client and send only to the one who made the request (include client_id task)
    def _handle_replies(self):
        """Thread function to handle query responses from replies queue"""

        def reply_callback(dataset_type, records, eof):
            """Process a reply message and send to connected clients"""
            try:
                with self._client_connections_lock:
                    for client_socket in self._client_connections[:]:
                        try:
                            send_batch_message(
                                client_socket, dataset_type, records, eof
                            )
                            logging.info(
                                f"action: reply_sent | result: success | "
                                f"dataset_type: {dataset_type} | "
                                f"record_count: {len(records)} | "
                                f"eof: {eof}"
                            )
                        except Exception as e:
                            logging.error(
                                f"action: reply_sent | result: fail | error: {e}"
                            )
                            # Remove disconnected client
                            self._client_connections.remove(client_socket)

            except Exception as e:
                logging.error(f"action: process_reply | result: fail | error: {e}")

        try:
            # Start consuming replies (this blocks until stopped)
            self._queue_manager.start_consuming_replies(reply_callback)
        except Exception as e:
            if not self._shutdown_requested:
                logging.error(f"action: handle_replies | result: fail | error: {e}")

    def add_client_connection(self, client_socket):
        """Add a client connection for receiving replies"""
        with self._client_connections_lock:
            self._client_connections.append(client_socket)

    def remove_client_connection(self, client_socket):
        """Remove a client connection"""
        with self._client_connections_lock:
            if client_socket in self._client_connections:
                self._client_connections.remove(client_socket)

    def _shutdown(self):
        self._shutdown_requested = True

        try:
            self._queue_manager.stop_consuming()

            self._queue_manager.disconnect()

            if self._server_socket:
                self._server_socket.close()

            logging.info("action: server_shutdown | result: success")

        except Exception as e:
            logging.error(f"action: server_shutdown | result: fail | error: {e}")
