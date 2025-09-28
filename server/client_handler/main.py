import socket
import logging
import threading
import queue
from threading import Thread
from protocol.protocol import send_response
from .socket_reader import SocketReader
from .socket_writer import SocketWriter
import pika


class ClientHandler(Thread):
    def __init__(
        self,
        client_socket,
        server_callbacks,
        rabbitmq_connection,
        cleanup_callback,
        client_queue,
    ):
        """
        Initialize the client handler

        Args:
            client_socket: The client socket connection
            client_address: The client address tuple (ip, port)
            server_callbacks: Dictionary with callback functions to server methods:
                - handle_batch_message: callback for batch messages
                - add_client_connection: callback to register client connection
                - remove_client_connection: callback to unregister client connection
            cleanup_callback: Optional callback function to call when handler finishes
                             Should accept (handler_instance) as parameter
            client_queue: Queue to receive reply messages from QueryRepliesHandler
            rabbitmq_connection: RabbitMQ connection to create a new channel
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_socket.getpeername()
        self.server_callbacks = server_callbacks
        self.cleanup_callback = cleanup_callback
        self.client_queue = client_queue or queue.Queue(maxsize=100)
        self.rabbitmq_connection = rabbitmq_connection

        # Shared shutdown event for both threads
        self.shutdown_event = threading.Event()

        # The two separate thread instances
        self.socket_reader = None
        self.socket_writer = None

        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"

    def request_shutdown(self):
        """Request graceful shutdown of this handler"""

        # Signal both threads to stop
        self.shutdown_event.set()
        # Signal the queue-waiting thread to stop by putting None in the queue
        self.client_queue.get_nowait()  # Remove one item (in case the queue is full)
        self.client_queue.put_nowait(None)  # Put shutdown signal
        self._wait_for_threads()  # Wait for both threads to complete
        self._cleanup_connection()  # Close the client socket

    def run(self):
        """Handle persistent communication with a client using two separate threads"""
        try:
            # Create and start the socket reader thread
            self.socket_reader = SocketReader(
                client_socket=self.client_socket,
                client_address=self.client_address,
                server_callbacks=self.server_callbacks,
                shutdown_event=self.shutdown_event,
                rabbitmq_connection=self.rabbitmq_connection,
            )
            self.socket_reader.start()

            # Create and start the socket writer thread
            self.socket_writer = SocketWriter(
                client_socket=self.client_socket,
                client_address=self.client_address,
                client_queue=self.client_queue,
                shutdown_event=self.shutdown_event,
            )
            self.socket_writer.start()

            self._log_action(
                "client_handler_start", "success", extra_fields={"threads": 2}
            )

        except Exception as e:
            self._log_action(
                "client_handler_start", "fail", level=logging.ERROR, error=e
            )
            # Signal shutdown in case of startup failure
            self.shutdown_event.set()

        finally:
            self._wait_for_threads()

            self._cleanup_connection()
            # Call cleanup callback to notify listener this handler is done
            if self.cleanup_callback:
                try:
                    self.cleanup_callback(self, self.client_id)
                except Exception as e:
                    self._log_action(
                        "cleanup_callback", "fail", level=logging.ERROR, error=e
                    )

    def _wait_for_threads(self):
        """Wait for both socket reader and writer threads to complete"""
        # Wait for socket reader thread if it was created and started
        if self.socket_reader and self.socket_reader.is_alive():
            self.socket_reader.join()

        # Wait for socket writer thread if it was created and started
        if self.socket_writer and self.socket_writer.is_alive():
            self.socket_writer.join()

        self._log_action("threads_joined", "success")

    def _log_action(
        self, action, result, level=logging.INFO, error=None, extra_fields=None
    ):
        """
        Centralized logging function for consistent log format

        Args:
            action: The action being performed
            result: The result of the action (success, fail, etc.)
            level: Logging level (INFO, ERROR, DEBUG, etc.)
            error: Optional error information
            extra_fields: Optional dict with additional fields to log
        """
        log_parts = [
            f"action: {action}",
            f"result: {result}",
            f"ip: {self.client_address[0]}",
        ]

        if error:
            log_parts.append(f"error: {error}")

        if extra_fields:
            for key, value in extra_fields.items():
                log_parts.append(f"{key}: {value}")

        log_message = " | ".join(log_parts)
        logging.log(level, log_message)

    def _cleanup_connection(self):
        """Clean up client connection and channel"""
        try:
            # Close RabbitMQ channel first
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                logging.debug(f"action: close_channel | result: success | client: {self.client_address}")
        except Exception as e:
            logging.error(f"action: close_channel | result: fail | client: {self.client_address} | error: {e}")
        
        try:
            self.client_socket.shutdown(
                socket.SHUT_RDWR
            )  # First notify the client about the disconnection
            self.client_socket.close()
        except Exception as e:
            # Socket possibly already closed
            self._log_action(
                "cleanup_connection", "already_closed", level=logging.DEBUG
            )
