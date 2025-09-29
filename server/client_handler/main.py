import socket
import logging
import multiprocessing
import queue
from multiprocessing import Process
import threading
from protocol.protocol import send_response
from .socket_reader import SocketReader
from .socket_writer import SocketWriter
import pika

from common.shutdown_monitor import ShutdownMonitor


class ClientHandler(Process):
    def __init__(
        self,
        client_socket,
        server_callbacks,
        middleware_config,
        remove_from_server_callback,
        client_queue,
        shutdown_queue,
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
            middleware_config: RabbitMQ configuration for thread connections
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_socket.getpeername()
        self.server_callbacks = server_callbacks
        self.remove_from_server_callback = remove_from_server_callback
        self.client_queue = client_queue
        self.middleware_config = middleware_config

        # Shared shutdown event for all threads
        self.shutdown_event = threading.Event()

        # Shutdown queue for monitor thread
        self.shutdown_queue = shutdown_queue 

        # The three thread instances
        self.socket_reader = None
        self.socket_writer = None
        self.shutdown_monitor = None

        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"

    def _handle_shutdown_signal(self):
        """Callback function called by shutdown monitor when shutdown is requested"""
        try:
            try:
                self.client_queue.get_nowait()  # Remove one item (in case the queue is full)
            except:
                pass  # Queue might be empty
            self.client_queue.put_nowait(None)  # Put shutdown signal to SocketWriter
            self.shutdown_event.set() # Signal shutdown event for SocketReader and SocketWriter
            self._wait_for_threads()  # Wait for all threads to complete
            self._close_connection()  # Close the client socket
        except Exception as e:
            self.logger.error(
                "action: handle_shutdown | result: fail | msg: error stopping consumption | error: %s",
                e,
            )
    
    def run(self):
        """Handle persistent communication with a client using three separate threads"""
        try:
            # Create and start the socket reader thread
            self.socket_reader = SocketReader(
                client_socket=self.client_socket,
                client_address=self.client_address,
                server_callbacks=self.server_callbacks,
                shutdown_event=self.shutdown_event,
                middleware_config=self.middleware_config,
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

            # Create and start the shutdown monitor thread using the common ShutdownMonitor
            self.shutdown_monitor = ShutdownMonitor(
                shutdown_queue=self.shutdown_queue,
                shutdown_callback=self._handle_shutdown_signal,
            )
            self.shutdown_monitor.start()

            self._log_action(
                "client_handler_start", "success", extra_fields={"threads": 3}
            )

        except Exception as e:
            self._log_action(
                "client_handler_start", "fail", level=logging.ERROR, error=e
            )
            # Signal shutdown in case of startup failure
            self.shutdown_event.set()

        finally:
            self._wait_for_threads()
            self._close_connection()

            # Call "remove_from_server" callback to notify listener this process is done
            if self.remove_from_server:
                try:
                    self.remove_from_server(self, self.client_id)
                except Exception as e:
                    self._log_action(
                        "cleanup_callback", "fail", level=logging.ERROR, error=e
                    )

    def _set_shutdown_event(self):
        """Callback to set the shutdown event when called by ShutdownMonitor."""
        self.shutdown_event.set()
        self._log_action("shutdown_monitor", "shutdown_event_set")

    def _wait_for_threads(self):
        """Wait for all threads to complete"""
        # Wait for socket reader thread if it was created and started
        if self.socket_reader and self.socket_reader.is_alive():
            self.socket_reader.join()

        # Wait for socket writer thread if it was created and started
        if self.socket_writer and self.socket_writer.is_alive():
            self.socket_writer.join()

        # Wait for shutdown monitor thread if it was created and started
        if self.shutdown_monitor and self.shutdown_monitor.is_alive():
            self.shutdown_monitor.join()

        self._log_action("threads_joined", "success")

    def _close_connection(self):
        """Clean up client connection and channel"""
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
