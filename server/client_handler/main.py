import socket
import logging
import multiprocessing
from multiprocessing import Process
import threading
from .socket_reader import SocketReader
from .socket_writer import SocketWriter
import pika
from common.utils import log_action
from common.persistence import SessionManager
from middleware.publisher import RabbitMQPublisher

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
        client_id_queue=None,
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
            client_id_queue: Queue to send client UUID back to Listener
        """
        super().__init__(daemon=False)
        self.client_socket = client_socket
        self.client_address = client_socket.getpeername()
        self.server_callbacks = server_callbacks
        self.remove_from_server_callback = remove_from_server_callback
        self.client_queue = client_queue
        self.middleware_config = middleware_config
        self.client_id_queue = client_id_queue

        # Shared shutdown event for all threads
        self.shutdown_event_thread = threading.Event()
        self.shutdown_event_process = multiprocessing.Event()

        # Shutdown queue for monitor thread
        self.shutdown_queue = shutdown_queue

        # The three thread instances
        self.socket_reader = None
        self.socket_writer = None
        self.shutdown_monitor = None

        # Session persistence and cleanup
        self.session_manager = SessionManager()
        self.client_id = None
        self.session_persisted = False
        self.cleanup_publisher = None
        
        # Create a separate queue for ClientHandler to get client_id
        self.client_id_queue_handler = multiprocessing.Queue(maxsize=1)

    def _handle_shutdown_signal(self):
        """Callback function called by shutdown monitor when shutdown is requested"""
        try:
            try:
                self.client_queue.get_nowait()  # Remove one item (in case the queue is full)
            except:
                pass  # Queue might be empty
            self.client_queue.put_nowait(None)  # Put shutdown signal to SocketWriter
            self.shutdown_event.set()  # Signal shutdown event for SocketReader and SocketWriter
            self._wait_for_threads()  # Wait for all threads to complete
            self._close_connection()  # Close the client socket
        except Exception as e:
            self.logger.error(
                "action: handle_shutdown | result: fail | msg: error stopping consumption | error: %s",
                e,
            )

    def run(self):
        """Handle persistent communication with a client using three separate threads"""
        disconnection_was_unexpected = False
        
        try:
            # Create and start the socket reader Process
            # Pass our handler-specific queue so SocketReader can send us the client_id too
            self.socket_reader = SocketReader(
                client_socket=self.client_socket,
                client_address=self.client_address,
                server_callbacks=self.server_callbacks,
                shutdown_event=self.shutdown_event_process,
                middleware_config=self.middleware_config,
                client_id_queue=self.client_id_queue,
                client_id_queue_handler=self.client_id_queue_handler,
            )
            self.socket_reader.start()

            # Wait for the client_id from SocketReader via our dedicated queue
            try:
                # Wait with timeout for client_id
                self.client_id = self.client_id_queue_handler.get(timeout=5)
                log_action(
                    action="get_client_id",
                    result="success",
                    extra_fields={"client_id": self.client_id},
                )
            except Exception as e:
                log_action(
                    action="get_client_id",
                    result="fail",
                    level=logging.WARNING,
                    error=e,
                )

            # Persist session once we have client_id
            if self.client_id:
                self.session_manager.add_session(self.client_id)
                self.session_persisted = True
                log_action(
                    action="session_persist",
                    result="success",
                    extra_fields={"client_id": self.client_id},
                )
            else:
                log_action(
                    action="session_persist",
                    result="skipped",
                    level=logging.WARNING,
                    extra_fields={"reason": "client_id_not_available"},
                )

            # Create and start the socket writer thread
            self.socket_writer = SocketWriter(
                client_socket=self.client_socket,
                client_address=self.client_address,
                client_queue=self.client_queue,
                shutdown_event=self.shutdown_event_thread,
            )
            self.socket_writer.start()

            # Create and start the shutdown monitor thread
            self.shutdown_monitor = ShutdownMonitor(
                shutdown_queue=self.shutdown_queue,
                shutdown_callback=self._handle_shutdown_signal,
            )
            self.shutdown_monitor.start()

            log_action(
                action="client_handler_start",
                result="success",
                extra_fields={"threads": 3},
            )

        except (socket.error, ConnectionError, BrokenPipeError) as e:
            # Network error = unexpected disconnection
            disconnection_was_unexpected = True
            log_action(
                action="client_handler_run",
                result="fail",
                level=logging.ERROR,
                error=f"Unexpected disconnection: {e}",
            )
            # Signal shutdown in case of failure
            self.shutdown_event_thread.set()
            self.shutdown_event_process.set()

        except Exception as e:
            # Other exceptions = unexpected disconnection
            disconnection_was_unexpected = True
            log_action(
                action="client_handler_start",
                result="fail",
                level=logging.ERROR,
                error=e,
            )
            # Signal shutdown in case of startup failure
            self.shutdown_event_thread.set()
            self.shutdown_event_process.set()

        finally:
            # Set shutdown events to signal all handlers to stop
            self.shutdown_event_thread.set()
            self.shutdown_event_process.set()
            
            self._wait_for_handlers()
            self._close_connection()

            # Try one more time to get client_id if we don't have it yet
            if not self.client_id and self.client_id_queue:
                try:
                    self.client_id = self.client_id_queue.get_nowait()
                except:
                    pass

            # Remove session from persistence
            if self.client_id and self.session_persisted:
                self.session_manager.remove_session(self.client_id)
                log_action(
                    action="session_remove",
                    result="success",
                    extra_fields={"client_id": self.client_id},
                )

            # Send cleanup signal if disconnection was unexpected
            if disconnection_was_unexpected and self.client_id:
                self._send_cleanup_signal()

            # Call "remove_from_server" callback to notify listener this process is done
            # Pass client_address instead of client_id since we don't track temp_client_id anymore
            if self.remove_from_server_callback:
                try:
                    self.remove_from_server_callback(self.client_address)
                except Exception as e:
                    log_action(
                        action="cleanup_callback",
                        result="fail",
                        level=logging.ERROR,
                        error=e,
                    )

    def _set_shutdown_event(self):
        """Callback to set the shutdown event when called by ShutdownMonitor."""
        self.shutdown_event.set()
        log_action(action="shutdown_monitor", result="shutdown_event_set")

    def _wait_for_handlers(self):
        """Wait for all threads to complete"""
        # Wait for socket reader process if it was created and started
        if self.socket_reader and self.socket_reader.is_alive():
            self.socket_reader.join()

        # Wait for socket writer thread if it was created and started
        if self.socket_writer and self.socket_writer.is_alive():
            self.socket_writer.join()

        # Wait for shutdown monitor thread if it was created and started
        if self.shutdown_monitor and self.shutdown_monitor.is_alive():
            self.shutdown_monitor.join()

        log_action(action="threads_joined", result="success")

    def _close_connection(self):
        """Clean up client connection and channel"""
        try:
            self.client_socket.shutdown(
                socket.SHUT_RDWR
            )  # First notify the client about the disconnection
            self.client_socket.close()
        except Exception as e:
            # Socket possibly already closed
            log_action(
                action="cleanup_connection",
                result="already_closed",
                level=logging.DEBUG,
            )

    def _send_cleanup_signal(self):
        """Send cleanup signal to all nodes to remove client state"""
        try:
            # Create a publisher for sending cleanup signal
            if not self.cleanup_publisher:
                self.cleanup_publisher = RabbitMQPublisher(self.middleware_config)
            
            # Send cleanup signal
            success = self.cleanup_publisher.send_cleanup_signal(self.client_id)
            
            if success:
                log_action(
                    action="send_cleanup_signal",
                    result="success",
                    extra_fields={"client_id": self.client_id},
                )
            else:
                log_action(
                    action="send_cleanup_signal",
                    result="fail",
                    level=logging.ERROR,
                    extra_fields={"client_id": self.client_id},
                )
            
            # Close the publisher
            self.cleanup_publisher.close()
            
        except Exception as e:
            log_action(
                action="send_cleanup_signal",
                result="fail",
                level=logging.ERROR,
                error=e,
                extra_fields={"client_id": self.client_id},
            )
