import socket
import logging
import multiprocessing
from multiprocessing import Process
import threading
from .socket_reader import SocketReader
from .socket_writer import SocketWriter
from common.utils import log_action
from common.persistence import SessionManager
from middleware.publisher import RabbitMQPublisher
from common.shutdown_monitor import ShutdownMonitor


class ClientHandler(Process):
    """
    Handles a single client connection using three threads:
    - SocketReader: reads batches from client socket
    - SocketWriter: writes responses to client socket
    - ShutdownMonitor: monitors for server shutdown signal
    """

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
        super().__init__(daemon=False)
        self.client_socket = client_socket
        self.client_address = client_socket.getpeername()
        self.server_callbacks = server_callbacks
        self.remove_from_server_callback = remove_from_server_callback
        self.client_queue = client_queue
        self.middleware_config = middleware_config
        self.client_id_queue = client_id_queue
        self.shutdown_queue = shutdown_queue

        # Shutdown events for threads
        self.shutdown_event_thread = threading.Event()
        self.shutdown_event_process = multiprocessing.Event()
        self.socket_broken_event = threading.Event()
        self.all_responses_sent_event = threading.Event()

        # Handler instances
        self.socket_reader = None
        self.socket_writer = None
        self.shutdown_monitor = None

        # Session management
        self.session_manager = SessionManager()
        self.client_id = None
        self.session_persisted = False
        self.cleanup_publisher = None
        self._cleanup_in_progress = False

        # Queues for client_id
        self.client_id_queue_handler = multiprocessing.Queue(maxsize=1)
        self.all_input_eofs_queue = multiprocessing.Queue(maxsize=1)

    def _handle_shutdown_signal(self):
        """Callback function called by shutdown monitor when shutdown is requested"""
        try:
            # If cleanup is already in progress just return
            if self._cleanup_in_progress:
                log_action(
                    action="handle_shutdown",
                    result="skipped",
                    extra_fields={
                        "client_id": self.client_id if self.client_id else "unknown",
                        "msg": "Cleanup already in progress, skipping redundant shutdown handling",
                    },
                )
                return

            try:
                self.client_queue.get_nowait()  # Remove one item (in case the queue is full)
            except:
                pass  # Queue might be empty
            self.client_queue.put_nowait(None)  # Put shutdown signal to SocketWriter
            self.shutdown_event_thread.set()  # Signal shutdown event for SocketWriter
            self.shutdown_event_process.set()  # Signal shutdown event for SocketReader
            self._wait_for_handlers()  # Wait for all threads to complete
            self._close_connection()  # Close the client socket
        except Exception as e:
            log_action(
                action="handle_shutdown",
                result="fail",
                level=logging.ERROR,
                error=e,
            )

    def run(self):
        """Handle persistent communication with a client using three separate threads"""
        # Simple flag: True = server requested shutdown, False = client disconnected
        server_shutdown_requested = self.shutdown_event_process.is_set()

        try:
            self._start_handlers()
            self._wait_for_handlers()

        except Exception as e:
            log_action(
                action="client_handler_error",
                result="fail",
                level=logging.ERROR,
                error=e,
            )

        finally:
            self._cleanup(server_shutdown_requested)

    def _start_handlers(self):
        """Start all handler threads/processes"""
        # Start SocketReader
        self.socket_reader = SocketReader(
            client_socket=self.client_socket,
            client_address=self.client_address,
            server_callbacks=self.server_callbacks,
            shutdown_event=self.shutdown_event_process,
            middleware_config=self.middleware_config,
            client_id_queue=self.client_id_queue,
            client_id_queue_handler=self.client_id_queue_handler,
            all_eofs_received_queue=self.all_input_eofs_queue,
        )
        self.socket_reader.start()

        # Wait for client_id from first message
        try:
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

        # Persist session
        if self.client_id:
            self.session_manager.add_session(self.client_id)
            self.session_persisted = True
            log_action(
                action="session_persist",
                result="success",
                extra_fields={"client_id": self.client_id},
            )

        # Start SocketWriter
        self.socket_writer = SocketWriter(
            client_socket=self.client_socket,
            client_address=self.client_address,
            client_queue=self.client_queue,
            shutdown_event=self.shutdown_event_thread,
            socket_broken_event=self.socket_broken_event,
            all_responses_sent_event=self.all_responses_sent_event,
            users_joiners_count=self.middleware_config.users_joiners_count,
        )
        self.socket_writer.start()

        # Start ShutdownMonitor
        self.shutdown_monitor = ShutdownMonitor(
            shutdown_queue=self.shutdown_queue,
            shutdown_callback=self._handle_shutdown_signal,
        )
        self.shutdown_monitor.start()

        log_action(
            action="client_handler_start",
            result="success",
            extra_fields={"client_id": self.client_id, "threads": 3},
        )

    def _cleanup(self, server_shutdown_requested):
        """
        Cleanup after client handler finishes.

        Args:
            server_shutdown_requested: True if server initiated shutdown, False otherwise
        """
        self._cleanup_in_progress = True

        # Signal all handlers to stop
        self.shutdown_event_thread.set()
        self.shutdown_event_process.set()

        # Wait for all handlers to finish
        self._wait_for_handlers()

        # Check if all responses were sent (client completed normally)
        all_responses_sent = self.all_responses_sent_event.is_set()

        # Determine if we need to send cleanup:
        # - Server shutdown: NO cleanup (graceful shutdown)
        # - Otherwise: YES cleanup (even if completed, to ensure no residual data in nodes)
        need_cleanup = not server_shutdown_requested

        log_action(
            action="cleanup_start",
            result="in_progress",
            extra_fields={
                "client_id": self.client_id if self.client_id else "unknown",
                "server_shutdown_requested": server_shutdown_requested,
                "all_responses_sent": all_responses_sent,
                "need_cleanup": need_cleanup,
            },
        )

        # Close socket
        self._close_connection()

        # Try to get client_id if we don't have it
        if not self.client_id:
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

        # Send cleanup if needed
        if need_cleanup and self.client_id:
            reason = "client_completed" if all_responses_sent else "client_disconnected"
            log_action(
                action="sending_cleanup",
                result="in_progress",
                extra_fields={
                    "client_id": self.client_id,
                    "reason": reason,
                    "all_responses_sent": all_responses_sent,
                },
            )
            self._send_cleanup_signal()
        elif need_cleanup and not self.client_id:
            log_action(
                action="cleanup_skipped",
                result="skipped",
                level=logging.WARNING,
                extra_fields={"reason": "no_client_id"},
            )
        else:
            log_action(
                action="cleanup_skipped",
                result="skipped",
                extra_fields={"reason": "server_shutdown"},
            )

        # Notify Listener
        if self.remove_from_server_callback:
            try:
                self.remove_from_server_callback(self.client_address)
            except Exception as e:
                log_action(
                    action="remove_from_server",
                    result="fail",
                    level=logging.ERROR,
                    error=e,
                )

    def _wait_for_handlers(self):
        """Wait for all handler threads/processes to complete"""
        # Wait for SocketReader
        if self.socket_reader and self.socket_reader.is_alive():
            self.socket_reader.join()

        # Signal SocketWriter to stop (it might be blocked waiting for messages)
        self.shutdown_event_thread.set()
        try:
            self.client_queue.put_nowait(None)
        except:
            pass

        # Wait for SocketWriter
        if self.socket_writer and self.socket_writer.is_alive():
            self.socket_writer.join()

        # Signal ShutdownMonitor to stop (it's blocking on queue.get())
        try:
            self.shutdown_queue.put_nowait(None)
        except:
            pass

        # Wait for ShutdownMonitor
        if self.shutdown_monitor and self.shutdown_monitor.is_alive():
            self.shutdown_monitor.join()

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
            success = self.cleanup_publisher.send_cleanup_signal(self.client_id)
            if success:
                log_action(
                    action="send_cleanup_signal",
                    result="success",
                    extra_fields={
                        "client_id": self.client_id,
                        "exchange": "cleanup_exchange",
                        "msg": "Cleanup signal successfully published to all nodes",
                    },
                )
            else:
                log_action(
                    action="send_cleanup_signal",
                    result="fail",
                    level=logging.ERROR,
                    extra_fields={
                        "client_id": self.client_id,
                        "exchange": "cleanup_exchange",
                        "msg": "Failed to publish cleanup signal",
                    },
                )
            self.cleanup_publisher.close()
            log_action(
                action="closing_cleanup_publisher",
                result="success",
                extra_fields={
                    "client_id": self.client_id,
                    "msg": "Cleanup publisher closed",
                },
            )

        except Exception as e:
            log_action(
                action="send_cleanup_signal",
                result="fail",
                level=logging.ERROR,
                error=e,
                extra_fields={
                    "client_id": self.client_id,
                    "msg": "Exception while sending cleanup signal",
                },
            )
