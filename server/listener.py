import socket
from threading import Thread
import logging
import threading
import multiprocessing
from typing import Dict

from .client_handler import ClientHandler


class Listener(Thread):
    def __init__(
        self, server_socket, server_callbacks, shutdown_event, middleware_config
    ):
        """
        Initialize the listener

        Args:
            server_socket: The server socket to listen on
            server_callbacks: Dictionary with callback functions to server methods:
                - add_client: callback to register client with QueryRepliesHandler
                - remove_client: callback to unregister client from QueryRepliesHandler
            shutdown_event: Threading event to signal shutdown from server
            middleware_config: RabbitMQ configuration to pass to client handlers
        """
        super().__init__()  # Properly initialize the Thread base class
        self._server_socket = server_socket
        self._server_callbacks = server_callbacks
        self.shutdown_event = shutdown_event
        self.middleware_config = middleware_config

        # Track active client handlers
        self._active_handlers: Dict[str, tuple] = (
            {}
        )  # client_id -> (client_handler, shutdown_queue)
        self._handlers_lock = threading.Lock()

    def run(self):
        """Main listener loop with graceful shutdown support and concurrent connection handling"""
        logging.info(
            "action: listener_start | result: success | msg: listener started, waiting for connections"
        )

        while not self.shutdown_event.is_set():
            try:
                client_sock, client_address = self._server_socket.accept()
                if client_sock and not self.shutdown_event.is_set():
                    SOCKET_TIMEOUT = 15.0
                    client_sock.settimeout(SOCKET_TIMEOUT)
                    logging.info(
                        "action: client_connect | result: success | msg: new client connected | address: %s | timeout: %s",
                        client_address,
                        SOCKET_TIMEOUT,
                    )

                    # Create a queue for this client to receive replies
                    client_queue = multiprocessing.Queue(maxsize=100000)

                    # Create a queue for the client to send its UUID back to us
                    client_id_queue = multiprocessing.Queue(maxsize=1)

                    # DON'T register yet - wait for UUID from client's first message
                    # We'll register in a separate thread that waits for the UUID

                    shutdown_queue = multiprocessing.Queue()
                    # Create a new ClientHandler for each connection
                    client_handler = ClientHandler(
                        client_socket=client_sock,
                        server_callbacks=self._server_callbacks,
                        remove_from_server_callback=self._remove_handler,
                        client_queue=client_queue,
                        middleware_config=self.middleware_config,
                        shutdown_queue=shutdown_queue,
                        client_id_queue=client_id_queue,
                    )

                    # Start a thread to wait for UUID and register the client
                    registration_thread = threading.Thread(
                        target=self._register_client_with_uuid,
                        args=(client_queue, client_id_queue),
                        daemon=True,
                    )
                    registration_thread.start()

                    # Track the handler for shutdown (we'll update with UUID later)
                    # We use the client_address as a temporary key since we don't have UUID yet
                    temp_key = f"{client_address[0]}:{client_address[1]}"
                    with self._handlers_lock:
                        self._active_handlers[temp_key] = (
                            client_handler,
                            shutdown_queue,
                            client_id_queue,  # Store the queue to get UUID later
                        )

                    # Start the process
                    client_handler.start()

            except socket.error as e:
                if not self.shutdown_event.is_set():
                    logging.error(
                        "action: client_accept | result: fail | msg: socket error accepting connections | error: %s",
                        e,
                    )
            except Exception as e:
                logging.error(
                    "action: listener_loop | result: fail | msg: error in listener loop | error: %s",
                    e,
                )

        # Wait for all handlers to complete and final cleanup
        self._wait_for_handlers()

    def _register_client_with_uuid(self, client_queue, client_id_queue):
        """Wait for UUID from client's first message and register with QueryRepliesHandler

        This thread waits for the SocketReader to extract the UUID from the first message
        and send it back via client_id_queue. Once received, we register the client with
        the QueryRepliesHandler so replies can be routed correctly.
        """
        try:
            # Wait for UUID from SocketReader (with timeout)
            client_uuid = client_id_queue.get(timeout=10)

            if client_uuid:
                logging.info(
                    "action: register_client_uuid | result: success | uuid: %s",
                    client_uuid,
                )

                # Register client with QueryRepliesHandler using the UUID
                if "add_client" in self._server_callbacks:
                    self._server_callbacks["add_client"](client_uuid, client_queue)

        except Exception as e:
            logging.error(
                "action: register_client_uuid | result: fail | error: %s",
                str(e),
            )

    def _wait_for_handlers(self):
        """Wait for all active client handlers to complete"""
        logging.info(
            "action: shutdown | result: in_progress | msg: waiting for handlers to complete"
        )

        # Other threads could try to modify _active_handlers
        # so we need to create a copy of the handlers to
        # avoid iteration issues during shutdown and let them
        # finish naturally
        with self._handlers_lock:
            handlers_to_wait = list(
                self._active_handlers.items()
            )  # (temp_key, (handler, shutdown_queue, client_id_queue))

        # Send shutdown signal and wait for each handler
        for temp_key, handler_info in handlers_to_wait:
            handler = handler_info[0]
            shutdown_queue = handler_info[1]

            if handler and handler.is_alive():
                try:
                    # Notify the process to shutdown
                    shutdown_queue.put(None)
                    handler.join()
                except Exception as e:
                    logging.error(
                        "action: shutdown | result: fail | msg: error waiting for handler | temp_key: %s | error: %s",
                        temp_key,
                        e,
                    )

    # Callback function to remove a handler when it finishes
    def _remove_handler(self, client_address_tuple):
        """Remove a finished handler and its shutdown queue from the active handlers

        Args:
            client_address_tuple: The (ip, port) tuple of the client
        """
        try:
            # Try to get the UUID from the client_id_queue if available
            temp_key = f"{client_address_tuple[0]}:{client_address_tuple[1]}"
            client_uuid = None

            with self._handlers_lock:
                if temp_key in self._active_handlers:
                    handler_info = self._active_handlers[temp_key]
                    if len(handler_info) >= 3:
                        client_id_queue = handler_info[2]
                        try:
                            # Try to get UUID without blocking
                            client_uuid = client_id_queue.get_nowait()
                        except:
                            pass
                    del self._active_handlers[temp_key]

            # Remove from QueryRepliesHandler if we have the UUID
            if client_uuid and "remove_client" in self._server_callbacks:
                self._server_callbacks["remove_client"](client_uuid)
                logging.info(
                    "action: remove_handler | result: success | client_uuid: %s | address: %s",
                    client_uuid,
                    client_address_tuple,
                )
            else:
                logging.info(
                    "action: remove_handler | result: success | msg: no uuid | address: %s",
                    client_address_tuple,
                )

        except Exception as e:
            logging.error(
                "action: remove_handler | result: fail | error: %s",
                e,
            )
