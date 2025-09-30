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
                    logging.info(
                        "action: client_connect | result: success | msg: new client connected | address: %s",
                        client_address,
                    )

                    # Create a queue for this client to receive replies
                    client_queue = multiprocessing.Queue(maxsize=100)

                    # Create client_id
                    client_id = f"client_{client_address[0]}_{client_address[1]}"

                    # Register client with QueryRepliesHandler via server callback
                    if "add_client" in self._server_callbacks:
                        self._server_callbacks["add_client"](client_id, client_queue)

                    shutdown_queue = multiprocessing.Queue()
                    # Create a new ClientHandler for each connection
                    client_handler = ClientHandler(
                        client_socket=client_sock,
                        server_callbacks=self._server_callbacks,
                        remove_from_server_callback=self._remove_handler,
                        client_queue=client_queue,
                        middleware_config=self.middleware_config,
                        shutdown_queue=shutdown_queue,
                    )

                    # Track the handler
                    with self._handlers_lock:
                        self._active_handlers[client_id] = (
                            client_handler,
                            shutdown_queue,
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
        logging.info(
            "action: listener_shutdown | result: success | msg: listener shutdown completed"
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
            )  # (client_id, (handler, shutdown_queue))

        # Send shutdown signal and wait for each handler
        for client_id, (handler, shutdown_queue) in handlers_to_wait:
            if handler.is_alive():
                try:
                    # Notify the process to shutdown
                    shutdown_queue.put(None)
                    handler.join()
                except Exception as e:
                    logging.error(
                        "action: shutdown | result: fail | msg: error waiting for handler | error: %s",
                        e,
                    )

    # Callback function to remove a handler when it finishes
    def _remove_handler(self, client_id):
        """Remove a finished handler and its shutdown queue from the active handlers"""
        try:
            # Remove from QueryRepliesHandler via server callback
            if "remove_client" in self._server_callbacks:
                self._server_callbacks["remove_client"](client_id)

            # Remove from active handlers dict
            with self._handlers_lock:
                if client_id in self._active_handlers:
                    del self._active_handlers[client_id]

            logging.info(
                "action: remove_handler | result: success | msg: client handler removed | address: %s",
                client_id,
            )
        except Exception as e:
            logging.error(
                "action: remove_handler | result: fail | msg: error removing handler | error: %s",
                e,
            )
