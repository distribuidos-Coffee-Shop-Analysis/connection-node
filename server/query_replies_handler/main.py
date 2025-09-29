import threading
import logging
import queue
from typing import Dict
from common.shutdown_monitor import ShutdownMonitor
from .replies_handler import RepliesHandler


class QueryRepliesHandler(threading.Thread):
    """Main orchestrator for handling replies from RabbitMQ and distributing them to client queues"""

    def __init__(self, middleware, shutdown_queue, middleware_config):
        super().__init__(name="QueryRepliesHandler")
        self.middleware = middleware
        self.shutdown_queue = shutdown_queue
        self.logger = logging.getLogger(__name__)

        # Hash map: client_id -> client_queue
        self.client_queues: Dict[str, queue.Queue] = {}
        self.clients_lock = threading.Lock()

        # Thread instances
        self.shutdown_monitor = None
        self.message_consumer = None

        self._middleware_config = middleware_config

        self.daemon = True

    def _handle_shutdown_signal(self):
        """Callback function called by shutdown monitor when shutdown is requested"""
        try:
            self.message_consumer.request_shutdown()
        except Exception as e:
            self.logger.error(
                "action: handle_shutdown | result: fail | msg: error stopping consumption | error: %s",
                e,
            )

    def run(self):
        """Main thread loop - orchestrates shutdown monitor and message consumer"""
        try:
            self.logger.info(
                "action: query_replies_handler_start | result: success | msg: QueryRepliesHandler started"
            )

            # Create and start the shutdown monitor thread
            self.shutdown_monitor = ShutdownMonitor(
                shutdown_queue=self.shutdown_queue,
                shutdown_callback=self._handle_shutdown_signal,
            )
            self.shutdown_monitor.start()

            # Create and start the message consumer thread
            self.message_consumer = RepliesHandler(
                get_client_queue_callback=self.get_queue_for_client,
                middleware_config=self._middleware_config,
            )
            self.message_consumer.start()

            self.logger.info(
                "action: threads_started | result: success | msg: shutdown monitor and message consumer threads started"
            )

        except Exception as e:
            self.logger.error(
                "action: query_replies_handler_start | result: fail | msg: error starting QueryRepliesHandler | error: %s",
                e,
            )

        finally:
            self._wait_for_threads()

    def _wait_for_threads(self):
        """Wait for both shutdown monitor and message consumer threads to complete"""
        try:
            if self.shutdown_monitor.is_alive():
                self.shutdown_monitor.join()

            if self.message_consumer.is_alive():
                self.message_consumer.join()

            self.logger.info(
                "action: threads_joined | result: success | msg: all threads joined successfully"
            )

        except Exception as e:
            self.logger.error(
                "action: wait_for_threads | result: fail | msg: error waiting for threads | error: %s",
                e,
            )

    def add_client(self, client_id: str, client_queue: queue.Queue):
        """Add a client and its queue to the distribution map"""
        with self.clients_lock:
            self.client_queues[client_id] = client_queue
            self.logger.info(
                "action: add_client | result: success | msg: client added to reply handler | client_id: %s",
                client_id,
            )

    def remove_client(self, client_id: str):
        """Remove a client from the distribution map"""
        with self.clients_lock:
            if client_id in self.client_queues:
                del self.client_queues[client_id]
                self.logger.info(
                    "action: remove_client | result: success | msg: client removed from reply handler | client_id: %s",
                    client_id,
                )

    # Callback function to get the queue for a client
    def get_queue_for_client(self, client_id: str):
        """Get the queue associated with a client_id"""
        with self.clients_lock:
            return self.client_queues.get(client_id, None)
