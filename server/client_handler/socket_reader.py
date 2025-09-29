import socket
import logging
import threading
from protocol.protocol import (
    read_packet_from,
    MESSAGE_TYPE_BATCH,
    serialize_batch_message,
)
from protocol.messages import DatasetType
from middleware.publisher import RabbitMQPublisher
from common.utils import TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE


class SocketReader(threading.Thread):
    """Thread class that handles reading from client socket"""

    def __init__(
        self,
        client_socket,
        client_address,
        server_callbacks,
        shutdown_event,
        rabbitmq_connection,
    ):
        """
        Initialize the socket reader thread

        Args:
            client_socket: The client socket to read from
            client_address: The client address tuple (ip, port)
            server_callbacks: Dictionary with callback functions to server methods
            shutdown_event: Threading event to signal shutdown
            rabbitmq_connection: RabbitMQ connection to create publisher
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.server_callbacks = server_callbacks
        self.shutdown_event = shutdown_event

        # Create our own publisher with its own channel
        self.publisher = RabbitMQPublisher(rabbitmq_connection)

        # Generate client ID for logging
        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"
        self.name = f"SocketReader-{self.client_id}"

        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main socket reading loop"""
        self._log_action("socket_reader_start", "success")

        while not self.shutdown_event.is_set():
            try:
                # Read message from client socket
                message = read_packet_from(self.client_socket)

                if message is None:  # Client disconnected
                    self._log_action("client_disconnect", "detected")
                    break

                # Process the message based on type
                session_completed = self._process_message(message)
                if session_completed:
                    break

                self._log_action(
                    "process_message",
                    "success",
                    extra_fields={"type": message.type},
                )

            except socket.error as e:
                if not self.shutdown_event.is_set():  # Only log if not shutting down
                    self._log_action(
                        "socket_error", "fail", level=logging.ERROR, error=e
                    )
                break
            except ValueError as e:
                self._log_action(
                    "message_processing", "fail", level=logging.ERROR, error=e
                )
            except Exception as e:
                self._log_action("socket_read", "fail", level=logging.ERROR, error=e)
                break

        # Clean up publisher
        self.publisher.close()
        self._log_action("socket_reader_end", "success")

    def _process_message(self, message):
        """
        Process a received message and return whether the session is complete.

        Returns:
            bool: True if the client session is complete, False otherwise
        """
        if message.type == MESSAGE_TYPE_BATCH:
            self._handle_batch(message)
            return False
        else:
            self._log_action(
                "unknown_message_type",
                "fail",
                level=logging.ERROR,
                error=f"Unknown message type: {message.type}",
            )
            return False

    def _handle_batch(self, batch):
        """Handle batch message processing - publish directly to RabbitMQ"""
        try:
            self.logger.debug(
                "action: handle_batch_message | result: in_progress | dataset_type: %s | records: %d | eof: %s",
                batch.dataset_type,
                len(batch.records),
                batch.eof,
            )

            # Handle transactions and transaction items
            if batch.dataset_type in [
                DatasetType.TRANSACTIONS,
                DatasetType.TRANSACTION_ITEMS,
            ]:
                # Serialize the batch message
                serialized_message = serialize_batch_message(
                    dataset_type=batch.dataset_type,
                    records=batch.records,
                    eof=batch.eof,
                )

                # Publish directly using our publisher
                success = self.publisher.publish(
                    routing_key="",
                    message=serialized_message,
                    exchange_name=TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                )

                if success:
                    self._log_action(
                        "publish_batch",
                        "success",
                        extra_fields={
                            "dataset_type": batch.dataset_type,
                            "exchange": TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                            "records": len(batch.records),
                        },
                    )
                else:
                    self._log_action(
                        "publish_batch",
                        "fail",
                        level=logging.ERROR,
                        extra_fields={
                            "dataset_type": batch.dataset_type,
                            "exchange": TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                        },
                    )
            else:
                self.logger.debug(
                    "action: handle_batch_message | result: skipped | reason: not_transaction_dataset | dataset_type: %s",
                    batch.dataset_type,
                )

        except Exception as e:
            self._log_action("handle_batch", "fail", level=logging.ERROR, error=e)

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
            f"client: {self.client_id}",
        ]

        if error:
            log_parts.append(f"error: {error}")

        if extra_fields:
            for key, value in extra_fields.items():
                log_parts.append(f"{key}: {value}")

        log_message = " | ".join(log_parts)
        self.logger.log(level, log_message)
