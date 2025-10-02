import socket
import logging
from multiprocessing import Process
from protocol.protocol import (
    read_packet_from,
    MESSAGE_TYPE_BATCH,
    serialize_batch_message,
)
from protocol.messages import DatasetType
from middleware.publisher import RabbitMQPublisher
from common.utils import (
    TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
    USERS_EXCHANGE,
    STORES_EXCHANGE,
    MENU_ITEMS_EXCHANGE,
    get_joiner_partition,
    log_action,
)


class SocketReader(Process):
    """Thread class that handles reading from client socket"""

    def __init__(
        self,
        client_socket,
        client_address,
        server_callbacks,
        shutdown_event,
        middleware_config,
    ):
        """
        Initialize the socket reader process

        Args:
            client_socket: The client socket to read from
            client_address: The client address tuple (ip, port)
            server_callbacks: Dictionary with callback functions to server methods
            shutdown_event: Multiprocessing event to signal shutdown
            middleware_config: RabbitMQ configuration to create own connection
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.server_callbacks = server_callbacks
        self.shutdown_event = shutdown_event

        # Create our own publisher with its own connection
        self.publisher = RabbitMQPublisher(middleware_config)

        # Store users joiners count for user partitioning
        self.users_joiners_count = middleware_config.users_joiners_count

        # Generate client ID for logging
        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"
        self.name = f"SocketReader-{self.client_id}"

        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main socket reading loop"""
        log_action(action="socket_reader_start", result="success")

        while not self.shutdown_event.is_set():
            try:
                # Read message from client socket
                message = read_packet_from(self.client_socket)

                if message is None:  # Client disconnected
                    log_action(action="client_disconnect", result="detected")
                    break

                # Process the message based on type
                session_completed = self._process_message(message)
                if session_completed:
                    break

            except socket.error as e:
                if not self.shutdown_event.is_set():  # Only log if not shutting down
                    log_action(
                        action="socket_error",
                        result="fail",
                        level=logging.ERROR,
                        error=e,
                    )
                break
            except ValueError as e:
                log_action(
                    action="message_processing",
                    result="fail",
                    level=logging.ERROR,
                    error=e,
                )
            except Exception as e:
                log_action(
                    action="socket_read", result="fail", level=logging.ERROR, error=e
                )
                break

        # Clean up publisher
        self.publisher.close()
        log_action(action="socket_reader_end", result="success")

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
            log_action(
                action="unknown_message_type",
                result="fail",
                level=logging.ERROR,
                error=f"Unknown message type: {message.type}",
            )
            return False

    def _handle_batch(self, batch):
        """Handle batch message processing - publish directly to RabbitMQ"""
        try:
            # Handle transactions and transaction items
            if batch.dataset_type in [
                DatasetType.TRANSACTIONS,
                DatasetType.TRANSACTION_ITEMS,
            ]:
                # Serialize the batch message
                serialized_message = serialize_batch_message(
                    dataset_type=batch.dataset_type,
                    batch_index=batch.batch_index,
                    records=batch.records,
                    eof=batch.eof,
                )

                # Publish directly using our publisher
                success = self.publisher.publish(
                    routing_key="",
                    message=serialized_message,
                    exchange_name=TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                )

                if not success:
                    log_action(
                        action="publish_batch",
                        result="fail",
                        level=logging.ERROR,
                        extra_fields={
                            "dataset_type": batch.dataset_type,
                            "exchange": TRANSACTIONS_AND_TRANSACTION_ITEMS_EXCHANGE,
                        },
                    )

            elif batch.dataset_type == DatasetType.USERS:
                self._handle_users_batch(batch)

            elif batch.dataset_type == DatasetType.STORES:
                self._handle_stores_batch(batch)

            elif batch.dataset_type == DatasetType.MENU_ITEMS:
                self._handle_menu_items_batch(batch)

            else:
                self.logger.debug(
                    "action: handle_batch_message | result: skipped | reason: unhandled_dataset_type | dataset_type: %s",
                    batch.dataset_type,
                )

        except Exception as e:
            log_action(
                action="handle_batch", result="fail", level=logging.ERROR, error=e
            )

    def _publish_users_to_partition(
        self, partition, dataset_type, batch_index, records, eof
    ):
        """
        Helper method to publish a users batch to a specific partition.

        Args:
            partition: The partition number (1-based)
            dataset_type: The dataset type
            batch_index: The batch index
            records: List of user records (can be empty)
            eof: Whether this is the final batch

        Returns:
            bool: True if publish succeeded, False otherwise
        """
        routing_key = f"joiner.{partition}.users"

        serialized_message = serialize_batch_message(
            dataset_type=dataset_type,
            batch_index=batch_index,
            records=records,
            eof=eof,
        )

        success = self.publisher.publish(
            routing_key=routing_key,
            message=serialized_message,
            exchange_name=USERS_EXCHANGE,
        )

        if not success:
            log_action(
                action="publish_users_partition",
                result="fail",
                level=logging.ERROR,
                extra_fields={
                    "dataset_type": dataset_type,
                    "exchange": USERS_EXCHANGE,
                    "routing_key": routing_key,
                    "partition": partition,
                    "record_count": len(records),
                    "eof": eof,
                },
            )

        return success

    def _handle_users_batch(self, batch):
        """
        Handle users batch with partitioning across multiple joiner nodes.

        For each record, applies hash function on user_id to determine partition.
        Creates a list (batch) for each routing key, then publishes each partitioned
        batch separately instead of sending all records together.

        This ensures the same user_id always goes to the same joiner node,
        which is critical for distributed join operations.

        EOF handling:
        - When eof=True: Send batches to ALL partitions (with data or empty) + eof=True
        - When eof=False: Only send to partitions that have data in this batch
        """
        try:
            # Partition records by user_id hash
            partitioned_records = {}
            for record in batch.records:
                partition = get_joiner_partition(
                    record.user_id, self.users_joiners_count
                )
                if partition not in partitioned_records:
                    partitioned_records[partition] = []
                partitioned_records[partition].append(record)

            if batch.eof:
                # EOF batch: Send to ALL partitions to ensure they all receive EOF signal
                for partition in range(1, self.users_joiners_count + 1):
                    records = partitioned_records.get(
                        partition, []
                    )  # Empty list if no data
                    self._publish_users_to_partition(
                        partition=partition,
                        dataset_type=batch.dataset_type,
                        batch_index=batch.batch_index,
                        records=records,
                        eof=True,
                    )
            else:
                # Normal batch: Only send to partitions with records
                for partition, records in partitioned_records.items():
                    self._publish_users_to_partition(
                        partition=partition,
                        dataset_type=batch.dataset_type,
                        batch_index=batch.batch_index,
                        records=records,
                        eof=False,
                    )

        except Exception as e:
            log_action(
                action="handle_users_batch",
                result="fail",
                level=logging.ERROR,
                error=e,
            )

    def _handle_stores_batch(self, batch):
        """
        Handle stores batch by publishing to a single routing key.

        Multiple joiners can consume from the same queue, so we only need
        to publish once with a fixed routing key.
        """
        try:
            serialized_message = serialize_batch_message(
                dataset_type=batch.dataset_type,
                batch_index=batch.batch_index,
                records=batch.records,
                eof=batch.eof,
            )

            routing_key = ""

            success = self.publisher.publish(
                routing_key=routing_key,
                message=serialized_message,
                exchange_name=STORES_EXCHANGE,
            )

            if not success:
                log_action(
                    action="publish_batch",
                    result="fail",
                    level=logging.ERROR,
                    extra_fields={
                        "dataset_type": batch.dataset_type,
                        "exchange": STORES_EXCHANGE,
                        "routing_key": routing_key,
                    },
                )

        except Exception as e:
            log_action(
                action="handle_stores_batch",
                result="fail",
                level=logging.ERROR,
                error=e,
            )

    def _handle_menu_items_batch(self, batch):
        """
        Handle menu items batch by publishing to a single routing key.

        Multiple joiners can consume from the same queue, so we only need
        to publish once with a fixed routing key.
        """
        try:
            serialized_message = serialize_batch_message(
                dataset_type=batch.dataset_type,
                batch_index=batch.batch_index,
                records=batch.records,
                eof=batch.eof,
            )

            routing_key = ""

            success = self.publisher.publish(
                routing_key=routing_key,
                message=serialized_message,
                exchange_name=MENU_ITEMS_EXCHANGE,
            )

            if not success:
                log_action(
                    action="publish_batch",
                    result="fail",
                    level=logging.ERROR,
                    extra_fields={
                        "dataset_type": batch.dataset_type,
                        "exchange": MENU_ITEMS_EXCHANGE,
                        "routing_key": routing_key,
                    },
                )

        except Exception as e:
            log_action(
                action="handle_menu_items_batch",
                result="fail",
                level=logging.ERROR,
                error=e,
            )
