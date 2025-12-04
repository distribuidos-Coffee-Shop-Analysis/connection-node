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

    # Input dataset types that we expect to receive EOFs for
    INPUT_DATASET_TYPES = {
        DatasetType.MENU_ITEMS,
        DatasetType.STORES,
        DatasetType.TRANSACTION_ITEMS,
        DatasetType.TRANSACTIONS,
        DatasetType.USERS,
    }

    def __init__(
        self,
        client_socket,
        client_address,
        server_callbacks,
        shutdown_event,
        middleware_config,
        client_id_queue=None,
        client_id_queue_handler=None,
        all_eofs_received_queue=None,
    ):
        """
        Initialize the socket reader process

        Args:
            client_socket: The client socket to read from
            client_address: The client address tuple (ip, port)
            server_callbacks: Dictionary with callback functions to server methods
            shutdown_event: Multiprocessing event to signal shutdown
            middleware_config: RabbitMQ configuration to create own connection
            client_id_queue: Queue to send client UUID back to Listener
            client_id_queue_handler: Queue to send client UUID to ClientHandler for session persistence
            all_eofs_received_queue: Queue to signal ClientHandler when all input EOFs are received
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.server_callbacks = server_callbacks
        self.shutdown_event = shutdown_event
        self.client_id_queue = client_id_queue
        self.client_id_queue_handler = client_id_queue_handler
        self.all_eofs_received_queue = all_eofs_received_queue

        # Create our own publisher with its own connection
        self.publisher = RabbitMQPublisher(middleware_config)

        # Store users joiners count for user partitioning
        self.users_joiners_count = middleware_config.users_joiners_count

        # Client ID will be set from first message (client sends UUID)
        self.client_id = None
        self.client_id_sent_to_listener = False
        self.client_id_sent_to_handler = False

        self.received_eofs = set()

        # Temporary ID for logging before UUID is received
        temp_id = f"temp_{self.client_address[0]}:{self.client_address[1]}"
        self.name = f"SocketReader-{temp_id}"

        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main socket reading loop"""
        log_action(action="socket_reader_start", result="success")

        while not self.shutdown_event.is_set():
            try:
                # Read message from client socket
                message = read_packet_from(self.client_socket)

                if message is None:
                    log_action(
                        action="client_disconnect",
                        result="detected",
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "msg": "Client disconnected (read returned None)",
                        },
                    )
                    break

                # Process the message based on type
                session_completed = self._process_message(message)
                if session_completed:
                    break

            except socket.timeout as e:
                if not self.shutdown_event.is_set():
                    log_action(
                        action="socket_timeout",
                        result="fail",
                        level=logging.WARNING,
                        error=e,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "shutdown_requested": False,
                            "msg": "Socket timeout: no data received, connection may be dead",
                        },
                    )
                    break
                else:
                    log_action(
                        action="socket_timeout",
                        result="ignored",
                        level=logging.DEBUG,
                        error=e,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "shutdown_requested": True,
                            "msg": "Socket timeout during shutdown, ignoring",
                        },
                    )
                    break
            except socket.error as e:
                if not self.shutdown_event.is_set():  # Only log if not shutting down
                    log_action(
                        action="socket_error",
                        result="fail",
                        level=logging.ERROR,
                        error=e,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "shutdown_requested": False,
                            "msg": "Socket error detected, terminating SocketReader",
                        },
                    )
                else:
                    log_action(
                        action="socket_error",
                        result="ignored",
                        level=logging.DEBUG,
                        error=e,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "shutdown_requested": True,
                            "msg": "Socket error during shutdown, ignoring",
                        },
                    )
                break
            except RuntimeError as e:
                # This is thrown by _read_exact when client closes the socket
                # recv() returns empty bytes when the client disconnects
                if not self.shutdown_event.is_set():
                    log_action(
                        action="client_disconnect",
                        result="detected",
                        level=logging.WARNING,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "error": str(e),
                            "msg": "Client closed socket connection",
                        },
                    )
                else:
                    log_action(
                        action="client_disconnect",
                        result="during_shutdown",
                        level=logging.DEBUG,
                        extra_fields={
                            "client_id": (
                                self.client_id if self.client_id else "unknown"
                            ),
                            "error": str(e),
                            "msg": "Client disconnected during shutdown",
                        },
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
        log_action(
            action="socket_reader_end",
            result="success",
            extra_fields={
                "client_id": self.client_id if self.client_id else "unknown",
                "shutdown_requested": self.shutdown_event.is_set(),
                "msg": "SocketReader process terminating",
            },
        )

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
            # Extract client_id from message if not yet set
            # CRITICAL: Do this BEFORE publishing to RabbitMQ to avoid race condition
            if self.client_id is None and batch.client_id:
                self.client_id = batch.client_id
                self.name = f"SocketReader-{self.client_id}"
                log_action(
                    action="client_id_extracted",
                    result="success",
                    extra_fields={"client_uuid": self.client_id},
                )

                # Send UUID to Listener via queue so it can register the client
                # IMPORTANT: This must happen BEFORE publishing to avoid replies arriving
                # before the queue is registered with the UUID
                if self.client_id_queue:
                    try:
                        self.client_id_queue.put_nowait(self.client_id)
                        self.client_id_sent_to_listener = True
                        log_action(
                            action="client_uuid_sent",
                            result="success",
                            extra_fields={"client_uuid": self.client_id},
                        )
                        # Give a tiny bit of time for registration to complete
                        # This helps avoid race condition with fast replies
                        import time

                        time.sleep(0.01)  # 10ms should be enough
                    except Exception as e:
                        log_action(
                            action="client_uuid_sent",
                            result="fail",
                            level=logging.ERROR,
                            error=e,
                        )

                # Also send UUID to ClientHandler for session persistence
                if self.client_id_queue_handler and not self.client_id_sent_to_handler:
                    try:
                        self.client_id_queue_handler.put_nowait(self.client_id)
                        self.client_id_sent_to_handler = True
                        log_action(
                            action="client_uuid_sent_to_handler",
                            result="success",
                            extra_fields={"client_uuid": self.client_id},
                        )
                    except Exception as e:
                        log_action(
                            action="client_uuid_sent_to_handler",
                            result="fail",
                            level=logging.ERROR,
                            error=e,
                        )

            # Track EOFs for input datasets
            if batch.eof and batch.dataset_type in self.INPUT_DATASET_TYPES:
                self._track_eof_received(batch.dataset_type)

            # Handle transactions and transaction items
            if batch.dataset_type in [
                DatasetType.TRANSACTIONS,
                DatasetType.TRANSACTION_ITEMS,
            ]:
                # Serialize the batch message with client_id from the batch
                serialized_message = serialize_batch_message(
                    dataset_type=batch.dataset_type,
                    batch_index=batch.batch_index,
                    records=batch.records,
                    eof=batch.eof,
                    client_id=batch.client_id,  # Use client_id from message
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
            client_id=self.client_id,  # Use UUID from client
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
                client_id=batch.client_id,  # Use client_id from message
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
                client_id=batch.client_id,  # Use client_id from message
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

    def _track_eof_received(self, dataset_type):
        """
        Track that an EOF was received for a dataset type.
        When all input dataset EOFs are received, notify ClientHandler.
        """
        self.received_eofs.add(dataset_type)

        log_action(
            action="eof_received",
            result="success",
            extra_fields={
                "client_id": self.client_id if self.client_id else "unknown",
                "dataset_type": dataset_type,
                "received_eofs_count": len(self.received_eofs),
                "expected_eofs_count": len(self.INPUT_DATASET_TYPES),
                "msg": f"EOF received for dataset {dataset_type}",
            },
        )

        # Check if all input EOFs have been received
        if self.received_eofs == self.INPUT_DATASET_TYPES:
            log_action(
                action="all_input_eofs_received",
                result="success",
                extra_fields={
                    "client_id": self.client_id if self.client_id else "unknown",
                    "msg": "All input dataset EOFs received - client finished sending data",
                },
            )
            # Notify ClientHandler that all input EOFs were received
            if self.all_eofs_received_queue:
                try:
                    self.all_eofs_received_queue.put_nowait(True)
                except Exception as e:
                    log_action(
                        action="notify_all_eofs_received",
                        result="fail",
                        level=logging.ERROR,
                        error=e,
                    )
