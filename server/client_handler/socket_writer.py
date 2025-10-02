import socket
import logging
import threading
import queue
from protocol.messages import BatchMessage
from protocol.protocol import _send_exact, send_batch_message


class SocketWriter(threading.Thread):
    """Thread class that handles writing to client socket"""

    def __init__(self, client_socket, client_address, client_queue, shutdown_event):
        """
        Initialize the socket writer thread

        Args:
            client_socket: The client socket to write to
            client_address: The client address tuple (ip, port)
            client_queue: Queue to receive messages to send to client
            shutdown_event: Threading event to signal shutdown
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.client_queue = client_queue
        self.shutdown_event = shutdown_event

        # Generate client ID for logging
        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"
        self.name = f"SocketWriter-{self.client_id}"

        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main socket writing loop - reads from queue and writes to socket"""
        self._log_action("socket_writer_start", "success")

        while not self.shutdown_event.is_set():
            try:
                # Block until message arrives from RepliesHandler
                # or a signal shutdown
                message = self.client_queue.get()

                if message is None:  # Shutdown signal
                    self._log_action("socket_writer_shutdown", "received_signal")
                    break

                # Send reply to client socket
                self._send_reply_to_client(message)

                # Mark task as done
                # self.client_queue.task_done()

                self._log_action("send_reply", "success")

            except Exception as e:
                self._log_action("socket_write", "fail", level=logging.ERROR, error=e)
                break

        self._log_action("socket_writer_end", "success")

    def _send_reply_to_client(self, message):
        """Send reply message to client socket"""
        try:
            # STEP 1: Log raw message received
            self.logger.info(
                "action: receive_raw_message | client: %s | message_length: %s | message_type_byte: %s",
                self.client_id,
                len(message),
                message[0] if len(message) > 0 else "EMPTY",
            )

            # STEP 2: Parse the message
            batch_message = BatchMessage.from_data(message)

            # STEP 3: Log parsed batch message details
            self.logger.info(
                "action: parsed_batch_message | client: %s | dataset_type: %s | batch_index: %s | eof: %s | total_records: %s",
                self.client_id,
                batch_message.dataset_type,
                batch_message.batch_index,
                batch_message.eof,
                len(batch_message.records),
            )

            # STEP 4: Log EVERY single record (this is critical for debugging)
            for i, record in enumerate(batch_message.records):
                record_data = (
                    record.serialize() if hasattr(record, "serialize") else str(record)
                )
                self.logger.info(
                    "action: record_detail | client: %s | record_index: %s | record_data: %s | record_type: %s",
                    self.client_id,
                    i,
                    record_data,
                    type(record).__name__,
                )

                # Extra logging for Q2 records
                if batch_message.dataset_type == 9:  # Q2
                    self.logger.info(
                        "action: q2_record_breakdown | client: %s | index: %s | year_month: %s | item_id: %s",
                        self.client_id,
                        i,
                        getattr(record, "year_month", "MISSING"),
                        getattr(
                            record,
                            "item_identifier",
                            getattr(record, "item_name", "MISSING"),
                        ),
                    )

            # STEP 5: Log what we're about to send to client
            self.logger.info(
                "action: about_to_send_to_client | client: %s | sending_records_count: %s | original_message_length: %s",
                self.client_id,
                len(batch_message.records),
                len(message),
            )

            # STEP 6: Send the ORIGINAL message bytes to client (not re-serialized)
            length = len(message)
            length_bytes = length.to_bytes(4, byteorder="big")

            self.logger.info(
                "action: sending_length_prefix | client: %s | length_to_send: %s",
                self.client_id,
                length,
            )

            _send_exact(self.client_socket, length_bytes)
            _send_exact(self.client_socket, message)

            # STEP 7: Confirm send completed
            self.logger.info(
                "action: send_completed | client: %s | records_sent: %s | bytes_sent: %s",
                self.client_id,
                len(batch_message.records),
                len(message) + 4,  # +4 for length prefix
            )

        except Exception as e:
            self.logger.error(
                "action: send_reply_error | client: %s | error: %s",
                self.client_id,
                str(e),
            )
            self._log_action("send_reply", "fail", level=logging.ERROR, error=e)

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
