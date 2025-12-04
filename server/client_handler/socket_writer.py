import socket
import logging
import threading
import queue
from protocol.messages import BatchMessage, QueryReplyMessage, DatasetType
from protocol.protocol import (
    _send_exact,
    send_batch_message,
    serialize_batch_message_for_client,
)


class SocketWriter(threading.Thread):
    """Thread class that handles writing to client socket"""

    def __init__(
        self,
        client_socket,
        client_address,
        client_queue,
        shutdown_event,
        socket_broken_event=None,
        all_responses_sent_event=None,
        users_joiners_count=1,
    ):
        """
        Initialize the socket writer thread

        Args:
            client_socket: The client socket to write to
            client_address: The client address tuple (ip, port)
            client_queue: Queue to receive messages to send to client
            shutdown_event: Threading event to signal shutdown
            socket_broken_event: Threading event to signal when socket is broken
            all_responses_sent_event: Threading event to signal when all response EOFs sent
            users_joiners_count: Number of Q4 EOFs to expect (one per joiner)
        """
        super().__init__(daemon=True)
        self.client_socket = client_socket
        self.client_address = client_address
        self.client_queue = client_queue
        self.shutdown_event = shutdown_event
        self.socket_broken_event = socket_broken_event
        self.all_responses_sent_event = all_responses_sent_event

        # Track response EOFs: Q1=1, Q2=1, Q3=1, Q4=N (where N=users_joiners_count)
        self.users_joiners_count = users_joiners_count
        self.q4_eofs_received = 0
        self.q1_eof_sent = False
        self.q2_eof_sent = False
        self.q3_eof_sent = False

        # Generate client ID for logging
        self.client_id = f"client_{self.client_address[0]}_{self.client_address[1]}"
        self.name = f"SocketWriter-{self.client_id}"

        self.logger = logging.getLogger(__name__)

    def run(self):
        """Main socket writing loop - reads from queue and writes to socket"""
        self._log_action("socket_writer_start", "success")

        while not self.shutdown_event.is_set():
            try:
                # Use timeout to periodically check shutdown_event
                # This prevents the thread from hanging indefinitely
                message = self.client_queue.get(timeout=1)

                if message is None:  # Shutdown signal
                    self._log_action("socket_writer_shutdown", "received_signal")
                    break

                # Send reply to client socket
                self._send_reply_to_client(message)

            except queue.Empty:
                # Timeout reached, loop back to check shutdown_event
                continue
            except Exception as e:
                self._log_action("socket_write", "fail", level=logging.ERROR, error=e)
                break

        self._log_action("socket_writer_end", "success")

    def _send_reply_to_client(self, message):
        """Send reply message to client socket

        Note: The message from RabbitMQ includes client_id, but we need to remove it
        before sending to the client (client protocol doesn't include client_id).
        Also handles Q2 dual format correctly.

        Handles broken pipes gracefully - if the socket is broken, we log and drop
        the message without propagating the exception. This allows the RepliesHandler
        to continue and ACK the message to remove it from RabbitMQ queue.
        """
        try:
            reply_message = QueryReplyMessage.from_data(message)

            serialized_data = serialize_batch_message_for_client(
                reply_message.dataset_type,
                reply_message.batch_index,
                reply_message.records,
                reply_message.eof,
            )

            length = len(serialized_data)
            length_bytes = length.to_bytes(4, byteorder="big")

            _send_exact(self.client_socket, length_bytes)
            _send_exact(self.client_socket, serialized_data)

            # Track EOFs for each query type
            if reply_message.eof:
                self._track_response_eof(reply_message.dataset_type)

        except socket.timeout as e:
            self.logger.warning(
                "action: send_reply | result: socket_timeout | client: %s | msg: write timeout, connection may be dead | error: %s",
                self.client_id,
                str(e),
            )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            # Socket is broken - client disconnected unexpectedly
            # Signal to ClientHandler that socket is broken so it can send cleanup
            self.logger.warning(
                "action: send_reply | result: socket_broken | client: %s | msg: client disconnected, signaling broken socket | error: %s",
                self.client_id,
                str(e),
            )
            # Signal that socket is broken - ClientHandler will handle cleanup
            if self.socket_broken_event:
                self.socket_broken_event.set()
            # Raise to exit the write loop - we can't send any more messages
            raise

        except Exception as e:
            # Other unexpected errors
            self.logger.error(
                "action: send_reply_error | client: %s | error: %s",
                self.client_id,
                str(e),
            )
            self._log_action("send_reply", "fail", level=logging.ERROR, error=e)

    def _track_response_eof(self, dataset_type):
        """Track EOF for a response query type and check if all responses are complete"""
        if dataset_type == DatasetType.Q1:
            self.q1_eof_sent = True
            self._log_action(
                "response_eof_sent", "success", extra_fields={"query": "Q1"}
            )
        elif dataset_type == DatasetType.Q2:
            self.q2_eof_sent = True
            self._log_action(
                "response_eof_sent", "success", extra_fields={"query": "Q2"}
            )
        elif dataset_type == DatasetType.Q3:
            self.q3_eof_sent = True
            self._log_action(
                "response_eof_sent", "success", extra_fields={"query": "Q3"}
            )
        elif dataset_type == DatasetType.Q4:
            self.q4_eofs_received += 1
            self._log_action(
                "response_eof_sent",
                "success",
                extra_fields={
                    "query": "Q4",
                    "count": self.q4_eofs_received,
                    "expected": self.users_joiners_count,
                },
            )

        # Check if all responses are complete
        if self._all_responses_sent():
            self._log_action(
                "all_responses_sent",
                "success",
                extra_fields={"msg": "All query responses sent to client"},
            )
            if self.all_responses_sent_event:
                self.all_responses_sent_event.set()

    def _all_responses_sent(self):
        """Check if all response EOFs have been sent"""
        return (
            self.q1_eof_sent
            and self.q2_eof_sent
            and self.q3_eof_sent
            and self.q4_eofs_received >= self.users_joiners_count
        )

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
