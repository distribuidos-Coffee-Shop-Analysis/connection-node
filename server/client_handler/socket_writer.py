import socket
import logging
import threading
import queue
from protocol.messages import BatchMessage, QueryReplyMessage
from protocol.protocol import _send_exact, send_batch_message, serialize_batch_message_for_client


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


            except Exception as e:
                self._log_action("socket_write", "fail", level=logging.ERROR, error=e)
                break

        self._log_action("socket_writer_end", "success")

    def _send_reply_to_client(self, message):
        """Send reply message to client socket
        
        Note: The message from RabbitMQ includes client_id, but we need to remove it
        before sending to the client (client protocol doesn't include client_id).
        Also handles Q2 dual format correctly.
        """
        try:
            reply_message = QueryReplyMessage.from_data(message)
            
            serialized_data = serialize_batch_message_for_client(
                reply_message.dataset_type,
                reply_message.batch_index,
                reply_message.records,
                reply_message.eof
            )
            
            length = len(serialized_data)
            length_bytes = length.to_bytes(4, byteorder="big")
            
            _send_exact(self.client_socket, length_bytes)
            _send_exact(self.client_socket, serialized_data)

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
