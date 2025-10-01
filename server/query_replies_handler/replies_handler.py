import threading
import logging
from middleware.consumer import RabbitMQConsumer
from protocol.messages import BatchMessage, DatasetType


class RepliesHandler(threading.Thread):
    """Handles RabbitMQ message consumption and processing"""

    def __init__(self, get_client_queue_callback, middleware_config):
        super().__init__(name="QueryRepliesHandler-MessageConsumer", daemon=True)
        self.get_client_queue_callback = get_client_queue_callback
        self.logger = logging.getLogger(__name__)

        # Create our own consumer for replies_queue
        self.consumer = RabbitMQConsumer(
            middleware_config=middleware_config,
            queue_name="replies_queue",
            callback_function=self._message_callback,
        )

    def request_shutdown(self):
        """Request shutdown of message consumption"""
        try:
            self.consumer.stop_consuming()  # This will unblock start_consuming()
            self.consumer.close()  # Close the consumer's channel
        except Exception as e:
            self.logger.error(
                "action: request_shutdown | result: fail | msg: error stopping consumption | error: %s",
                e,
            )

    def _message_callback(self, ch, method, properties, body):
        """Callback function for processing messages from RabbitMQ"""
        try:
            # Delegate message processing to the message processor
            self.process_reply_message(body, properties)
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            self.logger.error(
                "action: message_callback | result: fail | msg: error processing reply message | error: %s",
                e,
            )
            # Reject and requeue on error
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def run(self):
        """Main message consumption loop"""
        try:
            self.logger.info(
                "action: message_consumer_start | result: success | msg: message consumer started"
            )

            # Start consuming using our consumer interface (blocking call)
            self.consumer.start_consuming()

            self.logger.info(
                "action: message_consumer_stop | result: success | msg: message consumer stopped"
            )

        except Exception as e:
            self.logger.error(
                "action: message_consumer | result: fail | msg: message consumer error | error: %s",
                e,
            )

    def process_reply_message(self, message_body, properties=None):
        """Process a reply message and route it to the appropriate client"""
        try:
            # Extract client_id from the message first
            client_id = self._extract_client_id_from_message(message_body, properties)

            if not client_id:
                self.logger.error(
                    "action: process_reply_message | result: fail | msg: could not extract client_id from message"
                )
                return

            # Extract the batch message data (remove client_id prefix if present)
            batch_message_data = self._extract_batch_message_data(
                message_body, client_id
            )

            # Parse the batch message from the replies_queue
            batch_message = BatchMessage.from_data(batch_message_data)

            # Get the client queue using the callback
            client_queue = self.get_client_queue_callback(client_id)

            if not client_queue:
                self.logger.error(
                    "action: process_reply_message | result: fail | msg: client queue not found | client_id: %s",
                    client_id,
                )
                return

            # Prepare the message for the socket writer
            reply_message = {
                "dataset_type": batch_message.dataset_type,
                "batch_index": batch_message.batch_index,
                "records": batch_message.records,
                "eof": batch_message.eof,
            }

            # Put the message in the client's queue
            client_queue.put(reply_message)

            self.logger.info(
                "action: process_reply_message | result: success | msg: reply routed to client | client_id: %s | dataset_type: %s | batch_index: %s | records_count: %s",
                client_id,
                batch_message.dataset_type,
                batch_message.batch_index,
                len(batch_message.records),
            )

        except Exception as e:
            self.logger.error(
                "action: process_reply_message | result: fail | msg: error processing reply message | error: %s",
                e,
            )
            raise

    def _extract_client_id_from_message(self, message_body, properties=None):
        """
        Extract client_id from the message body or properties.

        This method implements multiple strategies to extract the client_id:
        1. From message properties headers
        2. From message body prefix
        3. From batch message content

        Returns the client_id if found, None otherwise.
        """
        # Strategy 1: Try to extract from properties headers
        if properties and hasattr(properties, "headers") and properties.headers:
            client_id = properties.headers.get("client_id")
            if client_id:
                self.logger.debug(f"Extracted client_id from headers: {client_id}")
                return client_id

        # Strategy 2: Try to extract from message body prefix
        # The message format might be: [client_id_length][client_id][batch_message_data]
        try:
            if len(message_body) > 4:
                # Try to read client_id length (4 bytes)
                client_id_length = int.from_bytes(message_body[:4], byteorder="big")
                if client_id_length > 0 and client_id_length < len(message_body) - 4:
                    client_id = message_body[4 : 4 + client_id_length].decode("utf-8")
                    self.logger.debug(
                        f"Extracted client_id from message prefix: {client_id}"
                    )
                    return client_id
        except Exception as e:
            self.logger.debug(f"Failed to extract client_id from message prefix: {e}")

        # Strategy 3: Try to extract from batch message content
        # This would require parsing the batch message and looking for client_id
        # in the records or other fields

        # For now, return None to indicate we need to implement this
        self.logger.warning(
            "Could not extract client_id from message - need to implement based on Go node message format"
        )
        return None

    def _extract_batch_message_data(self, message_body, client_id):
        """
        Extract the batch message data from the message body.

        If the client_id was extracted from the message prefix, we need to
        remove that prefix to get the actual batch message data.

        Args:
            message_body: The raw message body
            client_id: The extracted client_id (if any)

        Returns:
            The batch message data (either the original message_body or
            the message_body with client_id prefix removed)
        """
        # If client_id was extracted from headers, the message_body should be the batch message
        if len(message_body) > 4:
            try:
                # Check if the message starts with client_id length prefix
                client_id_length = int.from_bytes(message_body[:4], byteorder="big")
                if client_id_length > 0 and client_id_length < len(message_body) - 4:
                    # Extract the actual client_id from the message
                    extracted_client_id = message_body[4 : 4 + client_id_length].decode(
                        "utf-8"
                    )
                    if extracted_client_id == client_id:
                        # Remove the client_id prefix and return the batch message data
                        return message_body[4 + client_id_length :]
            except Exception as e:
                self.logger.debug(f"Failed to extract batch message data: {e}")

        # If no client_id prefix was found, return the original message body
        return message_body
