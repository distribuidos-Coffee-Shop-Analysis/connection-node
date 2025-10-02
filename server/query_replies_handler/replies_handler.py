import threading
import logging
from middleware.consumer import RabbitMQConsumer
from protocol.messages import QueryReplyMessage, DatasetType

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
            self.process_reply_message(body)
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

    def process_reply_message(self, message_body):
        """Process a reply message and route it to the appropriate client"""
        try:
            reply_message = self._parse_batch_message(message_body)
            if not reply_message:
                self.logger.error(
                    "action: process_reply_message | result: fail | step: parse | msg: failed to parse reply message"
                )
                return

            self._route_message_to_clients(message_body, reply_message.dataset_type)

        except Exception as e:
            self.logger.error(
                "action: process_reply_message | result: fail | error: %s", str(e)
            )

    def _parse_batch_message(self, message_body):
        """Parse raw message body into QueryReplyMessage"""
        try:
            if not message_body:
                self.logger.error(
                    "action: parse_batch_message | result: fail | error: empty message body"
                )
                return None

            # Parse the message using the new QueryReplyMessage class
            reply_message = QueryReplyMessage.from_data(message_body)

            if not reply_message:
                self.logger.error(
                    "action: parse_batch_message | result: fail | error: QueryReplyMessage.from_data returned None"
                )
                return None

            # Validate it's a query response
            if reply_message.dataset_type not in [
                DatasetType.Q1,
                DatasetType.Q2,
                DatasetType.Q3,
                DatasetType.Q4,
            ]:
                self.logger.warning(
                    "action: parse_batch_message | result: skip | dataset_type: %s | msg: not a query response",
                    reply_message.dataset_type,
                )
                return None

            return reply_message

        except Exception as e:
            self.logger.error(
                "action: parse_batch_message | result: fail | error: %s", str(e)
            )
            return None

    def _route_message_to_clients(self, message_body, dataset_type):
        """Route raw message bytes to all connected clients"""

        queues = self.get_client_queue_callback()

        if not queues:
            self.logger.warning(
                "action: route_message | result: no_clients | dataset_type: %s",
                dataset_type,
            )
            return

        success_count = 0
        failed_count = 0

        for client_id, client_queue in queues.items():
            try:
                if client_queue is None:
                    self.logger.warning(
                        "action: route_message | result: skip | client_id: %s | msg: queue is None",
                        client_id,
                    )
                    failed_count += 1
                    continue

                # Put raw bytes into queue
                client_queue.put_nowait(message_body)
                success_count += 1

            except Exception as e:
                self.logger.error(
                    "action: route_failed | client_id: %s | error: %s",
                    client_id,
                    str(e),
                )
                failed_count += 1
