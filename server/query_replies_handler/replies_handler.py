import threading
import logging


class RepliesHandler(threading.Thread):
    """Handles RabbitMQ message consumption and processing"""

    def __init__(self, middleware, get_client_queue_callback):
        super().__init__(name="QueryRepliesHandler-MessageConsumer", daemon=True)
        self.middleware = middleware
        self.get_client_queue_callback = get_client_queue_callback
        self.logger = logging.getLogger(__name__)

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

            # Call "message_callback" for each incoming message in the replies queue
            # The replies_queue should already be declared by middleware during startup
            success = self.middleware.basic_consume(
                "replies_queue", self._message_callback
            )

            if not success:
                self.logger.error(
                    "action: basic_consume | result: fail | msg: failed to start consuming from replies queue"
                )
                return

            # Once a shutdown signal is received, the middleware.stop_consuming() will be called
            # and this blocking call will return
            # If not shutdown signal is received, this will block indefinitely
            self.middleware.start_consuming()

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

        # aca deberiamos:
        # 1) parsear el mensaje, obtener el client_id
        # 2) con el client_id buscar en el hash (self.client_queues) y obtener la cola del cliente
        # 3) poner el mensaje en la cola del cliente para que lo consuma y lo envie
        # no lo hice prq todavia no se bien la estructura del mensaje q recibimos

        client_id = "xd"
        self.get_client_queue_callback(client_id)
