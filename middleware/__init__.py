"""
RabbitMQ interfaces for clean thread-based messaging.

This package provides lightweight interfaces for RabbitMQ operations:
- RabbitMQPublisher: For publishing messages (each thread gets its own channel)
- RabbitMQConsumer: For consuming messages (each thread gets its own channel)
- Middleware: For connection management and setup (exchanges/queues declaration)

Usage:
    # Setup (once per application)
    middleware = Middleware(config)
    middleware.start()  # declares exchanges/queues using init channel

    # In each thread that needs to publish:
    publisher = RabbitMQPublisher(middleware.get_connection())
    publisher.publish(routing_key, message, exchange_name)

    # In each thread that needs to consume:
    consumer = RabbitMQConsumer(middleware.get_connection(), queue_name, callback)
    consumer.start_consuming()
"""

from .middleware import Middleware
from .publisher import RabbitMQPublisher
from .consumer import RabbitMQConsumer

__all__ = ["Middleware", "RabbitMQPublisher", "RabbitMQConsumer"]
