# src/minimal_middleware.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Callable, List, Optional
import threading
import logging

import pika
from pika.exceptions import (
    AMQPConnectionError,
    AMQPError,
    ChannelClosedByBroker,
    ConnectionClosedByBroker,
)

logging.basicConfig(level=logging.DEBUG)


# Exceptions

class MessageMiddlewareMessageError(Exception):
    pass


class MessageMiddlewareDisconnectedError(Exception):
    pass


class MessageMiddlewareCloseError(Exception):
    pass


class MessageMiddlewareDeleteError(Exception):
    pass


# Interfaz

class MessageMiddleware(ABC):
    @abstractmethod
    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        pass

    @abstractmethod
    def stop_consuming(self) -> None:
        pass

    @abstractmethod
    def send(self, message) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass


# Utilidad base Pika


class _BasePika(MessageMiddleware):
    """
    Implementa conexión básica a RabbitMQ con pika.BlockingConnection.
    - start_consuming es bloqueante; stop_consuming es thread-safe.
    - Todos los métodos hacen lo mínimo para pasar las pruebas.
    """

    def __init__(self, host: str):
        self._host = host
        self._conn: Optional[pika.BlockingConnection] = None
        self._ch: Optional[pika.adapters.blocking_connection.BlockingChannel] = None
        self._lock = threading.RLock()
        self._is_consuming = False

    def _ensure_channel(self) -> None:
        with self._lock:
            if self._conn and self._conn.is_open and self._ch and self._ch.is_open:
                return
            try:
                params = pika.ConnectionParameters(host=self._host)
                self._conn = pika.BlockingConnection(params)
                self._ch = self._conn.channel()
                self._ch.basic_qos(prefetch_count=1)
            except AMQPConnectionError as e:
                raise MessageMiddlewareDisconnectedError(str(e)) from e
            except Exception as e:
                raise MessageMiddlewareMessageError(str(e)) from e

    def _to_bytes(self, message) -> bytes:
        if isinstance(message, (bytes, bytearray)):
            return bytes(message)
        return str(message).encode("utf-8")

    def _start_blocking_loop(self) -> None:
        """
        Arranca el loop bloqueante de pika.
        Mapea desconexiones a MessageMiddlewareDisconnectedError.
        """
        self._is_consuming = True
        try:
            self._ch.start_consuming()
        except (AMQPConnectionError, ConnectionClosedByBroker) as e:
            # Mapeamos pérdida de conexión/desconexión del broker
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except AMQPError as e:
            raise MessageMiddlewareMessageError(str(e)) from e
        finally:
            self._is_consuming = False

    def stop_consuming(self) -> None:
        """
        Detiene el loop de start_consuming() de forma thread-safe e idempotente.
        Si no se está consumiendo, no hace nada.
        """
        with self._lock:
            if not (
                self._conn and self._conn.is_open and self._ch and self._ch.is_open
            ):
                return
            if not self._is_consuming:
                return

            def _stop():
                try:
                    self._ch.stop_consuming()
                except Exception:
                    pass

            self._conn.add_callback_threadsafe(_stop)

    def close(self) -> None:
        """
        Cierra canal + conexión localmente.
        """
        with self._lock:
            try:
                if self._ch and self._ch.is_open:
                    try:
                        self._ch.close()
                    except Exception:
                        pass
                if self._conn and self._conn.is_open:
                    try:
                        self._conn.close()
                    except Exception:
                        pass
            except Exception as e:
                raise MessageMiddlewareCloseError(str(e)) from e
            finally:
                self._ch = None
                self._conn = None


# Cola


class MessageMiddlewareQueue(_BasePika):
    """
    Working Queue:
    - send(): publica al exchange default '' usando routing_key = queue_name.
    - start_consuming(): consume de la cola y ACKea después de invocar el callback.
    - delete(): borra la cola abriendo una conexión efímera.
    """

    def __init__(self, host: str, queue_name: str):
        super().__init__(host)
        self._queue = queue_name

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        logging.debug(f"[Queue:{self._queue}] Starting consumer...")
        self._ensure_channel()
        result = self._ch.queue_declare(
            queue=self._queue, durable=True, auto_delete=False
        )
        logging.debug(
            f"[Queue:{self._queue}] Queue declared, consumers: {result.method.consumer_count}, messages: {result.method.message_count}"
        )

        def _cb(ch, method, properties, body: bytes):
            logging.debug(f"[Queue:{self._queue}] Received message: {body}")
            try:
                on_message_callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logging.debug(f"[Queue:{self._queue}] Message processed and ack'd")
            except Exception as e:
                logging.error(f"[Queue:{self._queue}] Callback error: {e}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self._ch.basic_consume(
            queue=self._queue, on_message_callback=_cb, auto_ack=False
        )
        logging.debug(f"[Queue:{self._queue}] Consumer registered, starting loop...")
        self._start_blocking_loop()

    def send(self, message) -> None:
        logging.debug(f"[Queue:{self._queue}] Sending message: {message}")
        self._ensure_channel()
        self._ch.queue_declare(queue=self._queue, durable=True, auto_delete=False)
        body = self._to_bytes(message)
        try:
            self._ch.basic_publish(exchange="", routing_key=self._queue, body=body)
            logging.debug(f"[Queue:{self._queue}] Message sent successfully")
        except AMQPConnectionError as e:
            logging.error(f"[Queue:{self._queue}] Connection error: {e}")
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except AMQPError as e:
            logging.error(f"[Queue:{self._queue}] AMQP error: {e}")
            raise MessageMiddlewareMessageError(str(e)) from e

    def delete(self) -> None:
        """
        Borra la cola incluso si esta instancia ya fue cerrada.
        Para eso abre una conexión efímera.
        """
        try:
            params = pika.ConnectionParameters(host=self._host)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            try:
                ch.queue_delete(
                    queue=self._queue
                )
            except ChannelClosedByBroker as e:
                if getattr(e, "reply_code", None) != 404:
                    raise
            finally:
                try:
                    ch.close()
                except Exception:
                    pass
                conn.close()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e)) from e


# Exchange


class MessageMiddlewareExchange(_BasePika):
    """
    Exchange directo:
    - El productor publica con la primera routing key de route_keys.
    - Cada consumidor crea una cola efímera y la bindea a todas las routing keys.
    - delete(): borra el exchange abriendo una conexión efímera.
    """

    def __init__(self, host: str, exchange_name: str, route_keys: List[str]):
        super().__init__(host)
        if not route_keys:
            raise ValueError("route_keys no puede ser vacío")
        self._exchange = exchange_name
        self._route_keys = list(route_keys)
        self._exchange_type = "direct"

    def _declare_exchange(self):
        self._ch.exchange_declare(
            exchange=self._exchange,
            exchange_type=self._exchange_type,
            durable=True,
            auto_delete=False,
        )

    def start_consuming(self, on_message_callback: Callable[[bytes], None]) -> None:
        logging.debug(
            f"[Exchange:{self._exchange}] Starting consumer for routing_keys: {self._route_keys}"
        )
        self._ensure_channel()
        self._declare_exchange()

        result = self._ch.queue_declare(queue="", exclusive=True, auto_delete=True)
        qname = result.method.queue
        logging.debug(f"[Exchange:{self._exchange}] Created exclusive queue: {qname}")

        for rk in self._route_keys:
            self._ch.queue_bind(exchange=self._exchange, queue=qname, routing_key=rk)
            logging.debug(
                f"[Exchange:{self._exchange}] Queue '{qname}' bound with routing_key '{rk}'"
            )

        def _cb(ch, method, properties, body: bytes):
            logging.debug(
                f"[Exchange:{self._exchange}] Message received on queue '{qname}', routing_key '{method.routing_key}'"
            )
            try:
                on_message_callback(body)
                ch.basic_ack(delivery_tag=method.delivery_tag)
                logging.debug(
                    f"[Exchange:{self._exchange}] Message processed and acknowledged"
                )
            except Exception as e:
                logging.error(
                    f"[Exchange:{self._exchange}] Error processing message: {e}"
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

        self._ch.basic_consume(queue=qname, on_message_callback=_cb, auto_ack=False)
        logging.debug(
            f"[Exchange:{self._exchange}] Consumer setup complete, starting to consume..."
        )
        self._start_blocking_loop()

    def send(self, message) -> None:
        logging.debug(
            f"[Exchange:{self._exchange}] Sending message with routing_key '{self._route_keys[0]}': {message}"
        )
        self._ensure_channel()
        self._declare_exchange()
        rk = self._route_keys[0]
        body = self._to_bytes(message)
        try:
            self._ch.basic_publish(exchange=self._exchange, routing_key=rk, body=body)
            logging.debug(
                f"[Exchange:{self._exchange}] Message sent successfully with routing_key '{rk}'"
            )
        except AMQPConnectionError as e:
            logging.error(f"[Exchange:{self._exchange}] Connection error: {e}")
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except AMQPError as e:
            logging.error(f"[Exchange:{self._exchange}] AMQP error: {e}")
            raise MessageMiddlewareMessageError(str(e)) from e

    def delete(self) -> None:
        """
        Borra el exchange incluso si esta instancia ya fue cerrada.
        Usa if_unused=False para forzar borrado.
        """
        try:
            params = pika.ConnectionParameters(host=self._host)
            conn = pika.BlockingConnection(params)
            ch = conn.channel()
            try:
                ch.exchange_delete(exchange=self._exchange, if_unused=False)
            except ChannelClosedByBroker as e:
                if getattr(e, "reply_code", None) != 404:
                    raise
            finally:
                try:
                    ch.close()
                except Exception:
                    pass
                conn.close()
        except AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareDeleteError(str(e)) from e
