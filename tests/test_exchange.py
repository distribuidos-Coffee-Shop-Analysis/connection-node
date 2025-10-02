import queue as pyqueue
import time
import pytest
from .conftest import ConsumerRunner, unique_name, wait_until
import queue as pyqueue

def test_exchange_1_to_1_routing(make_exchange):
    """
    Caso: Exchange 1→1
    - ruteo por routing key
    - Hay dos consumidores con routing keys distintas: rk.a y rk.b
    - El productor publica con rk.a
    - Debe recibir SOLO el consumidor suscripto a rk.a
    """
    exchange_name = unique_name("ex-1to1")
    producer = make_exchange(exchange_name, ["rk.a"])  # publica con 'rk.a'
    consumer_a = make_exchange(exchange_name, ["rk.a"])
    consumer_b = make_exchange(exchange_name, ["rk.b"])

    ra = rb = None
    try:
        ra = ConsumerRunner(consumer_a)
        rb = ConsumerRunner(consumer_b)
        ra.start()
        rb.start()

        time.sleep(0.1)

        payload = "hola-ex-1a1"
        producer.send(payload)

        # Esperamos que A reciba
        assert wait_until(
            lambda: not ra.messages.empty(), timeout=5.0
        ), "El consumidor A no recibió el mensaje"

        got_a = ra.messages.get_nowait()
        assert got_a == payload

        # B NO debería recibir nada
        with pytest.raises(pyqueue.Empty):
            rb.messages.get_nowait()

    finally:
        if ra:
            ra.stop()
            ra.join()
        if rb:
            rb.stop()
            rb.join()
        consumer_a.close()
        consumer_b.close()
        producer.close()
        producer.delete()


def test_exchange_1_to_N_pubsub(make_exchange):
    """
    Caso: Exchange 1→N
    - Dos consumidores suscriptos a la MISMA routing key pero cada uno con su propia cola.
    - Ambos deben recibir el mismo mensaje.
    """
    exchange_name = unique_name("ex-1toN")
    producer = make_exchange(exchange_name, ["rk.pub"])
    consumer1 = make_exchange(exchange_name, ["rk.pub"])
    consumer2 = make_exchange(exchange_name, ["rk.pub"])

    r1 = r2 = None
    try:
        r1 = ConsumerRunner(consumer1)
        r2 = ConsumerRunner(consumer2)
        r1.start()
        r2.start()

        time.sleep(0.1)

        payload = "hola-ex-1aN"
        producer.send(payload)

        # Ambos deben recibir 1 mensaje
        assert wait_until(
            lambda: not r1.messages.empty(), timeout=5.0
        ), "El consumidor 1 no recibió el mensaje"
        assert wait_until(
            lambda: not r2.messages.empty(), timeout=5.0
        ), "El consumidor 2 no recibió el mensaje"

        got1 = r1.messages.get_nowait()
        got2 = r2.messages.get_nowait()
        assert (
            got1 == payload and got2 == payload
        ), "No llegó el mismo mensaje a ambos consumidores"

        # No deberían llegar duplicados extra
        with pytest.raises(pyqueue.Empty):
            r1.messages.get_nowait()
        with pytest.raises(pyqueue.Empty):
            r2.messages.get_nowait()

    finally:
        if r1:
            r1.stop()
            r1.join()
        if r2:
            r2.stop()
            r2.join()
        consumer1.close()
        consumer2.close()
        producer.close()
        producer.delete()
