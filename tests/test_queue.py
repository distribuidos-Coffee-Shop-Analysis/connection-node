import queue as pyqueue
import pytest
from .conftest import ConsumerRunner, unique_name, wait_until


def test_working_queue_1_to_1(make_queue):
    """
    Caso: Working Queue 1→1
    - Un productor envía a una cola.
    - Un solo consumidor recibe el mensaje.
    """
    queue_name = "q-" + __name__.split(".")[-1]

    producer = make_queue(queue_name)
    consumer = make_queue(queue_name)
    runner = None

    try:
        runner = ConsumerRunner(consumer)
        runner.start()

        payload = "hola-1a1"
        producer.send(payload)

        # Esperamos a que llegue exactamente 1 mensaje
        assert wait_until(
            lambda: not runner.messages.empty(), timeout=5.0
        ), "No llegó ningún mensaje al consumidor"

        got = runner.messages.get_nowait()
        assert got == payload

        # Aseguramos que no llegue un duplicado extra
        with pytest.raises(pyqueue.Empty):
            runner.messages.get_nowait()

    finally:
        if runner:
            runner.stop()
            runner.join()
        consumer.close()
        producer.close()
        producer.delete()


def test_working_queue_1_to_N(make_queue):
    """
    Caso: Working Queue 1→N
    - Varios consumidores sobre la misma cola
    - Se envían N mensajes.
    - Entre los 2 consumidores, se deben consumir exactamente una vez cada mensaje
    """
    queue_name = unique_name("q-1toN")
    producer = make_queue(queue_name)
    consumer1 = make_queue(queue_name)
    consumer2 = make_queue(queue_name)

    r1 = r2 = None
    try:
        r1 = ConsumerRunner(consumer1)
        r2 = ConsumerRunner(consumer2)
        r1.start()
        r2.start()

        N = 20
        msgs = [f"m-{i}" for i in range(N)]
        for m in msgs:
            producer.send(m)

        # Esperamos hasta que la suma de recibidos sea N
        def total_received():
            return r1.messages.qsize() + r2.messages.qsize()

        assert wait_until(
            lambda: total_received() == N, timeout=10.0
        ), f"Se recibieron {total_received()} de {N} mensajes"

        # Cada mensaje aparece exactamente 1 vez
        seen = []
        while not r1.messages.empty():
            seen.append(r1.messages.get_nowait())
        while not r2.messages.empty():
            seen.append(r2.messages.get_nowait())

        assert sorted(seen) == sorted(
            msgs
        ), "Mensajes perdidos o duplicados en la distribución 1→N de la cola"

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
