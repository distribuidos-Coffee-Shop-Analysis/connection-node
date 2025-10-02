import os
import uuid
import time
import queue as pyqueue
import threading
import importlib
import pytest


@pytest.fixture(scope="session")
def mw_module():
    return importlib.import_module("tests.middleware")


@pytest.fixture(scope="session")
def host():
    return os.environ.get("MW_HOST", "localhost")


# --------- Utilidades comunes para los tests ----------


class ConsumerRunner:
    """
    Envuelve un MessageMiddleware (Queue o Exchange) para consumir en un hilo aparte,
    encolando los mensajes recibidos en self.messages (queue.Queue).
    """

    def __init__(self, mw_obj):
        self.mw = mw_obj
        self.messages = pyqueue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._started = threading.Event()
        self._stopped = threading.Event()

    def _run(self):
        def _cb(msg):
            if isinstance(msg, (bytes, bytearray)):
                msg = msg.decode()
            self.messages.put(msg)

        self._started.set()
        try:
            self.mw.start_consuming(_cb)
        finally:
            self._stopped.set()

    def start(self):
        self._thread.start()
        if not self._started.wait(timeout=2.0):
            raise RuntimeError("El consumidor no pudo inicializarse a tiempo.")

    def stop(self):
        self.mw.stop_consuming()
        self._stopped.wait(timeout=2.0)

    def join(self):
        self._thread.join(timeout=2.0)


def unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def wait_until(predicate, timeout: float, check_interval: float = 0.05) -> bool:
    """
    Evalúa predicate() cada check_interval hasta timeout.
    Retorna True si se cumplió, False si expiró.
    """
    end = time.time() + timeout
    while time.time() < end:
        if predicate():
            return True
        time.sleep(check_interval)
    return False


@pytest.fixture
def make_queue(mw_module, host):
    def _make(qname: str):
        return mw_module.MessageMiddlewareQueue(host, qname)

    return _make


@pytest.fixture
def make_exchange(mw_module, host):
    def _make(exchange_name: str, route_keys):
        return mw_module.MessageMiddlewareExchange(host, exchange_name, route_keys)

    return _make
