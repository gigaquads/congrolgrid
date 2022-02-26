import pickle
import traceback

from queue import Queue, Empty
from threading import Thread
from typing import Any, Callable, Iterator, Optional
from appyratus.logging import ConsoleLoggerInterface

from appyratus.utils.type_utils import TypeUtils

import zmq


class Service(Thread):
    def __init__(
        self,
        addr: str,
        context: Optional[zmq.Context] = None,
        log: ConsoleLoggerInterface = None,
    ) -> None:
        super().__init__(daemon=True)
        self._addr = addr
        self._context = context or zmq.Context()
        self._log = log
        self._routes = {}

    def run(self):
        socket = self._context.socket(zmq.REP)
        socket.bind(self._addr)

        while True:
            try:
                # route request to appropriate handler
                # to generate a response
                response = None
                request = pickle.loads(socket.recv())
                for rule, callback in self._routes.items():
                    if rule(request):
                        response = callback(request)
                # pickle and send response
                response = pickle.dumps(response)
                socket.send(response)
            except:
                if self._log is not None:
                    self._log.exception(
                        "failed to read from registration socket"
                    )
                else:
                    traceback.print_exc()

    def add_route(self, rule: Callable, callback: Callable):
        self._routes[rule] = callback


class Client:
    def __init__(
        self,
        addr: str,
        context: Optional[zmq.Context] = None,
        log: ConsoleLoggerInterface = None,
    ) -> None:
        self._addr = addr
        self._context = context or zmq.Context()
        self._socket = self._context.socket(zmq.REQ)
        self._socket.connect(self._addr)
        self._log = log

    def request(self, data: Any) -> Any:
        request = pickle.dumps(data)
        self._socket.send(request)
        return pickle.loads(self._socket.recv())


class Channel:
    """
    ZMQ pubsub channel. Provides interface for both sides.

    ## Publisher:
    ```python
    channel = Channel(addr)
    channel.publish({'foo': 'bar'})
    ```

    ## Subscriber:
    ```python
    channel = Channel(addr)
    subscription = channel.subscribe()

    while True:
        obj = subscription.receive()
        print(obj)
    ```

    """

    def __init__(
        self, addr: str, context: Optional[zmq.Context] = None
    ) -> None:
        self._addr = addr
        self._zmq_context = context or zmq.Context()
        self._pub_socket: zmq.Socket = None
        self._sub_socket: zmq.Socket = None
        self._recv_queue = Queue()
        self._recv_thread: Thread = None

    def __repr__(self) -> str:
        return f"{TypeUtils.get_class_name(self)}({self._addr})"

    @property
    def addr(self) -> str:
        return self._addr

    def serialize(self, data: Any) -> bytes:
        return pickle.dumps(data)

    def deserialize(self, data: bytes) -> Any:
        return pickle.loads(data)

    def publish(self, data: Any):
        # lazy bind publisher socket
        if self._pub_socket is None:
            self._pub_socket = self._zmq_context.socket(zmq.PUB)
            self._pub_socket.bind(self._addr)

        # serialize and send
        payload = self.serialize(data)
        self._pub_socket.send(payload)

    def subscribe(self, callback: Optional[Callable] = None) -> "Subscription":
        socket = self._zmq_context.socket(zmq.SUB)
        socket.connect(self._addr)
        socket.setsockopt_string(zmq.SUBSCRIBE, "")

        sub = Subscription(socket, self.deserialize, callback=callback)
        sub.start()

        return sub


class Subscription(Thread):
    def __init__(
        self,
        socket: zmq.Socket,
        deserialize: Callable,
        callback: Optional[Callable] = None,
        unpack_callback_kwargs: bool = False,
    ):
        super().__init__(daemon=True)
        self._socket = socket
        self._deserialize = deserialize
        self._queue = Queue()
        self._callback = callback
        self._unpack_kwargs = unpack_callback_kwargs

    def __iter__(self) -> Iterator:
        def iterator():
            count = self._queue.qsize()
            if count > 0:
                for i in range(count):
                    try:
                        yield self._queue.get(timeout=0.5)
                    except Empty:
                        break

        return iterator()

    def run(self):
        while True:
            try:
                data = self._socket.recv()
                obj = self._deserialize(data)
                self._queue.put(obj)
                if self._callback is not None:
                    if self._unpack_kwargs:
                        self._callback(**obj)
                    else:
                        self._callback(obj)

            except:
                traceback.print_exc()

    def receive(self, timeout=None, default=None) -> Any:
        try:
            return self._queue.get(timeout=timeout)
        except Empty:
            return default


# Example Usage:
if __name__ == "__main__":
    from sys import argv
    from time import sleep
    from datetime import datetime

    channel = Channel("ipc:///tmp/example.pipe")
    side = argv[1]

    if side == "publisher":
        while True:
            sleep(0.5)
            print("publishing...")
            channel.publish(
                {"timestamp": datetime.now(), "message": "Hello, subscriber!"}
            )
    elif side == "subscriber":
        subscription = channel.subscribe()
        while True:
            data = subscription.receive()
            print(data)
