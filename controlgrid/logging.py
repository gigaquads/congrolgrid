import sys

from threading import Thread
from multiprocessing import Queue
from appyratus.logging import ConsoleLoggerInterface


class LoggerDaemon:
    def __init__(self):
        self._queue = Queue()
        self._is_running = False

    def debug(self, message: str):
        self._queue.put(("debug", message))

    def info(self, message: str):
        self._queue.put(("info", message))

    def warn(self, message: str):
        self._queue.put(("warn", message))

    def error(self, message: str):
        self._queue.put(("error", message))

    def exception(self, message: str):
        self._queue.put(("error", message))

    def start(self) -> "LoggerDaemon":
        if not self._is_running:
            thread = Thread(target=self.run, daemon=True)
            thread.start()
        return self

    def run(self):
        log = ConsoleLoggerInterface("controlgrid")
        while True:
            level, msg = self._queue.get()
            log_func = getattr(log, level)
            log_func(msg)
