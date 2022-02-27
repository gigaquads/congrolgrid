import os
import pexpect

from uuid import uuid4
from queue import Empty, Queue
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Coroutine

from appyratus.utils.time_utils import TimeUtils

from controlgrid.processing.job import Job, Line, Message


class JobDispatcher:
    def __init__(self, max_workers: int = None) -> None:
        self._max_workers = max(4, max_workers or (os.cpu_count() * 2))
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._message_queue = Queue()

    @property
    def is_ready(self) -> bool:
        return len(self._message_queue) > 0

    def dispatch(self, job: Job):
        self._executor.submit(self._process_job, job)

    def consume(self, callback: Callable[[Message], None]):
        while self._message_queue.qsize() > 0:
            msg = None
            try:
                msg = self._message_queue.get(timeout=0.1)
                if msg is not None:
                    callback(msg)
            except Empty:
                return

    async def consume_async(self, callback: Callable[[Message], Coroutine]):
        while self._message_queue.qsize() > 0:
            msg = None
            try:
                msg = self._message_queue.get(timeout=0.1)
                if msg is not None:
                    await callback(msg)
            except Empty:
                return

    def _process_job(self, job: Job):
        def readlines(child: pexpect.spawn):
            while not child.closed:
                line = child.readline()
                if not line:
                    return StopIteration()
                yield line

        # spawn subprocess and push each line of stdout & stderr onto
        # output queue as they come in.
        child = pexpect.spawn(job.command, job.args, encoding="utf-8")
        try:
            for idx, line in enumerate(readlines(child)):
                timestamp = TimeUtils.utc_timestamp()
                text = line.rstrip()
                msg = Message(job.id, timestamp, Line(idx, text), None)
                self._message_queue.put(msg)
        finally:
            child.close()

        # after the subprocess has finished, push a final item on
        # the output queue containing the exit status code.
        self._message_queue.put(
            Message(job.id, TimeUtils.utc_timestamp(), None, child.exitstatus)
        )


if __name__ == "__main__":
    from threading import Thread
    from time import sleep

    def main():
        dispatcher = JobDispatcher()

        def consume():
            while True:
                dispatcher.consume(
                    lambda msg: print(msg.line.text) if msg.line else None
                )
                sleep(0.1)

        consumer = Thread(target=consume, daemon=True)
        consumer.start()
        while True:
            input()
            job = Job(id=uuid4().hex, command="do-it", args=[])
            dispatcher.dispatch(job)

    main()
