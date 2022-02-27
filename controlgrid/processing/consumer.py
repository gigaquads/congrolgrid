from collections import deque
from threading import Thread
from typing import Callable

from controlgrid.ipc import Channel
from controlgrid.log import log

from .job import Job, JobResult


class JobResultConsumer(Thread):
    def __init__(
        self,
        channel: Channel,
        on_result: Callable[[JobResult], None] = None,
        on_exception: Callable[[JobResult, BaseException], None] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.channel = channel
        self.output = deque()
        self.on_result = on_result or (lambda *args: None)
        self.on_exception = on_exception or (lambda *args: None)

    def run(self):
        subscription = self.channel.subscribe()
        while True:
            result: JobResult = subscription.receive()
            has_error = False
            exception = None

            # try to process the job result in custom callback
            try:
                if result:
                    self.on_result(result)
                    self.output.append(result)
            except BaseException as exc:
                log.exception(f"error processing job {result.job.id}")
                has_error = True
                exception = exc

            # send exception to custom exception handler
            if has_error:
                try:
                    self.on_exception(result, exception)
                except:
                    continue


if __name__ == "__main__":
    from .dispatcher import JobDispatcher

    worker = JobDispatcher()
    consumer = JobResultConsumer()
    consumer.start()

    while True:
        input()
        job = Job("lsa", ["-l", "-a"])
        worker.submit(job)
