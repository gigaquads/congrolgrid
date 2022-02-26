from collections import deque
from queue import Queue
from threading import Thread

from appyratus.utils.type_utils import TypeUtils

from controlgrid.ipc import Channel
from controlgrid.log import log

from .job import Job, JobResult


class JobResultConsumer(Thread):
    def __init__(self, channel: Channel, use_output_queue=False) -> None:
        super().__init__(daemon=True)
        self.channel = channel
        self.use_output_queue = use_output_queue
        self.output = deque()

    def run(self):
        subscription = self.channel.subscribe()
        while True:
            result: JobResult = subscription.receive()
            exception = None

            # try to process the job result in custom callback
            try:
                if result:
                    if self.use_output_queue:
                        self.output.append(result)
                    self.on_result(result)
            except BaseException as exc:
                exception = exc
                exc_name = TypeUtils.get_class_name(exc)
                log.exception(
                    f"{exc_name} occured while processing job result {result.job.id}"
                )

            # send exception to custom exception handler
            if exception is not None:
                try:
                    self.on_exception(exception)
                except:
                    continue

    def on_result(self, result: JobResult):
        pass

    def on_exception(self, exc: BaseException):
        pass


if __name__ == "__main__":
    worker = JobDispatcher()
    consumer = JobResultConsumer()
    consumer.start()
    while True:
        input()
        job = Job("lsa", ["-l", "-a"])
        worker.submit(job)
