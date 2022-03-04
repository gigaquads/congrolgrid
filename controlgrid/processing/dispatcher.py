import os
import pexpect

from uuid import uuid4
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Coroutine

from appyratus.utils.time_utils import TimeUtils

from controlgrid.processing.data import Job, JobStatus, OutputLine
from controlgrid.log import log


class JobDispatcher:
    def __init__(self, max_workers: int = None) -> None:
        self._max_workers = max(4, max_workers or (os.cpu_count() * 2))
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._command_timeout: int = 5
        self._output_buffer = deque()

    @property
    def has_output(self) -> bool:
        return len(self._output_buffer) > 0

    def dispatch(self, job: Job):
        # spawn subprocess and push each line of stdout & stderr onto
        # output queue as they come in.
        job.status = JobStatus.running
        log.info(f"spawning subprocess for job {job.job_id}")
        try:
            child = pexpect.spawn(
                job.command, job.args, timeout=job.timeout, encoding="utf-8"
            )
            job.pid = child.pid
        except Exception as exc:
            log.exception(f"error spawning subprocess for job {job.job_id}")
            job.status = JobStatus.error
            job.error = str(exc)
            return

        # in a separate thread, push command output on output queue line by line
        self._executor.submit(self._write_output_buffer, job, child)

    def consume(self, callback: Callable[[OutputLine], None]):
        while len(self._output_buffer):
            line = self._output_buffer.popleft()
            if line is not None:
                callback(line)

    async def consume_async(self, callback: Callable[[OutputLine], Coroutine]):
        while len(self._output_buffer):
            line = self._output_buffer.popleft()
            if line is not None:
                await callback(line)

    def _write_output_buffer(self, job: Job, child: pexpect.spawn):
        def readlines(child: pexpect.spawn):
            while not child.closed:
                line = child.readline()
                if not line:
                    return StopIteration()
                yield line

        log.info(
            f"running job {job.job_id} (cmd: {job.command}, pid: {child.pid})"
        )

        try:
            exit_code = None
            for line_no, text in enumerate(readlines(child)):
                data = OutputLine.Data(line_no, text.rstrip())
                self._output_buffer.append(
                    OutputLine(
                        job.job_id,
                        job.tag,
                        TimeUtils.utc_timestamp(),
                        data,
                        exit_code,
                        job.pid,
                    )
                )

            job.status = JobStatus.completed

            log.info(
                f"completed job {job.job_id} (cmd: {job.command}, pid: {job.pid})"
            )
        except Exception as exc:
            job.status = JobStatus.error
            job.error = str(exc)
            log.exception(
                f"error running job {job.job_id} (cmd: {job.command}, pid: {job.pid})"
            )
        finally:
            child.close()

        job.exit_code = child.exitstatus

        # after the subprocess has finished, push a final item on
        # the output queue containing the exit status code.
        data = None
        exit_code = child.exitstatus
        self._output_buffer.append(
            OutputLine(
                job.job_id,
                job.tag,
                TimeUtils.utc_timestamp(),
                data,
                exit_code,
            )
        )


if __name__ == "__main__":
    from threading import Thread
    from time import sleep

    def main():
        dispatcher = JobDispatcher()

        def consume():
            while True:
                dispatcher.consume(
                    lambda msg: print(msg.data.text) if msg.data else None
                )
                sleep(0.1)

        consumer = Thread(target=consume, daemon=True)
        consumer.start()

        while True:
            input()
            job = Job(job_id=uuid4().hex, command="ls", args=["-l"])
            dispatcher.dispatch(job)

    main()
