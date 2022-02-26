import os
import subprocess as sp

from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List

from appyratus.utils.type_utils import TypeUtils

from controlgrid.ipc import Channel
from controlgrid.log import log

from .job import Job, JobResult


class JobDispatcher:
    def __init__(self, max_workers: int = None):
        self._max_worker = max_workers or os.cpu_count()
        self._executor = ThreadPoolExecutor(max_workers=self._max_worker)
        self._output_channel = Channel("ipc:///tmp/cg-runner-output.sock")
        self._output_channel.publish(None)

    def submit(self, job: Job):
        self._executor.submit(self.target, job)

    def target(self, job: Job):
        args = [job.command] + job.args
        exception: BaseException = None
        stdout = []
        stderr = []
        log.info(f"running job {job.id}")

        try:
            start = datetime.now()
            proc = sp.run(args, capture_output=True)
            if proc.stdout:
                stdout = proc.stdout.decode().split("\n")
            if proc.stderr:
                stderr = proc.stderr.decode().split("\n")
        except BaseException as exc:
            exc_name = TypeUtils.get_class_name(exc)
            log.exception(f"unexpected {exc_name} processing job {job.id}")
            exception = exc
        finally:
            end = datetime.now()

        if exception is None:
            log.info(f"job {job.id} finished running")

        try:
            self._output_channel.publish(
                JobResult(
                    job=job,
                    time=(start - end),
                    exc=exception,
                    stdout=stdout,
                    stderr=stderr,
                )
            )
        except Exception:
            import traceback

            traceback.print_exc()
