import subprocess as sp
import pexpect

from typing import Iterator, Optional
from appyratus.utils.time_utils import TimeUtils

from controlgrid.log import log
from controlgrid.processing.data import (
    Job,
    JobResult,
    JobStatus,
    JobStreamEvent,
    OutputLine,
)


class Runner:
    def __init__(self) -> None:
        pass

    def run(self, job: Job) -> JobResult:
        job.status = JobStatus.running
        output = []
        try:
            proc = sp.Popen(
                [job.command] + job.args,
                stdout=sp.PIPE,
                stderr=sp.STDOUT,
                universal_newlines=True,
            )
            # persist OS process ID
            job.pid = proc.pid

            # if there's a timeout, wait for the process to complete
            if job.timeout is not None:
                proc.wait(timeout=job.timeout)

            # the job finished normally
            job.exit_code = proc.returncode
            job.status = JobStatus.completed

            # split the stdout (which also has stderr) into lines
            stdout = proc.communicate()[0]
            output = stdout.split()

        except sp.TimeoutExpired as exc:
            job.status = JobStatus.failed
            job.error = str(exc)
            log.exception(f"job {job.job_id} timed out")
        except Exception as exc:
            job.status = JobStatus.error
            job.error = str(exc)
            log.exception(f"error in subprocess for job {job.job_id}")

        return JobResult.create(job, output)

    def stream(self, job: Job) -> Iterator[JobStreamEvent]:
        child = self.spawn(job)
        if child is None:
            yield JobResult.create(job, [])
        else:
            job.pid = child.pid
            line_no = 0
            try:
                while not child.closed:
                    line = child.readline()
                    if line:
                        yield JobStreamEvent(
                            result=None,
                            line=OutputLine(
                                job.job_id,
                                job.tag,
                                TimeUtils.utc_timestamp(),
                                OutputLine.Data(line_no, line.rstrip()),
                                job.pid,
                            ),
                        )
                        line_no += 1
                    else:
                        job.status = JobStatus.completed
                        job.exit_code = child.exitstatus
                        yield JobStreamEvent(
                            result=JobResult.create(job, []), line=None
                        )
                        break
            except Exception as exc:
                log.exception(
                    f"error reading subprocess stdout for job {job.job_id}"
                )
                job.status = JobStatus.error
                job.error = str(exc)
                yield JobStreamEvent(
                    result=JobResult.create(job, []), line=None
                )

    @staticmethod
    def spawn(job: Job) -> Optional[pexpect.spawn]:
        try:
            child = pexpect.spawn(
                job.command, job.args, timeout=job.timeout, encoding="utf-8"
            )
            job.pid = child.pid
            return child
        except Exception as exc:
            log.exception(f"error spawning subprocess for job {job.job_id}")
            job.status = JobStatus.error
            job.error = str(exc)
            return None
