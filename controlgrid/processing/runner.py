import subprocess as sp

from controlgrid.log import log
from controlgrid.db.database import Database
from controlgrid.db.models import (
    Job,
    JobResult,
    JobStatus,
)


class Runner:
    async def run(self, db: Database, job: Job) -> JobResult:
        await job.save(db, status=JobStatus.running)

        output = []
        try:
            proc = sp.Popen(
                [job.command] + job.args,
                stdout=sp.PIPE,
                stderr=sp.STDOUT,
                universal_newlines=True,
            )
            # persist OS process ID
            await job.save(db, pid=proc.pid)

            # if there's a timeout, wait for the process to complete
            if job.timeout is not None:
                proc.wait(timeout=job.timeout)

            # the job finished normally
            await job.save(
                db, exit_code=proc.returncode, status=JobStatus.completed
            )

            # split the stdout (which also has stderr) into lines
            stdout = proc.communicate()[0]
            output = stdout.split("\n")

        except sp.TimeoutExpired as exc:
            log.exception(f"job {job.job_id} timed out")
            await job.save(db, status=JobStatus.failed, error=str(exc))
        except Exception as exc:
            log.exception(f"error in subprocess for job {job.job_id}")
            await job.save(db, status=JobStatus.error, error=str(exc))

        return JobResult.create(job, output)
