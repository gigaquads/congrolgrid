import asyncio
import sys

import pexpect

from collections import deque
from typing import AsyncIterator, Deque, Dict, List, Optional

from appyratus.utils.time_utils import TimeUtils
from appyratus.logging import ConsoleLoggerInterface
from appyratus.env import Environment
from appyratus.json import JsonEncoder

from controlgrid.db.models import Job, JobResult, JobStreamEvent, JobOutput
from controlgrid.db.database import Database
from controlgrid.constants import JobStatus


class Streamer:
    def __init__(
        self,
        db: Database,
        stream_name: str,
        batch_size: int = 100,
    ) -> None:
        self._job_queue: Deque[Job] = deque()
        self._batch_size = max(1, batch_size)
        self._env = Environment()
        self._stream_name = stream_name
        self._log = ConsoleLoggerInterface(f"stream[{stream_name}]")
        self._json = JsonEncoder()
        self._db = db

    async def generate(self) -> AsyncIterator[str]:
        await self._enqueue_jobs()

        # start streaming the next job in queue
        generators: Dict[Job, AsyncIterator[JobStreamEvent]] = {}

        if self._job_queue and len(generators) < 4:
            job = self._job_queue.popleft()
            generators[job] = self._generate_stream_events(job)

        while generators:
            # emit job stream events for each running job in a round-robin
            # fashion, clearing job from the generators dict when work is
            # done.
            for job, generator in list(generators.items()):
                batch: List[dict] = []
                try:
                    async for event in generator:
                        batch.append(event.dict())
                        if len(batch) == self._batch_size:
                            yield self._json.encode(batch)
                            batch.clear()
                            break
                        if event.result is not None:
                            del generators[job]
                            # send any data still accumulated in final batch
                            if batch:
                                yield self._json.encode(batch)
                            break
                except ValueError:
                    self._log.exception(
                        f"JSON encode error streaming "
                        f"output for job {job.job_id}"
                    )
                except Exception:
                    self._log.exception(
                        f"unhandled exception streaming"
                        f"output for job {job.job_id}"
                    )

    async def _enqueue_jobs(self):
        # gather new jobs from db and update status to "enqeueued"
        db = self._db
        async with db.transaction():
            jobs = await Job.get_pending_jobs(db, stream=self._stream_name)
            for job in jobs:
                await job.save(db, status=JobStatus.enqueued)
                self._job_queue.append(job)

    async def _generate_stream_events(
        self, job: Job
    ) -> AsyncIterator[JobStreamEvent]:
        db = self._db
        child = await self._spawn(db, job)
        if child is None:
            yield JobStreamEvent(result=JobResult.create(job, []), line=None)
        else:
            await job.save(db, pid=child.pid)
            line_no = 0
            try:
                while not child.closed:
                    line = child.readline()
                    if line:
                        yield JobStreamEvent(
                            result=None,
                            line=JobOutput(
                                job_id=job.job_id,
                                tag=job.tag,
                                timestamp=TimeUtils.utc_timestamp(),
                                data=JobOutput.Data(
                                    line_no=line_no, text=line.rstrip()
                                ),
                                pid=job.pid,
                            ),
                        )
                        line_no += 1
                    else:
                        await job.save(
                            db,
                            status=JobStatus.completed,
                            exit_code=child.exitstatus,
                        )
                        yield JobStreamEvent(
                            result=JobResult.create(job, []), line=None
                        )
                        break
            except Exception as exc:
                self._log.exception(
                    f"error reading subprocess stdout for job {job.job_id}"
                )
                await job.save(db, status=JobStatus.error, error=str(exc))
                yield JobStreamEvent(
                    result=JobResult.create(job, []), line=None
                )

    async def _spawn(self, db: Database, job: Job) -> Optional[pexpect.spawn]:
        try:
            child = pexpect.spawn(
                job.command, job.args, timeout=job.timeout, encoding="utf-8"
            )
            await job.save(db, pid=child.pid)
            return child
        except Exception as exc:
            self._log.exception(
                f"error spawning subprocess for job {job.job_id}"
            )
            await job.save(db, status=JobStatus.error, error=str(exc))
            return None
