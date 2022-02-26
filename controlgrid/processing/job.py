import uuid

from datetime import timedelta
from typing import List

from appyratus.enum import EnumValueStr


class JobStatus(EnumValueStr):
    error = "error"
    fail = "fail"
    complete = "complete"


class Job:
    def __init__(self, command: str, args: List[str] = None):
        self.id = uuid.uuid4().hex
        self.command = command
        self.args = args or []

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "command": self.command,
            "args": self.args,
        }


class JobResult:
    def __init__(
        self,
        job: Job,
        time: timedelta,
        exc: BaseException = None,
        stdout: List[str] = None,
        stderr: List[str] = None,
    ):
        self.job = job
        self.time = time
        self.exc = exc
        self.stdout = stdout
        self.stderr = stderr
        self.status = JobStatus.complete
        if exc is not None:
            self.status = JobStatus.error
        elif stderr:
            self.status = JobStatus.fail

    def to_dict(self) -> dict:
        return {
            "job": job.to_dict(),
            "time": self.time.total_seconds(),
            "stdout": self.stdout,
            "stderr": self.stderr,
            "status": self.status,
        }

    @property
    def is_complete(self) -> bool:
        return self.status == JobStatus.complete

    @property
    def has_error(self) -> bool:
        return self.status == JobStatus.error

    @property
    def failed(self) -> bool:
        return self.status == JobStatus.fail

    def __repr__(self) -> str:
        return f"JobResult({self.job.id}, status: {self.status})"
