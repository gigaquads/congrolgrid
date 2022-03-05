from typing_extensions import Self
import uuid

from dataclasses import dataclass
from typing import List, Optional, Set, Union

from appyratus.enum import EnumValueStr


class JobStatus(EnumValueStr):
    @staticmethod
    def values() -> Set[str]:
        return {
            "created",
            "enqueued",
            "running",
            "error",
            "failed",
            "completed",
        }


@dataclass
class Job:
    job_id: str
    command: str
    args: List[str]
    status: JobStatus
    tag: Optional[str]
    timeout: int
    exit_code: int
    pid: Optional[int]
    error: Optional[str]

    @classmethod
    def create(
        cls,
        command: str,
        args: List[str],
        tag: Optional[str] = None,
        timeout: Optional[int] = None,
    ) -> Self:
        return Job(
            uuid.uuid4().hex,
            command,
            args,
            JobStatus.created,
            tag,
            timeout,
            0,
            None,
            None,
        )


@dataclass
class JobResult:
    job: Job
    output: List[str]

    @classmethod
    def create(cls, job: Job, output: Union[str, List[str]]) -> Self:
        output = output.split() if not isinstance(output, list) else output
        return cls(job, output)


@dataclass
class OutputLine:
    @dataclass
    class Data:
        line_no: int
        text: str

    job_id: str
    tag: Optional[str]
    timestamp: int
    data: Optional[Data]
    pid: int


@dataclass
class JobStreamEvent:
    result: Optional[JobResult]
    line: Optional[OutputLine]
