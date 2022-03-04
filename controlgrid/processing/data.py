from typing_extensions import Self
import uuid

from dataclasses import dataclass
from typing import List, Optional, Set

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
class OutputLine:
    @dataclass
    class Data:
        line_no: int
        text: str

    job_id: str
    tag: Optional[str]
    timestamp: int
    data: Optional[Data]
    exit_code: Optional[int]
    pid: int
