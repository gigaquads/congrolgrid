import uuid

from dataclasses import dataclass
from typing import List, Optional

from appyratus.enum import EnumValueStr


class JobStatus(EnumValueStr):
    error = "error"
    fail = "fail"
    complete = "complete"


@dataclass
class Job:
    id: str
    command: str
    args: List[str]

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "command": self.command,
            "args": self.args,
        }


@dataclass
class Line:
    index: int
    text: str


@dataclass
class Message:
    job: str
    timestamp: int
    line: Optional[Line]
    exit_code: Optional[int]
