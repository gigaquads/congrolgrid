from typing import Set

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
