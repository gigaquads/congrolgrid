from typing import List, Optional
from uuid import uuid4

from appyratus.utils.time_utils import TimeUtils
from pydantic import BaseModel

from controlgrid.db.models import Job, JobResult
from controlgrid.api.app import app
from controlgrid.constants import JobStatus


class Body(BaseModel):
    command: str
    args: List[str]
    tag: Optional[str]
    timeout: Optional[int]


@app.post("/run")
async def run(body: Body) -> JobResult:
    """
    Create new job and stream back output line by line.
    """
    job = await Job(
        job_id=uuid4().hex,
        command=body.command,
        created_at=TimeUtils.utc_now(),
        args=body.args,
        tag=body.tag,
        timeout=body.timeout,
        status=JobStatus.created,
    ).create(app.db)
    result = await app.runner.run(app.db, job)
    return result.dict()
