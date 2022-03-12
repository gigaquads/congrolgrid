from typing import List, Optional
from uuid import uuid4

from appyratus.utils.time_utils import TimeUtils
from pydantic import BaseModel

from controlgrid.db.models import Job
from controlgrid.api.app import app
from controlgrid.constants import JobStatus


class Body(BaseModel):
    command: str
    args: List[str]
    tag: Optional[str]
    timeout: Optional[int]
    stream: str


@app.post("/stream")
async def stream(body: Body) -> Job:
    """
    Create new job and write to DB, where it will be picked up by a named
    stream worker at `/stream/{stream_name}`.
    """
    job = await Job(
        job_id=uuid4().hex,
        command=body.command,
        created_at=TimeUtils.utc_now(),
        args=body.args,
        tag=body.tag,
        timeout=body.timeout,
        stream=body.stream,
        status=JobStatus.created,
    ).create(app.db)
    return job.dict()
