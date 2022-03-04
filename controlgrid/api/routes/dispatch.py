from typing import List, Optional
from dataclasses import asdict

from fastapi import Request
from pydantic import BaseModel

from controlgrid.processing.data import Job
from controlgrid.api.app import app


class Body(BaseModel):
    command: str
    args: List[str]
    tag: Optional[str]
    timeout: Optional[int]


@app.post("/dispatch")
async def dispatch(body: Body, _request: Request) -> None:
    """
    Create new job and enqueue for background execution.
    """
    job = Job.create(
        body.command, body.args, tag=body.tag, timeout=body.timeout
    )

    # TODO: write job to DB
    # TODO: dispatch job post-DB commit

    # queue job for execution in background process, writing each line of stdout
    # to and output queue (to be consumed by another thread)
    app.dispatcher.dispatch(job)

    return asdict(job)
