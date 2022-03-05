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


@app.post("/run")
async def run(body: Body, _request: Request) -> None:
    """
    Create new job and stream back output line by line.
    """
    job = Job.create(
        body.command, body.args, tag=body.tag, timeout=body.timeout
    )
    result = app.runner.run(job)
    return asdict(result)
