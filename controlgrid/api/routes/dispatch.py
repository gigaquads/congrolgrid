from typing import List
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel

from controlgrid.processing.job import Job

from ..app import app, dispatcher


class Body(BaseModel):
    command: str
    args: List[str]


@app.post("/jobs")
async def dispatch_job(body: Body, request: Request) -> None:
    job = Job(uuid4().hex, body.command, body.args)
    # TODO: write job to DB
    # TODO: dispatch job post-commit
    dispatcher.submit(job)
    return job
