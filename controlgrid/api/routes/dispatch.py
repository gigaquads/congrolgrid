from typing import List
from dataclasses import asdict
from uuid import uuid4

from fastapi import Request
from pydantic import BaseModel

from controlgrid.processing.job import Job

from controlgrid.processing.dispatcher import JobDispatcher
from ..app import app


class Body(BaseModel):
    command: str
    args: List[str]


@app.post("/dispatch")
async def dispatch(body: Body, request: Request) -> None:
    dispatcher: JobDispatcher = app.dispatcher
    job = Job(uuid4().hex, body.command, body.args)
    # TODO: write job to DB
    # TODO: dispatch job post-commit
    dispatcher.dispatch(job)
    return asdict(job)
