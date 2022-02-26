from typing import List

from fastapi import Request
from pydantic import BaseModel

from controlgrid.processing.job import Job

from ..app import app, dispatcher


class Body(BaseModel):
    command: str
    args: List[str]


@app.post("/jobs")
async def dispatch_job(body: Body, request: Request) -> None:
    dispatcher.submit(Job(body.command, body.args))
    return {}
