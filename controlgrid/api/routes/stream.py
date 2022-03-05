from typing import List, Optional
from dataclasses import asdict

from fastapi import Request
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from appyratus.json import JsonEncoder

from controlgrid.processing.data import Job, JobStreamEvent
from controlgrid.api.app import app


class Body(BaseModel):
    command: str
    args: List[str]
    tag: Optional[str]
    timeout: Optional[int]


@app.post("/stream")
async def stream(body: Body, request: Request) -> None:
    """
    Create new job and stream back output line by line.
    """
    json = JsonEncoder()
    job = Job.create(**body.dict())
    job_event_generator = app.runner.stream(job)

    async def stream(request: Request) -> str:
        is_completed = False
        while not await request.is_disconnected() and (not is_completed):
            try:
                for event in job_event_generator:
                    data = asdict(event)
                    yield json.encode(data)
                    if event.result:
                        is_completed = True
                        break
            except ValueError:
                app.log.exception(
                    f"JSON encode error when streaming "
                    f"output for job {job.job_id}"
                )

    return EventSourceResponse(stream(request))
