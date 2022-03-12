import asyncio

from typing import AsyncIterator

from fastapi import Request
from sse_starlette.sse import EventSourceResponse

from controlgrid.api.app import app
from controlgrid.processing.streamer import Streamer


@app.get("/streams/{name}")
async def stream_event_source(request: Request) -> EventSourceResponse:
    """
    Create new job and stream back output line by line.
    """
    stream_name = request.path_params["name"]

    app.log.info(f"starting '{stream_name}' stream")

    streamer = Streamer(app.db, stream_name)
    max_concurrency = 4

    async def stream(request: Request) -> AsyncIterator[str]:
        while not await request.is_disconnected():
            await asyncio.sleep(0.05)
            async for batch in streamer.generate():
                yield batch

    source = EventSourceResponse(stream(request))
    return source
