import asyncio

from appyratus.json import JsonEncoder
from fastapi import WebSocket

from controlgrid.processing.dispatcher import JobDispatcher, Message
from controlgrid.api.app import app
from controlgrid.log import log


@app.websocket("/ws")
async def stream(websocket: WebSocket) -> None:
    dispatcher: JobDispatcher = app.dispatcher
    json = JsonEncoder()

    async def send(message: Message):
        try:
            json_str = json.encode(message)
        except ValueError:
            log.exception("JSON encode error")

        await websocket.send_text(json_str)

    await websocket.accept()
    try:
        while True:
            await asyncio.sleep(0.1)
            if dispatcher.has_output:
                await dispatcher.consume_async(send)
    except Exception:
        log.exception("websocket error")
