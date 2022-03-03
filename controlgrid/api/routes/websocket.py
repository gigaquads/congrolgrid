import asyncio

from dataclasses import asdict

from appyratus.json import JsonEncoder
from fastapi import WebSocket, WebSocketDisconnect

from controlgrid.processing.dispatcher import JobDispatcher, Message
from controlgrid.api.app import app
from controlgrid.log import log


@app.websocket("/ws")
async def stream(websocket: WebSocket) -> None:
    dispatcher: JobDispatcher = app.dispatcher
    json = JsonEncoder()

    async def send(message: Message):
        try:
            json_str = json.encode(asdict(message))
        except ValueError:
            log.exception("JSON encode error")

        await websocket.send_text(json_str)

    await websocket.accept()
    reconnect = False
    try:
        while True:
            if reconnect:
                await websocket.accept()
                reconnect = False
            await asyncio.sleep(0.1)
            if dispatcher.is_ready:
                try:
                    await dispatcher.consume_async(send)
                except WebSocketDisconnect:
                    log.info("websocket disconnect. reconnecting to client")
                    reconnect = True
    except Exception:
        log.exception("websocket error")
