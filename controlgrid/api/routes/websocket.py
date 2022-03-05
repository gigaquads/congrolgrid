import asyncio

from fastapi import WebSocket, WebSocketDisconnect

from controlgrid.api.app import app
from controlgrid.log import log


@app.websocket("/websocket")
async def websocket(socket: WebSocket) -> None:
    await socket.accept()

    try:
        while True:
            await asyncio.sleep(0.1)
            # TODO: do something here
    except WebSocketDisconnect:
        await websocket.close()
    except Exception:
        log.exception("websocket error")
