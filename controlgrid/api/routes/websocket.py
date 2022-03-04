import asyncio

from dataclasses import asdict

from appyratus.json import JsonEncoder
from fastapi import WebSocket, WebSocketDisconnect

from controlgrid.processing.dispatcher import JobDispatcher
from controlgrid.processing.data import OutputLine
from controlgrid.api.app import app
from controlgrid.log import log


@app.websocket("/websocket")
async def websocket(websocket: WebSocket) -> None:
    dispatcher: JobDispatcher = app.dispatcher
    json = JsonEncoder()

    async def send_output_line(line: OutputLine):
        try:
            json_str = json.encode(asdict(line))
            await websocket.send_text(json_str)
        except ValueError:
            log.exception("JSON encode error")
        except Exception:
            log.exception(
                f"unhandled exception sending job {line.job_id} output"
            )

    await websocket.accept()
    reconnect = False
    try:
        while True:
            if reconnect:
                await websocket.accept()
                reconnect = False
            await asyncio.sleep(0.1)
            if dispatcher.has_output:
                try:
                    await dispatcher.consume_async(send_output_line)
                except WebSocketDisconnect:
                    log.warning("websocket disconnect. reconnecting to client")
                    reconnect = True
    except Exception:
        log.exception("websocket error")
