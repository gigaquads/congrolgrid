import asyncio
from queue import Empty
from time import sleep
import traceback
from fastapi import WebSocket, WebSocketDisconnect
from appyratus.json import JsonEncoder

from controlgrid.processing.consumer import JobResultConsumer
from controlgrid.processing.job import JobResult

from ..app import app, dispatcher


@app.websocket("/stream")
async def stream(websocket: WebSocket) -> None:
    json = JsonEncoder()
    consumer = JobResultConsumer(
        dispatcher._output_channel, use_output_queue=True
    )

    consumer.start()
    await websocket.accept()

    try:
        while True:
            await asyncio.sleep(0.1)
            while consumer.output:
                result = consumer.output.popleft()
                if result:
                    json_str = json.encode(result.to_dict())
                    await websocket.send_text(json_str)
    except WebSocketDisconnect as exc:
        traceback.print_exc()
    except Exception as exc:
        traceback.print_exc()
