from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from controlgrid.processing.dispatcher import JobDispatcher


app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://localhost:8000",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def init_dispatcher():
    app.dispatcher = JobDispatcher()
