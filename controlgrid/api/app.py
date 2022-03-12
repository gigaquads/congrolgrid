import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from appyratus.env import Environment
from databases import Database

from controlgrid.processing.streamer import Streamer
from controlgrid.processing.runner import Runner
from controlgrid.log import log
from controlgrid.db.database import DatabaseManager
from controlgrid.db import tables


class LocalDaemonAPI(FastAPI):
    log = log
    env = Environment()
    runner = Runner()
    databases = DatabaseManager()
    streamer: Streamer
    db: Database


app = LocalDaemonAPI()

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
async def startup():
    # etablish database connection. default to SQLite DB,
    # using a local database file in data/
    url = app.env.get("DATABASE_URL")
    if not url:
        os.makedirs("./data", exist_ok=True)
        url = "sqlite:///data/controlgrid.db"

    app.db = await app.databases.connect(url, "db", tables=[tables.jobs])
    app.db.create_tables()


@app.on_event("shutdown")
async def shutdown():
    await app.db.disconnect()
