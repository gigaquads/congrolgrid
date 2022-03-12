from typing import Iterable, Optional, List, Union
from datetime import datetime

from pydantic import BaseModel

from controlgrid.constants import JobStatus
from controlgrid.db.database import Database


class Job(BaseModel):
    job_id: str
    created_at: datetime
    command: str
    args: List[str]
    status: JobStatus
    tag: Optional[str]
    timeout: Optional[int]
    exit_code: Optional[int]
    pid: Optional[int]
    error: Optional[str]
    stream: Optional[str]

    def __hash__(self) -> int:
        return int(self.job_id, base=16)

    async def create(self, db: Database) -> "Job":
        table = db.tables["jobs"]
        query = table.insert().values(**self.dict())
        await db.execute(query)
        return self

    async def save(
        self, db: Database, fields: Union[str, Iterable[str]] = None, **values
    ) -> "Job":
        # first set any new field values
        if values:
            for key, val in values.items():
                setattr(self, key, val)
        else:
            values = self.dict()

        # get projection (a dict) of fields to save
        if fields:
            if isinstance(fields, str):
                name = fields
                values = {name: values[name]}
            else:
                values = {name: values[name] for name in fields}

        # perform db update
        if values:
            table = db.tables["jobs"]
            query = (
                table.update()
                .where(table.c.job_id == self.job_id)
                .values(**values)
            )
            await db.execute(query)

        return self

    @classmethod
    async def get(cls, db: Database, id: str) -> Optional["Job"]:
        table = db.tables["jobs"]
        query = table.select().where(table.c.job_id == id).limit(1)
        data = await db.fetch_one(query)
        return cls(**data) if data else None

    @classmethod
    async def get_pending_jobs(
        cls, db: Database, stream: Optional[str] = None
    ) -> List["Job"]:
        table = db.tables["jobs"]
        query = table.select()

        if stream is not None:
            # select pending jobs for a specific stream
            query = query.where(
                table.c.status == JobStatus.created, table.c.stream == stream
            )
        else:
            query = query.where(table.c.status == JobStatus.created)

        # sort jobs in FIFO order
        query = query.order_by(table.c.created_at.asc())

        rows = await db.fetch_all(query)
        return [cls(**data) for data in rows]


class JobResult(BaseModel):
    job: Job
    output: List[str]

    @classmethod
    def create(cls, job: Job, output: Union[str, List[str]]) -> "JobResult":
        output = output.split() if not isinstance(output, list) else output
        return cls(job=job, output=output)


class JobOutput(BaseModel):
    class Data(BaseModel):
        line_no: int
        text: str

    job_id: str
    tag: Optional[str]
    timestamp: int
    data: Optional[Data]
    pid: int


class JobStreamEvent(BaseModel):
    result: Optional[JobResult]
    line: Optional[JobOutput]
