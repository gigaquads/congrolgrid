import sqlalchemy as sa

from typing import Callable, Dict, List, Optional

from sqlalchemy import Table, MetaData
from sqlalchemy.engine import Engine
from databases import Database as BaseDatabase

from controlgrid.log import log

TableFactory = Callable[[MetaData], Table]


class Database(BaseDatabase):
    def __init__(
        self, url: str, metadata: MetaData, engine: Engine, *args, **kwargs
    ):
        super().__init__(url, *args, **kwargs)
        self.tables: Dict[str, Table] = {}
        self.databases = DatabaseManager()
        self.metadata = metadata
        self.engine = engine

    def add_table(self, factory: TableFactory) -> Table:
        table = factory(self.metadata)
        self.tables[table.name] = table
        return table

    def create_tables(self) -> None:
        log.info(f"creating database tables at {self.engine.url}")
        self.metadata.create_all(self.engine)


class DatabaseManager:
    def __init__(self):
        self._url2db: Dict[str, Database] = {}
        self._name2db: Dict[str, Database] = {}
        self._db2info: Dict[Database, Database] = {}

    async def connect(
        self, url: str, name: str, tables=Optional[List[TableFactory]]
    ) -> Database:
        # memoize database
        if url not in self._url2db:
            log.debug(f"connecting to database at {url}")
            connect_args = {"check_same_thread": False}
            engine = sa.create_engine(url, connect_args=connect_args)
            database = Database(url, MetaData(), engine)
            self._url2db[url] = database
        else:
            database = self._url2db[url]

        # map tags to Database instance
        self._name2db[name] = database

        # open connection to DB
        await database.connect()

        # register sqlalchemy tables with db metadata
        for factory in tables or []:
            database.add_table(factory)

        return database

    def get(self, name: str) -> Optional[Database]:
        return self._name2db.get(name)
