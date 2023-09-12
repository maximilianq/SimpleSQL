from uuid import UUID as uuid, uuid4
from json import loads, JSONDecodeError
from typing import Callable
from os import listdir
from os.path import splitext, isdir, isfile
from pathlib import Path as File
from string import Formatter

from logging import getLogger, Logger

from psycopg import AsyncConnection as Connection, AsyncCursor as Cursor, AsyncClientCursor
from psycopg.errors import Error

from asyncio import sleep, set_event_loop_policy, WindowsSelectorEventLoopPolicy
set_event_loop_policy(WindowsSelectorEventLoopPolicy())

formatter: Formatter = Formatter()

class Query:

    def __init__(self, database: "SimpleSQL", name: str, query: str) -> None:

        self.database: SimpleSQL = database

        self.name: str = name
        self.query: str = query

        self.parameters: list[str] = None

        self.prepare_query: str = None
        self.execute_query: str = None
        self.deallocate_query: str = None

        self._process_query()

    async def __call__(self, **kwargs: dict[str, object]) -> None:
        return await self.execute(kwargs)
    
    def _process_query(self):
        
        query_replace: str = self.query

        self.parameters = list(dict.fromkeys([item[1] for item in formatter.parse(query_replace) if item[1]]))
        for index, parameter in enumerate(self.parameters):
            query_replace = query_replace.replace('{' + parameter + '}', '$' + str(index + 1))

        self.guid: uuid = uuid4()

        self.prepare_query = f'PREPARE "{self.guid}" AS {query_replace.removesuffix(";")};'
        self.execute_query = f'EXECUTE "{self.guid}"({", ".join([f"%({parameter})s" for parameter in self.parameters])});'
        self.deallocate_query = f'DEALLOCATE "{self.guid}";'

    async def prepare(self) -> None:

        self.database.logger.debug(f'Preparing query {self.name} ({self.guid}).')

        await self.database._execute(self.prepare_query)
    
    async def execute(self, data: dict[str, object] = {}) -> object | dict[str, object] | list[object] | list[dict[str, object]]:

        self.database.logger.debug(f'Execute query {self.name} ({self.guid})')

        return await self.database._query(self.execute_query, {parameter: data.get(parameter, None) for parameter in self.parameters})

    async def deallocate(self) -> None:

        self.database.logger.debug(f'Deallocating query {self.name} ({self.guid}).')

        await self.database._execute(self.deallocate_query)

class SQLRouter:

    def __init__(self) -> None:
        
        self.database: SimpleSQL = None
        
        self.listeners: dict[str, list[Callable[[dict | str], None]]] = {}
    
    def __getattr__(self, name: str) -> Query:
        if self.database:
            return self.database.get_query(name)
        else:
            raise Error("Error: router needs to be bound to database to be queried!")
        
    def add_listener(self, event: str, callback: Callable[[dict | str], None]) -> None:
        if event in self.listeners:
            self.listeners[event].append(callback)
        else:
            self.listeners[event] = [callback]

    def listen(self, event: str) -> Callable[[str], Callable[[dict | str], None]]:
        def decorate(callback: Callable[[dict | str], None]) -> None:
            self.add_listener(event, callback)
        return decorate
    
    def include_router(self, router: "SQLRouter") -> None:
        for event, listeners in router.listeners.items():
            if event in self.listeners:
                self.listeners[event] = self.listeners[event] + listeners
            else:
                self.listeners[event] = listeners

class SimpleSQL:

    def __init__(self, host: str, port: int, username: str, password: str, database: str, logger: Logger = getLogger()) -> None:
        
        self.host: str = host
        self.port: int = port
        self.username: str = username
        self.password: str = password
        self.database: str = database

        self.logger: Logger = logger

        self.connection: Connection = None

        self.queries: dict[str, Query] = {}
        self.listeners: dict[str, list[Callable[[dict | str], None]]] = {}
        self.pipes: list[Callable[[str, dict | str], None]] = []
    
    def __getattr__(self, name: str) -> Query:
        return self.get_query(name)
    
    def get_query(self, name: str) -> Query:
        if name in self.queries:
            return self.queries.get(name, None)
        else:
            raise Error(f'Error: query "{name}" could not be found!')

    def add_listener(self, event: str, callback: Callable[[dict | str], None]) -> None:
        if event in self.listeners:
            self.listeners[event].append(callback)
        else:
            self.listeners[event] = [callback]

    def listen(self, event: str) -> Callable[[str], Callable[[dict | str], None]]:
        def decorate(callback: Callable[[dict | str], None]) -> None:
            self.add_listener(event, callback)
        return decorate
    
    def include_queries(self, path: str, prefix: str = None):

        for item in listdir(path):

            name, extension = splitext(item)
            identifier: str = (prefix + '_' if prefix else '') + name.upper()

            if isfile(f'{path}/{item}'):
                if extension.upper() == '.SQL':
                    self.queries[identifier] = Query(self, name.upper(), File(f'{path}/{item}').read_text())

            if isdir(f'{path}/{item}'):
                self.include_queries(f'{path}/{item}', prefix = identifier)

    def include_router(self, router: SQLRouter):

        if not router.database:
            router.database = self
        else:
            raise Error("Error: router can't be bound to multiple database instances!")

        for event, listeners in router.listeners.items():
            if event in self.listeners:
                self.listeners[event] = self.listeners[event] + listeners
            else:
                self.listeners[event] = listeners

    def include_pipe(self, callback: Callable[[str, dict | None], None]):
        self.pipes.append(callback)

    async def start(self):
        self.connection = await Connection.connect(f'host={self.host} port={self.port} dbname={self.database} user={self.username} password={self.password}', prepare_threshold = False)
        await self._prepare()

    async def listen(self):
        await self._listen()

    async def execute(self, query: Query, data: dict[str, object] = {}) -> object | dict[str, object] | list[object] | list[dict[str, object]]:
        await query.execute(self, data)

    async def stop(self):
        await self._deallocate()
        await self.connection.close()

    async def _execute(self, query: str) -> bool:

        try:
            cursor: Cursor = await self.connection.execute(query)

        except Error as error:
            print(error)
            await self.connection.rollback()
            return False

        await self.connection.commit()
        await cursor.close()

        return True
    
    async def _query(self, query: str, data: dict[str, object]) -> object | dict[str, object] | list[object] | list[dict[str, object]]:

        cursor: Cursor = AsyncClientCursor(self.connection)

        try:
            await cursor.execute(query, data)

        except Error as error:
            await self.connection.rollback()
            return None

        output: object | dict[str, object] | list[object] | list[dict[str, object]] = None

        if cursor.description:
            rows: list[tuple] = await cursor.fetchall()
            if len(rows) == 1:
                if len(cursor.description) == 1:
                    output = rows[0][0]
                else:
                    output = {column.name: rows[0][index] for index, column in enumerate(cursor.description)}
            else:
                if len(cursor.description) == 1:
                    output = [row[0] for row in rows]
                else:
                    output = [{column.name: row[index] for index, column in enumerate(cursor.description)} for row in rows]

        await self.connection.commit()
        await cursor.close()

        return output

    async def _prepare(self) -> None:
        
        for query in self.queries.values():
            await query.prepare()

    async def _listen(self):

        cursor: Cursor = self.connection.cursor()

        for event in self.listeners:
            await cursor.execute(f'LISTEN "{event}";')

        await self.connection.commit()

        try:

            while await sleep(0.1, True):

                async for notify in self.connection.notifies():

                    self.logger.debug(f'Recieved database event "{notify.channel}".')

                    print(f'Recieved database event "{notify.channel}".')

                    for listener in self.listeners.get(notify.channel, []):
                        try:
                            await listener(loads(notify.payload))
                        except JSONDecodeError:
                            await listener(notify.payload)

                    for pipe in self.pipes:
                        await pipe(notify.channel, notify.payload)
                    
        finally:

            for event in self.listeners:
                await cursor.execute(f'UNLISTEN "{event}";')

            await self.connection.commit()
            await cursor.close()

    async def _deallocate(self):

        for query in self.queries.values():
            await query.deallocate()