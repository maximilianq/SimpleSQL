from typing import Callable, Awaitable

from os import listdir
from os.path import splitext, isdir, isfile
from pathlib import Path as File

from asyncpg.connection import connect, Connection
from asyncpg.prepared_stmt import PreparedStatement
from asyncpg.transaction import Transaction
from asyncpg.exceptions import PostgresError

from json import loads, JSONDecodeError

from logging import getLogger, Logger

from string import Formatter

formatter: Formatter = Formatter()

class Query:

    def __init__(self, database: "SQLClient", name: str, query: str) -> None:

        self.database: SQLClient = database
        self.name: str = name
        self.query: str = query

        self.formatted: str = None
        self.parameters: list[str] = None

        self.statement: PreparedStatement = None

        self._format_query()
    
    async def __call__(self, **parameters: dict[str, object]) -> list[dict[str, object]]:
        return await self.execute(parameters)

    def _format_query(self) -> None:

        self.formatted = self.query

        self.parameters = list(dict.fromkeys([item[1] for item in formatter.parse(self.query) if item[1]]))
        for index, parameter in enumerate(self.parameters):
            self.formatted = self.formatted.replace('{' + parameter + '}', '$' + str(index + 1))

    async def prepare(self):
        self.database.logger.debug(f'Preparing query "{self.name}" for a later use.')
        self.statement = await self.database._prepare(self.formatted)

    async def execute(self, parameters: dict[str, object]):
        
        self.database.logger.debug(f'Executing query "{self.name}".')
        return await self.database._execute(self.statement, [parameters.get(parameter, None) for parameter in self.parameters])

class Listener:

    def __init__(self, name: str, callback: Callable[[str | dict[str, object] | list[dict[str, object]]], Awaitable]) -> None:
        self.name = name
        self.callback = callback
        
        self.database: SQLClient = None

    async def listen(self):
        await self.database._listen(self.name, self.callback)

class SQLRouter:

    def __init__(self) -> None:
        self.database: SQLClient = None
        self.parent: SQLRouter = None
        self.listeners: list[Listener] = []

    def get_query(self, name: str) -> Query:
        if self.parent:
            return self.parent.get_query(name)
        elif self.database:
            return self.database.get_query(name)
        else:
            raise Exception(f'Error: router is not directly or indirectly connected to database!')

    def listen(self, name: str) -> Callable[[str], Callable[[dict | str], None]]:
        def decorate(callback: Callable[[dict | str], None]) -> None:
            self.include_listener(Listener(name, callback))
        return decorate
    
    def include_listener(self, listener: Listener) -> None:
        self.listeners.append(listener)

    def include_router(self, router: "SQLRouter") -> None:
        router.parent = self
        self.listeners = self.listeners + router.listeners

class SQLClient:

    def __init__(self, host: str, port: int, user: str, password: str, database: str, logger: Logger = getLogger()) -> None:

        self.host: str = host
        self.port: int = port
        self.user: str = user
        self.password: str = password
        self.database: str = database

        self.logger: Logger = logger

        self.connection: Connection = None

        self.queries: dict[str, Query] = {}
        self.listeners: list[Listener] = []
    
    def __getattr__(self, name: str) -> list[dict[str, object]]:
        return self.get_query(name)

    def get_query(self, name: str) -> Query:
        if name in self.queries:
            return self.queries[name]
        else:
            raise Exception(f'Error: query "{name}" could not be found!')

    def include_router(self, router: SQLRouter):

        router.database = self
        
        for listener in router.listeners:
            self.include_listener(listener)

    def include_query(self, name: str, query: str):
        if name in self.queries:
            raise Exception('Error: multiple queries with the same alias!')
        else:
            self.queries[name] = Query(self, name, query)

    def include_queries(self, path: str, prefix: str = None):

        for item in listdir(path):

            name, extension = splitext(item)
            identifier: str = (prefix + '_' if prefix else '') + name.lower()

            if isfile(f'{path}/{item}'):
                if extension.lower() == '.sql':
                    self.queries[identifier] = Query(self, identifier, File(f'{path}/{item}').read_text())

            if isdir(f'{path}/{item}'):
                self.include_queries(f'{path}/{item}', prefix = identifier)

    def include_listener(self, listener: Listener):
        listener.database = self
        self.listeners.append(listener)

    async def start(self):
        self.connection = await connect(host = self.host, port = self.port, user = self.user, password = self.password, database = self.database)
    
    async def stop(self):
        await self.connection.close()
    
    async def prepare(self):
        for query in self.queries.values():
            await query.prepare()
    
    async def listen(self):
        for listener in self.listeners:
            await listener.listen()

    async def _prepare(self, query: str) -> PreparedStatement:
        return await self.connection.prepare(query)

    async def _execute(self, statement: PreparedStatement, parameters: list[object]):
        transaction: Transaction = self.connection.transaction()
        try:
            await transaction.start()
            records: list = await statement.fetch(*parameters)
        except PostgresError as error:
            await transaction.rollback()
            return None
        else:
            await transaction.commit()

        return [{key: value for key, value in record.items()} for record in records]

    async def _listen(self, channel: str, callback: Callable[[dict[str, object] | str], Awaitable]):
        
        async def handle(connection, pid, chanel, payload):
            self.logger.debug(f'Recieved event on "{channel}".')
            try:
                await callback(loads(payload))
            except JSONDecodeError:
                await callback(payload)

        await self.connection.add_listener(channel, handle)