from typing import Any, Callable, Awaitable

from os import listdir
from os.path import splitext, isdir, isfile
from pathlib import Path as File

from asyncpg.connection import connect, Connection
from asyncpg.prepared_stmt import PreparedStatement
from asyncpg.transaction import Transaction
from asyncpg.exceptions import PostgresError

from json import loads

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
    
    async def __call__(self, **parameters: dict[str, object]) -> object | list[object] | dict[str, object] | list[dict[str, object]]:
        return await self.execute(parameters)

    def _format_query(self) -> None:

        self.formatted = self.query

        self.parameters = list(dict.fromkeys([item[1] for item in formatter.parse(self.query) if item[1]]))
        for index, parameter in enumerate(self.parameters):
            self.formatted = self.formatted.replace('{' + parameter + '}', '$' + str(index + 1))

    async def prepare(self) -> None:
        self.database.logger.debug(f'Preparing query "{self.name}" for a later use.')
        self.statement = await self.database._prepare(self.formatted)

    async def execute(self, parameters: dict[str, object]) -> object | list[object] | dict[str, object] | list[dict[str, object]]:
        self.database.logger.debug(f'Executing query "{self.name}".')
        return await self.database._execute(self.statement, [parameters.get(parameter, None) for parameter in self.parameters])

class SQLRouter:

    def __init__(self) -> None:
        self.database: SQLClient = None
        self.parent: SQLRouter = None
        self.listeners: dict[str, list[Callable[[object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]]] = dict()

    def __getattr__(self, name: str) -> list[dict[str, object]]:
        return self.get_query(name)

    def get_query(self, name: str) -> Query:
        if self.parent:
            return self.parent.get_query(name)
        elif self.database:
            return self.database.get_query(name)
        else:
            raise Exception(f'Error: router is not directly or indirectly connected to database!')

    def listen(self, event: str) -> Callable[[str], Callable[[dict | str], None]]:
        def decorate(callback: Callable[[dict | str], None]) -> None:
            if event in self.listeners:
                self.listeners[event].append(callback)
            else:
                self.listeners[event] = [callback]
        return decorate
    
    def include_router(self, router: "SQLRouter") -> None:
        router.parent = self
        self.listeners = self.listeners + router.listeners

    def include_routers(self, routers: list["SQLRouter"]):
        for router in routers:
            self.include_router(router)

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

        self.events: set[str] = set()
        self.listeners: dict[str, list[Callable[[object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]]] = dict()
        self.pipes: list[Callable[[object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]] = list()
    
    def __getattr__(self, name: str) -> list[dict[str, object]]:
        return self.get_query(name)

    def get_query(self, name: str) -> Query:
        if name in self.queries:
            return self.queries[name]
        else:
            raise Exception(f'Error: query "{name}" could not be found!')

    def register_event(self, event: str):
        self.events.add(event)

    def register_events(self, events: list[str]):
        for event in events:
            self.register_event(event)

    def include_pipe(self, pipe: Callable[[str, object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]):
        self.pipes.append(pipe)

    def include_pipes(self, pipes: list[Callable[[str, object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]]):
        for pipe in pipes:
            self.include_pipe(pipe)

    def include_router(self, router: SQLRouter):
        router.database = self
        for event, listeners in router.listeners.items():
            self.events.add(event)
            self.listeners[event] = self.listeners.get(event, []) + listeners

    def include_routers(self, routers: list[SQLRouter]):
        for router in routers:
            self.include_router(router)

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

    async def start(self):

        self.connection: Connection = await connect(host = self.host, port = self.port, user = self.user, password = self.password, database = self.database)

        for query in self.queries.values():
            await query.prepare()

        for event, listeners in self.listeners.items():
            for listener in listeners:
                await self._listen(event, listener)

        for pipe in self.pipes:
            await self._pipe(pipe)

        await self._log()

    async def stop(self):
        await self.connection.close()
    
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
            if len(records) == 1:
                if len(records[0]) == 1:
                    return records[0][0]
                if len(records[0]) > 1:
                    return {name: value for name, value in records[0].items()}
            if len(records) > 1:
                if len(records[0]) == 1:
                    return [record[0] for record in records]
                if len(records[0]) > 1:
                    return [{name: value for name, value in record.items()} for record in records]

    async def _listen(self, event: str, callback: Callable[[object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]):
        
        async def handle(connection, pid, event, payload):
            try:
                await callback(loads(payload))
            except ValueError:
                await callback(payload)

        await self.connection.add_listener(event, handle)

    async def _pipe(self, callback: Callable[[str, object | list[object] | dict[str, object] | list[dict[str, object]]], Awaitable]):

        async def handle(connection, pid, event, payload):
            try:
                await callback(event, loads(payload))
            except ValueError:
                await callback(event, payload)

        for event in self.events:
            await self.connection.add_listener(event, handle)

    async def _log(self):

        async def handle(connection, pid, event, payload):
            self.logger.info(f'Event "{event}" was recieved.')

        for event in self.events:
            await self.connection.add_listener(event, handle)