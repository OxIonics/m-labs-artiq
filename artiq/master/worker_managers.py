from __future__ import annotations

import asyncio
from typing import Dict
import uuid

from sipyco import pyon
from sipyco.logging_tools import LogParser

from artiq.master.worker_transport import WorkerTransport


class WorkerManagerDB:

    @classmethod
    async def create(cls, host, port) -> WorkerManagerDB:
        async def handle_connection(reader, writer):
            await instance.handle_connection(reader, writer)

        server = await asyncio.start_server(
            handle_connection,
            host, port,
        )
        instance = cls(server)
        return instance

    def __init__(self, server):
        self._worker_managers: Dict[str, WorkerManagerProxy] = {}
        self._server: asyncio.AbstractServer = server

    def get_ports(self):
        return [
            socket.getsockname()[1]
            for socket in self._server.sockets
        ]

    async def handle_connection(
            self,
            reader: asyncio.StreamReader,
            writer: asyncio.StreamWriter,
    ):
        hello = pyon.decode(await reader.readline())
        if hello["action"] != "hello":
            # TODO: close connection etc.
            raise RuntimeError(
                f"Unexpected action {hello['action']}, expecting hello"
            )

        self._worker_managers[hello["manager_id"]] = WorkerManagerProxy(
            hello["manager_id"],
            hello["manager_description"],
            reader,
            writer,
        )

    def get_transport(self, worker_manager_id):
        return ManagedWorkerTransport(
            self._worker_managers[worker_manager_id],
            str(uuid.uuid4()),
        )


class _ManagedWorkerState:
    def __init__(self, log_source_cb):
        self.created = asyncio.get_event_loop().create_future()
        # If there's more than one item in this queue then that's quite
        # unexpected. It probably means that the `Worker` object and scheduling
        # machinery are getting quite behind.
        self.recv_queue = asyncio.Queue(10)
        self.stdout_parser = LogParser(log_source_cb)
        self.stderr_parser = LogParser(log_source_cb)
        self.closed = asyncio.get_event_loop().create_future()


class WorkerManagerProxy:

    worker_messages = {
        "worker_created",
        "worker_msg",
        "worker_stdout",
        "worker_stderr",
        "worker_exited",
        "worker_error",
    }

    def __init__(self, id_, description, reader, writer):
        self._id = id_
        self._description = description
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._run_task = asyncio.create_task(self._run())
        self._workers: Dict[str, _ManagedWorkerState] = {}

    async def _run(self):
        while True:
            line = await self._reader.readline()
            obj = pyon.decode(line.decode())
            action = obj["action"]

            if action in self.worker_messages:
                self._worker_action(obj)
            else:
                raise RuntimeError(f"Unexpected action {action}")

    def _worker_action(self, obj):
        action = obj["action"]
        worker_id = obj["worker_id"]
        state = self._workers[worker_id]

        if action == "worker_created":
            state.created.set_result(None)
        elif action == "worker_error":
            if state.created.done():
                # TODO probably signal an error through the recv queue
                pass
            else:
                state.created.set_exception(RuntimeError())

    async def _send(self, obj):
        self._writer.write(pyon.encode(obj).encode() + b"\n")
        await self._writer.drain()

    async def create_worker(self, worker_id, log_level, log_source_cb):
        self._workers[worker_id] = _ManagedWorkerState(log_source_cb)
        await self._send({
            "action": "create_worker",
            "worker_id": worker_id,
            "log_level": log_level,
        })
        await self._writer.drain()
        await self._workers[worker_id].created

    async def worker_msg(self, worker_id, msg: str):
        await self._send({
            "action": "worker_msg",
            "worker_id": worker_id,
            "msg": msg,
        })

    async def worker_recv(self, worker_id):
        return await self._workers[worker_id].recv_queue.get()

    async def close_worker(self, worker_id, term_timeout, rid):
        await self._send({
            "action": "worker_msg",
            "worker_id": worker_id,
        })
        await self._writer.drain()
        await self._workers[worker_id].closed


class ManagedWorkerTransport(WorkerTransport):

    def __init__(self, proxy: WorkerManagerProxy, id: str):
        self._proxy = proxy
        self._id = id

    async def create(self, log_level, log_source_cb):
        return await self._proxy.create_worker(self._id, log_level, log_source_cb)

    async def send(self, msg: str):
        return await self._proxy.worker_msg(self._id, msg)

    async def recv(self):
        return await self._proxy.worker_recv(self._id)

    async def close(self, term_timeout, rid):
        return await self._proxy.close_worker(self._id, term_timeout, rid)
