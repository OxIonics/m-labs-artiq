import asyncio
from collections import Callable
from typing import Dict

from sipyco import pyon

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport


class _WorkerState:

    def __init__(self, transport, msg_task):
        self.transport: WorkerTransport = transport
        self.msg_task = msg_task


class WorkerManager:

    @classmethod
    async def create(
            cls, host, port, worker_manager_id, description,
            transport_factory=PipeWorkerTransport,
    ) -> "WorkerManager":
        reader, writer = await asyncio.open_connection(host, port)
        instance = cls(
            worker_manager_id, description, reader, writer,
            transport_factory,
        )
        instance.start()
        await instance.send_hello()
        return instance

    def __init__(
            self, worker_manager_id, description, reader, writer,
            transport_factory,
    ):
        self._id = worker_manager_id
        self._description = description
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._transport_factory: Callable[[], WorkerTransport] = transport_factory
        self._task = None
        self._workers: Dict[str, WorkerTransport] = {}
        self._new_worker = asyncio.Queue(10)

    @property
    def id(self):
        return self._id

    async def _send(self, obj):
        self._writer.write(pyon.encode(obj).encode() + b"\n")
        await self._writer.drain()

    async def send_hello(self):
        await self._send({
            "action": "hello",
            "manager_id": self._id,
            "manager_description": self._description
        })

    def start(self):
        self._task = asyncio.create_task(self.process_master_msgs())

    async def process_master_msgs(self):
        while True:
            line = await self._reader.readline()
            if not line:
                break
            asyncio.create_task(self._handle_msg(line))

    async def _handle_msg(self, line):
        obj = pyon.decode(line.decode())
        action = obj["action"]
        if action == "create_worker":
            await self._create_worker(obj)
        elif action == "worker_msg":
            worker_id = obj["worker_id"]
            worker = self._workers[worker_id]
            await worker.send(obj["msg"])
        else:
            # TODO signal error to master
            raise RuntimeError(f"Unknown action {action}")

    async def _process_worker_msgs(self, worker_id, transport: WorkerTransport):
        while True:
            msg = await transport.recv()
            if not msg:
                # TODO signal worker exit
                break
            await self._send({
                "action": "worker_msg",
                "worker_id": worker_id,
                "msg": msg,
            })

    async def _create_worker(self, obj):
        worker_id = obj["worker_id"]
        print(f"Creating worker {worker_id}")
        worker = self._transport_factory()
        (stdout, stderr) = await worker.create(obj["log_level"])

        asyncio.create_task(self._process_worker_msgs(worker_id, worker))
        # TODO: start task to forward stdout and stderr

        self._workers[worker_id] = worker
        await self._send({
            "action": "worker_created",
            "worker_id": worker_id
        })

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()
