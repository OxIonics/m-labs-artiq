import asyncio
from typing import Dict

from sipyco import pyon

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport


class WorkerManager:

    @classmethod
    async def create(
            cls, host, port, worker_manager_id, description,
    ) -> "WorkerManager":
        reader, writer = await asyncio.open_connection(host, port)
        instance = cls(worker_manager_id, description, reader, writer)
        instance.start()
        await instance.send_hello()
        return instance

    def __init__(self, worker_manager_id, description, reader, writer):
        self._id = worker_manager_id
        self._description = description
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._task = None
        self._workers: Dict[str, WorkerTransport] = {}

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
        self._task = asyncio.create_task(self.run())

    async def run(self):
        while True:
            line = await self._reader.readline()
            obj = pyon.decode(line.decode())
            action = obj["action"]
            if action == "create_worker":
                asyncio.create_task(self._create_worker(obj))
            else:
                # TODO signal error to master
                raise RuntimeError(f"Unknown action {action}")

    async def _create_worker(self, obj):
        worker = PipeWorkerTransport()
        (stdout, stderr) = await worker.create(obj["log_level"])

        # TODO: start task to forward worker requests
        # TODO: start task to forward stdout and stderr

        worker_id = obj["worker_id"]
        self._workers[worker_id] = worker
        await self._send({
            "action": "worker_created",
            "worker_id": worker_id
        })

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()
