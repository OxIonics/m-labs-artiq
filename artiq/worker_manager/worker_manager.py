import asyncio
from collections import Callable
import logging
from typing import AsyncIterator, Dict
import uuid

from sipyco import pyon
from sipyco.logging_tools import LogParser

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport


log = logging.getLogger(__name__)


class _WorkerState:

    def __init__(self, transport, msg_task):
        self.transport: WorkerTransport = transport
        self.msg_task = msg_task


class WorkerManager:

    @classmethod
    async def create(
            cls, host, port, manager_id, description,
            transport_factory=PipeWorkerTransport,
    ) -> "WorkerManager":
        if manager_id is None:
            manager_id = str(uuid.uuid4())
        logging.info(f"Connecting to {host}:{port} with id {manager_id}")
        reader, writer = await asyncio.open_connection(host, port)
        instance = cls(
            manager_id, description, reader, writer,
            transport_factory,
        )
        logging.info(f"Connected, starting processors and sending hello")
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
        try:
            action = obj["action"]
            if action == "create_worker":
                await self._create_worker(obj)
            elif action == "worker_msg":
                log.info(f"Forwarding worker_msg '{obj['msg']}'")
                worker_id = obj["worker_id"]
                worker = self._workers[worker_id]
                await worker.send(obj["msg"])
            else:
                # TODO signal error to master
                raise RuntimeError(f"Unknown action {action}")
        except KeyError as ex:
            log.error(f"Missing key {ex} in msg {obj!r}", exc_info=True)
            # TODO reraise and signal failure to master

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

    async def _process_worker_output(
            self,
            worker_id: str,
            forward_action: str,
            output: AsyncIterator[str],
    ):
        log_parser = LogParser(lambda: worker_id)
        async for entry in output:
            if not entry:
                break
            entry = entry.rstrip("\r\n")
            try:
                log_parser.line_input(entry)
            except:
                log.info("exception in log forwarding", exc_info=True)
                break
            await self._send({
                "action": forward_action,
                "worker_id": worker_id,
                "data": entry,
            })
        log.info(
            "stopped log forwarding of stream %s of %s",
            forward_action, worker_id
        )
        await self._send({
            "action": forward_action,
            "worker_id": worker_id,
            "data": "",
        })

    async def _create_worker(self, obj):
        worker_id = obj["worker_id"]
        print(f"Creating worker {worker_id}")
        worker = self._transport_factory()
        (stdout, stderr) = await worker.create(obj["log_level"])

        asyncio.create_task(self._process_worker_msgs(worker_id, worker))
        asyncio.create_task(self._process_worker_output(worker_id, "worker_stdout", stdout))
        asyncio.create_task(self._process_worker_output(worker_id, "worker_stderr", stderr))

        self._workers[worker_id] = worker
        await self._send({
            "action": "worker_created",
            "worker_id": worker_id
        })

    async def close(self):
        self._writer.close()
        await self._writer.wait_closed()
