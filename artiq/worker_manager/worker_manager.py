from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterator, Awaitable, Dict, Callable, List, Optional
import uuid

from sipyco import pyon
from sipyco.logging_tools import LogParser

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport


log = logging.getLogger(__name__)


# Raising SystemExit prevents more async from running.
class GracefulExit(Exception):
    pass


class BackgroundTasks:
    """Run a dynamic set of tasks

    This is somewhat limited take on optional structured concurrency. See
    https://trio.readthedocs.io/en/stable/reference-core.html#nurseries-and-spawning
    for an intro in to the more fully featured form.

    This allows us to keep an eye on a bunch of background tasks whilst running
    a single foreground task.
    """

    def __init__(self):
        self._tasks = set()

    async def run_foreground(self, coro):
        """ Run coro, also propagating exception from any background tasks

        Args:
            coro: Any awaitable, that can be passed to ensure_future

        Returns:
            The return value from coro

        """
        # We must ensure that it's a future here so that we can do an is
        # comparison with the returns from wait.
        coro = asyncio.ensure_future(coro)
        self._tasks.add(coro)
        try:
            while True:
                (done, self._tasks) = await asyncio.wait(
                    self._tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    result = task.result()
                    if task is coro:
                        return result
                    elif result is not None:
                        self._tasks.update(result)
        except:
            self._tasks.discard(coro)
            coro.cancel()
            # Should we wait for coro to respond to the cancellation
            raise

    def add_background(self, coro: Awaitable[Optional[List[Awaitable]]]):
        """Add a background task

        This mustn't be called whilst a `run_foreground` call is in progress.
        `coro` may return additional background tasks to add to this when it
        completes.
        Any exceptions raised by coroutines running in the "background" will
        be propagated through calls to `run_foreground`.

        Args:
            coro: A coro to run in the "background"
        """
        self._tasks.add(asyncio.ensure_future(coro))

    def take_tasks(self):
        tasks = self._tasks
        self._tasks = set()
        return tasks


class _WorkerState:

    def __init__(self, transport, msg_task, stdout_task, stderr_task):
        self.transport: WorkerTransport = transport
        self.msg_task = msg_task
        self.stdout_task = stdout_task
        self.stderr_task = stderr_task


class WorkerManager:

    @classmethod
    async def create(
            cls, host, port, manager_id, description,
            transport_factory=PipeWorkerTransport,
            **kwargs
    ) -> WorkerManager:
        if manager_id is None:
            manager_id = str(uuid.uuid4())
        logging.info(f"Connecting to {host}:{port} with id {manager_id}")
        reader, writer = await asyncio.open_connection(host, port)
        instance = cls(
            manager_id, description, reader, writer,
            transport_factory, **kwargs,
        )
        logging.info(f"Connected, starting processors and sending hello")
        instance.start()
        await instance.send_hello()
        return instance

    def __init__(
            self, worker_manager_id, description, reader, writer,
            transport_factory, *, exit_on_idle=False,
    ):
        self._id = worker_manager_id
        self._description = description
        self._reader: asyncio.StreamReader = reader
        self._writer: asyncio.StreamWriter = writer
        self._transport_factory: Callable[[], WorkerTransport] = transport_factory
        self._task: Optional[asyncio.Task] = None
        self._workers: Dict[str, _WorkerState] = {}
        self._new_worker = asyncio.Queue(10)
        self._exit_on_idle = exit_on_idle
        self.stop_request = asyncio.Event()

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
        """Start the worker manager
        """
        self._task = asyncio.create_task(self.process_master_msgs())

    async def wait_for_exit(self):
        await self._task

    async def stop(self):
        """ Stop the worker manager

        Waits for the teardown to complete
        """
        self._task.cancel()
        await self.wait_for_exit()

    async def process_master_msgs(self):
        tasks = BackgroundTasks()
        try:
            while True:
                line = await tasks.run_foreground(self._reader.readline())
                if not line:
                    log.warning("Connection to master lost")
                    break
                tasks.add_background(self._handle_msg(line))
        except asyncio.CancelledError:
            pass
        finally:
            await self._clean_up()
            tasks = [
                task
                for task in tasks.take_tasks()
                if not task.done()
            ]
            if tasks:
                log.info("Cancelling remaining tasks")
                for task in tasks:
                    task.cancel()
                (_, tasks) = await asyncio.wait(tasks, timeout=0.5)
                if tasks:
                    log.warning(f"{len(tasks)} tasks did not exit")

    async def _handle_msg(self, line):
        obj = pyon.decode(line.decode())
        action = obj["action"]
        if action == "create_worker":
            return await self._create_worker(obj)
        elif action == "worker_msg":
            worker_id = obj["worker_id"]
            log.debug(f"Forwarding master message to {worker_id}")
            worker = self._workers[worker_id]
            await worker.transport.send(obj["msg"])
        elif action == "close_worker":
            worker_id = obj["worker_id"]
            log.info(f"Closing worker {worker_id}")
            worker = self._workers.pop(worker_id)
            await self._close_worker(worker, obj["term_timeout"], obj["rid"])
            await self._send({
                "action": "worker_closed",
                "worker_id": worker_id,
            })
            if self._exit_on_idle and len(self._workers) == 0:
                raise GracefulExit()
        else:
            raise RuntimeError(f"Unknown action {action}")

    async def _process_worker_msgs(self, worker_id, transport: WorkerTransport):
        while True:
            msg = await transport.recv()
            if not msg:
                break
            await self._send({
                "action": "worker_msg",
                "worker_id": worker_id,
                "msg": msg,
            })

        if worker_id in self._workers:
            log.warning(f"Worker {worker_id} exited unexpectedly")
            await self._send({
                "action": "worker_error",
                "worker_id": worker_id,
                "msg": "Worker exited unexpectedly"
            })
        else:
            log.info(f"Worker {worker_id} exited")

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
        log.info(f"Creating worker {worker_id}")
        worker = self._transport_factory()
        (stdout, stderr) = await worker.create(obj["log_level"])

        msg_task = asyncio.create_task(self._process_worker_msgs(worker_id, worker))
        stdout_task = asyncio.create_task(self._process_worker_output(worker_id, "worker_stdout", stdout))
        stderr_task = asyncio.create_task(self._process_worker_output(worker_id, "worker_stderr", stderr))

        self._workers[worker_id] = _WorkerState(
            worker, msg_task, stdout_task, stderr_task
        )
        log.info(f"Created worker {worker_id}")
        await self._send({
            "action": "worker_created",
            "worker_id": worker_id
        })
        return [msg_task, stdout_task, stderr_task]

    async def _close_worker(self, worker, term_timeout, rid):
        await worker.transport.close(term_timeout, rid)
        (_, pending) = await asyncio.wait(
            [worker.msg_task, worker.stdout_task, worker.stderr_task],
            return_when=asyncio.ALL_COMPLETED,
            timeout=0.5,
        )
        if worker.msg_task in pending:
            log.warning("Msg task didn't exit")
        if worker.stdout_task in pending:
            log.warning("Stdout task didn't exit")
        if worker.stderr_task in pending:
            log.warning("Stderr task didn't exit")

    async def _clean_up(self):
        log.info("Closing worker manager")
        workers = list(self._workers.values())
        self._workers.clear()
        if workers:
            log.info("Stopping workers")
            await asyncio.gather(*[
                self._close_worker(worker, 1, "shutdown")
                for worker in workers
            ])

        self._writer.close()
        await self._writer.wait_closed()
