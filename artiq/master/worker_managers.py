from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import logging
from typing import Awaitable, Callable, Dict, List, Optional
import uuid

from sipyco import pyon
from sipyco.logging_tools import log_with_name
from sipyco.sync_struct import Notifier

from artiq.master.worker_transport import OutputHandler, WorkerTransport
from artiq.tools import shutdown_and_drain

log = logging.getLogger(__name__)


class WorkerManagerDB:

    @classmethod
    async def create(cls, host, port, limit=8 * 1024 * 1024) -> WorkerManagerDB:
        async def handle_connection(reader, writer):
            await instance.handle_connection(reader, writer)

        server = await asyncio.start_server(
            handle_connection,
            host, port,
            limit=limit,
        )
        instance = cls(server)
        return instance

    def __init__(self, server):
        self._worker_managers: Dict[str, WorkerManagerProxy] = {}
        self._server: asyncio.AbstractServer = server

        # For notifying clients of changes to worker manager connections. This must be
        # suitable for PYON serialisation.
        # Modifications to self._worker_managers must also modify this notifier.
        self.notifier: Notifier = Notifier({})
        self._reconnection_timeout_handle: Optional[asyncio.TimerHandle] = None
        self._next_worker_manager_to_dispose: Optional[WorkerManagerProxy] = None

    def _dispose_of_worker_manager(self, mgr):
        del self._worker_managers[mgr.id]
        del self.notifier[mgr.id]
        if mgr is not None:
            mgr.dispose()

    def _update_connection_state(self, manager_id):
        mgr = self._worker_managers[manager_id]
        self.notifier[manager_id] = mgr.get_info()
        if not mgr.connected:
            self._check_disconnecting_worker_manager_is_next_to_dispose(mgr)
        elif manager_id == self._next_worker_manager_to_dispose.id:
            self._reconnection_timeout_handle.cancel()
            self._find_next_worker_manager_to_dispose()

    def _check_disconnecting_worker_manager_is_next_to_dispose(self, mgr):
        if self._reconnection_timeout_handle is None:
            pass
        elif mgr.disposal_time < self._next_worker_manager_to_dispose.disposal_time:
            self._reconnection_timeout_handle.cancel()
        else:
            return

        self._set_next_worker_manager_to_dispose(mgr)

    def _find_next_worker_manager_to_dispose(self):
        disconnected_worker_managers = [
            mgr
            for mgr in self._worker_managers.values()
            if not mgr.connected
        ]
        if disconnected_worker_managers:
            mgr = min(
                disconnected_worker_managers,
                key=lambda mgr: mgr.disposal_time
            )
            self._set_next_worker_manager_to_dispose(mgr)
        else:
            self._next_worker_manager_to_dispose = None
            self._reconnection_timeout_handle = None

    def _set_next_worker_manager_to_dispose(self, mgr):
        log.info(
            f"Setting next worker manager to be disposed to {mgr.id} "
            f"at {mgr.disposal_time} now: {datetime.now()}"
        )
        self._next_worker_manager_to_dispose = mgr
        self._reconnection_timeout_handle = asyncio.get_event_loop().call_later(
            (mgr.disposal_time - datetime.now()).total_seconds(),
            self._reconnect_timeout,
        )

    def _reconnect_timeout(self):
        log.info(
            f"Worker manager not reconnected in timeout, disposing of "
            f"{self._next_worker_manager_to_dispose.id}",
        )
        self._dispose_of_worker_manager(self._next_worker_manager_to_dispose)
        self._find_next_worker_manager_to_dispose()

    def _create_worker_manager(self, manager_id, hello, reader, writer):
        mgr = self._worker_managers[manager_id] = WorkerManagerProxy(
            manager_id,
            hello,
            reader,
            writer,
        )
        mgr.add_on_disconnect(lambda: self._update_connection_state(manager_id))
        self.notifier[manager_id] = mgr.get_info()
        log.info(
            f"New worker manager connection id={manager_id} "
            f"description={mgr.description} repo_root={mgr.repo_root} "
            f"metadata={mgr.metadata}"
        )

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
        try:
            hello = pyon.decode(await reader.readline())
            if hello["action"] != "hello":
                raise RuntimeError(
                    f"Unexpected action {hello['action']}, expecting hello"
                )

            manager_id = hello["manager_id"]
            existing_proxy = self._worker_managers.get(manager_id)
            if existing_proxy and existing_proxy.connected:
                raise RuntimeError(
                    "Worker manager connection attempt with ID that's already "
                    f"connected. ID {manager_id}. "
                    f"Already in use by {existing_proxy.description} "
                    f"metadata: {existing_proxy.metadata}"
                )
        except Exception as ex:
            log.exception("Failed to handle worker manager connection")
            writer.write(pyon.encode({
                "action": "error",
                "msg": str(ex),
            }).encode())
            await shutdown_and_drain(reader, writer)
            writer.close()
            await writer.wait_closed()
        else:
            if existing_proxy:
                existing_proxy.reconnect(hello, reader, writer)
                log.info(
                    f"Worker manager re-connected id={manager_id} "
                    f"description={existing_proxy.description} "
                    f"repo_root={existing_proxy.repo_root} "
                    f"metadata={existing_proxy.metadata}"
                )
                self._update_connection_state(manager_id)
            else:
                self._create_worker_manager(manager_id, hello, reader, writer)

    def get_proxy(self, worker_manager_id) -> WorkerManagerProxy:
        try:
            return self._worker_managers[worker_manager_id]
        except KeyError:
            raise ValueError(f"Unknown worker_manager_id {worker_manager_id}")

    def get_transport(self, worker_manager_id):
        return self.get_proxy(worker_manager_id).get_transport()

    async def close(self):
        self._server.close()
        await self._server.wait_closed()

        # We could use `gather` or `wait` here.
        # With `gather` we have to spread the arguments, but the first exception
        # is propagated.
        # `wait` accepts an iterable, but we'd have to checks for exceptions
        # manually.
        await asyncio.gather(*[
            proxy.close()
            for proxy in self._worker_managers.values()
        ])


class _ManagedWorkerState:
    def __init__(self, stdout_handler, stderr_handler):
        self.created = asyncio.get_event_loop().create_future()
        # If there's more than one item in this queue then that's quite
        # unexpected. It probably means that the `Worker` object and scheduling
        # machinery are getting quite behind.
        self.recv_queue = asyncio.Queue(10)
        # These need to be bigger because sometimes we get large multiline log
        # message like stack traces. It's nice not to await in the message
        # handling code so we don't to worry that we might block when we have a
        # waiting message.
        self.stdout_handler = stdout_handler
        self.stderr_handler = stderr_handler
        self.closed = asyncio.get_event_loop().create_future()


class WorkerManagerProxy:

    worker_messages = {
        "worker_created",
        "worker_msg",
        "worker_stdout",
        "worker_stderr",
        "worker_closed",
        "worker_error",
    }
    scan_messages = {
        "scan_result",
        "scan_failed",
    }

    def __init__(self, id_, hello, reader, writer):
        self._id = id_
        self.description: str
        self.repo_root: str
        self.metadata: Dict
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._run_task: Optional[asyncio.Task] = None
        self._workers: Dict[str, _ManagedWorkerState] = {}
        self._inprogress_scans: Dict[str, asyncio.Future] = {}
        self._on_disconnect: List[Callable[[], None]] = []
        self._on_dispose: List[Callable[[], None]] = []
        self.connection_time: datetime
        self.disconnection_time: Optional[datetime] = None
        self.reconnect_timeout: timedelta
        self.reconnect(hello, reader, writer)

    @property
    def id(self):
        return self._id

    @property
    def connected(self):
        return self._run_task is not None

    @property
    def disposal_time(self):
        return self.disconnection_time + self.reconnect_timeout

    def reconnect(self, hello, reader, writer):
        if self.connected:
            raise RuntimeError("reconnecting already connected worker manager proxy")
        self.connection_time = datetime.now()
        self.description = hello["manager_description"]
        self.repo_root = hello["repo_root"]
        self.reconnect_timeout = timedelta(
            seconds=hello.get("reconnect_timeout_seconds", 0),
        )
        self.metadata = hello.get("metadata", {})
        self._reader = reader
        self._writer = writer
        self._run_task = asyncio.create_task(self._run())

    def get_transport(self):
        id_ = str(uuid.uuid4())
        log.debug(f"New worker transport {id_} in manager {self._id}")
        return ManagedWorkerTransport(
            self,
            id_,
        )

    async def _run(self):
        initiate_disconnect = False
        try:
            while True:
                line = await self._reader.readline()
                if not line:
                    log.info("Worker manager disconnected")
                    break
                obj = pyon.decode(line.decode())
                action = obj["action"]

                if action in self.worker_messages:
                    await self._worker_action(obj)
                elif action in self.scan_messages:
                    self._scan_action(obj)
                elif action == "manager_log":
                    logs = obj["logs"]
                    for l in logs:
                        log_with_name(
                            l["name"],
                            l["levelno"],
                            l["msg"],
                            extra={"source": f"mgr({self.description})"}
                        )
                else:
                    raise RuntimeError(f"Unexpected action {action}")
        except asyncio.CancelledError:
            log.info("Worker manager proxy stopping")
            initiate_disconnect = True
        except ConnectionError as ex:
            log.exception(f"Connection error: {ex}")
        except Exception:
            # This is signalled to the worker manager by closing
            # the connection and to all ManagedWorkerTransports in `_disconnect`
            log.exception(
                "Unhandled exception in handling message from worker manager",
            )
            initiate_disconnect = True
        finally:
            log.info(f"Shutting down worker manager {self._id}")
            await self._disconnect(initiate_disconnect)

    async def _disconnect(self, initiate):
        self.disconnection_time = datetime.now()
        self._run_task = None
        for cb in self._on_disconnect:
            cb()
        workers = list(self._workers.values())
        self._workers.clear()
        for worker in workers:
            if not worker.created.done():
                worker.created.set_exception(RuntimeError(
                    "WorkerManger closing during create"
                ))
            if not worker.closed.done():
                worker.closed.set_result(None)
        for scan_future in self._inprogress_scans.values():
            scan_future.set_exception(RuntimeError(
                "Worker proxy closed during scan",
            ))
        self._inprogress_scans.clear()
        if workers:
            (_, pending) = await asyncio.wait(
                [worker.recv_queue.put("") for worker in workers],
                timeout=0.5,
                )
            if pending:
                log.warning(
                    f"Failed to put end sentinels in {len(pending)} worker queues"
                )
        if initiate:
            await shutdown_and_drain(self._reader, self._writer)
        self._writer.close()
        await self._writer.wait_closed()
        self._reader = None
        self._writer = None

    async def _worker_action(self, obj):
        action = obj["action"]
        worker_id = obj["worker_id"]
        state = self._workers[worker_id]

        if action == "worker_created":
            state.created.set_result(None)
            log.debug(f"Worker {worker_id} created")
        elif action == "worker_error":
            log.error(f"Worker {worker_id} error: {obj['msg']}")
            if not state.created.done():
                state.created.set_exception(RuntimeError(
                    "Worker failed during create"
                ))
            state.recv_queue.put_nowait("")
        elif action == "worker_msg":
            try:
                state.recv_queue.put_nowait(obj["msg"])
            except asyncio.QueueFull:
                raise RuntimeError(
                    "Receive queue full. The master must be running behind"
                )
        elif action == "worker_stdout":
            # Old clients send a None sentinel at the end that was needed
            # because it allowed a separate task to handle these asynchronously
            # now that we have a synchronous callback this is not useful.
            if obj["data"] is not None:
                await state.stdout_handler(obj["data"])
        elif action == "worker_stderr":
            if obj["data"] is not None:
                await state.stderr_handler(obj["data"])
        elif action == "worker_closed":
            state.closed.set_result(None)
            state.recv_queue.put_nowait("")
            del self._workers[worker_id]
        else:
            raise RuntimeError(f"Unexpected worker action {action}")

    async def _send(self, obj):
        self._writer.write(pyon.encode(obj).encode() + b"\n")
        await self._writer.drain()

    async def create_worker(
            self, worker_id, rid, log_level,
            stdout_handler: OutputHandler,
            stderr_handler: OutputHandler,
    ):
        if not self.connected:
            # It's possible if we're busy that:
            # * receive scheduler.submit, which creates a ManagedWorkerTransport
            #   for experiment RID=X
            # * The connection to the worker manager fails and this is marked as
            #   closed
            # * The experiment RID=X gets to the build stage. build calls this
            # When that happens we need to ensure that `close_worker` can be
            # called successfully. This is achieved by never adding this worker
            # to the `_workers` dict so that `close_worker` short circuits.
            raise RuntimeError(
                f"WorkerManager {self._id} closed cannot create worker "
                f"{worker_id} (RID {rid})"
            )
        state = self._workers[worker_id] = _ManagedWorkerState(
            stdout_handler, stderr_handler
        )
        await self._send({
            "action": "create_worker",
            "worker_id": worker_id,
            "rid": rid,
            "log_level": log_level,
        })
        await self._writer.drain()
        await state.created
        log.info(f"Created worker {worker_id} on manager {self._id} (RID {rid})")

    async def worker_msg(self, worker_id, msg: str):
        await self._send({
            "action": "worker_msg",
            "worker_id": worker_id,
            "msg": msg,
        })

    async def worker_recv(self, worker_id):
        try:
            return await self._workers[worker_id].recv_queue.get()
        except KeyError:
            # If there's no worker here the worker has either been removed
            # because it was closed or the proxy has shutdown its connection
            # to the worker manager.
            log.debug(f"No worker known by id {worker_id} in worker_recv")
            return ""

    async def close_worker(self, worker_id, term_timeout, rid):
        if worker_id not in self._workers:
            log.debug(
                f"Ignoring close request for unknown worker {worker_id}. "
                "Probably never created or already closed"
            )
            return
        await self._send({
            "action": "close_worker",
            "worker_id": worker_id,
            "term_timeout": term_timeout,
            "rid": rid,
        })
        await self._workers[worker_id].closed

    async def scan_dir(self, path):
        # TODO should reject if disconnected
        scan_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self._inprogress_scans[scan_id] = future
        await self._send({
            "action": "scan_dir",
            "scan_id": scan_id,
            "path": path,
        })
        return await future

    def _scan_action(self, obj):
        action = obj["action"]
        scan_id = obj["scan_id"]
        future = self._inprogress_scans.pop(scan_id)
        if action == "scan_result":
            future.set_result(obj)
        elif action == "scan_failed":
            future.set_exception(RuntimeError(obj["msg"]))
        else:
            raise RuntimeError(f"Unexpected scan action {action}")

    async def close(self):
        self._run_task.cancel()
        try:
            await self._run_task
        except asyncio.CancelledError:
            pass

    def add_on_disconnect(self, cb):
        self._on_disconnect.append(cb)

    def add_on_dispose(self, cb):
        self._on_dispose.append(cb)

    def get_info(self):
        if self.disconnection_time is None:
            disconnection_time = None
        else:
            disconnection_time = self.disconnection_time.isoformat()
        return {
            "id": self.id,
            "description": self.description,
            "repo_root": self.repo_root,
            "metadata": self.metadata,
            "connected": self.connected,
            "connection_time": self.connection_time.isoformat(),
            "disconnection_time": disconnection_time,
        }

    def dispose(self):
        for cb in self._on_dispose:
            cb()


class ManagedWorkerTransport(WorkerTransport):

    def __init__(self, proxy: WorkerManagerProxy, id: str):
        self._proxy = proxy
        self._id = id

    async def create(
            self, rid: str, log_level: int,
            stdout_handler: OutputHandler,
            stderr_handler: OutputHandler,
    ):
        return await self._proxy.create_worker(
            self._id, rid, log_level, stdout_handler, stderr_handler,
        )

    async def send(self, msg: str):
        return await self._proxy.worker_msg(self._id, msg)

    async def recv(self):
        return await self._proxy.worker_recv(self._id)

    async def close(self, term_timeout, rid):
        return await self._proxy.close_worker(self._id, term_timeout, rid)

    def description(self):
        return self._proxy.description


class WorkerManagerControl:

    def __init__(self, db: WorkerManagerDB):
        self.db = db

    async def disconnect(self, worker_manager_id):
        await self.db.get_proxy(worker_manager_id).close()
