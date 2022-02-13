import asyncio
from asyncio import iscoroutine
from contextlib import AsyncExitStack
from dataclasses import dataclass
import logging
import time
from typing import AsyncIterator, Tuple
import uuid

import pytest

from artiq.master.worker_managers import ManagedWorkerTransport, WorkerManagerDB
from artiq.master.worker_transport import WorkerTransport
from artiq.queue_tools import iterate_queue
from artiq.worker_manager.worker_manager import WorkerManager

BIND = "127.0.0.1"

try:
    # anext not defined until python 3.10
    # https://bugs.python.org/issue31861
    anext
except NameError:
    # this is an example definition from that issue
    def anext(iterator):
        if not isinstance(iterator, AsyncIterator):
            raise TypeError(
                '%r object is not an asynchronous iterator'
                % (type(iterator).__name__,)
            )

        return iterator.__anext__()


class FakeWorker:

    def __init__(self):
        self.received = []
        self.closed = False
        self.send_queue = asyncio.Queue()
        self.stdout_queue = asyncio.Queue()
        self.stderr_queue = asyncio.Queue()

    def send(self, msg):
        self.send_queue.put_nowait(msg)

    def put_stdout(self, data: str):
        self.stdout_queue.put_nowait(data)

    def put_stderr(self, data: str):
        self.stderr_queue.put_nowait(data)

    async def assert_recv(self, msg):
        assert len(self.received) > 0
        assert self.received[-1] == msg


class FakeWorkerTransport(WorkerTransport):

    def __init__(self):
        self.worker = FakeWorker()

    async def create(self, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        return (
            iterate_queue(self.worker.stdout_queue),
            iterate_queue(self.worker.stderr_queue)
        )

    async def send(self, msg: str):
        self.worker.received.append(msg)

    async def recv(self):
        return await self.worker.send_queue.get()

    async def close(self, term_timeout, rid):
        self.closed = True


@pytest.fixture()
async def worker_manager_db():
    async def handle_connection(reader, writer):
        await instance.handle_connection(reader, writer)

    print("Making WorkerManagerDB")
    server = await asyncio.start_server(
        handle_connection,
        BIND,
    )
    async with server:
        instance = WorkerManagerDB(server)
        yield instance


@pytest.fixture()
def worker_manager_port(worker_manager_db: WorkerManagerDB):
    return worker_manager_db.get_ports()[0]


@pytest.fixture()
async def worker_manager(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with AsyncExitStack() as exit:
        worker_manager = await WorkerManager.create(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport
        )
        exit.push_async_callback(worker_manager.close)
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        yield worker_manager


@dataclass
class WorkerPair:
    master: ManagedWorkerTransport
    worker: FakeWorker
    forwarded_stdout: AsyncIterator[str]
    forwarded_stderr: AsyncIterator[str]


@pytest.fixture()
async def worker_pair(worker_manager_db, worker_manager):
    transport = worker_manager_db.get_transport(worker_manager.id)
    (stdout, stderr) = await transport.create(logging.DEBUG)
    worker = cast(FakeWorkerTransport, worker_manager._workers[transport._id])
    return WorkerPair(transport, worker.worker, stdout, stderr)


async def wait_for(check, *args, timeout=1, period=0.01, exc=(AssertionError,)):
    """ Test utility to wait for an assertion to pass

    function:
    ```
    def check():
        assert x == 1

    wait_for(check, timeout=2)
    ```

    Args:
        check: The function to wait to pass. It should raise an exception if
            it's not ready yet.
        timeout: How long to wait
        period: How long to wait between check invocations
        exc: A tuple of exceptions. Exceptions not in this tuple will
            propagate immediately. Exceptions in this tuple will not be
            propagated unless the timeout has been hit.

    Returns:
        The return value of check if any.
    """
    async def check_wrapper():
        if not iscoroutine(check):
            value = check(*args)
            if iscoroutine(value):
                check_ = value
            else:
                return value
        else:
            check_ = check
        return await asyncio.wait_for(
            check_,
            timeout=max(deadline - time.time(), 0.01),
        )

    deadline = time.time() + timeout
    while time.time() < deadline:
        iteration_start = time.time()
        try:
            return await check_wrapper()
        except exc:
            elapsed = time.time() - iteration_start
            if elapsed < period:
                await asyncio.sleep(period - elapsed)

    # one last try. Let the exception propagate this time
    return await check_wrapper()


def assert_num_connection(worker_manager_db, num=1):
    assert len(worker_manager_db._worker_managers) >= num


async def test_worker_manager_connection(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with AsyncExitStack() as exit:
        print(f"Making WorkerManager connecting to {worker_manager_port}")
        worker_manager = await wait_for(WorkerManager.create(
            BIND, worker_manager_port, manager_id, description,
        ))
        exit.push_async_callback(worker_manager.close)

        await wait_for(lambda: assert_num_connection(worker_manager_db))

        assert worker_manager_db._worker_managers.keys() == {manager_id}
        worker_manager_proxy = worker_manager_db._worker_managers[manager_id]
        assert worker_manager_proxy._description == description


async def test_worker_manager_create_worker(worker_manager_db, worker_manager):
    transport = worker_manager_db.get_transport(worker_manager.id)

    await wait_for(transport.create(logging.DEBUG))

    assert worker_manager._workers.keys() == {transport._id}


async def test_send_message_to_worker(worker_pair: WorkerPair):
    msg = "Some message would normally be pyon"

    await wait_for(worker_pair.master.send(msg))

    await wait_for(worker_pair.worker.assert_recv, msg)


async def test_send_message_from_worker(worker_pair: WorkerPair):
    msg = "Some message would normally be pyon"

    worker_pair.worker.send(msg)

    actual = await wait_for(worker_pair.master.recv())

    assert msg == actual


async def test_forward_stdout_from_worker_to_master(worker_pair: WorkerPair):
    stdout = "Some stdout"

    worker_pair.worker.put_stdout(stdout)

    actual = await wait_for(anext(worker_pair.forwarded_stdout))

    assert stdout == actual


async def test_forward_stderr_from_worker_to_master(worker_pair: WorkerPair):
    stderr = "Some stderr"

    worker_pair.worker.put_stderr(stderr)

    actual = await wait_for(anext(worker_pair.forwarded_stderr))

    assert stderr == actual
