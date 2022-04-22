import asyncio
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from typing import AsyncIterator, Tuple
import uuid

import pytest

from artiq.master.worker_managers import ManagedWorkerTransport
from artiq.master.worker_transport import WorkerTransport
from artiq.queue_tools import iterate_queue
from artiq.test.consts import BIND
from artiq.test.helpers import assert_num_connection, wait_for
from artiq.worker_manager.worker_manager import WorkerManager


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

    def close_unexpectedly(self):
        self.send_queue.put_nowait("")


class FakeWorkerTransport(WorkerTransport):

    def __init__(self):
        self.worker = FakeWorker()

    async def create(self, rid, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        return (
            iterate_queue(self.worker.stdout_queue),
            iterate_queue(self.worker.stderr_queue)
        )

    async def send(self, msg: str):
        self.worker.received.append(msg)

    async def recv(self):
        return await self.worker.send_queue.get()

    async def close(self, term_timeout, rid):
        self.worker.closed = True
        self.worker.send_queue.put_nowait("")
        self.worker.stdout_queue.put_nowait(None)
        self.worker.stderr_queue.put_nowait(None)


@pytest.fixture()
async def worker_manager(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with WorkerManager.context(
        BIND, worker_manager_port, manager_id, description,
        transport_factory=FakeWorkerTransport
    ) as worker_manager:
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        yield worker_manager


@dataclass
class ConnectedWorker:
    master: ManagedWorkerTransport
    worker: FakeWorker
    forwarded_stdout: AsyncIterator[str]
    forwarded_stderr: AsyncIterator[str]


@pytest.fixture()
async def worker_pair(worker_manager_db, worker_manager):
    transport = worker_manager_db.get_transport(worker_manager.id)
    (stdout, stderr) = await transport.create("test", logging.DEBUG)
    worker = worker_manager._workers[transport._id].transport.worker
    return ConnectedWorker(transport, worker, stdout, stderr)


async def test_worker_manager_connection(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with AsyncExitStack() as exit:
        print(f"Making WorkerManager connecting to {worker_manager_port}")
        worker_manager = await wait_for(WorkerManager.create(
            BIND, worker_manager_port, manager_id, description,
        ))
        exit.push_async_callback(lambda: wait_for(worker_manager.stop))

        await wait_for(lambda: assert_num_connection(worker_manager_db))

        assert worker_manager_db._worker_managers.keys() == {manager_id}
        worker_manager_proxy = worker_manager_db.get_proxy(manager_id)
        assert worker_manager_proxy.description == description


async def test_worker_manager_create_worker(worker_manager_db, worker_manager):
    transport = worker_manager_db.get_transport(worker_manager.id)

    await wait_for(transport.create("test", logging.DEBUG))

    assert worker_manager._workers.keys() == {transport._id}


async def test_send_message_to_worker(worker_pair: ConnectedWorker):
    msg = "Some message would normally be pyon"

    await wait_for(worker_pair.master.send(msg))

    await wait_for(worker_pair.worker.assert_recv, msg)


async def test_send_message_from_worker(worker_pair: ConnectedWorker):
    msg = "Some message would normally be pyon"

    worker_pair.worker.send(msg)

    actual = await wait_for(worker_pair.master.recv())

    assert msg == actual


async def test_forward_stdout_from_worker_to_master(worker_pair: ConnectedWorker):
    stdout = "Some stdout"

    worker_pair.worker.put_stdout(stdout)

    actual = await wait_for(anext(worker_pair.forwarded_stdout))

    assert stdout == actual


async def test_forward_stderr_from_worker_to_master(worker_pair: ConnectedWorker):
    stderr = "Some stderr"

    worker_pair.worker.put_stderr(stderr)

    actual = await wait_for(anext(worker_pair.forwarded_stderr))

    assert stderr == actual


async def atake(iter, n):
    rv = []
    for _ in range(n):
        rv.append(await anext(iter))
    return rv


async def test_forward_blank_lines_from_worker_to_master(worker_pair: ConnectedWorker):
    stderr1 = "Some stderr"
    stderr2 = "Some more"

    worker_pair.worker.put_stderr(stderr1)
    worker_pair.worker.put_stderr("")
    worker_pair.worker.put_stderr(stderr2)

    actual = await wait_for(atake(worker_pair.forwarded_stderr, 3))

    assert actual == [
        stderr1, "", stderr2,
    ]


async def acollect(iter):
    return [x async for x in iter]


async def test_termination_of_forwarding_from_worker_to_master(worker_pair: ConnectedWorker):
    last_err = "Last error message"

    worker_pair.worker.put_stderr(last_err)
    worker_pair.worker.put_stderr(None)

    actual = await wait_for(acollect(worker_pair.forwarded_stderr))

    assert actual == [last_err]


async def test_closing_worker(worker_pair: ConnectedWorker):

    await wait_for(worker_pair.master.close(1, 10))

    assert worker_pair.worker.closed


async def test_worker_closing_unexpectedly_forwarded(worker_pair: ConnectedWorker):

    worker_pair.worker.close_unexpectedly()

    actual = await wait_for(worker_pair.master.recv())

    assert actual == ""


def assert_disconnected(proxy):
    assert not proxy.connected


async def test_late_worker_create_and_then_close(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport
    ) as worker_manager:
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        proxy = worker_manager_db.get_proxy(worker_manager.id)
        transport = proxy.get_transport()

    # Ensure that the proxy has noticed that the worker manager has gone away.
    await wait_for(lambda: assert_disconnected(proxy))

    with pytest.raises(Exception) as exc_info:
        await transport.create("test", logging.DEBUG)

    logging.info("create failed as expected", exc_info=exc_info.value)

    await transport.close(1.0, "test")
    # Assert no raise in transport.close

    # Defer assertions about exception raised by create until after the more
    # import implicit assertion that transport.close is successful
    assert isinstance(exc_info.value, RuntimeError)


async def test_duplicate_worker_manager_connection_rejected(worker_manager_db, worker_manager_port):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        async with WorkerManager.context(
                BIND, worker_manager_port, manager_id, description,
                transport_factory=FakeWorkerTransport
        ) as worker_manager2:

            await wait_for(worker_manager2.wait_for_exit())
            # Assert wait for exit returns promptly with no
            # exception


async def test_tracks_worker_manager_connection_time(
        worker_manager_db, worker_manager_port
):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    start = datetime.now()
    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        end = datetime.now()

        proxy = worker_manager_db.get_proxy(manager_id)
        assert start <= proxy.connection_time <= end


def assert_disconnection_time(proxy):
    assert proxy.disconnection_time is not None


def assert_connection_later_than(proxy, ts):
    assert proxy.connection_time >= ts


async def test_tracks_worker_manager_disconnection_time(
        worker_manager_db, worker_manager_port
):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=timedelta(seconds=1),
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        proxy = worker_manager_db.get_proxy(manager_id)
        assert proxy.disconnection_time is None

        start = datetime.now()
    await wait_for(lambda: assert_disconnection_time(proxy))
    end = datetime.now()

    assert start <= proxy.disconnection_time <= end
    assert worker_manager_db.notifier.raw_view[manager_id]["connected"] is False
    assert datetime.fromisoformat(
        worker_manager_db.notifier.raw_view[manager_id]["disconnection_time"]
    ) == proxy.disconnection_time


async def test_allows_reconnection(
        worker_manager_db, worker_manager_port
):
    manager_id = str(uuid.uuid4())
    description = "Test workers"

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=timedelta(seconds=1),
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        proxy = worker_manager_db.get_proxy(manager_id)

    await wait_for(lambda: assert_disconnection_time(proxy))

    start = datetime.now()
    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport
    ):
        await wait_for(lambda: assert_connection_later_than(proxy, start))
        end = datetime.now()

        assert start <= proxy.connection_time <= end
        assert worker_manager_db.notifier.raw_view[manager_id]["connected"] is True
        assert datetime.fromisoformat(
            worker_manager_db.notifier.raw_view[manager_id]["connection_time"]
        ) == proxy.connection_time


def assert_proxy_removed(manager_id, worker_manager_db):
    assert manager_id not in worker_manager_db.notifier.raw_view


async def test_disposes_of_proxy_when_worker_manager_has_been_disconnected(
        worker_manager_db, worker_manager_port,
):

    manager_id = str(uuid.uuid4())
    description = "Test workers"
    reconnect_timeout = timedelta(seconds=0.5)

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout,
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        start = datetime.now()

    assert manager_id in worker_manager_db.notifier.raw_view

    await wait_for(
        lambda: assert_proxy_removed(manager_id, worker_manager_db),
        timeout=reconnect_timeout.total_seconds() * 1.5
    )

    assert (datetime.now() - start).total_seconds() == pytest.approx(
        reconnect_timeout.total_seconds(),
        rel=0.1,
    )


async def test_disposes_of_proxies_in_order(
        worker_manager_db, worker_manager_port,
):

    reconnect_timeout = timedelta(seconds=0.5)

    async with WorkerManager.context(
            BIND, worker_manager_port,
            description="Mgr1",
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout,
    ) as mgr1:
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        start1 = datetime.now()

    await asyncio.sleep(0.02)

    async with WorkerManager.context(
            BIND, worker_manager_port,
            description="Mgr2",
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout,
    ) as mgr2:
        await wait_for(lambda: assert_num_connection(worker_manager_db, 2))
        start2 = datetime.now()

    assert mgr1.id in worker_manager_db.notifier.raw_view
    await wait_for(
        lambda: assert_proxy_removed(mgr1.id, worker_manager_db),
        timeout=reconnect_timeout.total_seconds()
    )
    end1 = datetime.now()

    assert mgr2.id in worker_manager_db.notifier.raw_view
    await wait_for(
        lambda: assert_proxy_removed(mgr2.id, worker_manager_db),
        timeout=reconnect_timeout.total_seconds()
    )
    end2 = datetime.now()

    assert (end1 - start1).total_seconds() == pytest.approx(
        reconnect_timeout.total_seconds(),
        rel=0.1,
    )
    assert (end2 - start2).total_seconds() == pytest.approx(
        reconnect_timeout.total_seconds(),
        rel=0.1,
    )


async def test_diposes_of_proxies_in_revers_order(
        worker_manager_db, worker_manager_port,
):
    reconnect_timeout1 = timedelta(seconds=1)
    reconnect_timeout2 = timedelta(seconds=0.5)

    async with WorkerManager.context(
            BIND, worker_manager_port,
            description="Mgr1",
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout1,
    ) as mgr1:
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        start1 = datetime.now()

    await asyncio.sleep(0.02)

    async with WorkerManager.context(
            BIND, worker_manager_port,
            description="Mgr2",
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout2,
    ) as mgr2:
        await wait_for(lambda: assert_num_connection(worker_manager_db, 2))
        start2 = datetime.now()

    assert mgr2.id in worker_manager_db.notifier.raw_view
    await wait_for(
        lambda: assert_proxy_removed(mgr2.id, worker_manager_db),
        timeout=reconnect_timeout2.total_seconds() * 1.5
    )
    end2 = datetime.now()

    assert mgr1.id in worker_manager_db.notifier.raw_view
    await wait_for(
        lambda: assert_proxy_removed(mgr1.id, worker_manager_db),
        timeout=reconnect_timeout1.total_seconds()
    )
    end1 = datetime.now()

    assert (end1 - start1).total_seconds() == pytest.approx(
        reconnect_timeout1.total_seconds(),
        rel=0.1,
    )
    assert (end2 - start2).total_seconds() == pytest.approx(
        reconnect_timeout2.total_seconds(),
        rel=0.1,
    )


async def test_doesnt_dispose_of_reconnected_proxies(
        worker_manager_db, worker_manager_port,
):
    manager_id = str(uuid.uuid4())
    description = "Test workers"
    reconnect_timeout = timedelta(seconds=0.2)

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout,
    ):
        await wait_for(lambda: assert_num_connection(worker_manager_db))
        proxy = worker_manager_db.get_proxy(manager_id)

    await wait_for(lambda: assert_disconnected(proxy))
    assert manager_id in worker_manager_db.notifier.raw_view

    async with WorkerManager.context(
            BIND, worker_manager_port, manager_id, description,
            transport_factory=FakeWorkerTransport,
            reconnect_timeout=reconnect_timeout,
    ):
        await asyncio.sleep(reconnect_timeout.total_seconds() * 2)
        assert manager_id in worker_manager_db.notifier.raw_view
