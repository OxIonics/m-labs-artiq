import asyncio
from asyncio import iscoroutine
from contextlib import AsyncExitStack
import logging

import time
import uuid

import pytest

from artiq.master.worker_managers import WorkerManagerDB
from artiq.worker_manager.worker_manager import WorkerManager

BIND = "127.0.0.1"


@pytest.fixture()
async def worker_manager_db():
    async def handle_connection(reader, writer):
        await instance.handle_connection(reader, writer)

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
        )
        exit.push_async_callback(worker_manager.close)
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        yield worker_manager


async def wait_for(check, *, timeout=1, period=0.01, exc=(AssertionError,)):
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
    if iscoroutine(check):
        async def check_wrapper():
            return await asyncio.wait_for(
                check,
                timeout=max(deadline - time.time(), 0.01),
            )
    else:
        async def check_wrapper():
            return check()

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
        worker_manager = await WorkerManager.create(
            BIND, worker_manager_port, manager_id, description,
        )
        exit.push_async_callback(worker_manager.close)

        await wait_for(lambda: assert_num_connection(worker_manager_db))

        assert worker_manager_db._worker_managers.keys() == {manager_id}
        worker_manager_proxy = worker_manager_db._worker_managers[manager_id]
        assert worker_manager_proxy._description == description


async def test_worker_manager_create_worker(worker_manager_db, worker_manager):
    transport = worker_manager_db.get_transport(worker_manager.id)

    await wait_for(transport.create(logging.DEBUG))

    assert worker_manager._workers.keys() == {transport._id}

