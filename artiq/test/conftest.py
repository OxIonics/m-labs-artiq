import asyncio

import pytest

from artiq.master.worker_managers import WorkerManagerDB
from artiq.test.consts import BIND


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
