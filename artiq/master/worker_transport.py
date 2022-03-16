import asyncio
import logging
import os
import subprocess
from typing import AsyncIterator, Tuple

import sys

from sipyco import pipe_ipc, pyon

logger = logging.getLogger(__name__)


class WorkerTransport:

    async def create(
            self, rid: str, log_level: int
    ) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        raise NotImplementedError()

    async def send(self, msg: str):
        raise NotImplementedError()

    async def recv(self):
        raise NotImplementedError()

    async def close(self, term_timeout: float, rid: str):
        raise NotImplementedError()

    def description(self):
        raise NotImplementedError()


async def _output_to_iter(input: AsyncIterator[bytes]) -> AsyncIterator[str]:
    async for line in input:
        yield line.decode().rstrip("\r\n")


class PipeWorkerTransport(WorkerTransport):

    def __init__(self):
        self.ipc = None

    async def create(self, rid, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        ipc = pipe_ipc.AsyncioParentComm()
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        await ipc.create_subprocess(
            sys.executable, "-m", "artiq.master.worker_impl",
            ipc.get_address(), str(log_level),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, start_new_session=True)
        self.ipc = ipc
        logger.info(f"Created worker process pid={ipc.process.pid} (RID {rid})")
        return (
            _output_to_iter(ipc.process.stdout),
            _output_to_iter(ipc.process.stderr),
        )

    async def close(self, term_timeout, rid):
        if self.ipc is None:
            # Note the %s - self.rid can be None or a user string
            logger.debug("worker was not created (RID %s)", rid)
            return
        if self.ipc.process.returncode is not None:
            logger.debug("worker already terminated (RID %s)", rid)
            if self.ipc.process.returncode != 0:
                logger.warning("worker finished with status code %d"
                               " (RID %s)", self.ipc.process.returncode,
                               rid)
            return
        try:
            await asyncio.wait_for(
                self.send(pyon.encode({"action": "terminate"})),
                timeout=term_timeout,
            )
            await asyncio.wait_for(self.ipc.process.wait(), term_timeout)
            logger.debug("worker exited on request (RID %s)", rid)
            return
        except:
            logger.debug("worker failed to exit on request"
                         " (RID %s), ending the process", rid,
                         exc_info=True)
        if os.name != "nt":
            try:
                self.ipc.process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.ipc.process.wait(),
                                       term_timeout)
                logger.debug("worker terminated (RID %s)", rid)
                return
            except asyncio.TimeoutError:
                logger.warning(
                    "worker did not terminate (RID %s), killing", rid)
        try:
            self.ipc.process.kill()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(self.ipc.process.wait(), term_timeout)
            logger.debug("worker killed (RID %s)", rid)
            return
        except asyncio.TimeoutError:
            logger.warning("worker refuses to die (RID %s)", rid)

    async def send(self, msg: str):
        self.ipc.write((msg + "\n").encode())
        await self.ipc.drain()

    async def recv(self):
        return await self.ipc.readline()

    def description(self):
        return "builtin"
