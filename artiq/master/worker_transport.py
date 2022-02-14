import asyncio
import logging
import os
import subprocess
from typing import AsyncIterator, Tuple

import sys

from sipyco import pipe_ipc, pyon

logger = logging.getLogger(__name__)


class WorkerTransport:

    async def create(self, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        raise NotImplementedError()

    async def send(self, msg: str):
        raise NotImplementedError()

    async def recv(self):
        raise NotImplementedError()

    async def close(self, term_timeout, rid):
        raise NotImplementedError()


async def _decode_iter(input: AsyncIterator[bytes]) -> AsyncIterator[str]:
    async for line in input:
        yield line.decode()


class PipeWorkerTransport(WorkerTransport):

    def __init__(self):
        self.io_lock = asyncio.Lock()
        self.ipc = None

    async def create(self, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        if self.ipc is not None:
            # TODO: Need to avoid this really, I'm not sure that this is
            #   reachable even now. But we need to detect this condition in
            #   Worker and not call create if we're reusing.
            return  # process already exists, recycle
        async with self.io_lock:
            self.ipc = pipe_ipc.AsyncioParentComm()
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            await self.ipc.create_subprocess(
                sys.executable, "-m", "artiq.master.worker_impl",
                self.ipc.get_address(), str(log_level),
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                env=env, start_new_session=True)
            logger.debug(f"Created worker process pid={self.ipc.process.pid}")
            return (
                _decode_iter(self.ipc.process.stdout),
                _decode_iter(self.ipc.process.stderr),
            )

    async def close(self, term_timeout, rid):
        async with self.io_lock:
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
                    self._send(pyon.encode({"action": "terminate"})),
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
        async with self.io_lock:
            await self._send(msg)

    async def _send(self, msg: str):
        assert self.io_lock.locked()
        self.ipc.write((msg + "\n").encode())
        await self.ipc.drain()

    async def recv(self):
        return await self.ipc.readline()
