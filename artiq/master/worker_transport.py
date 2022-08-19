import asyncio
import logging
import os
import subprocess
from typing import AsyncIterator, Awaitable, Callable

import sys

from sipyco import pipe_ipc, pyon

logger = logging.getLogger(__name__)
OutputHandler = Callable[[str], Awaitable[None]]


class WorkerTransport:

    async def create(
            self, rid: str, log_level: int,
            stdout_handler: OutputHandler,
            stderr_handler: OutputHandler,
    ):
        """ Create the underlying worker

        Args:
            rid: Request ID for logging purposes
            log_level: The log level to be configured with-in the worker
            stdout_handler: Callback for forwarding a single line of stdout.
            stderr_handler: Callback for stderr, similar to `stdout_handler`

        Returns:

        """
        raise NotImplementedError()

    async def send(self, msg: str):
        raise NotImplementedError()

    async def recv(self):
        raise NotImplementedError()

    async def close(self, term_timeout: float, rid: str):
        raise NotImplementedError()

    def description(self):
        raise NotImplementedError()


async def _output_process(input: AsyncIterator[bytes], handler: OutputHandler):
    async for line in input:
        await handler(line.decode().rstrip("\r\n"))


class PipeWorkerTransport(WorkerTransport):

    def __init__(self):
        self.ipc = None
        self._stdout_task = None
        self._stderr_task = None

    async def create(
            self, rid: str, log_level: int,
            stdout_handler: OutputHandler,
            stderr_handler: OutputHandler,
    ):
        ipc = pipe_ipc.AsyncioParentComm()
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        # We're going to capture the stdout and stderr of the subprocess and
        # decode it with UTF-8. Better make sure it's encoded in UTF-8.
        # On Windows it might not be by default.
        env["PYTHONIOENCODING"] = "utf-8"
        await ipc.create_subprocess(
            sys.executable, "-m", "artiq.master.worker_impl",
            ipc.get_address(), str(log_level),
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            env=env, start_new_session=True)
        self.ipc = ipc
        logger.debug(f"Created worker process pid={ipc.process.pid} (RID {rid})")
        self._stdout_task = asyncio.create_task(
            _output_process(ipc.process.stdout, stdout_handler)
        )
        self._stderr_task = asyncio.create_task(
            _output_process(ipc.process.stderr, stderr_handler)
        )

    async def _stop_process(self, term_timeout, rid):
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

    async def _stop_log_task(self, name, task, rid):
        try:
            await asyncio.wait_for(task, timeout=5)
        except asyncio.TimeoutError:
            logger.warning(
                f"Forwarding task for {name} didn't exit in a timely fasion and "
                f"has been cancelled (RID {rid})"
            )
        except Exception:
            logger.error(
                f"Error in forwarding task {name} (RID {rid})",
                exc_info=True,
            )

    async def close(self, term_timeout: float, rid: str):
        if self.ipc is None:
            # Note the %s - self.rid can be None or a user string
            logger.debug("worker was not created (RID %s)", rid)
            return
        await self._stop_process(term_timeout, rid)
        await asyncio.gather(
            self._stop_log_task("stdout", self._stdout_task, rid),
            self._stop_log_task("stderr", self._stderr_task, rid),
        )

    async def send(self, msg: str):
        self.ipc.write((msg + "\n").encode())
        await self.ipc.drain()

    async def recv(self):
        return await self.ipc.readline()

    def description(self):
        return "builtin"
