import asyncio
import logging
import os
import socket
import sys
import uuid

from sipyco.asyncio_tools import atexit_register_coroutine

log = logging.getLogger(__name__)


class LocalWorkerManager:

    def __init__(self, server, verbose):
        self._id = None
        self._server = server
        self._verbose = verbose
        self._process = None

    @property
    def id(self):
        return self._id

    def save_state(self):
        return {"id": self._id}

    def restore_state(self, state):
        self._id = state["id"]

    async def _start(self):

        cmd = [
            sys.executable, "-m", "artiq.frontend.artiq_worker_manager",
            "--id", self._id,
            "--parent", f"artiq_dashboard:{os.getpid()}",
            socket.gethostname(), self._server,
        ]
        if self._verbose:
            cmd.append("-" + "v" * self._verbose)

        self._process = await asyncio.create_subprocess_exec(*cmd)

    async def _stop(self):
        self._process.terminate()
        try:
            await asyncio.wait_for(self._process.wait(), 5)
        except asyncio.TimeoutError:
            log.error("Local worker manager didn't exit from terminate")
            self._process.kill()

    def start(self):
        if self._id is None:
            self._id = str(uuid.uuid4())

        asyncio.get_event_loop().run_until_complete(self._start())

        atexit_register_coroutine(self._stop)
