import asyncio
from enum import Enum
import logging
import os
import socket

import sys
import uuid

from sipyco.asyncio_tools import atexit_register_coroutine

log = logging.getLogger(__name__)


class LocalWorkerManagerStatus(Enum):
    not_started = 0
    running = 1
    failed = 2
    conflict = 3


class LocalWorkerManager:

    def __init__(self, server, verbose):
        self._id = None
        self._server = server
        self._verbose = verbose
        self._process = None
        self._task = None
        self.status = LocalWorkerManagerStatus.not_started
        self._on_status_changed = []

    @property
    def id(self):
        return self._id

    def save_state(self):
        return {"id": self._id}

    def restore_state(self, state):
        self._id = state["id"]

    async def _run(self):
        try:
            while True:
                cmd = [
                    sys.executable, "-m", "artiq.frontend.artiq_worker_manager",
                    "--id", self._id,
                    "--parent", f"artiq_dashboard:{os.getpid()}",
                    socket.gethostname(), self._server,
                ]
                if self._verbose:
                    cmd.append("-" + "v" * self._verbose)

                self._process = await asyncio.create_subprocess_exec(*cmd)
                self._status_changed(LocalWorkerManagerStatus.running)

                returncode = await self._process.wait()
                self._process = None
                if returncode == 0:
                    level = logging.INFO
                else:
                    level = logging.ERROR
                self._status_changed(LocalWorkerManagerStatus.failed)
                log.log(level, f"Local worker manager exited with code {returncode}")

                # TODO restart logic
                break

        finally:
            if self._process:
                self._process.terminate()
                try:
                    await asyncio.wait_for(self._process.wait(), 5)
                except asyncio.TimeoutError:
                    log.error("Local worker manager didn't exit from terminate")
                    self._process.kill()

    async def _stop(self):
        self._task.cancel()
        try:
            await asyncio.wait_for(self._task, 10)
        except asyncio.CancelledError:
            pass

    def on_status_changed(self, cb):
        self._on_status_changed.append(cb)

    def _status_changed(self, status):
        self.status = status
        for cb in self._on_status_changed:
            cb(status)

    def start(self):
        if self._task is not None:
            raise RuntimeError(
                "Don't call LocalWorkerManager.start twice",
            )
        if self._id is None:
            self._id = str(uuid.uuid4())

        self._task = asyncio.create_task(self._run())

        atexit_register_coroutine(self._stop)
