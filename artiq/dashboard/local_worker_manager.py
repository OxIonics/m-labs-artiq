import asyncio
from enum import Enum
import logging
import os
import socket

import sys
import uuid

from sipyco.asyncio_tools import atexit_register_coroutine

from artiq.gui.models import ModelSubscriber

log = logging.getLogger(__name__)


class LocalWorkerManagerStatus(Enum):
    not_started = 0
    running = 1
    failed = 2
    conflict = 3


class LocalWorkerManager:

    def __init__(
            self,
            server: str,
            verbose: int,
            worker_managers_sub: ModelSubscriber,
    ):
        self._id = None
        self._server = server
        self._verbose = verbose
        self._process = None
        self._task = None
        self._do_start = asyncio.Event()
        self.status = LocalWorkerManagerStatus.not_started
        self._on_status_changed = []
        self._worker_managers = None
        worker_managers_sub.add_setmodel_callback(self._set_worker_managers)
        worker_managers_sub.notify_cbs.append(self._worker_managers_changed)

    @property
    def id(self):
        return self._id

    def save_state(self):
        return {"id": self._id}

    def restore_state(self, state):
        self._id = state["id"]

    def _set_worker_managers(self, model):
        self._worker_managers = model
        if self.status == LocalWorkerManagerStatus.not_started and self._task is not None:
            self._check_start()

    def _check_start(self):
        if self._id in self._worker_managers:
            self._status_changed(LocalWorkerManagerStatus.conflict)
            conflict = self._worker_managers[self._id]
            log.warning(
                f"Found conflicting worker manager in initial data. "
                f"id: {self._id}, desc: {conflict['description']} "
                f"repo_root: {conflict.get('repo_root', '<unknown>')} "
                f"metadata: {conflict.get('metadata')}"
            )
        else:
            self._do_start.set()

    def _worker_managers_changed(self, mod):
        conflict = self._worker_managers.get(self._id)
        if (
            self.status == LocalWorkerManagerStatus.conflict
            and conflict is None
        ):
            log.info("Conflict resolved starting local worker manager")
            self._status_changed(LocalWorkerManagerStatus.not_started)
            self._do_start.set()
        elif (
            conflict is not None
            and self.status == LocalWorkerManagerStatus.failed
        ):
            self._status_changed(LocalWorkerManagerStatus.conflict)
            log.warning(
                f"Conflicting worker manager connected whilst we were failed "
                f"id: {self._id}, desc: {conflict['description']} "
                f"repo_root: {conflict.get('repo_root', '<unknown>')} "
                f"metadata: {conflict.get('metadata')}"
            )

    async def _run(self):
        try:
            while True:
                await self._do_start.wait()
                self._do_start.clear()

                try:
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
                    log.log(level, f"Local worker manager exited with code {returncode}")
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("Unhandled error with local worker manager")

                self._status_changed(LocalWorkerManagerStatus.failed)

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

        if self._worker_managers is not None:
            self._check_start()

    def restart(self):
        if self.status != LocalWorkerManagerStatus.failed:
            raise RuntimeError(
                "Only restart the local worker manager if it's failed",
            )
        self._do_start.set()

    def get_conflict_info(self):
        if self.status != LocalWorkerManagerStatus.conflict:
            raise RuntimeError(
                "No conflict",
            )
        return self._worker_managers[self._id]
