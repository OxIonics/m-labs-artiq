import sys
import os
import asyncio
import logging
import time

from sipyco import pyon
from sipyco.packed_exceptions import current_exc_packed

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport
from artiq.tools import asyncio_wait_or_cancel
from enum import Enum, unique


logger = logging.getLogger(__name__)


@unique
class RunResult(Enum):
    completed = 0
    running = 1
    idle = 2


@unique
class ResumeAction(Enum):
    check_still_idle = "check_still_idle"
    pause = "pause"
    resume = "resume"
    request_termination = "request_termination"


class WorkerTimeout(Exception):
    pass


class WorkerWatchdogTimeout(Exception):
    pass


class WorkerError(Exception):
    pass


class WorkerInternalException(Exception):
    """Exception raised inside the worker, information has been printed
    through logging."""
    pass


def log_worker_exception():
    exc, _, _ = sys.exc_info()
    if exc is WorkerInternalException:
        logger.debug("worker exception details", exc_info=True)
    else:
        logger.error("worker exception details", exc_info=True)


class Worker:
    def __init__(self, handlers=dict(), send_timeout=10.0):
        self._transport: WorkerTransport = PipeWorkerTransport()
        self.handlers = handlers
        self.send_timeout = send_timeout

        self.rid = None
        self.filename = None
        self.watchdogs = dict()  # wid -> expiration (using time.monotonic)

        self.closed = asyncio.Event()

    def create_watchdog(self, t):
        n_user_watchdogs = len(self.watchdogs)
        if -1 in self.watchdogs:
            n_user_watchdogs -= 1
        avail = set(range(n_user_watchdogs + 1)) \
            - set(self.watchdogs.keys())
        wid = next(iter(avail))
        self.watchdogs[wid] = time.monotonic() + t
        return wid

    def delete_watchdog(self, wid):
        del self.watchdogs[wid]

    def watchdog_time(self):
        if self.watchdogs:
            return min(self.watchdogs.values()) - time.monotonic()
        else:
            return None

    def _get_log_source(self):
        return "worker({},{})".format(self.rid, self.filename)

    async def _create_process(self, log_level):
        if self.closed.is_set():
            raise WorkerError("Attempting to create process after close")
        await self._transport.create(log_level, self._get_log_source)

    async def close(self, term_timeout=2.0):
        """Interrupts any I/O with the worker process and terminates the
        worker process.

        This method should always be called by the user to clean up, even if
        build() or examine() raises an exception."""
        self.closed.set()
        await self._transport.close(term_timeout, self.rid)

    async def _send(self, obj, cancellable=True):
        line = pyon.encode(obj)
        ifs = [self._transport.send(line)]
        if cancellable:
            ifs.append(self.closed.wait())
        fs = await asyncio_wait_or_cancel(
            ifs, timeout=self.send_timeout,
            return_when=asyncio.FIRST_COMPLETED)
        if all(f.cancelled() for f in fs):
            raise WorkerTimeout(
                "Timeout sending data to worker (RID {})".format(self.rid))
        for f in fs:
            if not f.cancelled() and f.done():
                f.result()  # raise any exceptions
        if cancellable and self.closed.is_set():
            raise WorkerError(
                "Data transmission to worker cancelled (RID {})".format(
                    self.rid))

    async def _recv(self, timeout):
        fs = await asyncio_wait_or_cancel(
            [self._transport.recv(), self.closed.wait()],
            timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if all(f.cancelled() for f in fs):
            raise WorkerTimeout(
                "Timeout receiving data from worker (RID {})".format(self.rid))
        if self.closed.is_set():
            raise WorkerError(
                "Receiving data from worker cancelled (RID {})".format(
                    self.rid))
        line = fs[0].result()
        if not line:
            raise WorkerError(
                "Worker ended while attempting to receive data (RID {})".
                format(self.rid))
        try:
            obj = pyon.decode(line.decode())
        except:
            raise WorkerError("Worker sent invalid PYON data (RID {})".format(
                self.rid))
        return obj

    async def _handle_worker_requests(self):
        while True:
            try:
                obj = await self._recv(self.watchdog_time())
            except WorkerTimeout:
                raise WorkerWatchdogTimeout
            action = obj["action"]
            if action == "completed":
                return RunResult.completed
            elif action == "idle":
                is_idle = obj["kwargs"]["is_idle"]
                return RunResult.idle if is_idle else RunResult.running
            elif action == "pause":
                return RunResult.running
            elif action == "exception":
                raise WorkerInternalException
            elif action == "create_watchdog":
                func = self.create_watchdog
            elif action == "delete_watchdog":
                func = self.delete_watchdog
            elif action == "register_experiment":
                func = self.register_experiment
            else:
                func = self.handlers[action]
            try:
                if asyncio.iscoroutinefunction(func):
                    data = (await func(*obj["args"], **obj["kwargs"]))
                else:
                    data = func(*obj["args"], **obj["kwargs"])
                reply = {"status": "ok", "data": data}
            except:
                reply = {
                    "status": "failed",
                    "exception": current_exc_packed()
                }
            await self._send(reply)

    async def _worker_action(self, obj, timeout=None):
        if timeout is not None:
            self.watchdogs[-1] = time.monotonic() + timeout
        try:
            await self._send(obj)
            try:
                completed = await self._handle_worker_requests()
            except WorkerTimeout:
                raise WorkerWatchdogTimeout
        finally:
            if timeout is not None:
                del self.watchdogs[-1]
        return completed

    async def build(self, rid, pipeline_name, wd, expid, priority,
                    timeout=15.0):
        self.rid = rid
        self.filename = os.path.basename(expid["file"])
        await self._create_process(expid["log_level"])
        await self._worker_action(
            {"action": "build",
             "rid": rid,
             "pipeline_name": pipeline_name,
             "wd": wd,
             "expid": expid,
             "priority": priority},
            timeout)

    async def prepare(self):
        await self._worker_action({"action": "prepare"})

    async def run(self):
        run_result = await self._worker_action({"action": "run"})
        if run_result != RunResult.completed:
            self.yield_time = time.monotonic()
        return run_result

    async def resume(self, resume_action):
        stop_duration = time.monotonic() - self.yield_time
        for wid in self.watchdogs.items():
            self.watchdogs[wid] += stop_duration
        timeout = None
        if resume_action == ResumeAction.check_still_idle:
            timeout = 1.0
        run_result = await self._worker_action({
            "status": "ok",
            "data": resume_action.value
        }, timeout)
        if not run_result:
            self.yield_time = time.monotonic()
        return run_result

    async def analyze(self):
        await self._worker_action({"action": "analyze"})

    async def examine(self, rid, file, timeout=20.0):
        self.rid = rid
        self.filename = os.path.basename(file)

        await self._create_process(logging.WARNING)
        r = dict()

        def register(class_name, name, arginfo, argument_ui, scheduler_defaults):
            r[class_name] = {"name": name, "arginfo": arginfo,
                             "scheduler_defaults": scheduler_defaults,
                             "argument_ui": argument_ui}
        self.register_experiment = register
        await self._worker_action({"action": "examine", "file": file},
                                  timeout)
        del self.register_experiment
        return r
