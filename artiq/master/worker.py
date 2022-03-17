import asyncio
from enum import Enum, unique
import logging
import os
import sys
import time
from typing import AsyncIterator

from sipyco import pyon
from sipyco.logging_tools import LogParser
from sipyco.packed_exceptions import current_exc_packed

from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport
from artiq.tools import asyncio_wait_or_cancel

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


class WorkerExit(Exception):
    pass


def log_worker_exception():
    exc, _, _ = sys.exc_info()
    if exc is WorkerInternalException:
        logger.debug("worker exception details", exc_info=True)
    else:
        logger.error("worker exception details", exc_info=True)


async def _iterate_logs(source_cb, stream_name, log_lines: AsyncIterator[str]):
    # Heavily inspired by sipyco.logging_tools.LogParser.stream_task but this
    # will work with any AsyncIterator not just StreamReaders and it expects the
    # iterator to terminate at the end and newlines to already be stripped.
    log_parser = LogParser(source_cb)
    async for entry in log_lines:
        try:
            log_parser.line_input(entry)
        except:
            logger.debug("exception in log forwarding", exc_info=True)
            break
    logger.debug(
        "stopped log forwarding of stream %s of %s",
        stream_name, source_cb()
    )


class Worker:
    def __init__(self, handlers=dict(), transport=None, send_timeout=10.0):
        if transport is None:
            transport = PipeWorkerTransport()
        self._transport: WorkerTransport = transport
        self.handlers = handlers
        self.send_timeout = send_timeout

        self.rid = None
        self.filename = None
        self.watchdogs = dict()  # wid -> expiration (using time.monotonic)

        self._created = False
        self._in_progress_action = None
        self._handle_worker_requests_task = None
        self._pending_exception = None
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
        return "worker({}, {},{})".format(
            self._transport.description(), self.rid, self.filename,
        )

    async def _create_process(self, log_level):
        if self.closed.is_set():
            raise WorkerError("Attempting to create process after close")
        if self._created:
            return  # process already exists, recycle
        (stdout, stderr) = await self._transport.create(self.rid, log_level)

        self._created = True
        self._handle_worker_requests_task = asyncio.create_task(self._handle_worker_requests())
        asyncio.create_task(_iterate_logs(self._get_log_source, "stdout", stdout))
        asyncio.create_task(_iterate_logs(self._get_log_source, "stderr", stderr))

    async def close(self, term_timeout=2.0):
        """Interrupts any I/O with the worker process and terminates the
        worker process.

        This method should always be called by the user to clean up, even if
        build() or examine() raises an exception."""
        self.closed.set()
        # We want to explicitly fail any in progress action here so that
        # there's an explicit message that this is the reason for action
        # failure. Otherwise we might get a worker exited message instead
        # that could be confusing.
        if self._in_progress_action:
            # Raise and catch to stick this line on the attached stack trace
            try:
                raise WorkerError(
                    f"Action cancelled by worker close (RID {self.rid})"
                )
            except WorkerError as ex:
                self._in_progress_action.set_exception(ex)
                self._in_progress_action = None
        await self._transport.close(term_timeout, self.rid)
        # If _handle_worker_requests_task then we didn't complete create
        if self._handle_worker_requests_task is not None:
            # This is likely to be unnecessary, because the send/recv calls will
            # get cancelled or return closed when the underlying transport is
            # closed. But it seems logically coherent to explicitly cancel this
            # here.
            self._handle_worker_requests_task.cancel()
            try:
                await self._handle_worker_requests_task
            except asyncio.CancelledError:
                pass
        if self._pending_exception:
            # This exception is probably irrelevant to the continued functioning
            # of the service but it seems incautious to leave unreported
            # exceptions.
            logger.warning(
                "Pending exception left after worker close",
                exc_info=self._pending_exception,
            )
            self._pending_exception = None

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
            [self._transport.recv()],
            timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        if all(f.cancelled() for f in fs):
            raise WorkerTimeout(
                "Timeout receiving data from worker (RID {})".format(self.rid)
            )
        line = fs[0].result()
        if not line:
            raise WorkerExit
        try:
            obj = pyon.decode(line.decode())
        except:
            raise WorkerError(
                "Worker sent invalid PYON data (RID {})".format(
                    self.rid
                )
            )
        return obj

    def _complete_action(self, result):
        if self._in_progress_action is None:
            raise RuntimeError(
                "Unexpected action completion (RID {})".format(self.rid),
            )
        self._in_progress_action.set_result(result)
        self._in_progress_action = None

    async def _handle_worker_requests(self):
        while True:
            try:
                try:
                    obj = await self._recv(self.watchdog_time())
                except WorkerTimeout:
                    raise WorkerWatchdogTimeout

                action = obj["action"]
                if action in ("completed", "idle", "pause"):

                    if action == "completed":
                        result = RunResult.completed
                    elif action == "idle":
                        is_idle = obj["kwargs"]["is_idle"]
                        result = RunResult.idle if is_idle else RunResult.running
                    elif action == "pause":
                        result = RunResult.running
                    else:
                        raise RuntimeError("unreachable")

                    self._complete_action(result)
                    continue
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
                await self._send(reply, cancellable=False)
            except WorkerExit:
                logger.debug(f"Worker receive loop exiting due to worker exit (RID {self.rid}")
                if self._in_progress_action:
                    # Raise and catch to stick this line on the attached stack trace
                    try:
                        raise WorkerError(
                            f"Worker ended whilst attempting to execute action (RID {self.rid})"
                        )
                    except WorkerError as ex:
                        self._in_progress_action.set_exception(ex)
                        self._in_progress_action = None
                break
            except asyncio.CancelledError:
                logger.debug(
                    f"Worker receive loop exiting due to cancel (RID {self.rid}"
                )
                if self._in_progress_action:
                    # We only expect a CancelledError when the worker is being
                    # closed and we should have already stopped in progress
                    # actions in close.
                    logger.warning(
                        f"Unexpected in-progress action when request handler "
                        f"cancelled (RID {self.rid}"
                    )
                    # Raise and catch to stick this line on the attached stack trace
                    try:
                        raise WorkerError(
                            f"Worker action cancelled because request handler "
                            f"cancelled (RID {self.rid})"
                        )
                    except WorkerError as ex:
                        self._in_progress_action.set_exception(ex)
                        self._in_progress_action = None
                raise
            except Exception as ex:
                if self._in_progress_action is not None:
                    self._in_progress_action.set_exception(ex)
                    self._in_progress_action = None
                else:
                    self._pending_exception = ex

    async def _worker_action(self, obj, timeout=None):
        if self._pending_exception:
            ex = self._pending_exception
            self._pending_exception = None
            raise ex
        if self.closed.is_set():
            raise WorkerError(
                f"Worker is closed can't perform action {obj['action']} (RID {self.rid})",
            )
        if self._in_progress_action:
            raise WorkerError(
                f"Worker may only have one action in progress at a time (RID {self.rid})"
            )
        try:
            self._in_progress_action = asyncio.get_event_loop().create_future()
            await self._send(obj)
            return await asyncio.wait_for(
                self._in_progress_action, timeout=timeout,
            )

        # If either of these exceptions occur we had better close this worker
        # now or else it's going to get very confused about whether it's running
        # an action or not. This has been the case longer than this commenting
        # drawing attention to it.
        except asyncio.CancelledError:
            # _in_progress_action has been cancelled
            self._in_progress_action = None
            raise
        except asyncio.TimeoutError:
            # _in_progress_action has been cancelled
            self._in_progress_action = None
            raise WorkerWatchdogTimeout()

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
