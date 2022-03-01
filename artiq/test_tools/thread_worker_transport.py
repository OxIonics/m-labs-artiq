import asyncio
from contextlib import ExitStack, contextmanager
import logging
import threading
from typing import Any, AsyncIterator, Optional, Tuple
import uuid

from sipyco import logging_tools, pyon

from artiq.compiler import import_cache
from artiq.master import worker_impl
from artiq.master.worker_transport import WorkerTransport
from artiq.queue_tools import iterate_queue
from artiq.test_tools import thread_pipe_ipc

logger = logging.getLogger(__name__)


class ThreadExit(RuntimeError):
    pass


class BadThreadAccess(RuntimeError):
    pass


class LoggingCapture(logging.Handler):

    def __init__(self, loop, queue: asyncio.Queue):
        super().__init__()
        self._loop = loop
        self._queue = queue

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            f = asyncio.run_coroutine_threadsafe(self._queue.put(msg), self._loop)
            f.result(1)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


@contextmanager
def capture_worker_logs(thread_name, level, queue: asyncio.Queue):
    def ignoreWorkerLogs(record):
        return record.threadName != thread_name

    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(ignoreWorkerLogs)

    handler = LoggingCapture(asyncio.get_running_loop(), queue)
    handler.addFilter(lambda record: record.threadName == thread_name)
    handler.setLevel(level)
    handler.setFormatter(logging_tools.MultilineFormatter())
    root_logger.addHandler(handler)

    try:
        yield
    finally:
        root_logger.removeHandler(handler)
        for handler in root_logger.handlers:
            handler.removeFilter(ignoreWorkerLogs)
        queue.put_nowait('')


async def aempty() -> AsyncIterator[Any]:
    """ An empty async iterator
    """
    for _ in []:
        yield


_done_import_hook = False


def install_import_hook():
    """Install the import_cache's import hook if it's not already been done

    This should be done before importing any experiment code.
    """
    global _done_import_hook
    if not _done_import_hook:
        import_cache.install_hook()
        _done_import_hook = True


def _worker_trampoline(address, log_level):
    try:
        worker_impl.main([
            "--skip-log-config",
            "--skip-import-hook",
            address,
            str(log_level)
        ])
    except SystemExit as ex:
        raise RuntimeError(f"System exit: {ex}")


class ThreadWorkerTransport(WorkerTransport):
    """ Worker in a thread

    An alternative Worker transport that runs the
    worker in the current process meaning that you can continue
    interactive debugger sessions in to the experiment.
    """
    def __init__(self):
        self._thread: Optional[threading.Thread] = None
        self._exit = ExitStack()
        self._stopped = False
        self.ipc: Optional[thread_pipe_ipc.AsyncioParentComm] = None

    async def create(self, rid, log_level) -> Tuple[AsyncIterator[str], AsyncIterator[str]]:
        install_import_hook()
        # A different uuid from the one we use to identify the worker else where
        # :sad_face:
        thread_name = f"worker-{uuid.uuid4()}"
        log_queue = asyncio.Queue(100)
        self.ipc = thread_pipe_ipc.AsyncioParentComm()
        await self.ipc.connect()
        self._exit.enter_context(capture_worker_logs(thread_name, log_level, log_queue))
        self._thread = threading.Thread(
            name=thread_name,
            target=lambda: _worker_trampoline(
                self.ipc.get_address(),
                log_level,
            ),
            # If we can't stop the thread in close we don't want it to prevent
            # the process exiting,
            daemon=True,
        )
        self._thread.start()

        return (
            aempty(),
            iterate_queue(log_queue)
        )

    async def send(self, msg: str):
        self.ipc.write((msg + "\n").encode())
        await self.ipc.drain()

    async def recv(self):
        return await self.ipc.readline()

    async def close(self, term_timeout, rid):
        loop = asyncio.get_event_loop()
        try:
            if self.ipc is None:
                # Note the %s - self.rid can be None or a user string
                logger.debug("worker was not created (RID %s)", rid)
                return

            if self._stopped:
                logger.debug(f"worker already terminated (RID {rid})")
                return

            self._stopped = True

            if not self._thread.is_alive():
                logger.warning(f"worker had ended unexpectedly (RID {rid})")
                return

            try:
                await asyncio.wait_for(
                    self.send(pyon.encode({"action": "terminate"})),
                    timeout=term_timeout,
                )
                await asyncio.wait_for(loop.run_in_executor(
                    None, lambda: self._thread.join(term_timeout),
                ), timeout=term_timeout+1)
                if self._thread.is_alive():
                    raise RuntimeError("Timed out joining thread after sending terminate")
                logger.info("worker exited on request (RID %s)", rid)
                return
            except Exception:
                logger.warning(
                    "worker failed to exit on request (RID %s)", rid,
                    exc_info=True
                )

            # We can't stop the thread
            # I tried hacks like this https://gist.github.com/liuw/2407154
            # which I've used successfully in the past. The docs for
            # PyThreadState_SetAsyncExc
            # https://docs.python.org/3/c-api/init.html#c.PyThreadState_SetAsyncExc
            # suggest that there's some protection to prevent naive mis-use
            # which maybe is new and prevents us calling this through ctypes.

        finally:
            self._exit.close()
