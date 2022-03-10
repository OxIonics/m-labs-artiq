import asyncio
import collections
import logging
import threading

log = logging.getLogger(__name__)
_MAX_MESSAGE_BACKLOG = 50


class ForwardHandler(logging.Handler):

    @classmethod
    def start(cls, send):
        root_logger = logging.getLogger()
        handler = ForwardHandler(send)
        # Don't forward the worker logs using this mechanism, they're already
        # handled specially.
        handler.addFilter(lambda record: getattr(record, "source", None) is None)
        root_logger.addHandler(handler)
        return handler

    def __init__(self, send):
        super().__init__()
        self._loop = asyncio.get_running_loop()
        self._asyncio_thread_id = threading.get_ident()
        self._send = send
        self._queue = collections.deque()
        self._discarded = 0
        self._wake_up = asyncio.Event()
        self._send_task = self._loop.create_task(self._do_send())

    def emit(self, record: logging.LogRecord) -> None:
        # This is only called by Handler.handle with self.lock held. We rely on
        # holding self.lock if we're on a different thread from the asyncio loop
        # to make updates to _discarded and _queue atomic, w.r.t to the code
        # that locks self.lock in _do_send.
        try:
            if len(self._queue) >= _MAX_MESSAGE_BACKLOG:
                self._discarded += 1
                return
            args = {
                "msg": record.getMessage(),
                "levelno": record.levelno,
                "name": record.name,
            }
            self._queue.append(args)
            if threading.get_ident() == self._asyncio_thread_id:
                self._wake_up.set()
            else:
                self._loop.call_soon_threadsafe(self._wake_up.set)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)

    async def _do_send(self):
        # Get batches of log messages to send to the master. `_wake_up` is set
        # if there is at least one pending message. If more that
        # _MAX_MESSAGE_BACKLOG messages are logged in the time it takes us to
        # log once batch of messages and grab the next batch then we log a
        # message indicating that some have been discarded.
        # If we encounter an exception we log it and give up.
        while True:
            await self._wake_up.wait()
            self._wake_up.clear()
            try:
                with self.lock:
                    discarded = self._discarded
                    to_send = list(self._queue)
                    self._discarded = 0
                    self._queue = collections.deque()

                if discarded:
                    to_send.append({
                        "msg": f"Discarded {discarded} log messages",
                        "levelno": logging.ERROR,
                        "name": __name__
                    })

                await self._send({
                    "action": "manager_log",
                    "logs": to_send,
                })
            except asyncio.CancelledError:
                # We should have been removed from the root logger before being
                # cancelled so this won't be queued back here
                log.error(
                    f"ForwardLogger was cancelled during send {len(to_send)} "
                    f"messages discarded"
                )
            except Exception:
                # traceback.print_exc()
                raise

    async def stop(self):
        # assume we're running on the same event loop and same thread
        # we were constructed on and `_do_send` is running on.
        # And that once we've removed the handler there can't be anymore calls
        # to emit on-going. So there's no need to lock anything
        root_logger = logging.getLogger()
        root_logger.removeHandler(self)
        # yield to the event loop and possibly _do_send
        await asyncio.sleep(0.01)
        if self._send_task.cancel():
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass
        else:
            log.error(
                "Logging forwarding failed before stop:",
                exc_info=self._send_task.exception()
            )
        if len(self._queue) > 0 or self._discarded > 0:
            # Is this really reachable? We removed the handler, yielded and then
            # awaited the _send_task finishing. So only if the event loop is running
            # slow.
            log.error(
                f"ForwardLogger stopped with {len(self._queue)} messages "
                f"pending and {self._discarded} messages discarded"
            )
