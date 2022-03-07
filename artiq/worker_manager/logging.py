import asyncio
import collections
import logging
import traceback


class ForwardHandler(logging.Handler):

    def __init__(self, loop, send):
        super().__init__()
        self._loop = loop
        self._send = send
        self._queue = collections.deque()
        self._discarded = 0
        self._pending_send = None
        self._send_lock = asyncio.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        # This is called with self.lock held
        try:
            if len(self._queue) >= 50:
                self._discarded += 1
                return
            args = {
                "msg": record.getMessage(),
                "levelno": record.levelno,
                "name": record.name,
            }
            self._queue.append(args)
            if self._pending_send is None:
                self._pending_send = asyncio.run_coroutine_threadsafe(self._do_send(), self._loop)
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)

    async def _do_send(self):
        # First we lock _send_lock, which forces us to wait for any in progress
        # send operations to complete. Then we grab all the queued messages and
        # reset the pending send. Then whilst we send these messages if another
        # log line is emitted that will start another send operation, that will
        # send all the messages accumulated, whilst this invocation was sending
        # its batch of messages.
        try:
            async with self._send_lock:
                with self.lock:
                    discarded = self._discarded
                    to_send = list(self._queue)
                    self._discarded = 0
                    self._queue = collections.deque()
                    self._pending_send = None

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
        except Exception:
            traceback.print_exc()
            raise


def init_log_forwarding(send):
    root_logger = logging.getLogger()
    handler = ForwardHandler(asyncio.get_event_loop(), send)
    # Don't forward the worker logs using this mechanism, they're already
    # handled specially.
    handler.addFilter(lambda record: getattr(record, "source", None) is None)
    root_logger.addHandler(handler)
