import asyncio
from asyncio import iscoroutine
import time

from artiq.master.worker_managers import WorkerManagerDB


async def wait_for(check, *args, timeout=1, period=0.01, exc=(AssertionError,)):
    """ Test utility to wait for an assertion to pass

    function:
    ```
    def check():
        assert x == 1

    wait_for(check, timeout=2)
    ```

    Args:
        check: The function to wait to pass. It should raise an exception if
            it's not ready yet.
        timeout: How long to wait
        period: How long to wait between check invocations
        exc: A tuple of exceptions. Exceptions not in this tuple will
            propagate immediately. Exceptions in this tuple will not be
            propagated unless the timeout has been hit.

    Returns:
        The return value of check if any.
    """
    async def check_wrapper():
        if not iscoroutine(check):
            value = check(*args)
            if iscoroutine(value):
                check_ = value
            else:
                return value
        else:
            check_ = check
        return await asyncio.wait_for(
            check_,
            timeout=max(deadline - time.time(), 0.01),
        )

    deadline = time.time() + timeout
    while time.time() < deadline:
        iteration_start = time.time()
        try:
            return await check_wrapper()
        except exc:
            elapsed = time.time() - iteration_start
            if elapsed < period:
                await asyncio.sleep(period - elapsed)

    # one last try. Let the exception propagate this time
    return await check_wrapper()


def assert_num_connection(worker_manager_db: WorkerManagerDB, num=1):
    assert len(worker_manager_db._worker_managers) >= num


def dummy_get_device(key, resolve_alias=False):
    raise KeyError(key)


DUMMY_WORKER_HANDLERS = {
    "get_device": dummy_get_device,
    "store_results": lambda t, f, d: None,
}

