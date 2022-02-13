import asyncio
from typing import AsyncIterator


async def iterate_queue(queue: asyncio.Queue) -> AsyncIterator:
    """ Iterate over the contents of the queue

    None must be used to indicate the end of the data. When
    this function sees None it will return.

    Args:
        queue: A queue
    """
    while True:
        x = await queue.get()
        if x is None:
            break
        yield x
