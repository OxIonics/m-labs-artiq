# A copy of sipyco.pipe_ipc.AsyncioParentComm
# with out the baggage of assuming that someone wants a new process
#
# Source:
# https://github.com/m-labs/sipyco/blob/8e4382352bc64bd01c9db35d9c9b0ef42b8b9d3b/sipyco/pipe_ipc.py

import os
import asyncio
from asyncio.streams import FlowControlMixin


__all__ = ["AsyncioParentComm"]


class _BaseIO:
    def write(self, data):
        self.writer.write(data)

    async def drain(self):
        await self.writer.drain()

    async def readline(self):
        return await self.reader.readline()

    async def read(self, n):
        return await self.reader.read(n)


if os.name != "nt":
    async def _fds_to_asyncio(rfd, wfd, loop):
        reader = asyncio.StreamReader(loop=loop, limit=100*1024*1024)
        reader_protocol = asyncio.StreamReaderProtocol(reader, loop=loop)
        rf = open(rfd, "rb", 0)
        rt, _ = await loop.connect_read_pipe(lambda: reader_protocol, rf)

        wf = open(wfd, "wb", 0)
        wt, _ = await loop.connect_write_pipe(FlowControlMixin, wf)
        writer = asyncio.StreamWriter(wt, reader_protocol, None, loop)

        return rt, reader, writer


    class AsyncioParentComm(_BaseIO):
        def __init__(self):
            self.c_rfd, self.p_wfd = os.pipe()
            self.p_rfd, self.c_wfd = os.pipe()

        def get_address(self):
            return "{},{}".format(self.c_rfd, self.c_wfd)

        async def connect(self):
            loop = asyncio.get_event_loop()

            self.reader_transport, self.reader, self.writer = \
                await _fds_to_asyncio(self.p_rfd, self.p_wfd, loop)


else:  # windows
    import itertools


    _pipe_count = itertools.count()


    class AsyncioParentComm:
        """Requires ProactorEventLoop"""
        def __init__(self):
            # We cannot use anonymous pipes on Windows, because we do not know
            # in advance if the child process wants a handle open in overlapped
            # mode or not.
            self.address = "\\\\.\\pipe\\sipyco-{}-{}".format(os.getpid(),
                                                              next(_pipe_count))
            self.ready = asyncio.Event()
            self.write_buffer = b""

        def get_address(self):
            return self.address

        async def create_subprocess(self, *args, **kwargs):
            loop = asyncio.get_event_loop()

            def factory():
                reader = asyncio.StreamReader(loop=loop, limit=100*1024*1024)
                protocol = asyncio.StreamReaderProtocol(reader,
                                                        self._child_connected,
                                                        loop=loop)
                return protocol
            self.server = await loop.start_serving_pipe(
                factory, self.address)

        def _child_connected(self, reader, writer):
            # HACK: We should shut down the pipe server here.
            # However, self.server[0].close() is racy, and will cause an
            # invalid handle error if loop.start_serving_pipe has not finished
            # its work in the background.
            # The bug manifests itself here frequently as the event loop is
            # reopening the server as soon as a new client connects.
            # There is still a race condition in the AsyncioParentComm
            # creation/destruction, but it is unlikely to cause problems
            # in most practical cases.
            if self.ready.is_set():
                # A child already connected before. We should have shut down
                # the server, but asyncio won't let us do that.
                # Drop connections immediately instead.
                writer.close()
                return
            self.reader = reader
            self.writer = writer
            if self.write_buffer:
                self.writer.write(self.write_buffer)
                self.write_buffer = b""
            self.ready.set()

        def write(self, data):
            if self.ready.is_set():
                self.writer.write(data)
            else:
                self.write_buffer += data

        async def drain(self):
            await self.ready.wait()
            await self.writer.drain()

        async def readline(self):
            await self.ready.wait()
            return await self.reader.readline()

        async def read(self, n):
            await self.ready.wait()
            return await self.reader.read(n)
