import base64
import gzip
from gzip import GzipFile
from io import BytesIO
import logging
import os
import shutil
import time
import zlib

log = logging.getLogger(__name__)


class ResultStore:

    def __init__(self, root):
        self._root = root

    def store(self, start_time_utc, filename, data):
        """ Append to a results file

        This assumes that filename is unique (e.g. contains the rid), and appends
        data to the file

        Args:
            start_time_utc: Time used to create the containing directory as
                seconds since Epoch
            filename: Filename of the file created.
            data: Base64 encoded - Gzipped - HDF5 data to store.
        """
        start_time_utc = time.gmtime(start_time_utc)
        dirname = os.path.join(
            self._root,
            time.strftime("%Y-%m-%d", start_time_utc),
            time.strftime("%H", start_time_utc)
        )
        os.makedirs(dirname, exist_ok=True)
        decoded = zlib.decompress(base64.b64decode(data))
        with open(os.path.join(dirname, filename), "ab") as f:
            f.write(decoded)
            final_size = f.tell()
        log.debug(
            f"Written {len(decoded)}B to results file {filename} "
            f"(total size={final_size}B, transfer size={len(data)}B)"
        )
