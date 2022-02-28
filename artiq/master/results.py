import logging
import os
import time

log = logging.getLogger(__name__)


class ResultStore:

    def __init__(self, root):
        self._root = root

    def store(self, start_time_utc, filename, data):
        """ Store a results file

        Args:
            start_time_utc: Time used to create the containing directory as
                seconds since Epoch
            filename: Filename of the file created.
            data: HDF5 encoded data to store
        """
        start_time_utc = time.gmtime(start_time_utc)
        dirname = os.path.join(
            self._root,
            time.strftime("%Y-%m-%d", start_time_utc),
            time.strftime("%H", start_time_utc)
        )
        os.makedirs(dirname, exist_ok=True)
        log.info(f"Writing results file {filename}")
        with open(os.path.join(dirname, filename), "wb") as f:
            f.write(data)
