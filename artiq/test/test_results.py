import base64
from datetime import datetime

import zlib

from artiq.master.results import ResultStore


def test_writes_file_in_correct_dir(tmpdir):
    results_dir = tmpdir.mkdir("results")
    t = datetime(2022, 2, 11, 13, 5)
    filename = "example.hdf5"
    data = b"HelloHDF\x05"

    results = ResultStore(str(results_dir))
    results.store(t.timestamp(), filename, base64.b64encode(zlib.compress(data)))

    actual = results_dir.join("2022-02-11").join("13").join(filename).read_binary()
    assert actual == data


def test_appends_to_file(tmpdir):
    results_dir = tmpdir.mkdir("results")
    t = datetime(2022, 2, 11, 13, 5)
    filename = "append.hdf5"
    data1 = b"HelloHDF\x05"
    data2 = b"My old friend"

    results = ResultStore(str(results_dir))
    results.store(t.timestamp(), filename, base64.b64encode(zlib.compress(data1)))
    results.store(t.timestamp(), filename, base64.b64encode(zlib.compress(data2)))

    actual = results_dir.join("2022-02-11").join("13").join(filename).read_binary()
    assert actual == (data1 + data2)

