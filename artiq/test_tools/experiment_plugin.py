import pytest

from artiq.test_tools import experiment, thread_worker_transport

thread_worker_transport.install_import_hook()


class ArtiqMasterProxy:

    def __init__(self, host):
        self.host = host

    async def run_experiment(self, experiment_cls, **kwargs):
        return await experiment.run_experiment(
            self.host,
            experiment_cls,
            **kwargs
        )


def pytest_addoption(parser):
    parser.addoption(
        "--artiq-master",
        default="tumbleweed.oxionics.com",
        help="Set the artiq master that integration tests run on. "
             "Default tumbleweed.oxionics.com",
    )


@pytest.fixture
def artiq_master(pytestconfig):
    """ Fixture provides access to an artiq master

    The returned object has a method `run_experiment` which can be used to call
    `.experiment.run_experiment` with the host prepopulated.
    """
    return ArtiqMasterProxy(pytestconfig.getoption("artiq_master"))
