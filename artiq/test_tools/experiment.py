import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from functools import wraps
import logging
import os
import socket
import sys
from typing import Type
import uuid

from sipyco import pc_rpc, sync_struct

from artiq.consts import CONTROL_PORT, NOTIFY_PORT, WORKER_MANAGER_PORT
from artiq.language import EnvExperiment
from artiq.test_tools.thread_worker_transport import ThreadWorkerTransport
from artiq.tools import summarise_mod
from artiq.worker_manager.worker_manager import WorkerManager

log = logging.getLogger(__name__)


class ExperimentFailure(Exception):
    """Represents an exception that was caught in the experiment"""

    def __init__(self, data):
        self.data = data

    @property
    def step(self):
        return self.data["exception.step"]

    @property
    def orig_exception_type(self):
        return self.data["exception.type"]

    @property
    def orig_exception_msg(self):
        return self.data["exception.msg"]

    def __str__(self):
        return (
            f"Experiment failed at {self.step} with {self.orig_exception_type}: "
            f"{self.orig_exception_msg}"
        )


class TestExperiment(EnvExperiment):
    """Experiments for use with `run_experiment`

    Catches exceptions and allows them to be reported by `run_experiment`.
    Use `set_test_data` to set data that will be returned from `run_experiment`
    """

    def __init__(self, managers_or_parent, *args, **kwargs):
        self.__scheduler = None
        self.build = self._wrap(self.build)
        self.prepare = self._wrap(self.prepare)
        self.run = self._wrap(self.run)
        self.analyze = self._wrap(self.analyze)

        # super init calls build so we want to wrap it first
        super().__init__(managers_or_parent, *args, **kwargs)

        self.set_test_data("build", "ok")

    def set_test_data(self, key, value):
        # If we did this in build and relied on the child class calling our
        # build and that didn't happen for whatever reason, then we would
        # completely miss exceptions.
        if self.__scheduler is None:
            self.__scheduler = self.get_device("scheduler")
        self.set_dataset(
            f"data.{self.__scheduler.rid}.{key}",
            value,
            broadcast=True,
        )

    def _wrap(self, meth):
        @wraps(meth)
        def wrapper():
            try:
                meth()
            except Exception as ex:
                self.set_test_data("exception", True)
                self.set_test_data("exception.step", meth.__name__)
                self.set_test_data("exception.type", ex.__class__.__qualname__)
                self.set_test_data("exception.msg", str(ex))
                raise

        return wrapper


@asynccontextmanager
async def _plain_dict_subscriber(master, port, notifier, notify_cb):
    data = {}

    def init_data(x):
        data.clear()
        data.update(x)
        return data

    subscriber = sync_struct.Subscriber(
        notifier,
        init_data,
        lambda mod: notify_cb(mod, data),
    )
    await subscriber.connect(master, port)
    try:
        yield data
    finally:
        await subscriber.close()


@asynccontextmanager
async def _rpc_client(master, control_port, target):
    master_client = pc_rpc.AsyncioClient()
    await master_client.connect_rpc(master, control_port, target)

    try:
        yield master_client
    finally:
        master_client.close_rpc()


async def _clean_datasets(dataset_client, dataset, rid):
    for key in list(dataset):
        if key.startswith(f"data.{rid}."):
            await dataset_client.delete(key)


async def run_experiment(
    master,
    experiment_class: Type,
    pipeline="main",
    log_level=logging.DEBUG,
    arguments=None,
    worker_manager_port=WORKER_MANAGER_PORT,
    control_port=CONTROL_PORT,
    notify_port=NOTIFY_PORT,
):
    """Run a test experiment.

    Connects a new worker manager to `master` and submits `experiment_class` as
    an experiment to run in that worker manager. That worker manger uses a
    thread worker by default, this allows you to run your tests with a debugger
    and set breakpoints in (non-kernel) experiment code.

    Tests which use this should have a call to
    thread_worker_transport.install_import_hook() before the experiment code
    is imported, so that artiq can cache files that contain kernel code and
    give us kernel tracebacks. If you're using pytest there's a pytest plugin
    which does this on import, so that probably handles this for you if you
    include that plugin in your conftest.

    Args:
        master: The host name of an artiq master, e.g. tumbleweed.oxionics.com
        experiment_class: Experiment to run, it should inherit from
            `TestExperiment`
        pipeline: Should be main, the default, if it uses any hardware.
        log_level: The log_level argument for the worker.
        arguments: Arguments to pass to the experiment
        worker_manager_port: Should match the master's --port-worker-manager
            option
        control_port: Should match the master's --port-control option
        notify_port: Should match the master's --port-notify option

    Returns:
        A dict containing all the data set by the experiment using
        `set_test_data`
    """
    async with AsyncExitStack() as stack:
        if arguments is None:
            arguments = {}

        rid = None
        complete = asyncio.get_event_loop().create_future()
        connected = asyncio.get_event_loop().create_future()
        worker_manager_id = str(uuid.uuid4())

        def schedule_cb(mod, _):
            try:
                log.debug(f"Got schedule update {summarise_mod(mod)}")
                if (
                    rid is not None
                    and mod["action"] == sync_struct.ModAction.delitem.name
                    and mod["path"] == []
                    and mod["key"] == rid
                ):
                    complete.set_result(None)
            except Exception as ex:
                complete.set_exception(ex)
                raise

        def dataset_cb(mod, _):
            log.debug(f"Got dataset update {summarise_mod(mod)}")

        def worker_managers_cb(mod, data):
            log.debug(f"Got available worker manager update {summarise_mod(mod)}")

            if worker_manager_id in data:
                connected.set_result(None)

        (
            _,
            dataset,
            available_worker_managers,
            worker_manager,
            dataset_client,
            master_client,
        ) = await asyncio.gather(
            stack.enter_async_context(
                _plain_dict_subscriber(master, notify_port, "schedule", schedule_cb)
            ),
            stack.enter_async_context(
                _plain_dict_subscriber(master, notify_port, "datasets", dataset_cb)
            ),
            stack.enter_async_context(
                _plain_dict_subscriber(
                    master, notify_port, "worker_managers", worker_managers_cb
                )
            ),
            stack.enter_async_context(
                WorkerManager.context(
                    master,
                    worker_manager_port,
                    worker_manager_id,
                    f"{socket.gethostname()}-tests",
                    transport_factory=ThreadWorkerTransport,
                )
            ),
            stack.enter_async_context(
                _rpc_client(master, control_port, "master_dataset_db"),
            ),
            stack.enter_async_context(
                _rpc_client(master, control_port, "master_schedule")
            ),
        )

        await connected

        # rid used by notify_cb
        rid = await _submit_experiment(
            master_client,
            pipeline,
            worker_manager.id,
            experiment_class,
            arguments,
            log_level,
        )
        stack.push_async_callback(_clean_datasets, dataset_client, dataset, rid)

        await complete

        prefix = f"data.{rid}."
        data = {
            key[len(prefix) :]: value
            for key, (_, value) in dataset.items()
            if key.startswith(prefix)
        }

        if "exception" in data:
            raise ExperimentFailure(data)

        if data.get("build") != "ok":
            raise RuntimeError(
                "Failed to build the experiment. Maybe import errors? "
                "Experiment test modules mustn't contain relative imports"
            )

        return data


async def _submit_experiment(
    master_client, pipeline, manager_id, experiment_class, arguments, log_level
):
    expid = {
        "log_level": log_level,
        "file": os.path.abspath(sys.modules[experiment_class.__module__].__file__),
        "class_name": experiment_class.__name__,
        "arguments": arguments,
        "worker_manager_id": manager_id,
    }

    rid = await master_client.submit(
        pipeline,
        expid,
        -1,  # priority
        None,  # due_date
        False,  # flush
    )
    log.info(f"Started experiment {experiment_class.__qualname__} with {rid}")
    return rid
