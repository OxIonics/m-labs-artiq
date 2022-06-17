"""Worker process implementation.

This module contains the worker process main() function and the glue code
necessary to connect the global artefacts used from experiment code (scheduler,
device database, etc.) to their actual implementation in the parent master
process via IPC.
"""
import argparse
import base64
from contextlib import ExitStack
import io
import sys
from tempfile import SpooledTemporaryFile
import time
import os
import inspect
import logging
import traceback
from collections import OrderedDict

import h5py
from opentelemetry import trace
from sipyco import pipe_ipc, pyon
from sipyco.packed_exceptions import raise_packed_exc
from sipyco.logging_tools import multiline_log_config
import zlib

import artiq
from artiq import tools
from artiq.master.worker_db import DeviceError, DeviceManager, DatasetManager, DummyDevice
from artiq.language.environment import (
    is_public_experiment, TraceArgumentManager, ProcessArgumentManager
)
from artiq.language.core import set_watchdog_factory, TerminationRequested
from artiq.language.types import TBool
from artiq.compiler import import_cache
from artiq.coredevice.core import CompileError, host_only, _render_diagnostic
from artiq import __version__ as artiq_version

logger = logging.getLogger(__name__)

ipc = None
skip_next_status = False


def get_object_inner():
    line = ipc.readline().decode()
    return pyon.decode(line)


def get_object():
    global skip_next_status
    if skip_next_status:
        # If we received terminate as the reply in `parent_action` then the real
        # response is still in flight. So we should skip it when it turns up.
        # If we receive another action then who knows what's going on.
        obj = get_object_inner()
        if "action" in obj:
            raise RuntimeError(
                f"Unexpected action when expecting to skip a status: {obj!r}"
            )
        skip_next_status = False
    return get_object_inner()


def put_object(obj):
    ds = pyon.encode(obj)
    ipc.write((ds + "\n").encode())


def make_parent_action(action):
    def parent_action(*args, **kwargs):
        global skip_next_status
        with trace.get_tracer(__name__).start_as_current_span(
                "parent_action",
                attributes={"action": action},
        ):
            request = {"action": action, "args": args, "kwargs": kwargs}
            put_object(request)
            reply = get_object()
            if "action" in reply:
                if reply["action"] == "terminate":
                    skip_next_status = True
                    sys.exit()
                else:
                    raise ValueError
            if reply["status"] == "ok":
                return reply["data"]
            else:
                raise_packed_exc(reply["exception"])
    return parent_action


class ParentDeviceDB:
    get_device_db = make_parent_action("get_device_db")
    get = make_parent_action("get_device")


class ParentDatasetDB:
    get = make_parent_action("get_dataset")
    update = make_parent_action("update_dataset")


class Watchdog:
    _create = make_parent_action("create_watchdog")
    _delete = make_parent_action("delete_watchdog")

    def __init__(self, t):
        self.t = t

    def __enter__(self):
        self.wid = Watchdog._create(self.t)

    def __exit__(self, type, value, traceback):
        Watchdog._delete(self.wid)


set_watchdog_factory(Watchdog)


class Scheduler:
    def set_run_info(self, rid, pipeline_name, expid, priority):
        self.rid = rid
        self.pipeline_name = pipeline_name
        self.expid = expid
        self.priority = priority

    idle_parent = staticmethod(make_parent_action("idle"))
    pause_parent = staticmethod(make_parent_action("pause"))

    @host_only
    def pause(self):
        cmd = self.pause_parent()
        if cmd == "request_termination":
            raise TerminationRequested
        elif cmd == "resume":
            return
        else:
            raise ValueError(
                "Unexpected master command after pause: '{}'".format(cmd))

    @host_only
    def idle(self, callback):
        """Suspend execution until some condition is fulfilled.

        This allows an experiment to voluntarily cede to other experiments,
        even if the latter have in general got a lower priority. An experiment
        calling this method needs to ensure that any connection to the core
        device has been closed beforehand.
        The difference to the :meth:`~artiq.master.worker_impl.pause` method
        can be summarised as follows:
            - Paused experiments will run unless there is another experiment
                with higher priority
            - Idle experiments will not run unless some condition is satisfied

        The scheduler regularly checks whether an experiment should remain
        idle by calling the ``callback`` argument passed to this method.
        This function is thus used to check whether the relevant condition for
        resuming execution is satisfied and should return accordingly.

        :param callback: Function to call by scheduler when checking whether
            the experiment should remain idle.
            If callback returns True, experiment will idle; if it returns
            False, the scheduler will consider the experiment eligible to run
            and will execute it as soon as there no higher-priority
            experiments that also want to run.

        Example::

            MyExp(EnvExperiment):
                def build(self):
                    self.setattr_device("core")
                    self.setattr_device("scheduler")

                def run(self):
                    while True:
                        self.core.close()
                        self.scheduler.idle(check_idle)
                        self.do()

                def check_idle(self):
                    condition = <describe when experiment should resume>
                    return False if condition else True
        """
        while True:
            resume = self.idle_raw(callback)
            if resume:
                return
            self.pause()

    @host_only
    def idle_raw(self, callback):
        while True:
            cmd = self.idle_parent(is_idle=bool(callback()))
            if cmd == "check_still_idle":
                continue
            elif cmd == "resume":
                return True
            elif cmd == "pause":
                return False
            else:
                raise ValueError(
                    "Unexpected master command while idle: '{}'".format(cmd))

    _check_pause = staticmethod(make_parent_action("scheduler_check_pause"))
    def check_pause(self, rid=None) -> TBool:
        if rid is None:
            rid = self.rid
        return self._check_pause(rid)

    _submit = staticmethod(make_parent_action("scheduler_submit"))
    def submit(self, pipeline_name=None, expid=None, priority=None, due_date=None, flush=False):
        if pipeline_name is None:
            pipeline_name = self.pipeline_name
        if expid is None:
            expid = self.expid
        if priority is None:
            priority = self.priority
        return self._submit(pipeline_name, expid, priority, due_date, flush)

    delete = staticmethod(make_parent_action("scheduler_delete"))
    request_termination = staticmethod(
        make_parent_action("scheduler_request_termination"))
    get_status = staticmethod(make_parent_action("scheduler_get_status"))


class CCB:
    issue = staticmethod(make_parent_action("ccb_issue"))


def get_experiment(file, class_name):
    module = tools.file_import(file, prefix="artiq_worker_")
    return tools.get_experiment(module, class_name)


register_experiment = make_parent_action("register_experiment")


class ExamineDeviceMgr:
    get_device_db = make_parent_action("get_device_db")

    @staticmethod
    def get(name):
        return DummyDevice()


class ExamineDatasetMgr:
    # The worker processes used for examine are quite short lived so we can use
    # a pretty dumb cache. This speeds up examines and scan where lots of
    # experiments use the same datasets. Especially if we're in sandboxing mode
    # and there's a ropey network hop.
    _cache = {}

    @staticmethod
    def get(key, archive=False):
        try:
            return ExamineDatasetMgr._cache[key]
        except KeyError:
            val = ParentDatasetDB.get(key)
            ExamineDatasetMgr._cache[key] = val
            return val

    @staticmethod
    def update(self, mod):
        pass


def examine(device_mgr, dataset_mgr, file):
    previous_keys = set(sys.modules.keys())
    try:
        module = tools.file_import(file)
        for class_name, exp_class in inspect.getmembers(module, is_public_experiment):
            if exp_class.__doc__ is None:
                name = class_name
            else:
                name = exp_class.__doc__.strip().splitlines()[0].strip()
                if name[-1] == ".":
                    name = name[:-1]
            argument_mgr = TraceArgumentManager()
            scheduler_defaults = {}
            cls = exp_class(  # noqa: F841 (fill argument_mgr)
                (device_mgr, dataset_mgr, argument_mgr, scheduler_defaults)
            )
            arginfo = OrderedDict(
                (k, (proc.describe(), group, tooltip))
                for k, (proc, group, tooltip) in argument_mgr.requested_args.items()
            )
            argument_ui = None
            if hasattr(exp_class, "argument_ui"):
                argument_ui = exp_class.argument_ui
            register_experiment(class_name, name, arginfo, argument_ui, scheduler_defaults)
    finally:
        new_keys = set(sys.modules.keys())
        for key in new_keys - previous_keys:
            del sys.modules[key]


def setup_diagnostics(experiment_file, repository_path):
    def render_diagnostic(self, diagnostic):
        message = "While compiling {}\n".format(experiment_file) + \
                    _render_diagnostic(diagnostic, colored=False)
        if repository_path is not None:
            message = message.replace(repository_path, "<repository>")

        if diagnostic.level == "warning":
            logging.warning(message)
        else:
            logging.error(message)

    # This is kind of gross, but 1) we do not have any explicit connection
    # between the worker and a coredevice.core.Core instance at all,
    # and 2) the diagnostic engine really ought to be per-Core, since
    # that's what uses it and the repository path is per-Core.
    # So I don't know how to implement this properly for now.
    #
    # This hack is as good or bad as any other solution that involves
    # putting inherently local objects (the diagnostic engine) into
    # global slots, and there isn't any point in making it prettier by
    # wrapping it in layers of indirection.
    artiq.coredevice.core._DiagnosticEngine.render_diagnostic = \
        render_diagnostic


def put_completed():
    put_object({"action": "completed"})


def put_exception_report():
    _, exc, _ = sys.exc_info()
    # When we get CompileError, a more suitable diagnostic has already
    # been printed.
    if not isinstance(exc, CompileError):
        short_exc_info = type(exc).__name__
        exc_str = str(exc)
        if exc_str:
            short_exc_info += ": " + exc_str.splitlines()[0]
        lines = ["Terminating with exception ("+short_exc_info+")\n"]
        if hasattr(exc, "artiq_core_exception"):
            lines.append(str(exc.artiq_core_exception))
        if hasattr(exc, "parent_traceback"):
            lines += exc.parent_traceback
            lines += traceback.format_exception_only(type(exc), exc)
        logging.error("".join(lines).rstrip(),
                      exc_info=not hasattr(exc, "parent_traceback"))
    put_object({"action": "exception"})


_store_results = make_parent_action("store_results")


def _compressed_blocks(tmpf, min_output_size=1024 * 1024):
    buf = io.BytesIO()
    compress = zlib.compressobj()
    for block in iter(lambda: tmpf.read(4 * 1024), b""):
        buf.write(compress.compress(block))
        if buf.tell() > min_output_size:
            buf.write(compress.flush())
            yield buf.getvalue()
            buf = io.BytesIO()
            compress = zlib.compressobj()
    buf.write(compress.flush())
    if buf.tell() > 0:
        yield buf.getvalue()


def main(argv=None):
    global ipc

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--skip-log-config",
        dest="log_config",
        default=True,
        action="store_false",
    )
    parser.add_argument(
        "--skip-import-hook",
        dest="import_hook",
        default=True,
        action="store_false",
    )
    parser.add_argument("pipe")
    parser.add_argument("log_level", type=int)
    args = parser.parse_args(args=argv)

    if args.log_config:
        multiline_log_config(level=args.log_level)
    ipc = pipe_ipc.ChildComm(args.pipe)

    start_time = None
    run_time = None
    rid = None
    expid = None
    exp = None
    exp_inst = None
    repository_path = None
    first = True

    def write_results():
        with \
                tracer.start_as_current_span("write_results"), \
                SpooledTemporaryFile(max_size=20 * 1024 * 1024) as tmpf:

            filename = "{:09}-{}.h5".format(rid, exp.__name__)
            with h5py.File(filename, "w", driver="fileobj", fileobj=tmpf) as f:
                dataset_mgr.write_hdf5(f)
                f["artiq_version"] = artiq_version
                f["rid"] = rid
                f["start_time"] = start_time
                f["run_time"] = run_time
                f["expid"] = pyon.encode(expid)

            tmpf.seek(0, io.SEEK_END)
            logger.debug(f"Sending results total size {tmpf.tell()}B")

            tmpf.seek(0)

            for block in _compressed_blocks(tmpf):
                _store_results(start_time, filename, base64.b64encode(block))

    device_mgr = DeviceManager(ParentDeviceDB,
                               virtual_devices={"scheduler": Scheduler(),
                                                "ccb": CCB()})
    dataset_mgr = DatasetManager(ParentDatasetDB)

    if args.import_hook:
        import_cache.install_hook()

    try:
        with ExitStack() as stack:
            while True:
                obj = get_object()

                if first:
                    first = False
                    try:
                        device_mgr.get("tracing").start("artiq-worker")
                    except DeviceError:
                        logger.debug("Couldn't start tracing", exc_info=True)
                    tracer = trace.get_tracer("worker")
                    worker = stack.enter_context(tracer.start_as_current_span("worker"))
                    logger.info(f"Starting trace with id: {worker.get_span_context().trace_id}")

                action = obj["action"]
                if action == "build":
                    with tracer.start_as_current_span("build"):
                        logger.debug("Starting build")
                        start_time = time.time()
                        rid = obj["rid"]
                        expid = obj["expid"]
                        if obj["wd"] is not None:
                            # Using repository
                            experiment_file = os.path.join(obj["wd"], expid["file"])
                            repository_path = obj["wd"]
                        else:
                            experiment_file = expid["file"]
                            repository_path = None
                        worker.set_attributes({
                            "type": "experiment",
                            "rid": rid,
                            "experiment_file": experiment_file,
                        })
                        if expid["class_name"] is not None:
                            worker.set_attribute("class_name", expid["class_name"])
                        setup_diagnostics(experiment_file, repository_path)
                        exp = get_experiment(experiment_file, expid["class_name"])
                        device_mgr.virtual_devices["scheduler"].set_run_info(
                            rid, obj["pipeline_name"], expid, obj["priority"])
                        start_local_time = time.localtime(start_time)
                        dirname = os.path.join("results",
                                           time.strftime("%Y-%m-%d", start_local_time),
                                           time.strftime("%H", start_local_time))
                        os.makedirs(dirname, exist_ok=True)
                        os.chdir(dirname)
                        argument_mgr = ProcessArgumentManager(expid["arguments"])
                        exp_inst = exp((device_mgr, dataset_mgr, argument_mgr, {}))
                        logger.debug("Completed build")
                        put_completed()
                elif action == "prepare":
                    with tracer.start_as_current_span("prepare"):
                        logger.debug("Starting prepare")
                        exp_inst.prepare()
                        logger.debug("Completed prepare")
                        put_completed()
                elif action == "run":
                    with tracer.start_as_current_span("run"):
                        logger.debug("Starting run")
                        run_time = time.time()
                        try:
                            exp_inst.run()
                        except:
                            # Only write results in run() on failure; on success wait
                            # for end of analyze stage.
                            write_results()
                            raise
                        logger.debug("Completed run")
                        put_completed()
                elif action == "analyze":
                    with tracer.start_as_current_span("analyze"):
                        logger.debug("Starting analyze")
                        try:
                            exp_inst.analyze()
                        finally:
                            write_results()
                        logger.debug("Completed analyze")
                        put_completed()
                elif action == "examine":
                    # No logging for examine it's not part of the main worker life
                    # cycle and I think it would get a bit chatty
                    examine(ExamineDeviceMgr, ExamineDatasetMgr, obj["file"])
                    put_completed()
                elif action == "terminate":
                    break
    except:
        put_exception_report()
    finally:
        device_mgr.close_devices()
        ipc.close()


if __name__ == "__main__":
    main()
