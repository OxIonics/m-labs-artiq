#!/usr/bin/env python3
"""
Client to send commands to :mod:`artiq_master` and display results locally.

The client can perform actions such as accessing/setting datasets,
scanning devices, scheduling experiments, and looking for experiments/devices.
"""

import argparse
from contextlib import AsyncExitStack, asynccontextmanager, contextmanager
import logging
from os.path import abspath
import socket
import uuid

import time
import asyncio
import sys
import os
from operator import itemgetter
from dateutil.parser import parse as parse_date

from prettytable import PrettyTable

from sipyco.pc_rpc import AsyncioClient, Client
from sipyco.sync_struct import Subscriber
from sipyco.broadcast import Receiver
from sipyco import common_args, pyon

from artiq.consts import CONTROL_PORT, NOTIFY_PORT
from artiq.schedule_display import make_exp_source
from artiq.tools import short_format, parse_arguments
from artiq import __version__ as artiq_version


def clear_screen():
    if os.name == "nt":
        os.system("cls")
    else:
        sys.stdout.write("\x1b[2J\x1b[H")


@contextmanager
def _rpc_client(args, target):
    port = CONTROL_PORT if args.port is None else args.port
    master_client = Client(args.server, port, target)

    try:
        yield master_client
    finally:
        master_client.close_rpc()


@asynccontextmanager
async def _async_rpc_client(args, target):
    port = CONTROL_PORT if args.port is None else args.port
    master_client = AsyncioClient()
    await master_client.connect_rpc(args.server, port, target)

    try:
        yield master_client
    finally:
        master_client.close_rpc()


def get_argparser():
    parser = argparse.ArgumentParser(description="ARTIQ CLI client")
    parser.add_argument(
        "-s", "--server", default="::1",
        help="hostname or IP of the master to connect to")
    parser.add_argument(
        "--port", default=None, type=int,
        help="TCP port to use to connect to the master")
    parser.add_argument("--version", action="version",
                        version="ARTIQ v{}".format(artiq_version),
                        help="print the ARTIQ version number")

    subparsers = parser.add_subparsers(dest="action")
    subparsers.required = True

    parser_add = subparsers.add_parser("submit", help="submit an experiment")
    parser_add.set_defaults(func=_action_submit)
    parser_add.add_argument("-p", "--pipeline", default="main", type=str,
                            help="pipeline to run the experiment in "
                                 "(default: %(default)s)")
    parser_add.add_argument("-P", "--priority", default=0, type=int,
                            help="priority (higher value means sooner "
                                 "scheduling, default: %(default)s)")
    parser_add.add_argument("-t", "--timed", default=None, type=str,
                            help="set a due date for the experiment")
    parser_add.add_argument("-f", "--flush", default=False,
                            action="store_true",
                            help="flush the pipeline before preparing "
                            "the experiment")
    parser_add.add_argument("-R", "--repository", default=False,
                            action="store_true",
                            help="use the experiment repository")
    parser_add.add_argument("-r", "--revision", default=None,
                            help="use a specific repository revision "
                                 "(defaults to head, ignored without -R)")
    parser_add.add_argument("-c", "--class-name", default=None,
                            help="name of the class to run")
    worker_mgr = parser_add.add_argument_group(
        "Worker Manager",
        "Optionally use a worker manager to run the experiment. If none of these "
        "options are specified then the experiment will be executed by the master "
        "and the experiment file must be readable by the master."
    ).add_mutually_exclusive_group()
    worker_mgr.add_argument(
        "--start-worker-mgr", default=False,
        action="store_true",
        help="Start a local worker manager and use that to run the experiment. "
             "The experiment file can be any path readable from the current "
             "working directory"
    )
    worker_mgr.add_argument(
        "--worker-mgr-id",
        help="The id of an existing worker manager to use to run the experiment"
    )
    parser_add.add_argument("file", metavar="FILE",
                            help="file containing the experiment to run")
    parser_add.add_argument("arguments", metavar="ARGUMENTS", nargs="*",
                            help="run arguments")

    parser_delete = subparsers.add_parser("delete",
                                          help="delete an experiment "
                                               "from the schedule")
    parser_delete.set_defaults(func=_action_delete)
    parser_delete.add_argument("-g", action="store_true",
                               help="request graceful termination")
    parser_delete.add_argument("rid", metavar="RID", type=int,
                               help="run identifier (RID)")

    parser_set_dataset = subparsers.add_parser(
        "set-dataset", help="add or modify a dataset")
    parser_set_dataset.set_defaults(func=_action_set_dataset)
    parser_set_dataset.add_argument("name", metavar="NAME",
                                    help="name of the dataset")
    parser_set_dataset.add_argument("value", metavar="VALUE",
                                    help="value in PYON format")

    persist_group = parser_set_dataset.add_mutually_exclusive_group()
    persist_group.add_argument("-p", "--persist", action="store_true",
                               help="make the dataset persistent")
    persist_group.add_argument("-n", "--no-persist", action="store_true",
                               help="make the dataset non-persistent")

    parser_del_dataset = subparsers.add_parser(
        "del-dataset", help="delete a dataset")
    parser_del_dataset.set_defaults(func=_action_del_dataset)
    parser_del_dataset.add_argument("name", help="name of the dataset")

    parser_show = subparsers.add_parser(
        "show", help="show schedule, log, devices or datasets")
    parser_show.set_defaults(func=_action_show)
    parser_show.add_argument(
        "what", metavar="WHAT",
        choices=["schedule", "log", "ccb", "devices", "datasets", "explist"],
        help="select object to show: %(choices)s")

    parser_scan_devices = subparsers.add_parser(
        "scan-devices", help="trigger a device database (re)scan")
    parser_scan_devices.set_defaults(func=_action_scan_devices)

    parser_scan_repos = subparsers.add_parser(
        "scan-repository", help="trigger a repository (re)scan")
    parser_scan_repos.set_defaults(func=_action_scan_repository)
    parser_scan_repos.add_argument("--async", action="store_true",
                                   help="trigger scan and return immediately")
    parser_scan_repos.add_argument("revision", metavar="REVISION",
                                   default=None, nargs="?",
                                   help="use a specific repository revision "
                                        "(defaults to head)")
    parser_scan_repos.add_argument(
        "--worker-mgr-id",
        help="Scan the repo associated with a worker manager",
    )

    parser_ls = subparsers.add_parser(
        "ls", help="list a directory on the master")
    parser_ls.set_defaults(func=_action_ls)
    parser_ls.add_argument("directory", default="", nargs="?")
    parser_ls.add_argument(
        "--worker-mgr-id",
        help="Scan the repo associated with a worker manager",
    )

    common_args.verbosity_args(parser)
    return parser


@asynccontextmanager
async def _plain_dict_subscriber(master, port, notifier, notify_cb):
    data = {}

    def init_data(x):
        data.clear()
        data.update(x)
        return data

    subscriber = Subscriber(
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
async def _make_worker_manager(args):
    worker_manager_id = str(uuid.uuid4())
    cmd = [
        sys.executable, "-m", "artiq.frontend.artiq_worker_manager",
        "--id", worker_manager_id, "--exit-on-idle",
        socket.gethostname(), args.server,
    ]
    if args.verbose:
        cmd.append("-" + "v" * args.verbose)

    connected = asyncio.get_event_loop().create_future()

    def worker_managers_cb(mod, data):
        if worker_manager_id in data and not connected.done():
            connected.set_result(None)

    async with AsyncExitStack() as stack:
        worker_manager, available_worker_managers = await asyncio.gather(
            asyncio.create_subprocess_exec(*cmd),
            stack.enter_async_context(
                _plain_dict_subscriber(
                    args.server, NOTIFY_PORT, "worker_managers", worker_managers_cb,
                )
            ),
        )

        try:
            await connected
            yield worker_manager_id
            await worker_manager.wait()
        finally:
            if worker_manager.returncode is None:
                print("Killing worker manager")
                worker_manager.kill()
                await asyncio.wait_for(worker_manager.wait(), 1)
            if worker_manager.returncode is None:
                print("Worker manager didn't exit")
            elif worker_manager.returncode != 0:
                print("Worker exited with non-zero return code")


async def _submit(args):
    try:
        arguments = parse_arguments(args.arguments)
    except Exception as err:
        raise ValueError("Failed to parse run arguments") from err

    async with AsyncExitStack() as stack:
        remote = await stack.enter_async_context(
            _async_rpc_client(args, "master_schedule")
        )
        worker_manager = None
        if args.start_worker_mgr:
            worker_manager_id = await stack.enter_async_context(
                _make_worker_manager(args)
            )
        elif args.worker_mgr_id:
            worker_manager_id = args.worker_mgr_id
        else:
            worker_manager_id = None

        if worker_manager_id is None:
            file = args.file
        else:
            file = abspath(args.file)

        expid = {
            "log_level": logging.WARNING + args.quiet*10 - args.verbose*10,
            "file": file,
            "class_name": args.class_name,
            "arguments": arguments,
            "worker_manager_id": worker_manager_id,
        }
        if args.repository:
            expid["repo_rev"] = args.revision
        if args.timed is None:
            due_date = None
        else:
            due_date = time.mktime(parse_date(args.timed).timetuple())
        rid = await remote.submit(args.pipeline, expid,
                                  args.priority, due_date, args.flush)
        print("RID: {}".format(rid))

        if worker_manager:
            worker_manager.wait()


def _action_submit(args):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(_submit(args))
    finally:
        loop.close()


def _action_delete(args):
    with _rpc_client(args, "master_schedule") as remote:
        if args.g:
            remote.request_termination(args.rid)
        else:
            remote.delete(args.rid)


def _action_set_dataset(args):
    with _rpc_client(args, "master_dataset_db") as remote:
        persist = None
        if args.persist:
            persist = True
        if args.no_persist:
            persist = False
        remote.set(args.name, pyon.decode(args.value), persist)


def _action_del_dataset(args):
    with _rpc_client(args, "master_dataset_db") as remote:
        remote.delete(args.name)


def _action_scan_devices(args):
    with _rpc_client(args, "master_device_db") as remote:
        remote.scan()


def _action_scan_repository(args):
    with _rpc_client(args, "master_experiment_db") as remote:
        if getattr(args, "async"):
            remote.scan_repository_async(args.revision, args.worker_mgr_id)
        else:
            remote.scan_repository(args.revision, args.worker_mgr_id)


def _action_ls(args):
    with _rpc_client(args, "master_experiment_db") as remote:
        contents = remote.list_directory(args.directory, args.worker_mgr_id)
        for name in sorted(contents, key=lambda x: (x[-1] not in "\\/", x)):
            print(name)


def _action_show(args):
    if args.what == "schedule":
        _show_dict(args, "schedule", _show_schedule)
    elif args.what == "log":
        _show_log(args)
    elif args.what == "ccb":
        _show_ccb(args)
    elif args.what == "devices":
        _show_dict(args, "devices", _show_devices)
    elif args.what == "datasets":
        _show_dict(args, "datasets", _show_datasets)
    elif args.what == "explist":
        _show_dict(args, "all_explist", _show_exp_list)
    else:
        raise ValueError(f"Unknown show option: {args.what}")


def _show_schedule(schedule):
    clear_screen()
    if schedule:
        sorted_schedule = sorted(schedule.items(),
                                 key=lambda x: (-x[1]["priority"],
                                                x[1]["due_date"] or 0,
                                                x[0]))
        table = PrettyTable(["RID", "Pipeline", "    Status    ", "Prio",
                             "Due date", "Source", "File", "Class name"])
        for rid, v in sorted_schedule:
            row = [rid, v["pipeline"], v["status"], v["priority"]]
            if v["due_date"] is None:
                row.append("")
            else:
                row.append(time.strftime("%m/%d %H:%M:%S",
                           time.localtime(v["due_date"])))
            expid = v["expid"]
            row.append(make_exp_source(expid, v.get("repo_msg")))
            row.append(expid["file"])
            if expid["class_name"] is None:
                row.append("")
            else:
                row.append(expid["class_name"])
            table.add_row(row)
        print(table)
    else:
        print("Schedule is empty")


def _show_devices(devices):
    clear_screen()
    table = PrettyTable(["Name", "Description"])
    table.align["Description"] = "l"
    for k, v in sorted(devices.items(), key=itemgetter(0)):
        table.add_row([k, pyon.encode(v, True)])
    print(table)


def _show_datasets(datasets):
    clear_screen()
    table = PrettyTable(["Dataset", "Persistent", "Value"])
    for k, (persist, value) in sorted(datasets.items(), key=itemgetter(0)):
        table.add_row([k, "Y" if persist else "N", short_format(value)])
    print(table)


def _show_exp_list(explist):
    clear_screen()
    table = PrettyTable(["ManagerID", "Name"])
    for mgr_id, exps in explist.items():
        for exp in exps:
            table.add_row([mgr_id, exp])
    print(table)


def _run_subscriber(host, port, subscriber):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(subscriber.connect(host, port))
        try:
            loop.run_until_complete(asyncio.wait_for(subscriber.receive_task,
                                                     None))
            print("Connection to master lost")
        finally:
            loop.run_until_complete(subscriber.close())
    finally:
        loop.close()


def _show_dict(args, notifier_name, display_fun):
    d = dict()

    def init_d(x):
        d.clear()
        d.update(x)
        return d
    subscriber = Subscriber(notifier_name, init_d,
                            lambda mod: display_fun(d))
    port = NOTIFY_PORT if args.port is None else args.port
    _run_subscriber(args.server, port, subscriber)


def _print_log_record(record):
    level, source, t, message = record
    t = time.strftime("%m/%d %H:%M:%S", time.localtime(t))
    print(level, source, t, message)


def _show_log(args):
    subscriber = Receiver("log", [_print_log_record])
    port = 1067 if args.port is None else args.port
    _run_subscriber(args.server, port, subscriber)


def _show_ccb(args):
    subscriber = Receiver("ccb", [
        lambda d: print(d["service"],
                        "args:", d["args"],
                        "kwargs:", d["kwargs"])
    ])
    port = 1067 if args.port is None else args.port
    _run_subscriber(args.server, port, subscriber)


def main():
    args = get_argparser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
