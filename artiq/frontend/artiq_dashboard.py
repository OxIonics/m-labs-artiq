#!/usr/bin/env python3

import argparse
import asyncio
import atexit
import importlib
import os
import logging
import signal
import socket
import sys
import uuid

from PyQt5 import QtCore, QtGui, QtWidgets
from qasync import QEventLoop

from sipyco.pc_rpc import AsyncioClient, Client
from sipyco.broadcast import Receiver
from sipyco import common_args
from sipyco.asyncio_tools import atexit_register_coroutine

from artiq import __artiq_dir__ as artiq_dir, __version__ as artiq_version
from artiq.consts import CONTROL_PORT, NOTIFY_PORT
from artiq.tools import get_user_config_dir
from artiq.gui.models import ModelSubscriber
from artiq.gui import state, log
from artiq.dashboard import (experiments, shortcuts, explorer,
                             moninj, datasets, schedule, applets_ccb)


def get_argparser():
    parser = argparse.ArgumentParser(description="ARTIQ Dashboard")
    parser.add_argument("--version", action="version",
                        version="ARTIQ v{}".format(artiq_version),
                        help="print the ARTIQ version number")
    parser.add_argument(
        "-s", "--server", default="::1",
        help="hostname or IP of the master to connect to")
    parser.add_argument(
        "--port-notify", default=NOTIFY_PORT, type=int,
        help="TCP port to connect to for notifications")
    parser.add_argument(
        "--port-control", default=CONTROL_PORT, type=int,
        help="TCP port to connect to for control")
    parser.add_argument(
        "--port-broadcast", default=1067, type=int,
        help="TCP port to connect to for broadcasts")
    parser.add_argument(
        "--db-file", default=None,
        help="database file for local GUI settings")
    parser.add_argument(
        "-p", "--load-plugin", dest="plugin_modules", action="append",
        help="Python module to load on startup")
    common_args.verbosity_args(parser)
    return parser


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self, server):
        QtWidgets.QMainWindow.__init__(self)

        icon = QtGui.QIcon(os.path.join(artiq_dir, "gui", "logo.svg"))
        self.setWindowIcon(icon)
        self.setWindowTitle("ARTIQ Dashboard - {}".format(server))

        qfm = QtGui.QFontMetrics(self.font())
        self.resize(140*qfm.averageCharWidth(), 38*qfm.lineSpacing())

        self.exit_request = asyncio.Event()

    def closeEvent(self, event):
        event.ignore()
        self.exit_request.set()

    def save_state(self):
        return {
            "state": bytes(self.saveState()),
            "geometry": bytes(self.saveGeometry())
        }

    def restore_state(self, state):
        self.restoreGeometry(QtCore.QByteArray(state["geometry"]))
        self.restoreState(QtCore.QByteArray(state["state"]))


class MdiArea(QtWidgets.QMdiArea):
    def __init__(self):
        QtWidgets.QMdiArea.__init__(self)
        self.pixmap = QtGui.QPixmap(os.path.join(
            artiq_dir, "gui", "logo_ver.svg"))

    def paintEvent(self, event):
        QtWidgets.QMdiArea.paintEvent(self, event)
        painter = QtGui.QPainter(self.viewport())
        x = (self.width() - self.pixmap.width())//2
        y = (self.height() - self.pixmap.height())//2
        painter.setOpacity(0.5)
        painter.drawPixmap(x, y, self.pixmap)


async def start_local_worker_manager(args):
    worker_manager_id = str(uuid.uuid4())

    cmd = [
        sys.executable, "-m", "artiq.frontend.artiq_worker_manager",
        "--id", worker_manager_id,
        socket.gethostname(), args.server,
    ]
    if args.verbose:
        cmd.append("-" + "v" * args.verbose)

    process = await asyncio.create_subprocess_exec(*cmd)

    async def stop_local_worker_manager():
        process.terminate()
        try:
            await asyncio.wait_for(process.wait(), 5)
        except asyncio.TimeoutError:
            logging.error("Local worker manager didn't exit from terminate")
            process.kill()

    atexit_register_coroutine(stop_local_worker_manager)

    return worker_manager_id



def main():
    # initialize application
    args = get_argparser().parse_args()
    widget_log_handler = log.init_log(args, "dashboard")
    if args.verbose == 2:
        # This is super chatty on debug. Limit it to info unless we get more
        # than two verbose flags.
        logging.getLogger("qasync").setLevel(logging.INFO)

    if args.plugin_modules:
        for mod in args.plugin_modules:
            importlib.import_module(mod)

    if args.db_file is None:
        args.db_file = os.path.join(get_user_config_dir(),
                           "artiq_dashboard_{server}_{port}.pyon".format(
                            server=args.server.replace(":","."),
                            port=args.port_notify))

    app = QtWidgets.QApplication(["ARTIQ Dashboard"])
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)
    atexit.register(loop.close)
    smgr = state.StateManager(args.db_file)

    local_worker_manager_id = loop.run_until_complete(start_local_worker_manager(args))

    # create connections to master
    rpc_clients = dict()
    for target in "schedule", "experiment_db", "dataset_db", "device_db":
        client = AsyncioClient()
        loop.run_until_complete(client.connect_rpc(
            args.server, args.port_control, "master_" + target))
        atexit.register(client.close_rpc)
        rpc_clients[target] = client

    config = Client(args.server, args.port_control, "master_config")
    try:
        server_name = config.get_name()
    finally:
        config.close_rpc()

    disconnect_reported = False
    def report_disconnect():
        nonlocal disconnect_reported
        if not disconnect_reported:
            logging.error("connection to master lost, "
                          "restart dashboard to reconnect", exc_info=True)
        disconnect_reported = True

    sub_clients = dict()
    for notifier_name, modelf in (("all_explist", explorer.AllExpListModel),
                                  ("all_explist_status", explorer.AllStatusUpdater),
                                  ("datasets", datasets.Model),
                                  ("schedule", schedule.Model),
                                  ("worker_managers", explorer.WorkerManagerModel)):
        subscriber = ModelSubscriber(notifier_name, modelf,
            report_disconnect)
        loop.run_until_complete(subscriber.connect(
            args.server, args.port_notify))
        atexit_register_coroutine(subscriber.close)
        sub_clients[notifier_name] = subscriber

    broadcast_clients = dict()
    for target in "log", "ccb":
        client = Receiver(target, [], report_disconnect)
        loop.run_until_complete(client.connect(
            args.server, args.port_broadcast))
        atexit_register_coroutine(client.close)
        broadcast_clients[target] = client

    # initialize main window
    main_window = MainWindow(args.server if server_name is None else server_name)
    smgr.register(main_window)
    mdi_area = MdiArea()
    mdi_area.setHorizontalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
    mdi_area.setVerticalScrollBarPolicy(QtCore.Qt.ScrollBarAsNeeded)
    main_window.setCentralWidget(mdi_area)

    # create UI components
    expmgr = experiments.ExperimentManager(main_window,
                                           sub_clients["datasets"],
                                           sub_clients["all_explist"],
                                           sub_clients["schedule"],
                                           rpc_clients["schedule"],
                                           rpc_clients["experiment_db"],
                                           local_worker_manager_id)
    smgr.register(expmgr)
    d_shortcuts = shortcuts.ShortcutsDock(main_window, expmgr)
    smgr.register(d_shortcuts)
    d_explorer = explorer.ExplorerDock(expmgr, d_shortcuts,
                                       sub_clients["all_explist"],
                                       sub_clients["all_explist_status"],
                                       sub_clients["worker_managers"],
                                       rpc_clients["schedule"],
                                       rpc_clients["experiment_db"],
                                       rpc_clients["device_db"],
                                       local_worker_manager_id)
    smgr.register(d_explorer)

    d_datasets = datasets.DatasetsDock(sub_clients["datasets"],
                                       rpc_clients["dataset_db"])
    smgr.register(d_datasets)

    d_applets = applets_ccb.AppletsCCBDock(main_window, sub_clients["datasets"],
        extra_substitutes={"server": args.server, "port_notify": args.port_notify,
        "port_control": args.port_control})
    atexit_register_coroutine(d_applets.stop)
    smgr.register(d_applets)
    broadcast_clients["ccb"].notify_cbs.append(d_applets.ccb_notify)

    d_ttl_dds = moninj.MonInj()
    loop.run_until_complete(d_ttl_dds.start(args.server, args.port_notify))
    atexit_register_coroutine(d_ttl_dds.stop)

    d_schedule = schedule.ScheduleDock(
        rpc_clients["schedule"], sub_clients["schedule"])
    smgr.register(d_schedule)

    logmgr = log.LogDockManager(main_window)
    smgr.register(logmgr)
    broadcast_clients["log"].notify_cbs.append(logmgr.append_message)
    widget_log_handler.callback = logmgr.append_message

    # lay out docks
    right_docks = [
        d_explorer, d_shortcuts,
        d_ttl_dds.ttl_dock, d_ttl_dds.dds_dock, d_ttl_dds.dac_dock,
        d_datasets, d_applets
    ]
    main_window.addDockWidget(QtCore.Qt.RightDockWidgetArea, right_docks[0])
    for d1, d2 in zip(right_docks, right_docks[1:]):
        main_window.tabifyDockWidget(d1, d2)
    main_window.addDockWidget(QtCore.Qt.BottomDockWidgetArea, d_schedule)

    # load/initialize state
    if os.name == "nt":
        # HACK: show the main window before creating applets.
        # Otherwise, the windows of those applets that are in detached
        # QDockWidgets fail to be embedded.
        main_window.show()
    smgr.load()
    smgr.start()
    atexit_register_coroutine(smgr.stop)

    # work around for https://github.com/m-labs/artiq/issues/1307
    d_ttl_dds.ttl_dock.show()
    d_ttl_dds.dds_dock.show()

    # create first log dock if not already in state
    d_log0 = logmgr.first_log_dock()
    if d_log0 is not None:
        main_window.tabifyDockWidget(d_schedule, d_log0)


    if server_name is not None:
        server_description = server_name + " ({})".format(args.server)
    else:
        server_description = args.server
    logging.info("ARTIQ dashboard %s connected to %s",
                 artiq_version, server_description)

    # run
    main_window.show()
    loop.run_until_complete(main_window.exit_request.wait())

if __name__ == "__main__":
    main()
