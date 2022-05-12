import asyncio
import pprint
import socket
from textwrap import dedent

from PyQt5 import QtCore, QtGui, QtWidgets, Qt

from sipyco.pc_rpc import AsyncioClient
from sipyco.sync_struct import Notifier

from artiq.dashboard.local_worker_manager import LocalWorkerManager, LocalWorkerManagerStatus


class StatusLight(QtWidgets.QWidget):

    def __init__(self):
        super(StatusLight, self).__init__()
        self.color = Qt.QColorConstants.Red
        self.setFixedSize(16, 16)

    def paintEvent(self, a0: QtGui.QPaintEvent) -> None:
        painter = QtGui.QPainter(self)
        painter.setBrush(QtGui.QBrush(self.color, QtCore.Qt.SolidPattern))
        painter.drawEllipse(0, 0, 14, 14)


class Status(QtWidgets.QFrame):

    def __init__(self, title):
        super(Status, self).__init__()
        self.setFrameStyle(
            QtWidgets.QFrame.StyledPanel | QtWidgets.QFrame.Raised
        )
        self.light = StatusLight()
        self.text = QtWidgets.QLabel("not set")

        self.layout = layout = QtWidgets.QHBoxLayout(self)
        layout.addWidget(QtWidgets.QLabel(title))
        layout.addWidget(self.light)
        layout.addWidget(self.text)

    def set_status(self, text, color):
        self.text.setText(text)
        self.light.color = color
        self.light.repaint()


class ConnectionStatuses:

    def __init__(
            self,
            main_window: QtWidgets.QMainWindow,
            moninj_notifier: Notifier,
            local_worker_manager: LocalWorkerManager,
            worker_managers_rpc: AsyncioClient,
            ):

        self.worker_managers_rpc = worker_managers_rpc
        self.main_window = main_window
        self.master_conn = Status("Master conn")
        self.moninj = Status("moninj (kasli)")
        self.local_worker_manager_status = Status("Local worker manager")
        self.local_worker_manager_button = QtWidgets.QPushButton()
        self.local_worker_manager_status.layout.addWidget(
            self.local_worker_manager_button
        )
        self.local_worker_manager_button.hide()
        self.local_worker_manager_button.clicked.connect(
            self._fix_local_worker_manager
        )

        statusBar = main_window.statusBar()
        statusBar.addWidget(self.master_conn)
        statusBar.addWidget(self.moninj)
        statusBar.addWidget(self.local_worker_manager_status)

        self.master_conn.set_status("Connected", Qt.QColorConstants.Green)
        moninj_notifier.publish = self._process_moninj_mod
        self.local_worker_manager = local_worker_manager
        self.local_worker_manager.on_status_changed(self._local_worker_manager_status_change)

    def master_disconnected(self):
        self.master_conn.set_status("Disconnected", Qt.QColorConstants.Red)

    def _process_moninj_mod(self, mod):
        if mod["action"] == "setitem" and mod["key"] == "connected" and mod["path"] == []:
            if mod["value"]:
                self.moninj.set_status("Connected", Qt.QColorConstants.Green)
            else:
                self.moninj.set_status("Disconnected", Qt.QColorConstants.Red)

    def _local_worker_manager_status_change(self, status):
        if status in [
            LocalWorkerManagerStatus.initial,
            LocalWorkerManagerStatus.starting,
        ]:
            self.local_worker_manager_status.set_status(
                "Not started", Qt.QColorConstants.LightGray,
            )
            self.local_worker_manager_button.hide()
        elif status == LocalWorkerManagerStatus.running:
            self.local_worker_manager_status.set_status(
                "Running", Qt.QColorConstants.Green,
            )
            self.local_worker_manager_button.hide()
        elif status == LocalWorkerManagerStatus.failed:
            self.local_worker_manager_status.set_status(
                "Failed", Qt.QColorConstants.Red,
            )
            self.local_worker_manager_button.setText("Restart")
            self.local_worker_manager_button.show()
        elif status == LocalWorkerManagerStatus.conflict:
            self.local_worker_manager_status.set_status(
                "Conflict", Qt.QColorConstants.Red,
            )
            self.local_worker_manager_button.setText("Fix")
            self.local_worker_manager_button.show()

    def _fix_local_worker_manager(self):
        if self.local_worker_manager.status == LocalWorkerManagerStatus.failed:
            self.local_worker_manager.restart()
        elif self.local_worker_manager.status == LocalWorkerManagerStatus.conflict:
            conflict = self.local_worker_manager.get_conflict_info()

            msg = (
                "Another worker manager with same ID is already connected to"
                "the artiq master. Worker manager IDs should be unique to the"
                "combination of (machine, working directory, artiq master)."
                "\n\n"
                "You could be seeing this error because:\n"
                "* You're running two dashboards from the same directory\n"
                "* You've copied or renamed your artiq dashboard configuration\n"
                "* A dashboard failed and exited without closing the associated "
                "worker manager.\n"
                "\n"
                "To start a worker manager with this dashboard you'll have to "
                "stop the other worker manager. You can click disconnect to "
                "disconnect the other worker manager, but it might be running "
                "an experiment"
                "\n\n"
            )

            try:
                if conflict["metadata"]["hostname"] != socket.getfqdn():
                    msg = dedent("""\
                        The conflicting worker manager appears to be 
                        different computer.
                        
                        Your computer: {socket.getfqdn()}
                        Conflicting computer: {conflict["metadata"]["hostname"]}
                        Conflicting username: {conflict["metadata"]["username"]}
                    """)
                else:
                    msg += (
                        f"The conflicting process ID: {conflict['metadata']['pid']}\n"
                    )
                    if conflict["metadata"].get("parent"):
                        msg += (
                            f"Started By: {conflict['metadata']['parent']}\n"
                        )
                    msg += (
                        f"In: {conflict['repo_root']}\n"
                    )

            except KeyError as ex:
                msg += (
                    "The other worker manager didn't report sensible information."
                    f"Sorry you're on your own. (Missing key: {ex})"
                )

            msgBox = QtWidgets.QMessageBox(self.main_window)
            msgBox.setText(msg)
            msgBox.setDetailedText(pprint.pformat(conflict))
            disconnect = msgBox.addButton(
                "Disconnect",
                QtWidgets.QMessageBox.ActionRole,
            )
            msgBox.setStandardButtons(QtWidgets.QMessageBox.Close)
            msgBox.setDefaultButton(QtWidgets.QMessageBox.Close)
            msgBox.exec()

            if msgBox.clickedButton() == disconnect:
                asyncio.ensure_future(
                    self.worker_managers_rpc.disconnect(self.local_worker_manager.id)
                )
