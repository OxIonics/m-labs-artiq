from PyQt5 import QtCore, QtGui, QtWidgets, Qt
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

        layout = QtWidgets.QHBoxLayout(self)
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
            ):

        self.master_conn = Status("Master conn")
        self.moninj = Status("moninj (kasli)")
        self.local_worker_manager_status = Status("Local worker manager")

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
        if status == LocalWorkerManagerStatus.not_started:
            self.local_worker_manager_status.set_status(
                "Not started", Qt.QColorConstants.LightGray,
            )
        elif status == LocalWorkerManagerStatus.running:
            self.local_worker_manager_status.set_status(
                "Running", Qt.QColorConstants.Green,
            )
        elif status == LocalWorkerManagerStatus.failed:
            self.local_worker_manager_status.set_status(
                "Failed", Qt.QColorConstants.Red,
            )
        elif status == LocalWorkerManagerStatus.conflict:
            self.local_worker_manager_status.set_status(
                "Conflict", Qt.QColorConstants.Red,
            )



