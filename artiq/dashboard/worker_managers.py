from PyQt5 import QtCore, QtWidgets

from artiq.dashboard.local_worker_manager import LocalWorkerManager
from artiq.gui.models import (
    DictSyncSimpleTableModel,
    ModelSubscriber,
    ReplicantModelManager,
)


# Most of the gets in the class are just needed for backwards compatibility
# with an earlier version of worker managers.
class _Model(DictSyncSimpleTableModel):
    def __init__(self, local_worker_manager, init):
        self.local_worker_manager: LocalWorkerManager = local_worker_manager
        super(_Model, self).__init__(
            [self.RowSpec("ID", lambda k, v: v["id"]),
             self.RowSpec("Local", self._is_local),
             self.RowSpec("Description", lambda k, v: v["description"]),
             self.RowSpec("Repo root", lambda k, v: v.get("repo_root")),
             self.RowSpec("PID", lambda k, v: v.get("metadata", {}).get("pid")),
             self.RowSpec("Host", lambda k, v: v.get("metadata", {}).get("hostname")),
             self.RowSpec("User", lambda k, v: v.get("metadata", {}).get("username")),
             self.RowSpec("Exe", lambda k, v: v.get("metadata", {}).get("exe")),
             self.RowSpec("Parent", lambda k, v: v.get("metadata", {}).get("parent")),
             ],
            init,
        )

    def sort_key(self, k, v):
        return (
            v["description"],
            v.get("repo_root"),
            v.get("metadata", {}).get("pid"),
            v["id"],
        )

    def _is_local(self, k, v):
        if v["id"] == self.local_worker_manager.id:
            return "Local"
        else:
            return "Remote"


class WorkerManagerDock(QtWidgets.QDockWidget):

    @staticmethod
    def _init_table():
        table = QtWidgets.QTableView()
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.verticalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeToContents)
        table.verticalHeader().hide()
        return table

    def __init__(
            self,
            worker_manager_sub: ModelSubscriber,
            local_worker_manager: LocalWorkerManager
    ):
        QtWidgets.QDockWidget.__init__(self, "WorkerManagers")
        self.setObjectName("WorkerManagers")
        self.setFeatures(
            QtWidgets.QDockWidget.DockWidgetMovable |
            QtWidgets.QDockWidget.DockWidgetFloatable
        )

        self.table = self._init_table()
        self.setWidget(self.table)

        self.model = _Model(local_worker_manager, {})
        ReplicantModelManager.with_setmodel_callback(
            worker_manager_sub,
            lambda init: _Model(local_worker_manager, init),
            self.set_model
        )

    def set_model(self, model):
        self.model = model
        self.table.setModel(model)

    def save_state(self):
        return bytes(self.table.horizontalHeader().saveState())

    def restore_state(self, state):
        self.table.horizontalHeader().restoreState(QtCore.QByteArray(state))
