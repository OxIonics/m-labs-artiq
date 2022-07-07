import asyncio
import logging

from PyQt5 import QtCore, QtWidgets

from artiq.dashboard.local_worker_manager import LocalWorkerManager
from artiq.display_tools import make_connection_string
from artiq.gui.models import (
    DictSyncSimpleTableModel,
    ModelSubscriber,
    ReplicantModelManager,
)
from sipyco.pc_rpc import AsyncioClient

log = logging.getLogger(__name__)


# Most of the gets in the class are just needed for backwards compatibility
# with an earlier version of worker managers.
class _Model(DictSyncSimpleTableModel):
    def __init__(self, local_worker_manager_id, init):
        self.local_worker_manager_id: str = local_worker_manager_id
        super(_Model, self).__init__(
            [self.RowSpec("ID", lambda k, v: v["id"]),
             self.RowSpec("Status", self._show_status),
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
            v["id"] != self.local_worker_manager_id,
            not v.get("connected", True),
            v["description"],
            v.get("repo_root"),
            v.get("metadata", {}).get("pid"),
            v["id"],
        )

    def _show_status(self, k, v):
        status = []
        if v["id"] == self.local_worker_manager_id:
            status.append("Local")

        try:
            status.append(make_connection_string(v))
        except KeyError:
            log.warning(
                f"Failed to generate status info for worker manager {v['id']}",
                exc_info=True,
            )
            status.append("Missing status info")

        return "\n".join(status)


class WorkerManagerDock(QtWidgets.QDockWidget):

    @staticmethod
    def _init_table(disconnect):
        table = QtWidgets.QTableView()
        table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        table.verticalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.ResizeToContents)
        table.verticalHeader().hide()
        table.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)

        request_disconnect_action = QtWidgets.QAction("Request disconnect", table)
        request_disconnect_action.triggered.connect(disconnect)
        request_disconnect_action.setShortcut("DELETE")
        request_disconnect_action.setShortcutContext(QtCore.Qt.WidgetShortcut)
        table.addAction(request_disconnect_action)
        return table

    def __init__(
            self,
            worker_managers_rpc: AsyncioClient,
    ):
        QtWidgets.QDockWidget.__init__(self, "WorkerManagers")
        self.worker_managers_rpc = worker_managers_rpc
        self.setObjectName("WorkerManagers")
        self.setFeatures(
            QtWidgets.QDockWidget.DockWidgetMovable |
            QtWidgets.QDockWidget.DockWidgetFloatable
        )

        self.table = self._init_table(
            self._disconnect
        )
        self.setWidget(self.table)

        self.model = None

    def start(self, local_worker_manager_id: str, worker_manager_sub: ModelSubscriber):
        ReplicantModelManager.with_setmodel_callback(
            worker_manager_sub,
            lambda init: _Model(local_worker_manager_id, init),
            self.set_model
        )

    def set_model(self, model):
        self.model = model
        self.table.setModel(model)

    def save_state(self):
        return bytes(self.table.horizontalHeader().saveState())

    def restore_state(self, state):
        self.table.horizontalHeader().restoreState(QtCore.QByteArray(state))

    def _disconnect(self):
        idx = self.table.selectedIndexes()
        if idx:
            row = idx[0].row()
            worker_manager_id = self.model.row_to_key[row]
            log.debug(f"Disconnect worker manager: {worker_manager_id}")
            asyncio.ensure_future(
                self.worker_managers_rpc.disconnect(worker_manager_id)
            )
