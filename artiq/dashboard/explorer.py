import asyncio
import logging
import re
from functools import partial
from typing import Generic, Optional, TypeVar

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QFileDialog

from artiq.dashboard.experiments import make_url
from artiq.gui.tools import LayoutWidget
from artiq.gui.models import DictSyncModel, DictSyncTreeSepModel, ReplicantModelManager
from artiq.gui.waitingspinnerwidget import QtWaitingSpinner


logger = logging.getLogger(__name__)


class _OpenFileDialog(QtWidgets.QDialog):
    def __init__(self, explorer, exp_manager, experiment_db_ctl):
        QtWidgets.QDialog.__init__(self, parent=explorer)
        self.resize(710, 700)
        self.setWindowTitle("Open file outside repository")

        self.explorer = explorer
        self.exp_manager = exp_manager
        self.experiment_db_ctl = experiment_db_ctl

        grid = QtWidgets.QGridLayout()
        self.setLayout(grid)

        grid.addWidget(QtWidgets.QLabel("Location:"), 0, 0)
        self.location_label = QtWidgets.QLabel("")
        grid.addWidget(self.location_label, 0, 1)
        grid.setColumnStretch(1, 1)

        self.file_list = QtWidgets.QListWidget()
        asyncio.ensure_future(self.refresh_view())
        grid.addWidget(self.file_list, 1, 0, 1, 2)
        self.file_list.doubleClicked.connect(self.accept)

        buttons = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Ok | QtWidgets.QDialogButtonBox.Cancel)
        grid.addWidget(buttons, 2, 0, 1, 2)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

    async def refresh_view(self):
        self.file_list.clear()
        if not self.explorer.current_directory:
            self.location_label.setText("<root>")
        else:
            self.location_label.setText(self.explorer.current_directory)

        item = QtWidgets.QListWidgetItem()
        item.setText("..")
        item.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_FileDialogToParent))
        self.file_list.addItem(item)

        try:
            contents = await self.experiment_db_ctl.list_directory(
                self.explorer.current_directory)
        except:
            logger.error("Failed to list directory '%s'",
                         self.explorer.current_directory, exc_info=True)
            return
        for name in sorted(contents, key=lambda x: (x[-1] not in "\\/", x)):
            if name[-1] in "\\/":
                icon = QtWidgets.QStyle.SP_DirIcon
            else:
                icon = QtWidgets.QStyle.SP_FileIcon
                if name[-3:] != ".py":
                    continue
            item = QtWidgets.QListWidgetItem()
            item.setText(name)
            item.setIcon(QtWidgets.QApplication.style().standardIcon(icon))
            self.file_list.addItem(item)

    def accept(self):
        selected = self.file_list.selectedItems()
        if selected:
            selected = selected[0].text()
            if selected == "..":
                if not self.explorer.current_directory:
                    return
                if re.fullmatch("[a-zA-Z]:\\\\",
                                self.explorer.current_directory):
                    self.explorer.current_directory = ""
                else:
                    idx = None
                    for sep in "\\/":
                        try:
                            idx = self.explorer.current_directory[:-1].rindex(sep)
                        except ValueError:
                            pass
                        else:
                            break
                    self.explorer.current_directory = \
                        self.explorer.current_directory[:idx+1]
                    if self.explorer.current_directory == "/":
                        self.explorer.current_directory = ""
                asyncio.ensure_future(self.refresh_view())
            elif selected[-1] in "\\/":
                self.explorer.current_directory += selected
                asyncio.ensure_future(self.refresh_view())
            else:
                file = self.explorer.current_directory + selected
                async def open_task():
                    try:
                        await self.exp_manager.open_file(file)
                    except:
                        logger.error("Failed to open file '%s'",
                                     file, exc_info=True)
                asyncio.ensure_future(open_task())
                QtWidgets.QDialog.accept(self)


Model = TypeVar("Model")


class ActiveWorkerManagerModel(Generic[Model]):

    def __init__(self, init):
        self.backing_store = init
        self._active_worker_manager_id: Optional[str] = None
        self._active_model: Optional[Model] = None
        self._on_model_change_cbs = []
        self.set_active_worker_manager(None)

    def set_active_worker_manager(self, worker_manager_id):
        self._active_worker_manager_id = worker_manager_id
        try:
            data = self.backing_store[worker_manager_id]
        except KeyError:
            model = None
        else:
            model = self._make_model(worker_manager_id, data)
        self._set_model(model)

    def _make_model(self, mgr_id, data) -> Model:
        raise NotImplementedError()

    def _set_model(self, model: Optional[Model]):
        self._active_model = model

    @property
    def active_model(self) -> Model:
        return self._active_model

    def __getitem__(self, key):
        if key == self._active_worker_manager_id:
            if self._active_model is None:
                raise KeyError(key)
            return self._active_model
        else:
            return self.backing_store[key]

    def __setitem__(self, key, value):
        self.backing_store[key] = value
        if key == self._active_worker_manager_id:
            self._set_model(self._make_model(key, value))

    def __delitem__(self, key):
        del self.backing_store[key]
        if key == self._active_worker_manager_id:
            self._set_model(None)


class ExpListModel(DictSyncTreeSepModel):
    def __init__(self, init):
        DictSyncTreeSepModel.__init__(self, "/", ["Experiment"], init)

    def convert_tooltip(self, k, v, column):
        return ("<b>File:</b> {file}<br><b>Class:</b> {cls}"
                .format(file=v["file"], cls=v["class_name"]))


class AllExpListModel(ActiveWorkerManagerModel[ExpListModel]):

    def __init__(self, init):
        self._on_model_change_cbs = []
        super().__init__(init)

    def add_model_change_cb(self, cb):
        self._on_model_change_cbs.append(cb)

    def _set_model(self, model):
        super()._set_model(model)
        for cb in self._on_model_change_cbs:
            cb()

    def _make_model(self, mgr_id, data):
        # ExpListModel effectively copies data. Grab a reference to that copy
        # so that we see any updates.
        model = ExpListModel(data)
        self.backing_store[mgr_id] = model.backing_store
        return model

    @property
    def active_explist(self):
        return self.backing_store[self._active_worker_manager_id]


class StatusUpdater:
    def __init__(self, init):
        self.status = init
        self.explorer = None

    def set_explorer(self, explorer):
        self.explorer = explorer
        self.explorer.update_scanning(self.status["scanning"])
        self.explorer.update_cur_rev(self.status["cur_rev"])

    def __setitem__(self, k, v):
        self.status[k] = v
        if self.explorer is not None:
            if k == "scanning":
                self.explorer.update_scanning(v)
            elif k == "cur_rev":
                self.explorer.update_cur_rev(v)


class AllStatusUpdater(ActiveWorkerManagerModel[StatusUpdater]):

    def __init__(self, init):
        self.explorer = None
        super().__init__(init)

    def set_explorer(self, explorer):
        self.explorer = explorer
        if self._active_model is not None:
            self._active_model.set_explorer(explorer)

    def _set_model(self, model):
        if self._active_model is not None:
            self._active_model.explorer = None
        super()._set_model(model)
        if self._active_model is not None and self.explorer is not None:
            self._active_model.set_explorer(self.explorer)

    def _make_model(self, mgr_id, data):
        return StatusUpdater(data)

    def has_active_repo_ever_been_scanned(self):
        if not self.backing_store:
            # If there's nothing in the backing store assume it hasn't been
            # populated yet and therefore assume the active repo has been
            # scanned we just don't know about it.
            return True
        return self._active_model is not None


class WorkerManagerModel(DictSyncModel):

    def __init__(self, init):
        init[None] = {
            "id": None,
            "description": "-- built in --"
        }
        self.local_worker_manager_id = None
        super().__init__(["Description"], init)

    def _id_sort_key(self, mgr_id):
        if mgr_id is None:
            return 0
        elif mgr_id == self.local_worker_manager_id:
            return 1
        else:
            return 2

    def sort_key(self, k, v):
        return self._id_sort_key(k), v["description"], k

    def convert(self, k, v, column, role):
        if column != 0:
            raise RuntimeError("Worker manager model expects column to always be 0")
        if role == QtCore.Qt.UserRole:
            return v
        elif role == QtCore.Qt.DisplayRole:
            if self.local_worker_manager is not None and v["id"] == self.local_worker_manager.id:
                return "-- local --"
            else:
                return v["description"]
        else:
            return None


class WaitingPanel(LayoutWidget):
    def __init__(self):
        LayoutWidget.__init__(self)

        self.waiting_spinner = QtWaitingSpinner()
        self.addWidget(self.waiting_spinner, 1, 1)
        self.addWidget(QtWidgets.QLabel("Repository scan in progress..."), 1, 2)

    def start(self):
        self.waiting_spinner.start()

    def stop(self):
        self.waiting_spinner.stop()


class ExplorerDock(QtWidgets.QDockWidget):
    def __init__(self, exp_manager, d_shortcuts,
                 explist_sub, explist_status_sub,
                 worker_manager_sub,
                 schedule_ctl, experiment_db_ctl, device_db_ctl,
                 local_worker_manager):
        QtWidgets.QDockWidget.__init__(self, "Explorer")
        self.active_worker_manager_id = None
        self.local_worker_manager = local_worker_manager
        self.experiment_db_ctl = experiment_db_ctl
        self.setObjectName("Explorer")
        self.setFeatures(QtWidgets.QDockWidget.DockWidgetMovable |
                         QtWidgets.QDockWidget.DockWidgetFloatable)

        top_widget = LayoutWidget()
        self.setWidget(top_widget)

        self.exp_manager = exp_manager
        self.d_shortcuts = d_shortcuts
        self.schedule_ctl = schedule_ctl

        top_widget.addWidget(QtWidgets.QLabel("Repo:"), 0, 0)
        self.repo_select = QtWidgets.QComboBox(self)
        ReplicantModelManager.with_setmodel_callback(
            worker_manager_sub,
            WorkerManagerModel,
            self.set_repo_model,
        )
        self.repo_select.currentIndexChanged.connect(self._repo_selected_changed)
        top_widget.addWidget(self.repo_select, 0, 1)

        top_widget.addWidget(QtWidgets.QLabel("Revision:"), 1, 0)
        self.revision = QtWidgets.QLabel()
        self.revision.setTextInteractionFlags(QtCore.Qt.TextSelectableByMouse)
        top_widget.addWidget(self.revision, 1, 1)

        self.stack = QtWidgets.QStackedWidget()
        top_widget.addWidget(self.stack, 2, 0, colspan=2)

        self.el_buttons = LayoutWidget()
        self.el_buttons.layout.setContentsMargins(0, 0, 0, 0)
        self.stack.addWidget(self.el_buttons)

        self.el = QtWidgets.QTreeView()
        self.el.setHeaderHidden(True)
        self.el.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectItems)
        self.el.doubleClicked.connect(
            partial(self.expname_action, "open_experiment"))
        self.el_buttons.addWidget(self.el, 0, 0, colspan=2)

        open = QtWidgets.QPushButton("Open")
        open.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOpenButton))
        open.setToolTip("Open the selected experiment (Return)")
        self.el_buttons.addWidget(open, 1, 0)
        open.clicked.connect(
            partial(self.expname_action, "open_experiment"))

        submit = QtWidgets.QPushButton("Submit")
        submit.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOkButton))
        submit.setToolTip("Schedule the selected experiment (Ctrl+Return)")
        self.el_buttons.addWidget(submit, 1, 1)
        submit.clicked.connect(
            partial(self.expname_action, "submit"))

        self.explist_model = AllExpListModel(dict())
        explist_sub.add_setmodel_callback(self.set_model)

        self.el.setContextMenuPolicy(QtCore.Qt.ActionsContextMenu)
        open_action = QtWidgets.QAction("Open", self.el)
        open_action.triggered.connect(
            partial(self.expname_action, "open_experiment"))
        open_action.setShortcut("RETURN")
        open_action.setShortcutContext(QtCore.Qt.WidgetShortcut)
        self.el.addAction(open_action)
        submit_action = QtWidgets.QAction("Submit", self.el)
        submit_action.triggered.connect(
            partial(self.expname_action, "submit"))
        submit_action.setShortcut("CTRL+RETURN")
        submit_action.setShortcutContext(QtCore.Qt.WidgetShortcut)
        self.el.addAction(submit_action)
        reqterm_action = QtWidgets.QAction("Request termination of instances", self.el)
        reqterm_action.triggered.connect(
            partial(self.expname_action, "request_inst_term"))
        reqterm_action.setShortcut("CTRL+BACKSPACE")
        reqterm_action.setShortcutContext(QtCore.Qt.WidgetShortcut)
        self.el.addAction(reqterm_action)

        set_shortcut_menu = QtWidgets.QMenu()
        for i in range(12):
            action = QtWidgets.QAction("F" + str(i+1), self.el)
            action.triggered.connect(partial(self.set_shortcut, i))
            set_shortcut_menu.addAction(action)

        set_shortcut_action = QtWidgets.QAction("Set shortcut", self.el)
        set_shortcut_action.setMenu(set_shortcut_menu)
        self.el.addAction(set_shortcut_action)

        sep = QtWidgets.QAction(self.el)
        sep.setSeparator(True)
        self.el.addAction(sep)

        scan_repository_action = QtWidgets.QAction("Scan repository HEAD",
                                                   self.el)
        scan_repository_action.triggered.connect(self.scan_repository)
        self.el.addAction(scan_repository_action)

        scan_ddb_action = QtWidgets.QAction("Scan device database", self.el)
        def scan_ddb():
            asyncio.ensure_future(device_db_ctl.scan())
        scan_ddb_action.triggered.connect(scan_ddb)
        self.el.addAction(scan_ddb_action)

        self.current_directory = ""
        open_file_action = QtWidgets.QAction("Open file outside repository",
                                             self.el)
        open_file_action.triggered.connect(
            lambda: _OpenFileDialog(self, self.exp_manager,
                                    experiment_db_ctl).open())
        self.el.addAction(open_file_action)

        local_file_action = QtWidgets.QAction(
            "Open local file", self.el
        )
        local_file_action.triggered.connect(self.open_local_file)
        self.el.addAction(local_file_action)

        self.waiting_panel = WaitingPanel()
        self.stack.addWidget(self.waiting_panel)
        self.status_model = AllStatusUpdater({})
        explist_status_sub.add_setmodel_callback(self.set_status_model)

    def open_local_file(self):
        fn = QFileDialog.getOpenFileName(
            self,
            "Select experiment",
            filter="Python files (*.py)",
        )[0]
        if fn:
            asyncio.create_task(self.exp_manager.open_local_file(fn))

    def set_model(self, model: AllExpListModel):
        def on_model_change():
            self.el.setModel(model.active_model)

        model.set_active_worker_manager(self.active_worker_manager_id)
        model.add_model_change_cb(on_model_change)
        self.explist_model = model
        self.el.setModel(model.active_model)

    def set_status_model(self, model: AllStatusUpdater):
        model.set_active_worker_manager(self.active_worker_manager_id)
        model.set_explorer(self)
        self.status_model = model

    def set_repo_model(self, model: WorkerManagerModel):
        model.local_worker_manager = self.local_worker_manager
        old_active_worker_manager_id = self.active_worker_manager_id
        self.repo_select.setModel(model)
        if self.repo_select.currentData()["id"] != old_active_worker_manager_id:
            self.active_worker_manager_id = old_active_worker_manager_id
            self._set_worker_manager_in_ui()

    def _get_selected_expname(self):
        selection = self.el.selectedIndexes()
        if selection:
            return self.explist_model.active_model.index_to_key(selection[0])
        else:
            return None

    def expname_action(self, action):
        expname = self._get_selected_expname()
        if expname is not None:
            action = getattr(self.exp_manager, action)
            action(make_url("repo", self.active_worker_manager_id, expname))

    def set_shortcut(self, nr):
        expname = self._get_selected_expname()
        if expname is not None:
            expurl = "repo:" + expname
            self.d_shortcuts.set_shortcut(nr, expurl)
            logger.info("Set shortcut F%d to '%s'", nr+1, expurl)

    def update_scanning(self, scanning):
        if scanning:
            self.stack.setCurrentWidget(self.waiting_panel)
            self.waiting_panel.start()
        else:
            self.stack.setCurrentWidget(self.el_buttons)
            self.waiting_panel.stop()

    def update_cur_rev(self, cur_rev):
        self.revision.setText(cur_rev)

    def save_state(self):
        return {
            "current_directory": self.current_directory,
            "active_worker_manager": self.active_worker_manager_id,
        }

    def restore_state(self, state):
        self.current_directory = state["current_directory"]
        self.active_worker_manager_id = state.get("active_worker_manager")
        if isinstance(self.repo_select.model(), WorkerManagerModel):
            self._set_worker_manager_in_ui()

    def scan_repository(self):
        asyncio.ensure_future(self.experiment_db_ctl.scan_repository_async(
            worker_manager_id=self.active_worker_manager_id,
        ))

    def _set_worker_manager_in_ui(self):
        repo_model = self.repo_select.model()
        try:
            v = repo_model.backing_store[self.active_worker_manager_id]
        except KeyError:
            pass
        else:
            # This triggers `_repo_selected_changed`
            self.repo_select.setCurrentIndex(
                repo_model._find_row(
                    self.active_worker_manager_id, v
                )
            )

    def _repo_selected_changed(self, index):
        data = self.repo_select.currentData()
        mgr_id = data["id"]
        self.active_worker_manager_id = mgr_id
        self.explist_model.set_active_worker_manager(mgr_id)
        self.status_model.set_active_worker_manager(mgr_id)
        if not self.status_model.has_active_repo_ever_been_scanned():
            self.scan_repository()

