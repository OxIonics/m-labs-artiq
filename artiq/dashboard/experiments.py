from __future__ import annotations

import dataclasses
import logging
import asyncio
import os
from functools import partial
from collections import OrderedDict
from typing import Any, Dict, Optional
import urllib.parse

from PyQt5 import QtCore, QtGui, QtWidgets
import h5py

from sipyco import pyon

from artiq.gui.entries import procdesc_to_entry, ScanEntry
from artiq.gui.tools import LayoutWidget, log_level_to_name, get_open_file_name
from artiq.tools import summarise_mod

logger = logging.getLogger(__name__)


# Experiment URLs come in two forms:
# 1. repo:[//worker_manager_id/]<experiment name>
#    (file name and class name to be retrieved from explist)
# 2. file:[//worker_manger_id/]<file name>#<class name>

def make_url(scheme, host, path, fragment=None):
    result = scheme + ":"
    if host is not None:
        result += "//" + host + "/"
    result += path
    if fragment is not None:
        result += "#" + fragment
    return result


class _WheelFilter(QtCore.QObject):
    def eventFilter(self, obj, event):
        if (event.type() == QtCore.QEvent.Wheel and
                event.modifiers() != QtCore.Qt.NoModifier):
            event.ignore()
            return True
        return False


@dataclasses.dataclass
class ExpUrlResolution:
    repo: bool
    worker_manager_id: Optional[str]
    file: str
    class_name: str
    repo_experiment_description: Optional[Dict[str, Any]] = None
    repo_experiment_id: Optional[str] = None


class _ArgumentEditor(QtWidgets.QTreeWidget):
    def __init__(self, manager, dock, expurl):
        self.manager = manager
        self.expurl = expurl

        QtWidgets.QTreeWidget.__init__(self)
        self.setColumnCount(3)
        self.header().setStretchLastSection(False)
        if hasattr(self.header(), "setSectionResizeMode"):
            set_resize_mode = self.header().setSectionResizeMode
        else:
            set_resize_mode = self.header().setResizeMode
        set_resize_mode(0, QtWidgets.QHeaderView.ResizeToContents)
        set_resize_mode(1, QtWidgets.QHeaderView.Stretch)
        set_resize_mode(2, QtWidgets.QHeaderView.ResizeToContents)
        self.header().setVisible(False)
        self.setSelectionMode(self.NoSelection)
        self.setHorizontalScrollMode(self.ScrollPerPixel)
        self.setVerticalScrollMode(self.ScrollPerPixel)

        self.setStyleSheet("QTreeWidget {background: " +
                           self.palette().midlight().color().name() + " ;}")

        self.viewport().installEventFilter(_WheelFilter(self.viewport()))

        self._groups = dict()
        self._arg_to_widgets = dict()

        arguments = self.manager.get_submission_arguments(self.expurl)

        if not arguments:
            self.addTopLevelItem(QtWidgets.QTreeWidgetItem(["No arguments"]))

        gradient = QtGui.QLinearGradient(
            0, 0, 0, QtGui.QFontMetrics(self.font()).lineSpacing()*2.5)
        gradient.setColorAt(0, self.palette().base().color())
        gradient.setColorAt(1, self.palette().midlight().color())
        for name, argument in arguments.items():
            widgets = dict()
            self._arg_to_widgets[name] = widgets

            entry = procdesc_to_entry(argument["desc"])(argument)
            widget_item = QtWidgets.QTreeWidgetItem([name])
            if argument["tooltip"]:
                widget_item.setToolTip(0, argument["tooltip"])
            widgets["entry"] = entry
            widgets["widget_item"] = widget_item

            for col in range(3):
                widget_item.setBackground(col, gradient)
            font = widget_item.font(0)
            font.setBold(True)
            widget_item.setFont(0, font)

            if argument["group"] is None:
                self.addTopLevelItem(widget_item)
            else:
                self._get_group(argument["group"]).addChild(widget_item)
            fix_layout = LayoutWidget()
            widgets["fix_layout"] = fix_layout
            fix_layout.addWidget(entry)
            self.setItemWidget(widget_item, 1, fix_layout)
            recompute_argument = QtWidgets.QToolButton()
            recompute_argument.setToolTip("Re-run the experiment's build "
                                          "method and take the default value")
            recompute_argument.setIcon(
                QtWidgets.QApplication.style().standardIcon(
                    QtWidgets.QStyle.SP_BrowserReload))
            recompute_argument.clicked.connect(
                partial(self._recompute_argument_clicked, name))

            tool_buttons = LayoutWidget()
            tool_buttons.addWidget(recompute_argument, 1)

            disable_other_scans = QtWidgets.QToolButton()
            widgets["disable_other_scans"] = disable_other_scans
            disable_other_scans.setIcon(
                QtWidgets.QApplication.style().standardIcon(
                    QtWidgets.QStyle.SP_DialogResetButton))
            disable_other_scans.setToolTip("Disable all other scans in "
                                           "this experiment")
            disable_other_scans.clicked.connect(
                partial(self._disable_other_scans, name))
            tool_buttons.layout.setRowStretch(0, 1)
            tool_buttons.layout.setRowStretch(3, 1)
            tool_buttons.addWidget(disable_other_scans, 2)
            if not isinstance(entry, ScanEntry):
                disable_other_scans.setVisible(False)

            self.setItemWidget(widget_item, 2, tool_buttons)

        widget_item = QtWidgets.QTreeWidgetItem()
        self.addTopLevelItem(widget_item)
        recompute_arguments = QtWidgets.QPushButton("Recompute all arguments")
        recompute_arguments.setIcon(
            QtWidgets.QApplication.style().standardIcon(
                QtWidgets.QStyle.SP_BrowserReload))
        recompute_arguments.clicked.connect(dock._recompute_arguments_clicked)

        load_hdf5 = QtWidgets.QPushButton("Load HDF5")
        load_hdf5.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOpenButton))
        load_hdf5.clicked.connect(dock._load_hdf5_clicked)

        buttons = LayoutWidget()
        buttons.addWidget(recompute_arguments, 1, 1)
        buttons.addWidget(load_hdf5, 1, 2)
        buttons.layout.setColumnStretch(0, 1)
        buttons.layout.setColumnStretch(1, 0)
        buttons.layout.setColumnStretch(2, 0)
        buttons.layout.setColumnStretch(3, 1)
        self.setItemWidget(widget_item, 1, buttons)

    def _get_group(self, name):
        if name in self._groups:
            return self._groups[name]
        group = QtWidgets.QTreeWidgetItem([name])
        for col in range(3):
            group.setBackground(col, self.palette().mid())
            group.setForeground(col, self.palette().brightText())
            font = group.font(col)
            font.setBold(True)
            group.setFont(col, font)
        self.addTopLevelItem(group)
        self._groups[name] = group
        return group

    def _recompute_argument_clicked(self, name):
        asyncio.ensure_future(self._recompute_argument(name))

    async def _recompute_argument(self, name):
        try:
            expdesc, _ = await self.manager.compute_expdesc(self.expurl)
        except:
            logger.error("Could not recompute argument '%s' of '%s'",
                         name, self.expurl, exc_info=True)
            return
        argument = self.manager.get_submission_arguments(self.expurl)[name]

        procdesc = expdesc["arginfo"][name][0]
        state = procdesc_to_entry(procdesc).default_state(procdesc)
        argument["desc"] = procdesc
        argument["state"] = state

        # Qt needs a setItemWidget() to handle layout correctly,
        # simply replacing the entry inside the LayoutWidget
        # results in a bug.

        widgets = self._arg_to_widgets[name]

        widgets["entry"].deleteLater()
        widgets["entry"] = procdesc_to_entry(procdesc)(argument)
        widgets["disable_other_scans"].setVisible(
            isinstance(widgets["entry"], ScanEntry))
        widgets["fix_layout"].deleteLater()
        widgets["fix_layout"] = LayoutWidget()
        widgets["fix_layout"].addWidget(widgets["entry"])
        self.setItemWidget(widgets["widget_item"], 1, widgets["fix_layout"])
        self.updateGeometries()

    def _disable_other_scans(self, current_name):
        for name, widgets in self._arg_to_widgets.items():
            if (name != current_name
                    and isinstance(widgets["entry"], ScanEntry)):
                widgets["entry"].disable()

    def save_state(self):
        expanded = []
        for k, v in self._groups.items():
            if v.isExpanded():
                expanded.append(k)
        return {
            "expanded": expanded,
            "scroll": self.verticalScrollBar().value()
        }

    def restore_state(self, state):
        for e in state["expanded"]:
            try:
                self._groups[e].setExpanded(True)
            except KeyError:
                pass
        self.verticalScrollBar().setValue(state["scroll"])

    def about_to_submit(self):
        pass

    def about_to_close(self):
        pass


log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class _ExperimentDock(QtWidgets.QMdiSubWindow):
    sigClosed = QtCore.pyqtSignal()

    @staticmethod
    def _make_title(manager: ExperimentManager, expurl):
        exp = manager.resolve_expurl(expurl)
        if exp.worker_manager_id is None:
            worker_manager_desc = "-- builtin --"
        elif exp.worker_manager_id == manager.local_worker_manager.id:
            worker_manager_desc = "-- local --"
        else:
            try:
                worker_manager_desc = manager.worker_managers[exp.worker_manager_id]["description"]
            except KeyError:
                worker_manager_desc = exp.worker_manager_id

        if exp.repo:
            exp_desc = exp.repo_experiment_id
        else:
            exp_desc = f"{exp.file}#{exp.class_name}"

        return f"{worker_manager_desc} :: {exp_desc}"

    def __init__(self, manager: ExperimentManager, expurl):
        QtWidgets.QMdiSubWindow.__init__(self)
        qfm = QtGui.QFontMetrics(self.font())
        self.resize(100*qfm.averageCharWidth(), 30*qfm.lineSpacing())

        self.setWindowIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_FileDialogContentsView))

        self.manager = manager
        self.expurl = expurl
        self.argeditor = None
        self.argeditor_state = None
        self.loaded = False

        self.layout = None
        self.repo_rev = None
        self.hdf5_load_directory = os.path.expanduser("~")

        self.try_load()

    def try_load(self):
        try:
            self.manager.resolve_expurl(self.expurl)
        except (UnknownWorkerManagerError, MissingExperimentError) as ex:
            logger.debug(f"Failed to load experiment: {ex}")
            self.show_missing(str(ex))
        else:
            self.show_experiment()

    def show_missing(self, msg):
        self.setWindowTitle(f"Missing: {self.expurl}")

        self.layout = QtWidgets.QVBoxLayout()
        self.layout.addStretch(2)
        self.layout.addWidget(QtWidgets.QLabel(
            f"Failed to load experiment. {msg}"
        ))
        self.layout.addStretch(2)

        self._replaceContents()

    def show_experiment(self):
        self.setWindowTitle(self._make_title(self.manager, self.expurl))

        self.layout = QtWidgets.QGridLayout()
        self.layout.setSpacing(5)
        self.layout.setContentsMargins(5, 5, 5, 5)

        self.layout.setRowStretch(0, 1)
        self._create_argeditor()

        scheduling = self.manager.get_submission_scheduling(self.expurl)
        options = self.manager.get_submission_options(self.expurl)

        datetime = QtWidgets.QDateTimeEdit()
        datetime.setDisplayFormat("MMM d yyyy hh:mm:ss")
        datetime_en = QtWidgets.QCheckBox("Due date:")
        self.layout.addWidget(datetime_en, 1, 0)
        self.layout.addWidget(datetime, 1, 1)

        if scheduling["due_date"] is None:
            datetime.setDate(QtCore.QDate.currentDate())
        else:
            datetime.setDateTime(QtCore.QDateTime.fromMSecsSinceEpoch(
                scheduling["due_date"]*1000))
        datetime_en.setChecked(scheduling["due_date"] is not None)

        def update_datetime(dt):
            scheduling["due_date"] = dt.toMSecsSinceEpoch()/1000
            datetime_en.setChecked(True)
        datetime.dateTimeChanged.connect(update_datetime)

        def update_datetime_en(checked):
            if checked:
                due_date = datetime.dateTime().toMSecsSinceEpoch()/1000
            else:
                due_date = None
            scheduling["due_date"] = due_date
        datetime_en.stateChanged.connect(update_datetime_en)

        pipeline_name = QtWidgets.QLineEdit()
        self.layout.addWidget(QtWidgets.QLabel("Pipeline:"), 1, 2)
        self.layout.addWidget(pipeline_name, 1, 3)

        pipeline_name.setText(scheduling["pipeline_name"])

        def update_pipeline_name(text):
            scheduling["pipeline_name"] = text
        pipeline_name.textChanged.connect(update_pipeline_name)

        priority = QtWidgets.QSpinBox()
        priority.setRange(-99, 99)
        self.layout.addWidget(QtWidgets.QLabel("Priority:"), 2, 0)
        self.layout.addWidget(priority, 2, 1)

        priority.setValue(scheduling["priority"])

        def update_priority(value):
            scheduling["priority"] = value
        priority.valueChanged.connect(update_priority)

        flush = QtWidgets.QCheckBox("Flush")
        flush.setToolTip("Flush the pipeline (of current- and higher-priority "
                         "experiments) before starting the experiment")
        self.layout.addWidget(flush, 2, 2, 1, 2)

        flush.setChecked(scheduling["flush"])

        def update_flush(checked):
            scheduling["flush"] = bool(checked)
        flush.stateChanged.connect(update_flush)

        log_level = QtWidgets.QComboBox()
        log_level.addItems(log_levels)
        log_level.setCurrentIndex(1)
        log_level.setToolTip("Minimum level for log entry production")
        log_level_label = QtWidgets.QLabel("Logging level:")
        log_level_label.setToolTip("Minimum level for log message production")
        self.layout.addWidget(log_level_label, 3, 0)
        self.layout.addWidget(log_level, 3, 1)

        log_level.setCurrentIndex(log_levels.index(
            log_level_to_name(options["log_level"])))

        def update_log_level(index):
            options["log_level"] = getattr(logging, log_level.currentText())
        log_level.currentIndexChanged.connect(update_log_level)
        self.log_level = log_level

        if "repo_rev" in options:
            repo_rev = QtWidgets.QLineEdit()
            repo_rev.setPlaceholderText("current")
            repo_rev_label = QtWidgets.QLabel("Revision:")
            repo_rev_label.setToolTip("Experiment repository revision "
                                      "(commit ID) to use")
            self.layout.addWidget(repo_rev_label, 3, 2)
            self.layout.addWidget(repo_rev, 3, 3)

            if options["repo_rev"] is not None:
                repo_rev.setText(options["repo_rev"])

            def update_repo_rev(text):
                if text:
                    options["repo_rev"] = text
                else:
                    options["repo_rev"] = None
            repo_rev.textChanged.connect(update_repo_rev)
            self.repo_rev = repo_rev

        submit = QtWidgets.QPushButton("Submit")
        submit.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogOkButton))
        submit.setToolTip("Schedule the experiment (Ctrl+Return)")
        submit.setShortcut("CTRL+RETURN")
        submit.setSizePolicy(QtWidgets.QSizePolicy.Expanding,
                             QtWidgets.QSizePolicy.Expanding)
        self.layout.addWidget(submit, 1, 4, 2, 1)
        submit.clicked.connect(self.submit_clicked)

        reqterm = QtWidgets.QPushButton("Terminate instances")
        reqterm.setIcon(QtWidgets.QApplication.style().standardIcon(
            QtWidgets.QStyle.SP_DialogCancelButton))
        reqterm.setToolTip("Request termination of instances (Ctrl+Backspace)")
        reqterm.setShortcut("CTRL+BACKSPACE")
        reqterm.setSizePolicy(QtWidgets.QSizePolicy.Expanding,
                              QtWidgets.QSizePolicy.Expanding)
        self.layout.addWidget(reqterm, 3, 4)
        reqterm.clicked.connect(self.reqterm_clicked)

        self.loaded = True
        self._replaceContents()

    def _replaceContents(self):
        old_widget = self.widget()
        if old_widget:
            old_widget.deleteLater()

        top_widget = QtWidgets.QWidget()
        top_widget.setLayout(self.layout)
        self.setWidget(top_widget)

    def submit_clicked(self):
        self.argeditor.about_to_submit()
        try:
            self.manager.submit(self.expurl)
        except:
            # May happen when experiment has been removed
            # from repository/explist
            logger.error("Failed to submit '%s'",
                         self.expurl, exc_info=True)

    def reqterm_clicked(self):
        try:
            self.manager.request_inst_term(self.expurl)
        except:
            # May happen when experiment has been removed
            # from repository/explist
            logger.error("Failed to request termination of instances of '%s'",
                         self.expurl, exc_info=True)

    def _recompute_arguments_clicked(self):
        asyncio.ensure_future(self._recompute_arguments_task())

    async def _recompute_arguments_task(self, overrides=dict()):
        try:
            expdesc, ui_name = await self.manager.compute_expdesc(self.expurl)
        except:
            logger.error("Could not recompute arguments of '%s'",
                         self.expurl, exc_info=True)
            return
        arginfo = expdesc["arginfo"]
        for k, v in overrides.items():
            # Some values (e.g. scans) may have multiple defaults in a list
            if ("default" in arginfo[k][0]
                    and isinstance(arginfo[k][0]["default"], list)):
                arginfo[k][0]["default"].insert(0, v)
            else:
                arginfo[k][0]["default"] = v
        self.manager.initialize_submission_arguments(self.expurl, arginfo, ui_name)

        self._recreate_argeditor()

    def _remove_argeditor(self):
        self.argeditor_state = self.argeditor.save_state()
        self.argeditor.deleteLater()
        self.argeditor = None

    def _create_argeditor(self):
        editor_class = self.manager.get_argument_editor_class(self.expurl)
        self.argeditor = editor_class(self.manager, self, self.expurl)
        if self.argeditor_state is not None:
            self.argeditor.restore_state(self.argeditor_state)
            self.argeditor_state = None
        self.layout.addWidget(self.argeditor, 0, 0, 1, 5)

    def _recreate_argeditor(self):
        self._remove_argeditor()
        self._create_argeditor()

    def contextMenuEvent(self, event):
        menu = QtWidgets.QMenu(self)
        reset_sched = menu.addAction("Reset scheduler settings")
        action = menu.exec_(self.mapToGlobal(event.pos()))
        if action == reset_sched:
            asyncio.ensure_future(self._recompute_sched_options_task())

    async def _recompute_sched_options_task(self):
        try:
            expdesc, _ = await self.manager.compute_expdesc(self.expurl)
        except:
            logger.error("Could not recompute experiment description of '%s'",
                         self.expurl, exc_info=True)
            return
        sched_defaults = expdesc["scheduler_defaults"]

        scheduling = self.manager.get_submission_scheduling(self.expurl)
        scheduling.update(sched_defaults)
        self.priority.setValue(scheduling["priority"])
        self.pipeline_name.setText(scheduling["pipeline_name"])
        self.flush.setChecked(scheduling["flush"])

    def _load_hdf5_clicked(self):
        asyncio.ensure_future(self._load_hdf5_task())

    async def _load_hdf5_task(self):
        try:
            filename = await get_open_file_name(
                self.manager.main_window, "Load HDF5",
                self.hdf5_load_directory,
                "HDF5 files (*.h5 *.hdf5);;All files (*.*)")
        except asyncio.CancelledError:
            return
        self.hdf5_load_directory = os.path.dirname(filename)

        try:
            with h5py.File(filename, "r") as f:
                expid = f["expid"][()]
            expid = pyon.decode(expid)
            arguments = expid["arguments"]
        except:
            logger.error("Could not retrieve expid from HDF5 file",
                         exc_info=True)
            return

        try:
            self.log_level.setCurrentIndex(log_levels.index(
                log_level_to_name(expid["log_level"])))
            if ("repo_rev" in expid and
                    expid["repo_rev"] != "N/A" and
                    hasattr(self, "repo_rev")):
                self.repo_rev.setText(expid["repo_rev"])
        except:
            logger.error("Could not set submission options from HDF5 expid",
                         exc_info=True)
            return

        await self._recompute_arguments_task(arguments)

    def closeEvent(self, event):
        if self.argeditor:
            self.argeditor.about_to_close()
        self.sigClosed.emit()
        QtWidgets.QMdiSubWindow.closeEvent(self, event)

    def save_state(self):
        if self.argeditor:
            argeditor_state = self.argeditor.save_state()
        else:
            argeditor_state = self.argeditor_state
        return {
            "args": argeditor_state,
            "geometry": bytes(self.saveGeometry()),
            "hdf5_load_directory": self.hdf5_load_directory
        }

    def restore_state(self, state):
        if self.argeditor:
            self.argeditor.restore_state(state["args"])
        else:
            self.argeditor_state = state["args"]
        self.restoreGeometry(QtCore.QByteArray(state["geometry"]))
        self.hdf5_load_directory = state["hdf5_load_directory"]


class UnknownWorkerManagerError(Exception):
    pass


class MissingExperimentError(Exception):
    pass


class ExperimentManager:
    #: Registry for custom argument editor classes, indexed by the experiment
    #: argument_ui key string.
    argument_ui_classes = dict()

    def __init__(self, main_window, dataset_sub,
                 explist_sub, schedule_sub,
                 worker_manager_sub,
                 schedule_ctl, experiment_db_ctl,
                 local_worker_manager,
                 ):
        self.main_window = main_window
        self.schedule_ctl = schedule_ctl
        self.experiment_db_ctl = experiment_db_ctl
        self.local_worker_manager = local_worker_manager

        self.dock_states = dict()
        self.submission_scheduling = dict()
        self.submission_options = dict()
        self.submission_arguments = dict()
        self.argument_ui_names = dict()

        self.datasets = dict()
        dataset_sub.add_setmodel_callback(self.set_dataset_model)
        self.explist = dict()
        explist_sub.add_setmodel_callback(self.set_explist_model)
        explist_sub.notify_cbs.append(self._explist_update)
        self.schedule = dict()
        schedule_sub.add_setmodel_callback(self.set_schedule_model)
        self.worker_managers = {}
        worker_manager_sub.add_setmodel_callback(self.set_worker_manager_model)
        worker_manager_sub.notify_cbs.append(self._worker_manager_update)

        self.open_experiments = dict()

    def set_dataset_model(self, model):
        self.datasets = model

    def set_explist_model(self, model):
        self.explist = model.backing_store

    def _explist_update(self, mod):
        logger.info(f"Explist {summarise_mod(mod)}")

        # This is pretty indiscriminate. In theory, we should be able to target
        # the load of only the experiments that have just been added to
        # explist. But that opens up an edge case around worker managers
        # appearing, but with a different set of experiments. And it seems like
        # an unnecessary optimisation at the moment.
        if mod["action"] in ["init", "setitem"]:
            for exp in self.open_experiments.values():
                if not exp.loaded:
                    exp.try_load()

    def _worker_manager_update(self, mod):
        # This is necessary so that any files (i.e. outside repo experiments)
        # can an opportunity to load after the worker manager.

        logger.info(f"Wkr-mgr {summarise_mod(mod)}")

        # This is pretty indiscriminate. See explist_update
        if mod["action"] in ["init", "setitem"]:
            for exp in self.open_experiments.values():
                if not exp.loaded:
                    exp.try_load()

    def set_schedule_model(self, model):
        self.schedule = model.backing_store

    def set_worker_manager_model(self, model):
        self.worker_managers = model.backing_store

    def resolve_expurl(self, expurl):
        parsed = urllib.parse.urlparse(expurl)
        urlpath = parsed.path
        worker_manager_id = None
        if parsed.hostname:
            worker_manager_id = parsed.hostname
            urlpath = urlpath[1:]
        # Check worker_managers not explist because this might be a file
        # experiment from a non-scanned worker manager
        if worker_manager_id not in self.worker_managers:
            raise UnknownWorkerManagerError(
                f"Experiment references unknown worker manager ({worker_manager_id})"
            )
        if parsed.scheme == "repo":
            try:
                explist = self.explist[worker_manager_id]
            except KeyError as ex:
                raise UnknownWorkerManagerError(
                    f"Repo experiment references worker manager "
                    f"({worker_manager_id}) which has not been scanned"
                ) from ex
            try:
                expinfo = explist[urlpath]
            except KeyError as ex:
                raise MissingExperimentError(
                    f"Experiment {urlpath} can't be found in repository"
                )
            return ExpUrlResolution(
                True,
                worker_manager_id,
                file=expinfo["file"],
                class_name=expinfo["class_name"],
                repo_experiment_description=expinfo,
                repo_experiment_id=urlpath,
            )
        elif parsed.scheme == "file":
            return ExpUrlResolution(
                False,
                worker_manager_id,
                file=urlpath,
                class_name=parsed.fragment,
            )
        else:
            raise ValueError("Malformed experiment URL: unrecognised scheme '{}'".format(
                parsed.scheme
            ))

    def get_argument_editor_class(self, expurl):
        ui_name = self.argument_ui_names.get(expurl, None)
        if not ui_name:
            exp = self.resolve_expurl(expurl)
            if exp.repo:
                ui_name = exp.repo_experiment_description.get("argument_ui", None)
        if ui_name:
            result = self.argument_ui_classes.get(ui_name, None)
            if result:
                return result
            logger.warning("Ignoring unknown argument UI '%s'", ui_name)
        return _ArgumentEditor

    def get_submission_scheduling(self, expurl):
        if expurl in self.submission_scheduling:
            return self.submission_scheduling[expurl]
        else:
            # mutated by _ExperimentDock
            scheduling = {
                "pipeline_name": "main",
                "priority": 0,
                "due_date": None,
                "flush": False
            }
            exp = self.resolve_expurl(expurl)
            if exp.repo:
                scheduling.update(exp.repo_experiment_description["scheduler_defaults"])
            self.submission_scheduling[expurl] = scheduling
            return scheduling

    def get_submission_options(self, expurl):
        if expurl in self.submission_options:
            return self.submission_options[expurl]
        else:
            # mutated by _ExperimentDock
            options = {
                "log_level": logging.WARNING
            }
            if expurl[:5] == "repo:":
                options["repo_rev"] = None
            self.submission_options[expurl] = options
            return options

    def initialize_submission_arguments(self, expurl, arginfo, ui_name):
        arguments = OrderedDict()
        for name, (procdesc, group, tooltip) in arginfo.items():
            state = procdesc_to_entry(procdesc).default_state(procdesc)
            arguments[name] = {
                "desc": procdesc,
                "group": group,
                "tooltip": tooltip,
                "state": state,  # mutated by entries
            }
        self.submission_arguments[expurl] = arguments
        self.argument_ui_names[expurl] = ui_name
        return arguments

    def get_submission_arguments(self, expurl):
        if expurl in self.submission_arguments:
            return self.submission_arguments[expurl]
        else:
            exp = self.resolve_expurl(expurl)
            if not exp.repo:
                raise ValueError("Submission arguments must be preinitialized "
                                 "when not using repository")
            class_desc = exp.repo_experiment_description
            return self.initialize_submission_arguments(expurl,
                class_desc["arginfo"], class_desc.get("argument_ui", None))

    def open_experiment(self, expurl):
        if expurl in self.open_experiments:
            dock = self.open_experiments[expurl]
            if dock.isMinimized():
                dock.showNormal()
            self.main_window.centralWidget().setActiveSubWindow(dock)
            return dock
        try:
            dock = _ExperimentDock(self, expurl)
        except:
            logger.warning("Failed to create experiment dock for %s, "
                           "attempting to reset arguments", expurl,
                           exc_info=True)
            del self.submission_arguments[expurl]
            dock = _ExperimentDock(self, expurl)
        self.open_experiments[expurl] = dock
        dock.setAttribute(QtCore.Qt.WA_DeleteOnClose)
        self.main_window.centralWidget().addSubWindow(dock)
        dock.show()
        dock.sigClosed.connect(partial(self.on_dock_closed, expurl))
        if expurl in self.dock_states:
            try:
                dock.restore_state(self.dock_states[expurl])
            except:
                logger.warning("Failed to restore dock state when opening "
                               "experiment %s", expurl,
                               exc_info=True)
        return dock

    def on_dock_closed(self, expurl):
        dock = self.open_experiments[expurl]
        self.dock_states[expurl] = dock.save_state()
        del self.open_experiments[expurl]

    async def _submit_task(self, expurl, *args):
        rid = await self.schedule_ctl.submit(*args)
        logger.info("Submitted '%s', RID is %d", expurl, rid)

    def submit(self, expurl):
        exp = self.resolve_expurl(expurl)
        scheduling = self.get_submission_scheduling(expurl)
        options = self.get_submission_options(expurl)
        arguments = self.get_submission_arguments(expurl)

        argument_values = dict()
        for name, argument in arguments.items():
            entry_cls = procdesc_to_entry(argument["desc"])
            argument_values[name] = entry_cls.state_to_value(argument["state"])

        expid = {
            "log_level": options["log_level"],
            "file": exp.file,
            "class_name": exp.class_name,
            "arguments": argument_values,
            "worker_manager_id": exp.worker_manager_id,
        }
        if "repo_rev" in options:
            expid["repo_rev"] = options["repo_rev"]
        asyncio.ensure_future(self._submit_task(
            expurl,
            scheduling["pipeline_name"],
            expid,
            scheduling["priority"], scheduling["due_date"],
            scheduling["flush"]))

    async def _request_term_multiple(self, rids):
        for rid in rids:
            try:
                await self.schedule_ctl.request_termination(rid)
            except:
                # May happen if the experiment has terminated by itself
                # while we were terminating others.
                logger.debug("failed to request termination of RID %d",
                             rid, exc_info=True)

    def request_inst_term(self, expurl):
        logger.info(
            "Requesting termination of all instances "
            "of '%s'", expurl)
        exp = self.resolve_expurl(expurl)
        rids = []
        for rid, desc in self.schedule.items():
            expid = desc["expid"]
            if exp.repo:
                repo_match = "repo_rev" in expid
            else:
                repo_match = "repo_rev" not in expid
            if (repo_match and
                    expid["file"] == exp.file and
                    expid["class_name"] == exp.class_name and
                    expid["worker_manager_id"] == exp.worker_manager_id):
                rids.append(rid)
        asyncio.ensure_future(self._request_term_multiple(rids))

    async def compute_expdesc(self, expurl):
        exp = self.resolve_expurl(expurl)
        if exp.repo:
            revision = self.get_submission_options(expurl)["repo_rev"]
        else:
            revision = None
        description = await self.experiment_db_ctl.examine(
            exp.file, exp.repo, revision, exp.worker_manager_id)
        class_desc = description[exp.class_name]
        return class_desc, class_desc.get("argument_ui", None)

    async def open_file(self, file, worker_manager_id=None):
        description = await self.experiment_db_ctl.examine(
            file, False,
            worker_manager_id=worker_manager_id,
        )
        for class_name, class_desc in description.items():
            expurl = make_url("file", worker_manager_id, file, class_name)
            self.initialize_submission_arguments(expurl, class_desc["arginfo"],
                class_desc.get("argument_ui", None))
            if expurl in self.open_experiments:
                self.open_experiments[expurl].close()
            self.open_experiment(expurl)

    async def open_local_file(self, file):
        await self.open_file(file, self.local_worker_manager.id)

    def save_state(self):
        for expurl, dock in self.open_experiments.items():
            self.dock_states[expurl] = dock.save_state()
        return {
            "scheduling": self.submission_scheduling,
            "options": self.submission_options,
            "arguments": self.submission_arguments,
            "docks": self.dock_states,
            "argument_uis": self.argument_ui_names,
            "open_docks": set(self.open_experiments.keys())
        }

    def restore_state(self, state):
        if self.open_experiments:
            raise NotImplementedError
        self.dock_states = state["docks"]
        self.submission_scheduling = state["scheduling"]
        self.submission_options = state["options"]
        self.submission_arguments = state["arguments"]
        self.argument_ui_names = state.get("argument_uis", {})
        for expurl in state["open_docks"]:
            self.open_experiment(expurl)
