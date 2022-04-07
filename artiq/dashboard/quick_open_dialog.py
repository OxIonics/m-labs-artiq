import logging

from PyQt5 import QtGui, QtWidgets
from qasync import QtCore

from artiq.dashboard.experiments import (
    ExperimentManager,
    MissingExperimentError,
    UnknownWorkerManagerError,
    make_url,
)
from artiq.dashboard.explorer import ExplorerDock
from artiq.gui.fuzzy_select import FuzzySelectWidget

logger = logging.getLogger(__name__)
_is_quick_open_shown = False


def init_quick_open_dialog(
        main_window,
        explorer: ExplorerDock,
        experiment_mgr: ExperimentManager
):
    def show_quick_open():
        global _is_quick_open_shown
        if _is_quick_open_shown:
            return

        _is_quick_open_shown = True
        dialog = _QuickOpenDialog(experiment_mgr, explorer.active_worker_manager_id)

        def closed():
            global _is_quick_open_shown
            _is_quick_open_shown = False
        dialog.closed.connect(closed)
        dialog.show()

    quick_open_shortcut = QtWidgets.QShortcut(
        QtCore.Qt.CTRL + QtCore.Qt.Key_P,
        main_window
    )
    quick_open_shortcut.setContext(QtCore.Qt.ApplicationShortcut)
    quick_open_shortcut.activated.connect(show_quick_open)


def _check_experiment_manager_id(manager, url, worker_manager_id):
    try:
        exp = manager.resolve_expurl(url)
    except (MissingExperimentError, UnknownWorkerManagerError):
        return None
    if exp.worker_manager_id == worker_manager_id:
        return exp
    else:
        return None


class _QuickOpenDialog(QtWidgets.QDialog):
    """Modal dialog for opening/submitting experiments from a
    FuzzySelectWidget."""
    closed = QtCore.pyqtSignal()

    def __init__(self, manager: ExperimentManager, active_worker_manager_id):
        super().__init__(manager.main_window)
        self.setModal(True)

        self.manager = manager

        self.setWindowTitle("Quick open...")

        layout = QtWidgets.QGridLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        self.setLayout(layout)

        # Find matching experiment names. Open experiments are preferred to
        # matches from the repository to ease quick window switching.
        open_exps = self.manager.open_experiments.keys()

        repo_exps = set(
            make_url("repo", active_worker_manager_id, k)
            for k in self.manager.explist[active_worker_manager_id].keys()
        ) - set(open_exps)
        choices = [
            (url,
             exp.repo_experiment_id if exp.repo else f"{exp.file}#{exp.class_name}",
             100,
             )
            for url in open_exps
            if (exp := _check_experiment_manager_id(manager, url, active_worker_manager_id))
        ] + [
            (url,
             manager.resolve_expurl(url).repo_experiment_id,
             0,
             )
            for url in repo_exps
        ]

        self.select_widget = FuzzySelectWidget(choices)
        layout.addWidget(self.select_widget)
        self.select_widget.aborted.connect(self.close)
        self.select_widget.finished.connect(self._open_experiment)

        font_metrics = QtGui.QFontMetrics(self.select_widget.line_edit.font())
        self.select_widget.setMinimumWidth(font_metrics.averageCharWidth() * 70)

    def done(self, r):
        if self.select_widget:
            self.select_widget.abort()
        self.closed.emit()
        QtWidgets.QDialog.done(self, r)

    def _open_experiment(self, expurl, modifiers):
        if modifiers & QtCore.Qt.ControlModifier:
            try:
                self.manager.submit(expurl)
            except:
                # Not all open_experiments necessarily still exist in the explist
                # (e.g. if the repository has been re-scanned since).
                logger.warning("failed to submit experiment '%s'",
                               expurl,
                               exc_info=True)
        else:
            self.manager.open_experiment(expurl)
        self.close()
