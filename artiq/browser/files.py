import logging
import os
from datetime import datetime

import h5py
from PyQt6 import QtCore, QtWidgets, QtGui

from artiq import compat


logger = logging.getLogger(__name__)


def open_h5(info):
    if not (info.isFile() and info.isReadable() and
            info.suffix() == "h5"):
        return
    try:
        return h5py.File(info.filePath(), "r")
    except OSError:  # e.g. file being written (see #470)
        logger.debug("OSError when opening HDF5 file %s", info.filePath(),
                     exc_info=True)
    except:
        logger.warning("unable to read HDF5 file %s", info.filePath(),
                       exc_info=True)


class ThumbnailIconProvider(QtWidgets.QFileIconProvider):
    def icon(self, info):
        icon = self.hdf5_thumbnail(info)
        if icon is None:
            icon = QtWidgets.QFileIconProvider.icon(self, info)
        return icon

    def hdf5_thumbnail(self, info):
        f = open_h5(info)
        if not f:
            return
        with f:
            try:
                t = f["datasets/thumbnail"]
            except KeyError:
                return
            try:
                img = QtGui.QImage.fromData(t[()])
            except:
                logger.warning("unable to read thumbnail from %s",
                               info.filePath(), exc_info=True)
                return
            pix = QtGui.QPixmap.fromImage(img)
            return QtGui.QIcon(pix)


class DirsOnlyProxy(QtCore.QSortFilterProxyModel):
    def filterAcceptsRow(self, row, parent):
        idx = self.sourceModel().index(row, 0, parent)
        if not self.sourceModel().fileInfo(idx).isDir():
            return False
        return QtCore.QSortFilterProxyModel.filterAcceptsRow(self, row, parent)


class ZoomIconView(QtWidgets.QListView):
    zoom_step = 2**.25
    aspect = 2/3
    default_size = 25
    min_size = 10
    max_size = 1000

    def __init__(self):
        QtWidgets.QListView.__init__(self)
        self._char_width = QtGui.QFontMetrics(self.font()).averageCharWidth()
        self.setViewMode(self.ViewMode.IconMode)
        w = self._char_width*self.default_size
        self.setIconSize(QtCore.QSize(w, int(w*self.aspect)))
        self.setFlow(self.Flow.LeftToRight)
        self.setResizeMode(self.ResizeMode.Adjust)
        self.setWrapping(True)

    def wheelEvent(self, ev):
        if ev.modifiers() & QtCore.Qt.KeyboardModifier.ControlModifier:
            a = self._char_width*self.min_size
            b = self._char_width*self.max_size
            w = self.iconSize().width()*self.zoom_step**(
                ev.angleDelta().y()/120.)
            if a <= w <= b:
                self.setIconSize(QtCore.QSize(int(w), int(w*self.aspect)))
        else:
            QtWidgets.QListView.wheelEvent(self, ev)


class Hdf5FileSystemModel(QtGui.QFileSystemModel):
    def __init__(self):
        QtGui.QFileSystemModel.__init__(self)
        self.setFilter(QtCore.QDir.Filter.Drives | QtCore.QDir.Filter.NoDotAndDotDot |
                       QtCore.QDir.Filter.AllDirs | QtCore.QDir.Filter.Files)
        self.setNameFilterDisables(False)
        self.setIconProvider(ThumbnailIconProvider())

    def data(self, idx, role):
        if role == QtCore.Qt.ItemDataRole.ToolTipRole:
            info = self.fileInfo(idx)
            h5 = open_h5(info)
            if h5 is not None:
                try:
                    expid = compat.pyon_decode(h5["expid"][()]) if "expid" in h5 else dict()
                    start_time = datetime.fromtimestamp(h5["start_time"][()]) if "start_time" in h5 else "<none>"
                    v = ("artiq_version: {}\nrepo_rev: {}\nfile: {}\n"
                         "class_name: {}\nrid: {}\nstart_time: {}").format(
                             h5["artiq_version"].asstr()[()] if "artiq_version" in h5 else "<none>",
                             expid.get("repo_rev", "<none>"),
                             expid.get("file", "<none>"), expid.get("class_name", "<none>"),
                             h5["rid"][()] if "rid" in h5 else "<none>", start_time)
                    return v
                except:
                    logger.warning("unable to read metadata from %s",
                                   info.filePath(), exc_info=True)
        return QtGui.QFileSystemModel.data(self, idx, role)


class FilesDock(QtWidgets.QDockWidget):
    dataset_activated = QtCore.pyqtSignal(str)
    dataset_changed = QtCore.pyqtSignal(str)
    metadata_changed = QtCore.pyqtSignal(dict)

    def __init__(self, datasets, browse_root=""):
        QtWidgets.QDockWidget.__init__(self, "Files")
        self.setObjectName("Files")
        self.setFeatures(self.DockWidgetFeature.DockWidgetMovable | self.DockWidgetFeature.DockWidgetFloatable)

        self.splitter = QtWidgets.QSplitter()
        self.setWidget(self.splitter)

        self.datasets = datasets

        self.model = Hdf5FileSystemModel()

        self.rt = QtWidgets.QTreeView()
        rt_model = DirsOnlyProxy()
        rt_model.setDynamicSortFilter(True)
        rt_model.setSourceModel(self.model)
        self.rt.setModel(rt_model)
        self.model.directoryLoaded.connect(
            lambda: self.rt.resizeColumnToContents(0))
        self.rt.setAnimated(False)
        if browse_root != "":
            browse_root = os.path.abspath(browse_root)
        self.rt.setRootIndex(rt_model.mapFromSource(
            self.model.setRootPath(browse_root)))
        self.rt.setHeaderHidden(True)
        self.rt.setSelectionBehavior(self.rt.SelectionBehavior.SelectRows)
        self.rt.setSelectionMode(self.rt.SelectionMode.SingleSelection)
        self.rt.selectionModel().currentChanged.connect(
            self.tree_current_changed)
        self.rt.setRootIsDecorated(False)
        for i in range(1, 4):
            self.rt.hideColumn(i)
        self.splitter.addWidget(self.rt)

        self.rl = ZoomIconView()
        self.rl.setModel(self.model)
        self.rl.selectionModel().currentChanged.connect(
            self.list_current_changed)
        self.rl.activated.connect(self.list_activated)
        self.splitter.addWidget(self.rl)

    def tree_current_changed(self, current, previous):
        idx = self.rt.model().mapToSource(current)
        self.rl.setRootIndex(idx)

    def list_current_changed(self, current, previous):
        info = self.model.fileInfo(current)
        f = open_h5(info)
        if not f:
            return
        logger.debug("loading datasets from %s", info.filePath())
        with f:
            try:
                expid = compat.pyon_decode(f["expid"][()]) if "expid" in f else dict()
                start_time = datetime.fromtimestamp(f["start_time"][()]) if "start_time" in f else "<none>"
                v = {
                    "artiq_version": f["artiq_version"].asstr()[()] if "artiq_version" in f else "<none>",
                    "repo_rev": expid.get("repo_rev", "<none>"),
                    "file": expid.get("file", "<none>"),
                    "class_name": expid.get("class_name", "<none>"),
                    "rid": f["rid"][()] if "rid" in f else "<none>",
                    "start_time": start_time,
                }
                self.metadata_changed.emit(v)
            except:
                logger.warning("unable to read metadata from %s",
                               info.filePath(), exc_info=True)

            rd = {}
            if "archive" in f:
                def visitor(k, v):
                    if isinstance(v, h5py.Dataset):
                        # v.attrs is a non-serializable h5py.AttributeManager, need to convert to dict
                        # See https://docs.h5py.org/en/stable/high/attr.html#h5py.AttributeManager
                        rd[k] = (True, v[()], dict(v.attrs))

                f["archive"].visititems(visitor)

            if "datasets" in f:
                def visitor(k, v):
                    if isinstance(v, h5py.Dataset):
                        if k in rd:
                            logger.warning("dataset '%s' is both in archive "
                                           "and outputs", k)
                        # v.attrs is a non-serializable h5py.AttributeManager, need to convert to dict
                        # See https://docs.h5py.org/en/stable/high/attr.html#h5py.AttributeManager
                        rd[k] = (True, v[()], dict(v.attrs))

                f["datasets"].visititems(visitor)

            self.datasets.init(rd)

        self.dataset_changed.emit(info.filePath())

    def list_activated(self, idx):
        info = self.model.fileInfo(idx)
        if not info.isDir():
            self.dataset_activated.emit(info.filePath())
            return
        self.rl.setRootIndex(idx)
        idx = self.rt.model().mapFromSource(idx)
        self.rt.expand(idx)
        self.rt.setCurrentIndex(idx)

    def select(self, path):
        f = os.path.abspath(path)
        if os.path.isdir(f):
            self.select_dir(f)
        else:
            self.select_file(f)

    def select_dir(self, path):
        if not os.path.exists(path):
            logger.warning("directory does not exist %s", path)
            return
        idx = self.model.index(path)
        if not idx.isValid():
            logger.warning("directory invalid %s", path)
            return
        self.rl.setRootIndex(idx)

        # ugly, see Spyder: late indexing, late scroll
        def scroll_when_loaded(p):
            if p != path:
                return
            self.model.directoryLoaded.disconnect(scroll_when_loaded)
            QtCore.QTimer.singleShot(
                100,
                lambda: self.rt.scrollTo(
                    self.rt.model().mapFromSource(self.model.index(path)),
                    self.rt.ScrollHint.PositionAtCenter)
            )
        self.model.directoryLoaded.connect(scroll_when_loaded)
        idx = self.rt.model().mapFromSource(idx)
        self.rt.expand(idx)
        self.rt.setCurrentIndex(idx)

    def select_file(self, path):
        if not os.path.exists(path):
            logger.warning("file does not exist %s", path)
            return
        self.select_dir(os.path.dirname(path))
        idx = self.model.index(path)
        if not idx.isValid():
            logger.warning("file invalid %s", path)
            return
        self.rl.setCurrentIndex(idx)

    def save_state(self):
        state = {
            "dir": self.model.filePath(self.rl.rootIndex()),
            "splitter": bytes(self.splitter.saveState()),
        }
        idx = self.rl.currentIndex()
        if idx.isValid():
            state["file"] = self.model.filePath(idx)
        else:
            state["file"] = None
        return state

    def restore_state(self, state):
        self.splitter.restoreState(QtCore.QByteArray(state["splitter"]))
        self.select_dir(state["dir"])
        if state["file"] is not None:
            self.select_file(state["file"])
