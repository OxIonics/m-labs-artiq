import asyncio
import os
import tempfile
import shutil
import time
import logging
from typing import List, Tuple

from sipyco.sync_struct import Notifier, process_mod, update_from_dict

from artiq.master.worker import (Worker, WorkerInternalException,
                                 log_worker_exception)
from artiq.master.worker_managers import WorkerManagerDB, WorkerManagerProxy
from artiq.master.worker_transport import PipeWorkerTransport, WorkerTransport
from artiq.tools import get_windows_drives, exc_to_warning


logger = logging.getLogger(__name__)


class _RepoScanner:
    def __init__(self, worker_handlers):
        self.worker_handlers = worker_handlers
        self.worker = None

    async def process_file(self, entry_dict, repo_rev: "RepositoryVersion", filename):
        logger.debug("processing file %s %s", repo_rev, filename)
        try:
            description = await self.worker.examine(
                "scan", os.path.join(repo_rev.get_working_dir(), filename))
        except:
            log_worker_exception()
            raise
        for class_name, class_desc in description.items():
            name = class_desc["name"]
            if "/" in name:
                logger.warning("Character '/' is not allowed in experiment "
                               "name (%s)", name)
                name = name.replace("/", "_")
            if name in entry_dict:
                basename = name
                i = 1
                while name in entry_dict:
                    name = basename + str(i)
                    i += 1
                logger.warning("Duplicate experiment name: '%s'\n"
                               "Renaming class '%s' in '%s' to '%s'",
                               basename, class_name, filename, name)
            entry = {
                "file": filename,
                "class_name": class_name,
                "arginfo": class_desc["arginfo"],
                "argument_ui": class_desc["argument_ui"],
                "scheduler_defaults": class_desc["scheduler_defaults"]
            }
            entry_dict[name] = entry

    async def _scan(self, repo_rev: "RepositoryVersion", subdir=""):
        entry_dict = dict()
        files, dirs = await repo_rev.scan_dir(os.path.join(repo_rev.get_working_dir(), subdir))
        for name in files:
            if name.startswith("."):
                continue
            if name.endswith(".py"):
                filename = os.path.join(subdir, name)
                try:
                    await self.process_file(entry_dict, repo_rev, filename)
                except Exception as exc:
                    logger.warning("Skipping file '%s'", filename,
                                   exc_info=not isinstance(exc, WorkerInternalException))
                    # restart worker
                    await self.worker.close()
                    self.worker = Worker(
                        self.worker_handlers, transport=repo_rev.get_worker_transport(),
                    )
        for name in dirs:
            if name.startswith("."):
                continue
            subentries = await self._scan(
                repo_rev, os.path.join(subdir, name)
            )
            entries = {name + "/" + k: v for k, v in subentries.items()}
            entry_dict.update(entries)

        return entry_dict

    async def scan(self, repo_rev, subdir=""):
        self.worker = Worker(
            self.worker_handlers, transport=repo_rev.get_worker_transport()
        )
        try:
            r = await self._scan(repo_rev, subdir)
        finally:
            await self.worker.close()
        return r


class ExperimentRepo:
    def __init__(
            self, repo_backend, worker_handlers, experiment_subdir,
            explist, status
    ):
        self.repo_backend: RepositoryBackend = repo_backend
        self.worker_handlers = worker_handlers
        self.experiment_subdir = experiment_subdir

        self.cur_rev = self.repo_backend.get_head_rev()
        self.repo_backend.request_rev(self.cur_rev)
        self.explist = explist
        self._scanning = False

        self.status = status
        self.status["cur_rev"] = self.cur_rev

    def close(self):
        # The object cannot be used anymore after calling this method.
        self.repo_backend.release_rev(self.cur_rev)

    async def scan_repository(self, new_cur_rev=None):
        if self._scanning:
            return
        self._scanning = True
        self.status["scanning"] = True
        try:
            if new_cur_rev is None:
                new_cur_rev = self.repo_backend.get_head_rev()
            repo_rev = self.repo_backend.request_rev(new_cur_rev)
            self.repo_backend.release_rev(self.cur_rev)
            self.cur_rev = new_cur_rev
            self.status["cur_rev"] = new_cur_rev
            t1 = time.monotonic()
            new_explist = await _RepoScanner(self.worker_handlers).scan(
                repo_rev, self.experiment_subdir)
            logger.info("repository scan took %d seconds", time.monotonic()-t1)
            update_from_dict(self.explist, new_explist)
        finally:
            self._scanning = False
            self.status["scanning"] = False

    def scan_repository_async(self, new_cur_rev=None):
        asyncio.ensure_future(
            exc_to_warning(self.scan_repository(new_cur_rev)))

    async def examine(
            self, filename, use_repository=True, revision=None,
    ):
        if use_repository:
            if revision is None:
                revision = self.cur_rev
            # If revision is not None and the backend is the FileSystemBackend
            # or a worker manager is selected then the revision is silently
            # ignored.
            repo_rev = self.repo_backend.request_rev(revision)
            filename = os.path.join(repo_rev.get_working_dir(), filename)
        worker = Worker(self.worker_handlers, transport=self.repo_backend.get_worker_transport())
        try:
            description = await worker.examine("examine", filename)
        finally:
            await worker.close()
        if use_repository:
            self.repo_backend.release_rev(revision)
        return description

    async def list_directory(self, directory):
        return await self.repo_backend.list_directory(directory)

    def get_worker_transport(self):
        return self.repo_backend.get_worker_transport()

    def request_rev(self, rev):
        return self.repo_backend.request_rev(rev)

    def release_rev(self, rev):
        return self.repo_backend.request_rev(rev)


class ExperimentDB:
    def __init__(
            self, repo_backend, worker_handlers, worker_manager_db,
            experiment_subdir="",
    ):
        local_repo = ExperimentRepo(
            repo_backend, worker_handlers, experiment_subdir,
            Notifier({}),
            Notifier({
                "cur_rev": "N/A",
                "scanning": None,
            }),
        )
        self.worker_handlers = worker_handlers
        self.worker_manager_db: WorkerManagerDB = worker_manager_db

        self._repos = {None: local_repo}

        self.local_explist = Notifier({})
        self.local_status = Notifier(local_repo.status.raw_view)
        self.all_explist = Notifier({None: {}})
        self.all_status = Notifier({None: local_repo.status.raw_view})

        def local_exp_publish(mod):
            process_mod(self.local_explist, mod)
            process_mod(self.all_explist[None], mod)

        def local_status_publish(mod):
            process_mod(self.local_status, mod)
            process_mod(self.all_status[None], mod)

        local_repo.explist.publish = local_exp_publish
        local_repo.status.publish = local_status_publish

    def get_repo(self, worker_manager_id):
        try:
            return self._repos[worker_manager_id]
        except KeyError:
            pass

        proxy = self.worker_manager_db.get_proxy(worker_manager_id)
        proxy.add_on_close(lambda: self._worker_manager_closed(worker_manager_id))
        self.all_explist[worker_manager_id] = {}
        self.all_status[worker_manager_id] = {
            "scanning": False,
            "cur_rev": "N/A",
        }
        self._repos[worker_manager_id] = ExperimentRepo(
            ManagedBackend(proxy),
            self.worker_handlers, "",
            self.all_explist[worker_manager_id],
            self.all_status[worker_manager_id],
        )
        return self._repos[worker_manager_id]

    def _worker_manager_closed(self, worker_manager_id):
        try:
            repo = self._repos.pop(worker_manager_id)
        except KeyError:
            pass
        else:
            repo.close()
        try:
            del self.all_explist[worker_manager_id]
        except KeyError:
            pass
        try:
            del self.all_status[worker_manager_id]
        except KeyError:
            pass

    def close(self):
        # The object cannot be used anymore after calling this method.
        for repo in self._repos.values():
            repo.close()

    async def scan_repository(self, new_cur_rev=None, worker_manager_id=None):
        return await self.get_repo(worker_manager_id).scan_repository(new_cur_rev)

    def scan_repository_async(self, new_cur_rev=None, worker_manager_id=None):
        asyncio.ensure_future(
            exc_to_warning(self.scan_repository(new_cur_rev, worker_manager_id)))

    async def examine(
            self, filename, use_repository=True, revision=None,
            worker_manager_id=None,
    ):
        return await self.get_repo(worker_manager_id).examine(filename, use_repository, revision)

    async def list_directory(self, directory, worker_manager_id=None):
        return await self.get_repo(worker_manager_id).list_directory(directory)


class RepositoryVersion:

    def get_working_dir(self):
        raise NotImplementedError()

    async def scan_dir(self, path) -> Tuple[List[str], List[str]]:
        raise NotImplementedError()

    def get_worker_transport(self) -> WorkerTransport:
        raise NotImplementedError()

    def get_msg(self):
        return None


class LocalScanMixin(RepositoryVersion):

    async def scan_dir(self, path) -> Tuple[List[str], List[str]]:
        files = []
        dirs = []
        for de in os.scandir(path):  # type: os.DirEntry
            if de.is_file():
                files.append(de.name)
            if de.is_dir():
                dirs.append(de.name)
        return files, dirs

    def get_worker_transport(self) -> WorkerTransport:
        return PipeWorkerTransport()


class RepositoryBackend:

    def get_head_rev(self):
        return "N/A"

    def request_rev(self, rev) -> RepositoryVersion:
        raise NotImplementedError

    def release_rev(self, rev):
        pass

    def get_worker_transport(self) -> WorkerTransport:
        raise NotImplementedError()

    async def list_directory(self, directory) -> List[str]:
        raise NotImplementedError()


class LocalBackend(RepositoryBackend):

    def get_worker_transport(self) -> WorkerTransport:
        return PipeWorkerTransport()

    async def list_directory(self, directory) -> List[str]:
        prefix = ""
        if not directory:
            if os.name == "nt":
                drives = get_windows_drives()
                return [drive + ":\\" for drive in drives]
            else:
                directory = "/"
                prefix = "/"
        r = []
        for de in os.scandir(directory):  # type: os.DirEntry
            if de.is_file():
                r.append(prefix + de.name)
            if de.is_dir():
                r.append(prefix + de.name + os.path.sep)
        return r


class FilesystemBackend(LocalBackend, LocalScanMixin):
    def __init__(self, root):
        self.root = os.path.abspath(root)

    def request_rev(self, rev):
        return self

    def get_working_dir(self):
        return self.root

    def __str__(self):
        return self.root


class _GitCheckout(LocalScanMixin):
    def __init__(self, git, rev):
        self.path = tempfile.mkdtemp()
        self.rev = rev
        commit = git.get(rev)
        git.checkout_tree(commit, directory=self.path)
        self.message = commit.message.strip()
        self.ref_count = 1
        logger.info("checked out revision %s into %s", rev, self.path)

    def dispose(self):
        logger.info("disposing of checkout in folder %s", self.path)
        shutil.rmtree(self.path)

    def get_working_dir(self):
        return self.path

    def get_msg(self):
        return self.message

    def __str__(self):
        return f"{self.path}@{self.rev}"


class GitBackend(LocalBackend):
    def __init__(self, root):
        # lazy import - make dependency optional
        import pygit2

        self.git = pygit2.Repository(root)
        self.checkouts = dict()

    def get_head_rev(self):
        return str(self.git.head.target)

    def request_rev(self, rev):
        if rev in self.checkouts:
            co = self.checkouts[rev]
            co.ref_count += 1
        else:
            co = _GitCheckout(self.git, rev)
            self.checkouts[rev] = co
        return co

    def release_rev(self, rev):
        co = self.checkouts[rev]
        co.ref_count -= 1
        if not co.ref_count:
            co.dispose()
            del self.checkouts[rev]


class ManagedBackend(RepositoryBackend, RepositoryVersion):
    def __init__(self, worker_manager_proxy):
        self._worker_manager_proxy: WorkerManagerProxy = worker_manager_proxy

    def request_rev(self, rev):
        return self

    def get_working_dir(self):
        return self._worker_manager_proxy.repo_root

    def __str__(self):
        return self._worker_manager_proxy.id

    async def scan_dir(self, path) -> Tuple[List[str], List[str]]:
        obj = await self._worker_manager_proxy.scan_dir(path)
        return obj["files"], obj["dirs"]

    async def list_directory(self, directory) -> List[str]:
        obj = await self._worker_manager_proxy.scan_dir(directory)
        rv = []
        prefix = obj["prefix"]
        for name in obj["files"]:
            rv.append(prefix + name)
        for name in obj["dirs"]:
            rv.append(prefix + name + os.path.sep)
        return rv

    def get_worker_transport(self) -> WorkerTransport:
        return self._worker_manager_proxy.get_transport()
