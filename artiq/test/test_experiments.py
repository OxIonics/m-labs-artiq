from collections import OrderedDict
from contextlib import asynccontextmanager
from unittest import mock
import uuid

import pygit2
import pytest

from artiq.master import experiments
from artiq.master.experiments import ExperimentDB
from artiq.master.worker_managers import WorkerManagerDB
from artiq.test.consts import BIND
from artiq.test.helpers import DUMMY_WORKER_HANDLERS, assert_num_connection, wait_for
from artiq.test_tools.thread_worker_transport import ThreadWorkerTransport
from artiq.worker_manager.worker_manager import WorkerManager

experiment_template = """
from artiq.language import EnvExperiment

class {name}(EnvExperiment):
    pass
"""
description_template = {
    "arginfo": OrderedDict(),
    "argument_ui": None,
    "class_name": mock.ANY,
    "file": mock.ANY,
    "scheduler_defaults": {},
}


def _render_experiment(**kwargs):
    return experiment_template.format(**kwargs)


def _make_experiment_db(backend, worker_manager_db=None):
    return experiments.ExperimentDB(
        backend,
        DUMMY_WORKER_HANDLERS,
        worker_manager_db,
    )


@pytest.fixture()
def managed_repo_root(tmpdir):
    return tmpdir.mkdir("managed_repo_root")


@asynccontextmanager
async def _connect_worker_manager(worker_manager_db: WorkerManagerDB, repo_root):
    description = "Test workers"

    async with WorkerManager.context(
            BIND, worker_manager_db.get_ports()[0],
            description=description,
            transport_factory=ThreadWorkerTransport,
            repo_root=repo_root,
    ) as worker_manager:
        await wait_for(lambda: assert_num_connection(worker_manager_db))

        yield worker_manager


@pytest.fixture(autouse=True)
def _use_thread_workers(monkeypatch):
    monkeypatch.setattr(experiments, "PipeWorkerTransport", ThreadWorkerTransport)


BASIC_REPO_CONTENTS = {
    "Expr1": {
        **description_template,
        "class_name": "Expr1",
        "file": "expr1.py",
    },
    "subdir/Expr2": {
        **description_template,
        "class_name": "Expr2",
        "file": "subdir/expr2.py",
    }
}


def _basic_fs_repo(repo):
    repo.join("expr1.py").write(_render_experiment(name="Expr1"))
    repo.mkdir("subdir").join("expr2.py").write(
        _render_experiment(name="Expr2")
    )


def _basic_git_repo(repo_path):
    repo = pygit2.init_repository(repo_path, bare=True, initial_head="master")
    subdir: pygit2.TreeBuilder = repo.TreeBuilder()
    subdir.insert(
        "expr2.py",
        repo.create_blob(_render_experiment(name="Expr2")),
        pygit2.GIT_FILEMODE_BLOB,
    )
    root: pygit2.TreeBuilder = repo.TreeBuilder()
    root.insert("subdir", subdir.write(), pygit2.GIT_FILEMODE_TREE)
    root.insert(
        "expr1.py",
        repo.create_blob(_render_experiment(name="Expr1")),
        pygit2.GIT_FILEMODE_BLOB,
    )
    sig = pygit2.Signature("Bob", "bob@example.com")
    repo.create_commit(
        "HEAD",
        sig, sig,
        "The commit message",
        root.write(),
        []
    )
    return repo


async def test_scan_local_file_system(tmpdir):
    repo = tmpdir.mkdir("repo")
    _basic_fs_repo(repo)

    expr_db = _make_experiment_db(experiments.FilesystemBackend(str(repo)))
    await expr_db.scan_repository()

    assert expr_db.local_explist.raw_view == BASIC_REPO_CONTENTS
    assert expr_db.all_explist.raw_view[None] == BASIC_REPO_CONTENTS


async def test_scan_bare_git_repo(tmpdir):
    repo_path = str(tmpdir.join("repo"))
    _basic_git_repo(repo_path)

    expr_db = _make_experiment_db(experiments.GitBackend(repo_path))
    await expr_db.scan_repository()

    assert expr_db.local_explist.raw_view == BASIC_REPO_CONTENTS
    assert expr_db.all_explist.raw_view[None] == BASIC_REPO_CONTENTS


async def test_scan_new_rev(tmpdir):
    repo_path = str(tmpdir.join("repo"))
    repo = _basic_git_repo(repo_path)
    expr_db = _make_experiment_db(experiments.GitBackend(repo_path))
    await expr_db.scan_repository()

    commit = repo.revparse_single("HEAD")
    root = repo.TreeBuilder(commit.tree)
    root.insert(
        "expr3.py",
        repo.create_blob(_render_experiment(name="Expr3")),
        pygit2.GIT_FILEMODE_BLOB,
    )
    sig = pygit2.Signature("Alice", "alice@example.com")
    commit2_id = repo.create_commit(
        "HEAD",
        sig, sig,
        "Add experiment 3",
        root.write(),
        [commit.id],
    )

    assert expr_db.get_repo(None).cur_rev == str(commit.id)
    await expr_db.scan_repository()

    expected = {
        **BASIC_REPO_CONTENTS,
        "Expr3": {
            **description_template,
            "class_name": "Expr3",
            "file": "expr3.py",
        },
    }

    assert expr_db.get_repo(None).cur_rev == str(commit2_id)
    assert expr_db.local_explist.raw_view == expected
    assert expr_db.all_explist.raw_view[None] == expected


async def test_scan_of_worker_manager(
        tmpdir,
        worker_manager_db,
):
    managed_repo = tmpdir.mkdir("managed_repo")
    _basic_fs_repo(managed_repo)

    expr_db = _make_experiment_db(
        experiments.FilesystemBackend(tmpdir.mkdir("empty")),
        worker_manager_db,
    )
    async with _connect_worker_manager(worker_manager_db, str(managed_repo)) as worker_manager:

        await expr_db.scan_repository(worker_manager_id=worker_manager.id)

        assert expr_db.all_explist.raw_view[worker_manager.id] == BASIC_REPO_CONTENTS


async def test_failing_scan_of_worker_manager(
        tmpdir,
        worker_manager_db,
):

    expr_db = _make_experiment_db(
        experiments.FilesystemBackend(tmpdir.mkdir("empty")),
        worker_manager_db,
    )
    missing = str(tmpdir.join("missing"))
    async with _connect_worker_manager(worker_manager_db, missing) as worker_manager:

        with pytest.raises(RuntimeError) as ex:
            await expr_db.scan_repository(worker_manager_id=worker_manager.id)

        assert str(ex.value) == f"[Errno 2] No such file or directory: '{missing}/'"
