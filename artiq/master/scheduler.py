import asyncio
import logging
from enum import Enum, unique
from time import time
from functools import partial

from sipyco.sync_struct import Notifier, process_mod
from sipyco.asyncio_tools import TaskObject, Condition

from artiq.master.experiments import ExperimentDB
from artiq.master.worker import (Worker, log_worker_exception, ResumeAction,
                                 RunResult)
from artiq.tools import asyncio_wait_or_cancel


logger = logging.getLogger(__name__)


@unique
class RunStatus(Enum):
    pending = 0
    flushing = 1
    preparing = 2
    prepare_done = 3
    running = 4
    run_done = 5
    analyzing = 6
    deleting = 7
    paused = 8
    idle = 9


def _mk_worker_method(name):
    async def worker_method(self, *args, **kwargs):
        if self.worker.closed.is_set():
            # Worker already killed.
            return RunResult.completed
        m = getattr(self.worker, name)
        try:
            return await m(*args, **kwargs)
        except Exception as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            if self.worker.closed.is_set():
                logger.debug("suppressing worker exception of terminated run",
                             exc_info=True)
                return RunResult.completed
            else:
                raise
    return worker_method


class Run:
    def __init__(self, rid, pipeline_name,
                 wd, expid, priority, due_date, flush,
                 pool, worker_transport, **kwargs):
        # called through pool
        self.rid = rid
        self.pipeline_name = pipeline_name
        self.wd = wd
        self.expid = expid
        self.priority = priority
        self.due_date = due_date
        self.flush = flush

        # Update datasets through rid namespace.
        handlers = pool.worker_handlers
        namespaces = pool.dataset_namespaces
        if namespaces:
            namespaces.init_rid(rid)
            handlers = {
                **handlers,
                "update_dataset": partial(namespaces.update_rid_namespace, rid)
            }
        self.worker = Worker(handlers, worker_transport)

        self.termination_requested = False

        self._status = RunStatus.pending

        notification = {
            "pipeline": self.pipeline_name,
            "expid": self.expid,
            "priority": self.priority,
            "due_date": self.due_date,
            "flush": self.flush,
            "status": self._status.name
        }
        notification.update(kwargs)
        self._notifier = pool.notifier
        self._notifier[self.rid] = notification
        self._state_changed = pool.state_changed

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        self._status = value
        if not self.worker.closed.is_set():
            self._notifier[self.rid]["status"] = self._status.name
        self._state_changed.notify()

    def priority_key(self):
        """Return a comparable value that defines a run priority order.

        Applies only to runs the due date of which has already elapsed.
        """
        return (self.priority, -(self.due_date or 0), -self.rid)

    async def close(self):
        # called through pool
        await self.worker.close()
        del self._notifier[self.rid]

    _build = _mk_worker_method("build")

    async def build(self):
        await self._build(self.rid, self.pipeline_name,
                          self.wd, self.expid,
                          self.priority)

    prepare = _mk_worker_method("prepare")
    run = _mk_worker_method("run")
    resume = _mk_worker_method("resume")
    analyze = _mk_worker_method("analyze")


class RunPool:
    def __init__(self, ridc, worker_handlers, notifier, experiment_db,
                 dataset_namespaces):
        self.runs = dict()
        self.state_changed = Condition()

        self.ridc = ridc
        self.worker_handlers = worker_handlers
        self.notifier = notifier
        self.experiment_db: ExperimentDB = experiment_db
        self.dataset_namespaces = dataset_namespaces

    def submit(self, expid, priority, due_date, flush, pipeline_name):
        # mutates expid to insert head repository revision if None.
        # called through scheduler.
        rid = self.ridc.get()
        repo = self.experiment_db.get_repo(expid.get("worker_manager_id"))
        if "repo_rev" in expid:
            if expid["repo_rev"] is None:
                expid["repo_rev"] = repo.cur_rev
            repo_rev = repo.request_rev(expid["repo_rev"])
            wd = repo_rev.get_working_dir()
            repo_msg = repo_rev.get_msg()
        else:
            wd, repo_msg = None, None
        run = Run(rid, pipeline_name, wd, expid, priority, due_date, flush,
                  self, repo.get_worker_transport(),
                  repo_msg=repo_msg)
        self.runs[rid] = run
        self.state_changed.notify()
        return rid

    async def delete(self, rid):
        # called through deleter
        if rid not in self.runs:
            return
        run = self.runs[rid]
        await run.close()
        if "repo_rev" in run.expid:
            repo = self.experiment_db.get_repo(run.expid.get("worker_manger_id"))
            repo.release_rev(run.expid["repo_rev"])
        del self.runs[rid]


class PrepareStage(TaskObject):
    def __init__(self, pool, delete_cb):
        self.pool = pool
        self.delete_cb = delete_cb

    def _get_run(self):
        """If a run should get prepared now, return it. Otherwise, return a
        float giving the time until the next check, or None if no time-based
        check is required.

        The latter can be the case if there are no due-date runs, or none
        of them are going to become next-in-line before further pool state
        changes (which will also cause a re-evaluation).
        """
        pending_runs = list(
            filter(lambda r: r.status == RunStatus.pending,
                   self.pool.runs.values()))

        now = time()
        def is_runnable(r):
            return (r.due_date or 0) < now

        prepared_max = max((r.priority_key() for r in self.pool.runs.values()
                            if r.status == RunStatus.prepare_done),
                           default=None)
        def takes_precedence(r):
            return prepared_max is None or r.priority_key() > prepared_max

        candidate = max(filter(is_runnable, pending_runs),
                        key=lambda r: r.priority_key(),
                        default=None)
        if candidate is not None and takes_precedence(candidate):
            return candidate

        return min((r.due_date - now for r in pending_runs
                    if (not is_runnable(r) and takes_precedence(r))),
                   default=None)

    async def _do(self):
        while True:
            run = self._get_run()
            if run is None:
                await self.pool.state_changed.wait()
            elif isinstance(run, float):
                await asyncio_wait_or_cancel([self.pool.state_changed.wait()],
                                             timeout=run)
            else:
                if run.flush:
                    run.status = RunStatus.flushing
                    while not all(
                            r.status in (RunStatus.pending, RunStatus.deleting)
                            or r.priority < run.priority or r is run
                            for r in self.pool.runs.values()):
                        ev = [
                            self.pool.state_changed.wait(),
                            run.worker.closed.wait()
                        ]
                        await asyncio_wait_or_cancel(
                            ev, return_when=asyncio.FIRST_COMPLETED)
                        if run.worker.closed.is_set():
                            break
                    if run.worker.closed.is_set():
                        continue
                run.status = RunStatus.preparing
                try:
                    await run.build()
                    await run.prepare()
                except:
                    logger.error("got worker exception in prepare stage, "
                                 "deleting RID %d", run.rid)
                    log_worker_exception()
                    self.delete_cb(run.rid)
                else:
                    run.status = RunStatus.prepare_done


class RunStage(TaskObject):
    def __init__(self, pool, delete_cb):
        self.pool = pool
        self.delete_cb = delete_cb

        #: Runs that are running/paused, kept in order of ascending priority.
        self._run_stack = []

        #: Runs that might be idle, i.e. should be resumed with the
        #: check_idle_only action to check before scheduling them in.
        self._idle_runs = set()

        self._runs_to_unidle = []

        #: Makes sure `highest_priority_run()` isn't invoked multiple times
        #: while waiting for experiments to report back idle status. The lock
        #: should never be contended in normal operation, as only this main
        #: coroutine (_do) should call `highest_priority_run()` (for
        #: scheduling, or from within `check_pause()`). Having it is cheap,
        #: though, and makes sure e.g. tests can call it out-of-band.
        self._priority_lock = asyncio.Lock()

    def _next_prepared_run(self):
        """Return the highest-priority run from the pool that is done preparing
        and ready to be run."""
        prepared_runs = filter(lambda r: r.status == RunStatus.prepare_done,
                               self.pool.runs.values())
        try:
            r = max(prepared_runs, key=lambda r: r.priority_key())
        except ValueError:
            # prepared_runs is an empty sequence
            r = None
        return r

    async def _check_if_still_idle(self, run):
        """Wake up a given run that is assumed to be paused and check whether
        it should still be considered idle."""
        if run.termination_requested:
            return False

        if run.status == RunStatus.deleting:
            # Idle run was forcibly terminated. Un-idle it so it can be cleaned up in
            # the main loop.
            return False

        delete_run = False
        if run.status == RunStatus.idle:
            try:
                run_result = await run.resume(ResumeAction.check_still_idle)
                # The worker just got killed, e.g. during shutdown, so we need to
                # make sure not to attempt communication again.
                if run_result == RunResult.completed:
                    delete_run = True
            except:
                logger.error(
                    "got worker exception in idle callback, deleting RID %d",
                    run.rid)
                log_worker_exception()
                delete_run = True
        else:
            logger.error(
                "got unexpected status while checking RID %s "
                "for idleness, deleting: %s", run.rid, run.status)
            delete_run = True

        if delete_run:
            self.delete_cb(run.rid)
            return False

        return run_result == RunResult.idle

    async def highest_priority_run(self):
        """Return the run to schedule on the next permanent context switch.

        Blocks for runs to become available if there aren't currently any.
        """
        async with self._priority_lock:
            while True:
                # Find the first non-idle run on the stack.
                top_run = None
                for run in reversed(self._run_stack):
                    if (run not in self._idle_runs
                            or not await self._check_if_still_idle(run)):
                        top_run = run
                        break

                prepared_run = self._next_prepared_run()
                if (prepared_run and
                    (not top_run or
                     (prepared_run.priority_key() > top_run.priority_key()))):

                    # Insert the prepared run at the correct position of the
                    # stack.
                    prepared_priority = prepared_run.priority_key()
                    idx = len(self._run_stack)
                    while idx > 0:
                        priority = self._run_stack[idx - 1].priority_key()
                        if prepared_priority > priority:
                            break
                        idx -= 1
                    self._run_stack.insert(idx, prepared_run)
                    top_run = prepared_run

                if top_run:
                    # Un-idle all runs with lower priority to pause them.
                    for run in self._run_stack:
                        if run in self._idle_runs:
                            self._runs_to_unidle.append(run)
                            self._idle_runs.remove(run)
                        if run == top_run:
                            break
                    return top_run

                timeout = None if not self._idle_runs else 0.2
                await asyncio_wait_or_cancel([self.pool.state_changed.wait()],
                    timeout=timeout)

    async def _do(self):
        while True:
            # Get the highest-priority run to execute, blocking if necessary.
            #
            # At the end of the iteration, `run` will either be removed from
            # the run stack, or will still be incomplete, with `resume()` to be
            # invoked again in the future.
            run = await self.highest_priority_run()

            # Un-idle all the runs that are of lower priority than the run about
            # to be executed. The worker should immediately pause.
            for unidle_run in self._runs_to_unidle:
                if unidle_run == run:
                    break
                if unidle_run.status != RunStatus.idle:
                    logger.error("unexpected status %s pausing RID %s from idle",
                                 unidle_run.status, unidle_run.rid)
                unidle_run.status = RunStatus.running
                unidle_result = RunResult(await
                                          unidle_run.resume(ResumeAction.pause))
                if unidle_run.status != RunStatus.running:
                    logger.error(
                        "unexpected status %s after pausing RID %s from idle",
                        unidle_run.status, unidle_run.rid)
                unidle_run.status = RunStatus.paused
            self._runs_to_unidle.clear()

            if run.status not in (RunStatus.paused, RunStatus.prepare_done,
                    RunStatus.idle):
                if run.status != RunStatus.deleting:
                    logger.error("unexpected run status %s for RID %s",
                        run.status, run.rid)
                if run in self._idle_runs:
                    self._idle_runs.remove(run)
                self._run_stack.remove(run)
                continue

            try:
                if run.status == RunStatus.prepare_done:
                    run.status = RunStatus.running
                    run_result = await run.run()
                else:
                    if run.termination_requested:
                        if run.status == RunStatus.idle:
                            # If the run is currently idle, tell it to pause
                            # (which will then throw).
                            action = ResumeAction.pause
                        else:
                            action = ResumeAction.request_termination
                            # Clear "termination requested" flag so another
                            # exception will be triggered if it is set again
                            # during the resumed run.
                            run.termination_requested = False
                    else:
                        action = ResumeAction.resume
                    run.status = RunStatus.running
                    run_result = await run.resume(action)

                run_result = RunResult(run_result)  # no-op type assertion
            except:
                logger.error(
                    "got worker exception in run stage, deleting RID %d",
                    run.rid)
                log_worker_exception()
                self.delete_cb(run.rid)
                self._run_stack.remove(run)
            else:
                if run_result == RunResult.completed:
                    run.status = RunStatus.run_done
                    self._run_stack.remove(run)
                else:
                    if run_result == RunResult.idle:
                        run.status = RunStatus.idle
                        self._idle_runs.add(run)
                    else:
                        run.status = RunStatus.paused


class AnalyzeStage(TaskObject):
    def __init__(self, pool, delete_cb):
        self.pool = pool
        self.delete_cb = delete_cb

    def _get_run(self):
        run_runs = filter(lambda r: r.status == RunStatus.run_done,
                          self.pool.runs.values())
        try:
            r = max(run_runs, key=lambda r: r.priority_key())
        except ValueError:
            # run_runs is an empty sequence
            r = None
        return r

    async def _do(self):
        while True:
            run = self._get_run()
            while run is None:
                await self.pool.state_changed.wait()
                run = self._get_run()
            run.status = RunStatus.analyzing
            try:
                await run.analyze()
            except:
                logger.error("got worker exception in analyze stage of RID %d.",
                             run.rid)
                log_worker_exception()
            self.delete_cb(run.rid)


class Pipeline:
    def __init__(self, ridc, deleter, worker_handlers, notifier, experiment_db,
                 dataset_namespaces):
        self.pool = RunPool(ridc, worker_handlers, notifier, experiment_db,
                            dataset_namespaces)
        self._prepare = PrepareStage(self.pool, deleter.delete)
        self._run = RunStage(self.pool, deleter.delete)
        self._analyze = AnalyzeStage(self.pool, deleter.delete)

    def start(self):
        self._prepare.start()
        self._run.start()
        self._analyze.start()

    async def stop(self):
        # NB: restart of a stopped pipeline is not supported
        await self._analyze.stop()
        await self._run.stop()
        await self._prepare.stop()


class Deleter(TaskObject):
    """Provides a synchronous interface for instigating deletion of runs.

    :meth:`RunPool.delete` is an async function (it needs to close the worker
    connection, etc.), so we maintain a queue of RIDs to delete on a background task.
    """
    def __init__(self, pipelines, dataset_namespaces):
        self._pipelines = pipelines
        self._dataset_namespaces = dataset_namespaces
        self._queue = asyncio.Queue()

    def delete(self, rid):
        """Delete the run with the given RID.

        Multiple calls for the same RID are silently ignored.
        """
        logger.debug("delete request for RID %d", rid)
        for pipeline in self._pipelines.values():
            if rid in pipeline.pool.runs:
                pipeline.pool.runs[rid].status = RunStatus.deleting
                break
        self._queue.put_nowait(rid)

    async def join(self):
        await self._queue.join()

    async def _delete(self, rid):
        # By looking up the run by RID, we implicitly make sure to delete each run only
        # once.
        for pipeline in self._pipelines.values():
            if rid in pipeline.pool.runs:
                logger.debug("deleting RID %d...", rid)
                await pipeline.pool.delete(rid)
                if self._dataset_namespaces:
                    self._dataset_namespaces.finish_rid(rid)
                logger.debug("deletion of RID %d completed", rid)
                break

    async def _gc_pipelines(self):
        pipeline_names = list(self._pipelines.keys())
        for name in pipeline_names:
            if not self._pipelines[name].pool.runs:
                logger.debug("garbage-collecting pipeline '%s'...", name)
                await self._pipelines[name].stop()
                del self._pipelines[name]
                logger.debug("garbage-collection of pipeline '%s' completed",
                             name)

    async def _do(self):
        while True:
            rid = await self._queue.get()
            await self._delete(rid)
            await self._gc_pipelines()
            self._queue.task_done()


class Scheduler:
    def __init__(
            self,
            ridc,
            worker_handlers,
            experiment_db,
            dataset_namespaces,
    ):
        self.notifier = Notifier(dict())

        self._pipelines = dict()
        self._worker_handlers = worker_handlers
        self._experiment_db = experiment_db
        self._dataset_namespaces = dataset_namespaces
        self._terminated = False

        self._ridc = ridc
        self._deleter = Deleter(self._pipelines, dataset_namespaces)

    def start(self):
        self._deleter.start()

    async def stop(self):
        # NB: restart of a stopped scheduler is not supported
        self._terminated = True  # prevent further runs from being created
        for pipeline in self._pipelines.values():
            for rid in pipeline.pool.runs.keys():
                self._deleter.delete(rid)
        await self._deleter.join()
        await self._deleter.stop()
        if self._pipelines:
            logger.warning("some pipelines were not garbage-collected")

    def submit(self, pipeline_name, expid, priority=0, due_date=None, flush=False):
        """Submits a new run.

        When called through an experiment, the default values of
        ``pipeline_name``, ``expid`` and ``priority`` correspond to those of
        the current run."""
        # mutates expid to insert head repository revision if None
        if self._terminated:
            return
        try:
            pipeline = self._pipelines[pipeline_name]
        except KeyError:
            logger.debug("creating pipeline '%s'", pipeline_name)
            pipeline = Pipeline(self._ridc, self._deleter,
                                self._worker_handlers, self.notifier,
                                self._experiment_db, self._dataset_namespaces)
            self._pipelines[pipeline_name] = pipeline
            pipeline.start()
        return pipeline.pool.submit(
            expid, priority, due_date, flush, pipeline_name,
        )

    def delete(self, rid):
        """Kills the run with the specified RID."""
        self._deleter.delete(rid)

    def request_termination(self, rid):
        """Requests graceful termination of the run with the specified RID."""
        for pipeline in self._pipelines.values():
            if rid in pipeline.pool.runs:
                run = pipeline.pool.runs[rid]
                if run.status in (RunStatus.running, RunStatus.paused, RunStatus.idle):
                    run.termination_requested = True
                else:
                    self.delete(rid)
                break

    def get_status(self):
        """Returns a dictionary containing information about the runs currently
        tracked by the scheduler.

        Must not be modified."""
        return self.notifier.raw_view

    async def check_pause(self, rid):
        """Returns ``True`` if there is a condition that could make ``pause``
        not return immediately (termination requested or higher priority run).

        This is typically used to check from a kernel whether pausing would
        have any effect, in order to avoid the cost of switching kernels in
        the common case where ``pause`` does nothing.

        This function returns quickly, and does not have to be followed by a
        call to ``pause``.
        """
        for p in self._pipelines.values():
            if rid in p.pool.runs:
                pipeline = p
                break
        else:
            raise KeyError("RID not found")

        run = pipeline.pool.runs[rid]
        if run.status != RunStatus.running:
            return False
        if run.termination_requested:
            return True

        return run != (await pipeline._run.highest_priority_run())
