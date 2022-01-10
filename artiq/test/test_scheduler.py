import unittest
import logging
import asyncio
import sys
from time import time, sleep

from artiq.experiment import *
from artiq.master.scheduler import Scheduler
from artiq.master.databases import make_dataset


class EmptyExperiment(EnvExperiment):
    def build(self):
        pass

    def run(self):
        pass


class BackgroundExperiment(EnvExperiment):
    def build(self):
        self.setattr_device("scheduler")

    def run(self):
        try:
            while True:
                self.scheduler.pause()
                sleep(0.2)
        except TerminationRequested:
            self.set_dataset("termination_ok", True,
                             broadcast=True, archive=False)


class IdleExperiment(EnvExperiment):
    def build(self):
        self.setattr_device("scheduler")

    def _be_idle(self):
        return self.get_dataset("be_idle", archive=False)

    def run(self):
        def is_idle():
            if self.get_dataset("take_long_in_idle", archive=False):
                sleep(10.0)
            return self._be_idle()

        try:
            while True:
                if not self._be_idle():
                    sleep(0.2)
                    self.scheduler.pause()
                    continue

                if not self.scheduler.idle(is_idle):
                    self.scheduler.pause()

        except TerminationRequested:
            self.set_dataset(
                "termination_ok", True, broadcast=True, archive=False)


class CheckPauseBackgroundExperiment(EnvExperiment):
    def build(self):
        self.setattr_device("scheduler")

    def run(self):
        while True:
            while not self.scheduler.check_pause():
                sleep(0.2)
            self.scheduler.pause()


def _get_expid(name):
    return {
        "log_level": logging.WARNING,
        "file": sys.modules[__name__].__file__,
        "class_name": name,
        "arguments": dict()
    }


def _get_basic_steps(rid, expid, priority=0, flush=False):
    return [
        {"action": "setitem", "key": rid, "value":
            {"pipeline": "main", "status": "pending", "priority": priority,
             "expid": expid, "due_date": None, "flush": flush,
             "repo_msg": None},
            "path": []},
        {"action": "setitem", "key": "status", "value": "preparing",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "prepare_done",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "running",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "run_done",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "analyzing",
            "path": [rid]},
        {"action": "setitem", "key": "status", "value": "deleting",
            "path": [rid]},
        {"action": "delitem", "key": rid, "path": []}
    ]


class _RIDCounter:
    def __init__(self, next_rid):
        self._next_rid = next_rid

    def get(self):
        rid = self._next_rid
        self._next_rid += 1
        return rid


class SchedulerCase(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def test_steps(self):
        loop = self.loop
        scheduler = Scheduler(_RIDCounter(0), dict(), None, None)
        expid = _get_expid("EmptyExperiment")

        expect = _get_basic_steps(1, expid)
        done = asyncio.Event()
        expect_idx = 0
        def notify(mod):
            nonlocal expect_idx
            self.assertEqual(mod, expect[expect_idx])
            expect_idx += 1
            if expect_idx >= len(expect):
                done.set()
        scheduler.notifier.publish = notify

        scheduler.start()

        # Verify that a timed experiment far in the future does not
        # get run, even if it has high priority.
        late = time() + 100000
        expect.insert(0,
            {"action": "setitem", "key": 0, "value":
                {"pipeline": "main", "status": "pending", "priority": 99,
                 "expid": expid, "due_date": late, "flush": False,
                 "repo_msg": None},
             "path": []})
        scheduler.submit("main", expid, 99, late, False)

        # This one (RID 1) gets run instead.
        scheduler.submit("main", expid, 0, None, False)

        loop.run_until_complete(done.wait())
        scheduler.notifier.publish = None
        loop.run_until_complete(scheduler.stop())

    def test_pending_priority(self):
        """Check due dates take precedence over priorities when waiting to
        prepare."""
        loop = self.loop
        handlers = {}
        scheduler = Scheduler(_RIDCounter(0), handlers, None, None)
        handlers["scheduler_check_pause"] = scheduler.check_pause

        expid_empty = _get_expid("EmptyExperiment")

        expid_bg = _get_expid("CheckPauseBackgroundExperiment")
        # Suppress the SystemExit backtrace when worker process is killed.
        expid_bg["log_level"] = logging.CRITICAL

        high_priority = 3
        middle_priority = 2
        low_priority = 1
        late = time() + 100000
        early = time() + 1

        expect = [
            {
                "path": [],
                "action": "setitem",
                "value": {
                    "repo_msg": None,
                    "priority": low_priority,
                    "pipeline": "main",
                    "due_date": None,
                    "status": "pending",
                    "expid": expid_bg,
                    "flush": False
                },
                "key": 0
            },
            {
                "path": [],
                "action": "setitem",
                "value": {
                    "repo_msg": None,
                    "priority": high_priority,
                    "pipeline": "main",
                    "due_date": late,
                    "status": "pending",
                    "expid": expid_empty,
                    "flush": False
                },
                "key": 1
            },
            {
                "path": [],
                "action": "setitem",
                "value": {
                    "repo_msg": None,
                    "priority": middle_priority,
                    "pipeline": "main",
                    "due_date": early,
                    "status": "pending",
                    "expid": expid_empty,
                    "flush": False
                },
                "key": 2
            },
            {
                "path": [0],
                "action": "setitem",
                "value": "preparing",
                "key": "status"
            },
            {
                "path": [0],
                "action": "setitem",
                "value": "prepare_done",
                "key": "status"
            },
            {
                "path": [0],
                "action": "setitem",
                "value": "running",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "preparing",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "prepare_done",
                "key": "status"
            },
            {
                "path": [0],
                "action": "setitem",
                "value": "paused",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "running",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "run_done",
                "key": "status"
            },
            {
                "path": [0],
                "action": "setitem",
                "value": "running",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "analyzing",
                "key": "status"
            },
            {
                "path": [2],
                "action": "setitem",
                "value": "deleting",
                "key": "status"
            },
            {
                "path": [],
                "action": "delitem",
                "key": 2
            },
        ]
        done = asyncio.Event()
        expect_idx = 0
        def notify(mod):
            nonlocal expect_idx
            self.assertEqual(mod, expect[expect_idx])
            expect_idx += 1
            if expect_idx >= len(expect):
                done.set()
        scheduler.notifier.publish = notify

        scheduler.start()

        scheduler.submit("main", expid_bg, low_priority)
        scheduler.submit("main", expid_empty, high_priority, late)
        scheduler.submit("main", expid_empty, middle_priority, early)

        loop.run_until_complete(done.wait())
        scheduler.notifier.publish = None
        loop.run_until_complete(scheduler.stop())

    def test_pause(self):
        loop = self.loop

        termination_ok = False
        def check_termination(mod):
            nonlocal termination_ok
            self.assertEqual(
                mod,
                {
                    "action": "setitem",
                    "key": "termination_ok",
                    "value": make_dataset(value=True),
                    "path": []
                }
            )
            termination_ok = True
        handlers = {
            "update_dataset": check_termination
        }
        scheduler = Scheduler(_RIDCounter(0), handlers, None, None)

        expid_bg = _get_expid("BackgroundExperiment")
        expid = _get_expid("EmptyExperiment")

        expect = _get_basic_steps(1, expid)
        background_running = asyncio.Event()
        empty_ready = asyncio.Event()
        empty_completed = asyncio.Event()
        background_completed = asyncio.Event()
        expect_idx = 0
        def notify(mod):
            nonlocal expect_idx
            if mod == {"path": [0],
                       "value": "running",
                       "key": "status",
                       "action": "setitem"}:
                background_running.set()
            if mod == {"path": [0],
                       "value": "deleting",
                       "key": "status",
                       "action": "setitem"}:
                background_completed.set()
            if mod == {"path": [1],
                       "value": "prepare_done",
                       "key": "status",
                       "action": "setitem"}:
                empty_ready.set()
            if mod["path"] == [1] or (mod["path"] == [] and mod["key"] == 1):
                self.assertEqual(mod, expect[expect_idx])
                expect_idx += 1
                if expect_idx >= len(expect):
                    empty_completed.set()
        scheduler.notifier.publish = notify

        scheduler.start()
        scheduler.submit("main", expid_bg, -99, None, False)
        loop.run_until_complete(background_running.wait())
        check_pause_0 = lambda: loop.run_until_complete(scheduler.check_pause(0))
        self.assertFalse(check_pause_0())
        scheduler.submit("main", expid, 0, None, False)
        self.assertFalse(check_pause_0())
        loop.run_until_complete(empty_ready.wait())
        self.assertTrue(check_pause_0())
        loop.run_until_complete(empty_completed.wait())
        self.assertFalse(check_pause_0())

        self.assertFalse(termination_ok)
        scheduler.request_termination(0)
        self.assertTrue(check_pause_0())
        loop.run_until_complete(background_completed.wait())
        self.assertTrue(termination_ok)

        loop.run_until_complete(scheduler.stop())

    def test_close_with_active_runs(self):
        """Check scheduler exits with experiments still running"""
        loop = self.loop

        scheduler = Scheduler(_RIDCounter(0), {}, None, None)

        expid_bg = _get_expid("BackgroundExperiment")
        # Suppress the SystemExit backtrace when worker process is killed.
        expid_bg["log_level"] = logging.CRITICAL
        expid = _get_expid("EmptyExperiment")

        background_running = asyncio.Event()
        empty_ready = asyncio.Event()
        background_completed = asyncio.Event()
        def notify(mod):
            if mod == {"path": [0],
                       "value": "running",
                       "key": "status",
                       "action": "setitem"}:
                background_running.set()
            if mod == {"path": [0],
                       "value": "deleting",
                       "key": "status",
                       "action": "setitem"}:
                background_completed.set()
            if mod == {"path": [1],
                       "value": "prepare_done",
                       "key": "status",
                       "action": "setitem"}:
                empty_ready.set()
        scheduler.notifier.publish = notify

        scheduler.start()
        scheduler.submit("main", expid_bg, -99, None, False)
        loop.run_until_complete(background_running.wait())

        scheduler.submit("main", expid, 0, None, False)
        loop.run_until_complete(empty_ready.wait())

        # At this point, (at least) BackgroundExperiment is still running; make
        # sure we can stop the scheduler without hanging.
        loop.run_until_complete(scheduler.stop())

    def test_flush(self):
        loop = self.loop
        scheduler = Scheduler(_RIDCounter(0), dict(), None, None)
        expid = _get_expid("EmptyExperiment")

        expect = _get_basic_steps(1, expid, 1, True)
        expect.insert(1, {"key": "status",
                          "path": [1],
                          "value": "flushing",
                          "action": "setitem"})
        first_preparing = asyncio.Event()
        done = asyncio.Event()
        expect_idx = 0
        def notify(mod):
            nonlocal expect_idx
            if mod == {"path": [0],
                       "value": "preparing",
                       "key": "status",
                       "action": "setitem"}:
                first_preparing.set()
            if mod["path"] == [1] or (mod["path"] == [] and mod["key"] == 1):
                self.assertEqual(mod, expect[expect_idx])
                expect_idx += 1
                if expect_idx >= len(expect):
                    done.set()
        scheduler.notifier.publish = notify

        scheduler.start()
        scheduler.submit("main", expid, 0, None, False)
        loop.run_until_complete(first_preparing.wait())
        scheduler.submit("main", expid, 1, None, True)
        loop.run_until_complete(done.wait())
        loop.run_until_complete(scheduler.stop())

    def _make_status_events(self, rid, previous_notify=None):
        status_events = {
            key: asyncio.Event()
            for key in ("prepare_done", "running", "paused", "run_done",
                        "deleting", "idle")
        }

        def notify(mod):
            if (mod["path"] == [rid] and mod["action"] == "setitem"
                    and mod["key"] == "status"):
                event = status_events.get(mod["value"], None)
                if event:
                    event.set()
            if previous_notify:
                previous_notify(mod)

        return status_events, notify

    def _make_scheduler_for_idle_test(self):
        class Flags:
            def __init__(self):
                self.be_idle = False
                self.take_long_in_idle = False
                self.termination_ok = False

        flags = Flags()

        def get_dataset(name):
            return getattr(flags, name)

        def check_termination(mod):
            self.assertEqual(
                mod, {
                    "action": "setitem",
                    "key": "termination_ok",
                    "value": (False, True),
                    "path": []
                })
            flags.termination_ok = True

        handlers = {
            "get_dataset": get_dataset,
            "update_dataset": check_termination
        }
        scheduler = Scheduler(_RIDCounter(0), handlers, None, None)

        status_events, notify = self._make_status_events(0)
        scheduler.notifier.publish = notify

        scheduler.start()

        expid_idle = _get_expid("IdleExperiment")
        # Suppress the SystemExit backtrace when worker process is killed.
        expid_idle["log_level"] = logging.CRITICAL
        scheduler.submit("main", expid_idle, 0, None, False)

        return scheduler, status_events, flags

    def test_close_with_idle_running(self):
        """Check scheduler exits with experiment still idle"""
        loop = self.loop
        scheduler, status_events, flags = self._make_scheduler_for_idle_test()
        loop.run_until_complete(status_events["running"].wait())
        flags.be_idle = True
        loop.run_until_complete(status_events["idle"].wait())
        loop.run_until_complete(scheduler.stop())

    def test_close_with_prepared_bg(self):
        """Check scheduler exits when there is still a prepare_done experiment"""
        loop = self.loop
        scheduler, status_events, _ = self._make_scheduler_for_idle_test()

        loop.run_until_complete(status_events["running"].wait())

        # Submit lower-priority experiment that is still waiting to be run when
        # the scheduler is terminated.
        expid = _get_expid("EmptyExperiment")
        scheduler.submit("main", expid, -1, None, False)

        loop.run_until_complete(scheduler.stop())

    def test_resume_idle(self):
        """Check scheduler resumes previously idle experiments"""
        loop = self.loop
        scheduler, status_events, flags = self._make_scheduler_for_idle_test()
        loop.run_until_complete(status_events["running"].wait())

        # Make sure we can un-idle by returning False from the idle callback.
        flags.be_idle = True
        loop.run_until_complete(status_events["idle"].wait())

        status_events["running"].clear()
        flags.be_idle = False
        loop.run_until_complete(status_events["running"].wait())

        # Make sure we can un-idle by requesting termination.
        flags.be_idle = True
        loop.run_until_complete(status_events["idle"].wait())

        self.assertFalse(flags.termination_ok)
        scheduler.request_termination(0)
        loop.run_until_complete(status_events["deleting"].wait())
        self.assertTrue(flags.termination_ok)

        loop.run_until_complete(scheduler.stop())

    def test_idle_bg(self):
        """Check scheduler runs lower-priority experiments while idle"""
        loop = self.loop
        scheduler, idle_status_events, flags = self._make_scheduler_for_idle_test()

        bg_status_events, notify = self._make_status_events(
            1, scheduler.notifier.publish)
        scheduler.notifier.publish = notify

        loop.run_until_complete(idle_status_events["running"].wait())

        # Submit experiment with lower priority.
        expid = _get_expid("BackgroundExperiment")
        scheduler.submit("main", expid, -1, None, False)

        check_pause_1 = lambda: loop.run_until_complete(scheduler.check_pause(1))

        loop.run_until_complete(bg_status_events["prepare_done"].wait())

        flags.be_idle = True
        loop.run_until_complete(bg_status_events["running"].wait())
        self.assertFalse(check_pause_1())

        idle_status_events["running"].clear()
        flags.be_idle = False
        loop.run_until_complete(idle_status_events["running"].wait())

        bg_status_events["running"].clear()
        flags.be_idle = True
        self.assertFalse(check_pause_1())
        loop.run_until_complete(bg_status_events["running"].wait())

        idle_status_events["running"].clear()
        flags.be_idle = False
        self.assertTrue(check_pause_1())
        loop.run_until_complete(idle_status_events["running"].wait())

        scheduler.request_termination(0)
        scheduler.request_termination(1)
        loop.run_until_complete(bg_status_events["deleting"].wait())

        loop.run_until_complete(scheduler.stop())

    def test_idle_timeout(self):
        """Check that blocking is_idle_callback times out"""
        loop = self.loop
        scheduler, status_events, flags = self._make_scheduler_for_idle_test()

        # Submit empty experiment with lower priority.
        empty_status_events, notify = self._make_status_events(
            1, scheduler.notifier.publish)
        scheduler.notifier.publish = notify

        expid = _get_expid("EmptyExperiment")
        scheduler.submit("main", expid, -1, None, False)

        # Make the is_idle_callback in block; silencing the expected error
        # message in the log output.
        logging.disable(logging.ERROR)
        flags.be_idle = True
        flags.take_long_in_idle = True
        loop.run_until_complete(status_events["deleting"].wait())
        logging.disable(logging.NOTSET)

        # Make sure EmptyExperiment completes normally now.
        loop.run_until_complete(empty_status_events["run_done"].wait())

        loop.run_until_complete(scheduler.stop())

    def tearDown(self):
        self.loop.close()
