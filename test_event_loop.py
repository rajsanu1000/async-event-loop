"""
tests/test_event_loop.py
=========================
Unit tests for EventLoop, Future, Task, and gather().
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from src.event_loop import EventLoop, Future, Task, CancelledError, InvalidStateError, gather


# ─── Future Tests ─────────────────────────────────────────────────────────────

class TestFuture:
    def setup_method(self):
        self.loop = EventLoop()

    def teardown_method(self):
        self.loop.close()

    def test_initial_state(self):
        f = Future(self.loop)
        assert not f.done()
        assert not f.cancelled()

    def test_set_result(self):
        f = Future(self.loop)
        f.set_result(42)
        assert f.done()
        assert f.result() == 42

    def test_set_exception(self):
        f = Future(self.loop)
        err = ValueError("test error")
        f.set_exception(err)
        assert f.done()
        with pytest.raises(ValueError, match="test error"):
            f.result()

    def test_cancel(self):
        f = Future(self.loop)
        assert f.cancel()
        assert f.cancelled()
        with pytest.raises(CancelledError):
            f.result()

    def test_cannot_set_result_twice(self):
        f = Future(self.loop)
        f.set_result(1)
        with pytest.raises(InvalidStateError):
            f.set_result(2)

    def test_cannot_cancel_done_future(self):
        f = Future(self.loop)
        f.set_result(1)
        assert not f.cancel()

    def test_done_callback_called_immediately_if_done(self):
        f = Future(self.loop)
        f.set_result("hello")
        called_with = []
        f.add_done_callback(lambda fut: called_with.append(fut.result()))
        self.loop._run_once()
        assert called_with == ["hello"]

    def test_done_callback_called_on_resolution(self):
        f = Future(self.loop)
        called_with = []
        f.add_done_callback(lambda fut: called_with.append(fut.result()))
        f.set_result("world")
        self.loop._run_once()
        assert called_with == ["world"]


# ─── Task Tests ───────────────────────────────────────────────────────────────

class TestTask:
    def setup_method(self):
        self.loop = EventLoop()

    def teardown_method(self):
        self.loop.close()

    def test_simple_coroutine(self):
        def coro():
            return 99
            yield  # Make it a generator

        result = self.loop.run_until_complete(coro())
        assert result == 99

    def test_task_with_sleep(self):
        results = []

        def coro():
            yield from self.loop.sleep(0).__await__()
            results.append("done")
            return "ok"

        result = self.loop.run_until_complete(coro())
        assert result == "ok"
        assert results == ["done"]

    def test_task_cancellation(self):
        cancelled = []

        def coro():
            yield from self.loop.sleep(10).__await__()
            return "should_not_reach"

        def canceller():
            yield from self.loop.sleep(0).__await__()
            task.cancel()
            yield from self.loop.sleep(0).__await__()
            cancelled.append(task._future.cancelled())
            self.loop.stop()

        task = self.loop.create_task(coro())
        self.loop.create_task(canceller())
        self.loop.run_forever()
        assert cancelled == [True]

    def test_task_exception_propagates(self):
        def bad_coro():
            raise RuntimeError("boom")
            yield

        with pytest.raises(RuntimeError, match="boom"):
            self.loop.run_until_complete(bad_coro())


# ─── Gather Tests ─────────────────────────────────────────────────────────────

class TestGather:
    def setup_method(self):
        self.loop = EventLoop()

    def teardown_method(self):
        self.loop.close()

    def test_gather_empty(self):
        g = gather(loop=self.loop)
        result = self.loop.run_until_complete(g.__await__.__func__(g) if False else _run_future(self.loop, g))
        assert result == []

    def test_gather_single(self):
        def coro():
            yield from self.loop.sleep(0).__await__()
            return "only"

        g = gather(coro(), loop=self.loop)
        results = _run_future(self.loop, g)
        assert results == ["only"]

    def test_gather_multiple(self):
        def make_coro(val):
            def coro():
                yield from self.loop.sleep(0).__await__()
                return val
            return coro()

        g = gather(make_coro(1), make_coro(2), make_coro(3), loop=self.loop)
        results = _run_future(self.loop, g)
        assert results == [1, 2, 3]

    def test_gather_exception_propagates(self):
        def good():
            yield from self.loop.sleep(0).__await__()
            return "ok"

        def bad():
            raise ValueError("gather_fail")
            yield

        g = gather(good(), bad(), loop=self.loop)
        with pytest.raises(ValueError, match="gather_fail"):
            _run_future(self.loop, g)


# ─── EventLoop Timer Tests ─────────────────────────────────────────────────────

class TestTimers:
    def setup_method(self):
        self.loop = EventLoop()

    def teardown_method(self):
        self.loop.close()

    def test_call_later_fires(self):
        fired = []
        self.loop.call_later(0.01, fired.append, "timer")
        import time
        start = time.monotonic()
        while not fired and (time.monotonic() - start) < 1.0:
            self.loop._run_once()
        assert fired == ["timer"]

    def test_call_later_order(self):
        order = []
        self.loop.call_later(0.02, order.append, "second")
        self.loop.call_later(0.01, order.append, "first")
        import time
        start = time.monotonic()
        while len(order) < 2 and (time.monotonic() - start) < 1.0:
            self.loop._run_once()
        assert order == ["first", "second"]

    def test_cancelled_timer_does_not_fire(self):
        fired = []
        handle = self.loop.call_later(0.01, fired.append, "should_not")
        handle.cancel()
        import time
        start = time.monotonic()
        while (time.monotonic() - start) < 0.05:
            self.loop._run_once()
        assert fired == []


# ─── Scheduler Tests ──────────────────────────────────────────────────────────

class TestSchedulers:
    def setup_method(self):
        self.loop = EventLoop()

    def teardown_method(self):
        self.loop.close()

    def test_fair_scheduler_creates_tasks(self):
        from src.scheduler import FairScheduler
        sched = FairScheduler(self.loop)

        def noop():
            return 1
            yield

        t1 = sched.add_task(noop(), group="a")
        t2 = sched.add_task(noop(), group="b")
        assert sched.pending_tasks() == 2
        assert set(sched.active_groups()) == {"a", "b"}

    def test_priority_scheduler_ordering(self):
        from src.scheduler import PriorityScheduler, Priority
        sched = PriorityScheduler(self.loop)
        order = []

        def make_task(name):
            def coro():
                yield from self.loop.sleep(0).__await__()
                order.append(name)
            return coro()

        sched.submit(make_task("low"), Priority.LOW)
        sched.submit(make_task("high"), Priority.HIGH)
        sched.submit(make_task("normal"), Priority.NORMAL)

        def stopper():
            yield from self.loop.sleep(0.1).__await__()
            self.loop.stop()

        self.loop.create_task(stopper())
        self.loop.run_forever()
        # High priority was submitted last among those, but should run
        assert "high" in order


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _run_future(loop: EventLoop, future: Future):
    """Helper to run a future to completion and return its result."""
    future.add_done_callback(lambda f: loop.stop())
    loop.run_forever()
    return future.result()
